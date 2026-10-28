#define _WIN32_WINNT 0x0601
#include <windows.h>
#include <stddef.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <delayimp.h>
#include "winfsp_driver_manifest.h"

/* No filesystem volume is created by this preflight. It loads the installed
   WinFsp DLL by absolute path, enforces the minimum version and binds the
   delay-loaded imports to that module before mount.c calls FspFileSystemCreate.
   The DLL starts its own matching SxS driver service. */
static WCHAR ErrorDetail[2048];
static SRWLOCK BindingLock=SRWLOCK_INIT;
static HMODULE volatile BindingModule;
static DWORD BindingPoison;
static DWORD failure(DWORD code,PCWSTR format,...) {
    va_list args;va_start(args,format);
    _vsnwprintf_s(ErrorDetail,ARRAYSIZE(ErrorDetail),_TRUNCATE,format,args);va_end(args);
    return code?code:ERROR_INVALID_DATA;
}
void tk_winfsp_error_detail(PWSTR buffer,DWORD count) {
    if(buffer&&count)wcsncpy_s(buffer,count,ErrorDetail,_TRUNCATE);
}

/* The delay-import descriptor names only a basename. An absolute explicit
   LoadLibraryEx elsewhere does not bind it: the default helper could select
   an older same-basename DLL. Always return this process's frozen verified
   module, with the reference that delayimp expects to own. */
static FARPROC WINAPI delay_hook(unsigned notification,PDelayLoadInfo info) {
    if(_stricmp(info->szDll,"winfsp-x64.dll"))return 0;
    HMODULE frozen=(HMODULE)InterlockedCompareExchangePointer((PVOID volatile*)&BindingModule,0,0);
    if(notification==dliNotePreLoadLibrary){
        HMODULE module=0;
        if(!frozen||!GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
            (PCWSTR)frozen,&module)||module!=frozen)
            RaiseException(VcppException(ERROR_SEVERITY_ERROR,ERROR_MOD_NOT_FOUND),0,0,0);
        return (FARPROC)module;
    }
    if(notification==dliNotePreGetProcAddress&&info->hmodCur!=frozen)
        RaiseException(VcppException(ERROR_SEVERITY_ERROR,ERROR_BAD_EXE_FORMAT),0,0,0);
    return 0;
}
const PfnDliHook __pfnDliNotifyHook2=delay_hook;

static BOOLEAN in_image(DWORD rva,SIZE_T bytes,DWORD size) {
    return rva<size&&bytes<=size-rva;
}
static BOOLEAN image_string(const UCHAR *image,DWORD rva,DWORD size) {
    return in_image(rva,1,size)&&memchr(image+rva,0,size-rva)!=0;
}
static DWORD check_imports(HMODULE module,BOOLEAN allow_unbound) {
    UCHAR *image=(UCHAR*)GetModuleHandleW(0);
    IMAGE_DOS_HEADER *dos=(IMAGE_DOS_HEADER*)image;
    IMAGE_NT_HEADERS64 *pe=(IMAGE_NT_HEADERS64*)(image+dos->e_lfanew);
    DWORD size=pe->OptionalHeader.SizeOfImage;
    IMAGE_DATA_DIRECTORY directory=pe->OptionalHeader.DataDirectory[IMAGE_DIRECTORY_ENTRY_DELAY_IMPORT];
    if(!directory.VirtualAddress||!in_image(directory.VirtualAddress,directory.Size,size))return ERROR_INVALID_DATA;
    DWORD found=0;BOOLEAN ended=FALSE;
    for(SIZE_T at=0;at+sizeof(ImgDelayDescr)<=directory.Size;at+=sizeof(ImgDelayDescr)){
        const ImgDelayDescr *descriptor=(const ImgDelayDescr*)(image+directory.VirtualAddress+at);
        if(!descriptor->rvaDLLName){ended=TRUE;break;}
        if(descriptor->grAttrs!=dlattrRva||!image_string(image,descriptor->rvaDLLName,size))return ERROR_INVALID_DATA;
        if(_stricmp((PCSTR)(image+descriptor->rvaDLLName),"winfsp-x64.dll"))continue;
        if(strcmp((PCSTR)(image+descriptor->rvaDLLName),"winfsp-x64.dll")||++found!=1||
            !in_image(descriptor->rvaHmod,sizeof(HMODULE),size))return ERROR_INVALID_DATA;
        HMODULE current=*(HMODULE*)(image+descriptor->rvaHmod);
        if((current&&current!=module)||(!allow_unbound&&current!=module))return ERROR_REVISION_MISMATCH;
        DWORD count=0;BOOLEAN terminated=FALSE;
        for(SIZE_T index=0;index<size/sizeof(IMAGE_THUNK_DATA64);index++){
            SIZE_T offset=index*sizeof(IMAGE_THUNK_DATA64);
            if(offset>MAXDWORD||descriptor->rvaINT>MAXDWORD-offset||descriptor->rvaIAT>MAXDWORD-offset||
                !in_image(descriptor->rvaINT+(DWORD)offset,sizeof(IMAGE_THUNK_DATA64),size)||
                !in_image(descriptor->rvaIAT+(DWORD)offset,sizeof(IMAGE_THUNK_DATA64),size))return ERROR_INVALID_DATA;
            const IMAGE_THUNK_DATA64 *name=(const IMAGE_THUNK_DATA64*)(image+descriptor->rvaINT+offset);
            const IMAGE_THUNK_DATA64 *slot=(const IMAGE_THUNK_DATA64*)(image+descriptor->rvaIAT+offset);
            if(!name->u1.AddressOfData){if(slot->u1.Function)return ERROR_INVALID_DATA;terminated=TRUE;break;}
            LPCSTR symbol;
            if(IMAGE_SNAP_BY_ORDINAL64(name->u1.Ordinal))symbol=(LPCSTR)(ULONG_PTR)IMAGE_ORDINAL64(name->u1.Ordinal);
            else{
                if(name->u1.AddressOfData>MAXDWORD-2||!image_string(image,(DWORD)name->u1.AddressOfData+2,size))return ERROR_INVALID_DATA;
                symbol=(PCSTR)(image+(DWORD)name->u1.AddressOfData+2);
            }
            FARPROC expected=GetProcAddress(module,symbol);
            if(!expected)return ERROR_PROC_NOT_FOUND;
            ULONG_PTR actual=(ULONG_PTR)slot->u1.Function;
            BOOLEAN thunk=actual>=(ULONG_PTR)image&&actual-(ULONG_PTR)image<size;
            if((!allow_unbound||!thunk)&&(FARPROC)actual!=expected)return ERROR_REVISION_MISMATCH;
            if(!current&&!thunk)return ERROR_REVISION_MISMATCH;
            count++;
        }
        if(!terminated||!count)return ERROR_INVALID_DATA;
    }
    return ended&&found==1?0:ERROR_MOD_NOT_FOUND;
}

static DWORD bind_imports(HMODULE module) {
    if(!module)return failure(ERROR_INVALID_PARAMETER,L"WinFsp binding requires a verified module.");
    if(BindingPoison)return failure(BindingPoison,L"WinFsp import binding previously failed; restart is required.");
    if(BindingModule&&BindingModule!=module)return failure(ERROR_REVISION_MISMATCH,
        L"WinFsp DLL/SxS selection changed after binding; restart is required.");
    DWORD error=check_imports(module,TRUE);
    if(error)return failure(error,L"WinFsp imports are prebound to another module, malformed, or missing required exports; mounting is refused (error %lu).",error);
    if(!BindingModule){
        HMODULE pinned=0;
        if(!GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS|GET_MODULE_HANDLE_EX_FLAG_PIN,
            (PCWSTR)module,&pinned)||pinned!=module)
            return failure(GetLastError(),L"Cannot pin the verified WinFsp module; mounting is refused.");
        InterlockedExchangePointer((PVOID volatile*)&BindingModule,module);
    }
    /* Pinning and freezing last for the process lifetime. If eager resolution
       partially writes the IAT and then fails, never unload its module or
       permit another attach to reuse that partially initialized state. */
    __try {
        HRESULT result=__HrLoadAllImportsForDll("winfsp-x64.dll");
        if(FAILED(result))error=ERROR_DLL_INIT_FAILED;
    } __except(GetExceptionCode()==VcppException(ERROR_SEVERITY_ERROR,ERROR_MOD_NOT_FOUND)||
        GetExceptionCode()==VcppException(ERROR_SEVERITY_ERROR,ERROR_PROC_NOT_FOUND)||
        GetExceptionCode()==VcppException(ERROR_SEVERITY_ERROR,ERROR_BAD_EXE_FORMAT)?EXCEPTION_EXECUTE_HANDLER:EXCEPTION_CONTINUE_SEARCH){
        error=ERROR_DLL_INIT_FAILED;
    }
    if(!error)error=check_imports(module,FALSE);
    if(error){BindingPoison=error;return failure(error,L"WinFsp eager import binding failed; its module is retained and restart is required (error %lu).",error);}
    return 0;
}
/* Internal no-mount fixture entry point. Production calls this same binder
   after the minimum-version check. */
DWORD tk_winfsp_bind_imports(HMODULE module) {
    AcquireSRWLockExclusive(&BindingLock);DWORD error=bind_imports(module);ReleaseSRWLockExclusive(&BindingLock);
    return error;
}

/* Minimum, not exact: any official WinFsp at or above the floor is accepted.
   Older drivers lack the notification/rename deadlock fix (WinFsp PR #669)
   that strands volume deletion, so they are refused before any volume exists. */
static DWORD check_version(PCWSTR path) {
    DWORD ignored=0,count=GetFileVersionInfoSizeW(path,&ignored);
    VS_FIXEDFILEINFO *version=0;UINT version_size=0;
    if(!count||count>1024*1024)return failure(GetLastError()?GetLastError():ERROR_INVALID_DATA,
        L"Cannot read WinFsp version from %s; mounting is refused.",path);
    PVOID info=malloc(count);if(!info)return failure(ERROR_NOT_ENOUGH_MEMORY,L"Cannot allocate WinFsp version buffer.");
    DWORD error=0;
    if(!GetFileVersionInfoW(path,0,count,info)||!VerQueryValueW(info,L"\\",(PVOID*)&version,&version_size)||
        version_size<sizeof *version||version->dwSignature!=0xfeef04bd)
        error=failure(ERROR_INVALID_DATA,L"Cannot read WinFsp version from %s; mounting is refused.",path);
    else if(version->dwFileVersionMS<TK_WINFSP_MIN_VERSION_MS||
        (version->dwFileVersionMS==TK_WINFSP_MIN_VERSION_MS&&version->dwFileVersionLS<TK_WINFSP_MIN_VERSION_LS))
        error=failure(ERROR_REVISION_MISMATCH,L"WinFsp %u.%u.%u at %s is too old; install WinFsp %u.%u.%u (2026 Beta5) or newer.",
            HIWORD(version->dwFileVersionMS),LOWORD(version->dwFileVersionMS),HIWORD(version->dwFileVersionLS),path,
            HIWORD(TK_WINFSP_MIN_VERSION_MS),LOWORD(TK_WINFSP_MIN_VERSION_MS),HIWORD(TK_WINFSP_MIN_VERSION_LS));
    free(info);return error;
}

/* Explicit override wins, then installer registry, then conventional path.
   Never search the launcher's cwd or change the process/system PATH. */
static DWORD load_winfsp(LPWSTR attempted,DWORD count) {
    ErrorDetail[0]=0;
    WCHAR root[32768];DWORD size=sizeof root;
    DWORD length=GetEnvironmentVariableW(L"WINFSP_DIR",root,32768);
    if(length>=32768)return failure(ERROR_INSUFFICIENT_BUFFER,L"WINFSP_DIR is too long; mounting is refused.");
    if(!length){
        LSTATUS result=RegGetValueW(HKEY_LOCAL_MACHINE,L"SOFTWARE\\WOW6432Node\\WinFsp",L"InstallDir",RRF_RT_REG_SZ,0,root,&size);
        if(result!=ERROR_SUCCESS){
            size=sizeof root;
            result=RegGetValueW(HKEY_LOCAL_MACHINE,L"SOFTWARE\\WinFsp",L"InstallDir",RRF_RT_REG_SZ,0,root,&size);
        }
        if(result!=ERROR_SUCCESS)wcscpy_s(root,32768,L"C:\\Program Files (x86)\\WinFsp");
    }
    if(!((root[0] && root[1]==L':' && (root[2]==L'\\' || root[2]==L'/')) || (root[0]==L'\\' && root[1]==L'\\')))
        return failure(ERROR_BAD_PATHNAME,L"WinFsp dependency directory must be an absolute path; mounting is refused.");
    if(!attempted||!count||_snwprintf_s(attempted,count,_TRUNCATE,L"%s\\bin\\winfsp-x64.dll",root)<0)
        return failure(ERROR_INSUFFICIENT_BUFFER,L"WinFsp dependency path exceeds the caller's buffer; mounting is refused.");
    DWORD error=check_version(attempted);
    HMODULE module=0;
    if(!error){
        module=LoadLibraryExW(attempted,0,LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR|LOAD_LIBRARY_SEARCH_SYSTEM32);
        if(!module)error=failure(GetLastError(),L"Cannot load WinFsp DLL: %s",attempted);
    }
    if(!error&&BindingPoison)error=failure(BindingPoison,L"WinFsp import binding previously failed; restart is required.");
    if(!error&&BindingModule&&BindingModule!=module)error=failure(ERROR_REVISION_MISMATCH,
        L"WinFsp DLL selection changed after binding; restart is required.");
    if(!error)error=bind_imports(module);
    if(error&&module&&module!=BindingModule)FreeLibrary(module);
    return error;
}
DWORD tk_winfsp_load(LPWSTR attempted,DWORD count) {
    AcquireSRWLockExclusive(&BindingLock);DWORD error=load_winfsp(attempted,count);ReleaseSRWLockExclusive(&BindingLock);
    return error;
}
