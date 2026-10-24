#define _WIN32_WINNT 0x0601
#include <windows.h>
#include <bcrypt.h>
#include <stddef.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <delayimp.h>
#include "winfsp_driver_manifest.h"

/* No filesystem volume is created by this preflight. WinFsp's version APIs
   report only major/minor from files, not the selected loaded driver build.
   Bind the exact qualified DLL/SYS to the SxS service and loaded-module
   metadata before mount.c can call FspFileSystemCreate. This is operational
   identity evidence, not a cryptographic hash of resident kernel bytes. */
static WCHAR ErrorDetail[2048];
static SRWLOCK BindingLock=SRWLOCK_INIT;
static HMODULE volatile BindingModule;
static WCHAR BindingSxs[17];
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
   only after the hash, full-version, service and loaded-driver checks. */
DWORD tk_winfsp_bind_imports(HMODULE module) {
    AcquireSRWLockExclusive(&BindingLock);DWORD error=bind_imports(module);ReleaseSRWLockExclusive(&BindingLock);
    return error;
}

typedef struct {
    HANDLE handle;
    WCHAR dos[32768],nt[32768];
    DWORD image_size,checksum,timestamp;
    BY_HANDLE_FILE_INFORMATION identity;
} VERIFIED_FILE;

static DWORD sha256(HANDLE file,PWSTR hex) {
    BCRYPT_ALG_HANDLE algorithm=0;BCRYPT_HASH_HANDLE hash=0;
    PUCHAR object=0;DWORD object_size=0,bytes=0,error=ERROR_CRC;
    UCHAR digest[32],block[32768];LARGE_INTEGER zero={0};
    LONG status=BCryptOpenAlgorithmProvider(&algorithm,BCRYPT_SHA256_ALGORITHM,0,0);
    if(status<0)goto done;
    status=BCryptGetProperty(algorithm,BCRYPT_OBJECT_LENGTH,(PUCHAR)&object_size,sizeof object_size,&bytes,0);
    if(status<0||!object_size||object_size>1024*1024)goto done;
    object=malloc(object_size);if(!object){error=ERROR_NOT_ENOUGH_MEMORY;goto done;}
    status=BCryptCreateHash(algorithm,&hash,object,object_size,0,0,0);if(status<0)goto done;
    if(!SetFilePointerEx(file,zero,0,FILE_BEGIN)){error=GetLastError();goto done;}
    for(;;){
        if(!ReadFile(file,block,sizeof block,&bytes,0)){error=GetLastError();goto done;}
        if(!bytes)break;
        status=BCryptHashData(hash,block,bytes,0);if(status<0)goto done;
    }
    status=BCryptFinishHash(hash,digest,sizeof digest,0);if(status<0)goto done;
    for(DWORD i=0;i<sizeof digest;i++)swprintf_s(hex+2*i,3,L"%02x",digest[i]);
    error=0;
done:
    if(hash)BCryptDestroyHash(hash);if(algorithm)BCryptCloseAlgorithmProvider(algorithm,0);free(object);
    return error;
}

static DWORD verify_file(PCWSTR path,PCWSTR expected,VERIFIED_FILE *out) {
    DWORD count,error;WCHAR digest[65];LARGE_INTEGER offset={0},size;
    IMAGE_DOS_HEADER dos;IMAGE_NT_HEADERS64 pe;VS_FIXEDFILEINFO *version=0;
    UINT version_size=0;PVOID version_info=0;
    memset(out,0,sizeof *out);out->handle=INVALID_HANDLE_VALUE;
    if(wcslen(expected)!=64)return failure(ERROR_INVALID_DATA,L"WinFsp dependency manifest is incomplete; mounting is refused.");
    /* Keep the verified file stable while service and loaded-image identities
       are checked. Neither replacement nor writes are shared. */
    out->handle=CreateFileW(path,GENERIC_READ,FILE_SHARE_READ,0,OPEN_EXISTING,FILE_ATTRIBUTE_NORMAL,0);
    if(out->handle==INVALID_HANDLE_VALUE)return failure(GetLastError(),L"Cannot read qualified WinFsp dependency: %s",path);
    count=GetFinalPathNameByHandleW(out->handle,out->dos,ARRAYSIZE(out->dos),FILE_NAME_NORMALIZED|VOLUME_NAME_DOS);
    if(!count||count>=ARRAYSIZE(out->dos)){error=GetLastError();goto io_fail;}
    count=GetFinalPathNameByHandleW(out->handle,out->nt,ARRAYSIZE(out->nt),FILE_NAME_NORMALIZED|VOLUME_NAME_NT);
    if(!count||count>=ARRAYSIZE(out->nt)){error=GetLastError();goto io_fail;}
    if(!GetFileInformationByHandle(out->handle,&out->identity)){error=GetLastError();goto io_fail;}
    error=sha256(out->handle,digest);if(error)goto io_fail;
    if(_wcsicmp(digest,expected))return failure(ERROR_REVISION_MISMATCH,
        L"Unsupported WinFsp dependency %s (SHA256 %s). Exact official WinFsp 2026 Beta5 x64 is required; mounting is refused.",out->dos,digest);
    count=GetFileVersionInfoSizeW(out->dos,&error);
    if(!count||count>1024*1024){error=ERROR_INVALID_DATA;goto io_fail;}
    version_info=malloc(count);if(!version_info){error=ERROR_NOT_ENOUGH_MEMORY;goto io_fail;}
    if(!GetFileVersionInfoW(out->dos,0,count,version_info)||
        !VerQueryValueW(version_info,L"\\",(PVOID*)&version,&version_size)||
        version_size<sizeof *version||version->dwSignature!=0xfeef04bd||
        version->dwFileVersionMS!=TK_WINFSP_VERSION_MS||version->dwFileVersionLS!=TK_WINFSP_VERSION_LS){
        free(version_info);return failure(ERROR_REVISION_MISMATCH,L"WinFsp full file version does not match the qualified Beta5 manifest: %s",out->dos);
    }
    free(version_info);
    if(!GetFileSizeEx(out->handle,&size)||!SetFilePointerEx(out->handle,offset,0,FILE_BEGIN)||
        !ReadFile(out->handle,&dos,sizeof dos,&count,0)||count!=sizeof dos||dos.e_magic!=IMAGE_DOS_SIGNATURE||
        dos.e_lfanew<sizeof dos||dos.e_lfanew>1024*1024||
        (LONGLONG)dos.e_lfanew+sizeof pe>size.QuadPart){error=ERROR_BAD_EXE_FORMAT;goto io_fail;}
    offset.QuadPart=dos.e_lfanew;
    if(!SetFilePointerEx(out->handle,offset,0,FILE_BEGIN)||!ReadFile(out->handle,&pe,sizeof pe,&count,0)||
        count!=sizeof pe||pe.Signature!=IMAGE_NT_SIGNATURE||pe.FileHeader.Machine!=IMAGE_FILE_MACHINE_AMD64||
        pe.FileHeader.SizeOfOptionalHeader<sizeof pe.OptionalHeader||pe.OptionalHeader.Magic!=IMAGE_NT_OPTIONAL_HDR64_MAGIC||
        !pe.OptionalHeader.SizeOfImage){error=ERROR_BAD_EXE_FORMAT;goto io_fail;}
    out->image_size=pe.OptionalHeader.SizeOfImage;out->checksum=pe.OptionalHeader.CheckSum;out->timestamp=pe.FileHeader.TimeDateStamp;
    return 0;
io_fail:
    return failure(error,L"Cannot verify WinFsp dependency %s (error %lu); mounting is refused.",path,error);
}

/* Native x64 layout of the private SystemModuleInformationEx interface.
   ImageBase may be zero without SeDebugPrivilege on Windows 11 24H2. */
typedef struct {
    PVOID section,mapped_base,image_base;ULONG image_size,flags;
    USHORT load_order,init_order,load_count,file_name_offset;UCHAR path[256];
} TK_MODULE;
typedef struct {
    USHORT next_offset;TK_MODULE base;ULONG checksum,timestamp;PVOID default_base;
} TK_MODULE_EX;
typedef char TK_MODULE_LAYOUT[(sizeof(void*)==8&&sizeof(TK_MODULE_EX)==320&&
    offsetof(TK_MODULE_EX,base)==8&&offsetof(TK_MODULE_EX,checksum)==304&&
    offsetof(TK_MODULE_EX,timestamp)==308)?1:-1];

static PCWSTR without_dos_prefix(PCWSTR path) {
    return (!_wcsnicmp(path,L"\\\\?\\",4)||!_wcsnicmp(path,L"\\??\\",4))?path+4:path;
}
static BOOLEAN same_module_path(PCWSTR candidate,PCWSTR dos,PCWSTR nt) {
    if(!_wcsicmp(candidate,nt)||!_wcsicmp(without_dos_prefix(candidate),without_dos_prefix(dos)))return TRUE;
    if(!_wcsnicmp(candidate,L"\\SystemRoot\\",12)){
        WCHAR expanded[32768];UINT count=GetWindowsDirectoryW(expanded,ARRAYSIZE(expanded));
        if(count&&count<ARRAYSIZE(expanded)&&
            swprintf_s(expanded+count,ARRAYSIZE(expanded)-count,L"\\%s",candidate+12)>=0)
            return !_wcsicmp(expanded,without_dos_prefix(dos));
    }
    return FALSE;
}

/* Exposed for non-mounted malformed-buffer and selection regressions. Zero
   timestamp is unavailable on some kernels, including the retained OS probe;
   size/checksum and the exact trusted file hash remain mandatory. */
DWORD tk_winfsp_check_modules(const void *buffer,DWORD length,PCWSTR dos,PCWSTR nt,
    DWORD image_size,DWORD checksum,DWORD timestamp) {
    const UCHAR *data=buffer;SIZE_T at=0;DWORD matches=0;
    if(!buffer||!dos||!nt||!dos[0]||!nt[0]||!image_size||!checksum)return ERROR_INVALID_PARAMETER;
    while(at+sizeof(USHORT)<=length){
        USHORT step;TK_MODULE_EX module;WCHAR path[256];
        memcpy(&step,data+at,sizeof step);
        if(!step){
            if(at+sizeof step!=length)return ERROR_INVALID_DATA;
            return matches==1?0:(matches?ERROR_DUP_NAME:ERROR_MOD_NOT_FOUND);
        }
        if(step<sizeof module||(step&7)||step>length-at||sizeof module>length-at)return ERROR_INVALID_DATA;
        memcpy(&module,data+at,sizeof module);
        const UCHAR *end=memchr(module.base.path,0,sizeof module.base.path);
        if(!end||module.base.file_name_offset>=(SIZE_T)(end-module.base.path))return ERROR_INVALID_DATA;
        if(!MultiByteToWideChar(CP_ACP,MB_ERR_INVALID_CHARS,(PCSTR)module.base.path,-1,path,ARRAYSIZE(path)))return ERROR_INVALID_DATA;
        if(same_module_path(path,dos,nt)){
            matches++;
            if(matches>1)return ERROR_DUP_NAME;
            if(module.base.image_size!=image_size||module.checksum!=checksum||
                (module.timestamp&&module.timestamp!=timestamp))return ERROR_REVISION_MISMATCH;
        }
        at+=step;
    }
    return ERROR_INVALID_DATA; /* a complete zero-offset sentinel is required */
}

static DWORD loaded_driver(const VERIFIED_FILE *driver) {
    typedef LONG (NTAPI *QUERY)(ULONG,PVOID,ULONG,PULONG);
    QUERY query=(QUERY)GetProcAddress(GetModuleHandleW(L"ntdll.dll"),"NtQuerySystemInformation");
    if(!query)return failure(ERROR_PROC_NOT_FOUND,L"Cannot query selected loaded WinFsp driver; mounting is refused.");
    ULONG capacity=64*1024,used=0;LONG status=0;PVOID buffer=0;DWORD error=ERROR_INSUFFICIENT_BUFFER;
    for(unsigned attempt=0;attempt<4;attempt++){
        if(capacity>16*1024*1024)break;
        buffer=malloc(capacity);if(!buffer){error=ERROR_NOT_ENOUGH_MEMORY;break;}
        status=query(77,buffer,capacity,&used);
        if(status==(LONG)0xc0000004||status==(LONG)0xc0000023){
            free(buffer);buffer=0;
            if(used>16*1024*1024-65536)break;
            capacity=used+65536;continue;
        }
        if(status<0){error=ERROR_ACCESS_DENIED;break;}
        if(!used||used>capacity){error=ERROR_INVALID_DATA;break;}
        error=tk_winfsp_check_modules(buffer,used,driver->dos,driver->nt,
            driver->image_size,driver->checksum,driver->timestamp);
        break;
    }
    free(buffer);
    if(error)return failure(error,L"Selected loaded WinFsp driver identity is unavailable or does not match qualified Beta5 (error %lu, NTSTATUS %08lx): %s. Mounting is refused.",error,(ULONG)status,driver->dos);
    return 0;
}

static DWORD verify_driver(HMODULE module,PCWSTR selected,VERIFIED_FILE *driver) {
    typedef PCWSTR (__cdecl *SXS)(void);
    typedef LONG (__cdecl *START)(void);
    SXS sxs=(SXS)GetProcAddress(module,"FspSxsIdent");
    START start=(START)GetProcAddress(module,"FspFsctlStartService");
    WCHAR service[64],path[32768],expanded[32768];DWORD error=ERROR_INVALID_DATA,needed=0;
    SC_HANDLE scm=0,handle=0;QUERY_SERVICE_CONFIGW *config=0;
    if(!sxs||!start)return failure(ERROR_PROC_NOT_FOUND,L"Qualified WinFsp DLL is missing required preflight exports.");
    PCWSTR ident=sxs();DWORD ident_length=0;
    if(ident)while(ident_length<17&&ident[ident_length])ident_length++;
    if(ident_length!=16)return failure(ERROR_INVALID_DATA,L"WinFsp SxS identity is missing or malformed; mounting is refused.");
    for(unsigned i=0;i<16;i++)if((i==8?ident[i]!=L'T':i==15?ident[i]!=L'Z':ident[i]<L'0'||ident[i]>L'9'))
        return failure(ERROR_INVALID_DATA,L"WinFsp SxS identity is malformed; mounting is refused.");
    if(BindingModule&&BindingSxs[0]&&wcscmp(BindingSxs,ident))
        return failure(ERROR_REVISION_MISMATCH,L"WinFsp SxS service selection changed after binding; restart is required.");
    swprintf_s(service,ARRAYSIZE(service),L"WinFsp+%s",ident);
    scm=OpenSCManagerW(0,0,SC_MANAGER_CONNECT);if(!scm){error=GetLastError();goto done;}
    handle=OpenServiceW(scm,service,SERVICE_QUERY_CONFIG);if(!handle){error=GetLastError();goto done;}
    QueryServiceConfigW(handle,0,0,&needed);
    if(GetLastError()!=ERROR_INSUFFICIENT_BUFFER||needed<sizeof *config||needed>65536){error=ERROR_INVALID_DATA;goto done;}
    config=malloc(needed);if(!config){error=ERROR_NOT_ENOUGH_MEMORY;goto done;}
    if(!QueryServiceConfigW(handle,config,needed,&needed)){error=GetLastError();goto done;}
    if(config->dwServiceType!=SERVICE_FILE_SYSTEM_DRIVER){error=ERROR_INVALID_DATA;goto done;}
    needed=ExpandEnvironmentStringsW(config->lpBinaryPathName,expanded,ARRAYSIZE(expanded));
    if(!needed||needed>ARRAYSIZE(expanded)){error=ERROR_INVALID_DATA;goto done;}
    PCWSTR image=expanded;SIZE_T length=wcslen(image);
    if(length>=2&&image[0]==L'"'&&image[length-1]==L'"'){expanded[length-1]=0;image++;}
    if(!wcsncmp(image,L"\\??\\",4))image+=4;
    if(!_wcsnicmp(image,L"\\SystemRoot\\",12)){
        UINT count=GetWindowsDirectoryW(path,ARRAYSIZE(path));
        if(!count||count>=ARRAYSIZE(path)||swprintf_s(path+count,ARRAYSIZE(path)-count,L"\\%s",image+12)<0){error=ERROR_INVALID_DATA;goto done;}
        image=path;
    }
    error=verify_file(image,TK_WINFSP_SYS_SHA256,driver);if(error)goto done;
    /* Bind the selected DLL's final SxS directory to this service image. The
       installer SxS ID is an installation timestamp, not a release version. */
    WCHAR expected[32768];wcscpy_s(expected,ARRAYSIZE(expected),selected);
    WCHAR *file=wcsrchr(expected,L'\\');
    if(!file||wcscpy_s(file+1,ARRAYSIZE(expected)-(file+1-expected),L"winfsp-x64.sys")){
        error=ERROR_INVALID_DATA;goto done;
    }
    if(_wcsicmp(expected,driver->dos)){error=ERROR_REVISION_MISMATCH;goto done;}
    LONG status=start();
    if(status<0){error=failure(ERROR_SERVICE_NOT_ACTIVE,L"Cannot start selected qualified WinFsp SxS service %s (NTSTATUS %08lx).",service,(ULONG)status);goto done;}
    error=loaded_driver(driver);
    if(!error)wcscpy_s(BindingSxs,ARRAYSIZE(BindingSxs),ident);
done:
    free(config);if(handle)CloseServiceHandle(handle);if(scm)CloseServiceHandle(scm);
    if(error&&!ErrorDetail[0])failure(error,L"Cannot bind qualified WinFsp DLL to selected SxS driver service %s (error %lu); mounting is refused.",service,error);
    return error;
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
    VERIFIED_FILE *dll=calloc(1,sizeof *dll),*driver=calloc(1,sizeof *driver);
    if(!dll||!driver){free(dll);free(driver);return failure(ERROR_NOT_ENOUGH_MEMORY,L"Cannot allocate WinFsp dependency preflight.");}
    dll->handle=driver->handle=INVALID_HANDLE_VALUE;
    DWORD error=verify_file(attempted,TK_WINFSP_DLL_SHA256,dll);
    HMODULE module=0;
    if(!error){
        module=LoadLibraryExW(attempted,0,LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR|LOAD_LIBRARY_SEARCH_SYSTEM32);
        if(!module)error=failure(GetLastError(),L"Cannot load qualified WinFsp DLL: %s",attempted);
    }
    if(!error){
        WCHAR actual[32768];BY_HANDLE_FILE_INFORMATION identity;
        DWORD length=GetModuleFileNameW(module,actual,ARRAYSIZE(actual));
        HANDLE file=length&&length<ARRAYSIZE(actual)?CreateFileW(actual,GENERIC_READ,FILE_SHARE_READ,0,OPEN_EXISTING,0,0):INVALID_HANDLE_VALUE;
        if(file==INVALID_HANDLE_VALUE||!GetFileInformationByHandle(file,&identity)||
            identity.dwVolumeSerialNumber!=dll->identity.dwVolumeSerialNumber||
            identity.nFileIndexHigh!=dll->identity.nFileIndexHigh||identity.nFileIndexLow!=dll->identity.nFileIndexLow)
            error=failure(ERROR_REVISION_MISMATCH,L"Loaded WinFsp DLL does not match the exact verified dependency; mounting is refused.");
        if(file!=INVALID_HANDLE_VALUE)CloseHandle(file);
    }
    if(!error&&BindingPoison)error=failure(BindingPoison,L"WinFsp import binding previously failed; restart is required.");
    if(!error&&BindingModule&&BindingModule!=module)error=failure(ERROR_REVISION_MISMATCH,
        L"WinFsp DLL/SxS selection changed after binding; restart is required.");
    if(!error)error=verify_driver(module,dll->dos,driver);
    if(!error)error=bind_imports(module);
    if(dll->handle!=INVALID_HANDLE_VALUE)CloseHandle(dll->handle);
    if(driver->handle!=INVALID_HANDLE_VALUE)CloseHandle(driver->handle);
    free(dll);free(driver);
    if(error&&module&&module!=BindingModule)FreeLibrary(module);
    return error;
}
DWORD tk_winfsp_load(LPWSTR attempted,DWORD count) {
    AcquireSRWLockExclusive(&BindingLock);DWORD error=load_winfsp(attempted,count);ReleaseSRWLockExclusive(&BindingLock);
    return error;
}
