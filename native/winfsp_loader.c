#define _WIN32_WINNT 0x0601
#include <windows.h>
#include <stdio.h>

/* Explicit override wins, then installer registry, then conventional path.
   Never search the launcher's cwd or change the process/system PATH. */
DWORD tk_winfsp_load(LPWSTR attempted,DWORD count) {
    WCHAR root[32768];DWORD size=sizeof root;
    DWORD length=GetEnvironmentVariableW(L"WINFSP_DIR",root,32768);
    if(length>=32768)return ERROR_INSUFFICIENT_BUFFER;
    if(!length){
        LSTATUS result=RegGetValueW(HKEY_LOCAL_MACHINE,L"SOFTWARE\\WOW6432Node\\WinFsp",L"InstallDir",RRF_RT_REG_SZ,0,root,&size);
        if(result!=ERROR_SUCCESS){
            size=sizeof root;
            result=RegGetValueW(HKEY_LOCAL_MACHINE,L"SOFTWARE\\WinFsp",L"InstallDir",RRF_RT_REG_SZ,0,root,&size);
        }
        if(result!=ERROR_SUCCESS)wcscpy_s(root,32768,L"C:\\Program Files (x86)\\WinFsp");
    }
    if(!((root[0] && root[1]==L':' && (root[2]==L'\\' || root[2]==L'/')) || (root[0]==L'\\' && root[1]==L'\\')))return ERROR_BAD_PATHNAME;
    if(swprintf_s(attempted,count,L"%s\\bin\\winfsp-x64.dll",root)<0)return ERROR_INSUFFICIENT_BUFFER;
    if(LoadLibraryExW(attempted,0,LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR|LOAD_LIBRARY_SEARCH_SYSTEM32))return 0;
    return GetLastError();
}
