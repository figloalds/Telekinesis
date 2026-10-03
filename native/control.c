#define _WIN32_WINNT 0x0601
#include <windows.h>
#include <sddl.h>
#include <aclapi.h>
#include <stdio.h>

static DWORD current_user(TOKEN_USER **out) {
    HANDLE token; DWORD size=0;
    if(!OpenProcessToken(GetCurrentProcess(),TOKEN_QUERY,&token))return GetLastError();
    GetTokenInformation(token,TokenUser,0,0,&size);
    *out=LocalAlloc(LPTR,size);
    if(!*out){CloseHandle(token);return ERROR_NOT_ENOUGH_MEMORY;}
    if(!GetTokenInformation(token,TokenUser,*out,size,&size)){DWORD e=GetLastError();LocalFree(*out);*out=0;CloseHandle(token);return e;}
    CloseHandle(token);return 0;
}
static DWORD owner_descriptor(PSECURITY_DESCRIPTOR *out) {
    TOKEN_USER *user=0;LPWSTR sid=0;WCHAR text[256];DWORD e=current_user(&user);
    if(e)return e;
    if(!ConvertSidToStringSidW(user->User.Sid,&sid)){e=GetLastError();LocalFree(user);return e;}
    swprintf_s(text,256,L"D:P(A;OICI;GA;;;%s)",sid);
    if(!ConvertStringSecurityDescriptorToSecurityDescriptorW(text,SDDL_REVISION_1,out,0))e=GetLastError();
    LocalFree(sid);LocalFree(user);return e;
}
DWORD tk_private_directory(LPWSTR path) {
    PSECURITY_DESCRIPTOR sd=0;PACL acl=0;BOOL present,defaulted;DWORD e=owner_descriptor(&sd);
    if(e)return e;
    if(!GetSecurityDescriptorDacl(sd,&present,&acl,&defaulted)){e=GetLastError();LocalFree(sd);return e;}
    e=SetNamedSecurityInfoW(path,SE_FILE_OBJECT,DACL_SECURITY_INFORMATION|PROTECTED_DACL_SECURITY_INFORMATION,0,0,acl,0);
    LocalFree(sd);return e;
}
DWORD tk_owner_sid(LPWSTR out,DWORD count) {
    TOKEN_USER *user=0;LPWSTR sid=0;DWORD e=current_user(&user);if(e)return e;
    if(!ConvertSidToStringSidW(user->User.Sid,&sid))e=GetLastError();
    else if(wcslen(sid)+1>count)e=ERROR_INSUFFICIENT_BUFFER;
    else wcscpy_s(out,count,sid);
    LocalFree(sid);LocalFree(user);return e;
}
DWORD tk_pipe_listen(LPCWSTR name,HANDLE *out) {
    PSECURITY_DESCRIPTOR sd=0;DWORD e=owner_descriptor(&sd);if(e)return e;
    SECURITY_ATTRIBUTES sa={sizeof sa,sd,FALSE};
    *out=CreateNamedPipeW(name,PIPE_ACCESS_DUPLEX|FILE_FLAG_FIRST_PIPE_INSTANCE|FILE_FLAG_OVERLAPPED,PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT|PIPE_REJECT_REMOTE_CLIENTS,1,65536,65536,5000,&sa);
    e=*out==INVALID_HANDLE_VALUE?GetLastError():0;LocalFree(sd);return e;
}
static DWORD finish_io(HANDLE pipe,OVERLAPPED *op,BOOL done,DWORD initial,DWORD timeout,DWORD *count) {
    DWORD e=0;
    if(!done && initial==ERROR_IO_PENDING){
        if(WaitForSingleObject(op->hEvent,timeout)!=WAIT_OBJECT_0){CancelIoEx(pipe,op);GetOverlappedResult(pipe,op,count,TRUE);e=ERROR_SEM_TIMEOUT;}
        else if(!GetOverlappedResult(pipe,op,count,FALSE))e=GetLastError();
    } else if(!done)e=initial;
    CloseHandle(op->hEvent);return e;
}
DWORD tk_pipe_accept(HANDLE pipe) {
    OVERLAPPED op={0};DWORD count=0;op.hEvent=CreateEventW(0,TRUE,FALSE,0);
    if(!op.hEvent)return GetLastError();DisconnectNamedPipe(pipe);
    BOOL done=ConnectNamedPipe(pipe,&op);DWORD e=done?0:GetLastError();
    if(e==ERROR_PIPE_CONNECTED){CloseHandle(op.hEvent);return 0;}
    return finish_io(pipe,&op,done,e,250,&count);
}
DWORD tk_pipe_io(HANDLE pipe,void *buffer,DWORD length,DWORD *count,DWORD write,DWORD timeout) {
    OVERLAPPED op={0};op.hEvent=CreateEventW(0,TRUE,FALSE,0);if(!op.hEvent)return GetLastError();
    BOOL done=write?WriteFile(pipe,buffer,length,count,&op):ReadFile(pipe,buffer,length,count,&op);
    DWORD e=done?0:GetLastError();return finish_io(pipe,&op,done,e,timeout,count);
}
/* Call after reading the request so impersonation observes the actual caller. */
DWORD tk_pipe_authorize(HANDLE pipe) {
    HANDLE token=0;TOKEN_USER *owner=0,*caller=0;DWORD size=0,e=current_user(&owner);
    if(e)return e;
    if(!ImpersonateNamedPipeClient(pipe)){e=GetLastError();goto done;}
    if(!OpenThreadToken(GetCurrentThread(),TOKEN_QUERY,TRUE,&token)){e=GetLastError();RevertToSelf();goto done;}
    RevertToSelf();GetTokenInformation(token,TokenUser,0,0,&size);
    caller=LocalAlloc(LPTR,size);
    if(!caller)e=ERROR_NOT_ENOUGH_MEMORY;
    else if(!GetTokenInformation(token,TokenUser,caller,size,&size))e=GetLastError();
    else if(!EqualSid(owner->User.Sid,caller->User.Sid))e=ERROR_ACCESS_DENIED;
done:
    if(token)CloseHandle(token);LocalFree(owner);LocalFree(caller);return e;
}
DWORD tk_pipe_connect(LPCWSTR name,HANDLE *out) {
    TOKEN_USER *owner=0,*server=0;DWORD size=0,e=0;ULONG pid=0;HANDLE process=0,token=0;
    if(!WaitNamedPipeW(name,5000))return GetLastError();
    *out=CreateFileW(name,GENERIC_READ|GENERIC_WRITE,0,0,OPEN_EXISTING,FILE_FLAG_OVERLAPPED|SECURITY_SQOS_PRESENT|SECURITY_IDENTIFICATION,0);
    if(*out==INVALID_HANDLE_VALUE)return GetLastError();
    e=current_user(&owner);if(e)goto done;
    if(!GetNamedPipeServerProcessId(*out,&pid)){e=GetLastError();goto done;}
    process=OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION,FALSE,pid);
    if(!process){e=GetLastError();goto done;}
    if(!OpenProcessToken(process,TOKEN_QUERY,&token)){e=GetLastError();goto done;}
    GetTokenInformation(token,TokenUser,0,0,&size);server=LocalAlloc(LPTR,size);
    if(!server)e=ERROR_NOT_ENOUGH_MEMORY;
    else if(!GetTokenInformation(token,TokenUser,server,size,&size))e=GetLastError();
    else if(!EqualSid(owner->User.Sid,server->User.Sid))e=ERROR_ACCESS_DENIED;
done:
    if(token)CloseHandle(token);if(process)CloseHandle(process);LocalFree(owner);LocalFree(server);
    if(e){CloseHandle(*out);*out=INVALID_HANDLE_VALUE;}return e;
}
