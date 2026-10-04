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
/* Pairing storage never repairs an existing descriptor or takes ownership. */
static DWORD storage_descriptor(PSECURITY_DESCRIPTOR *out) {
    TOKEN_USER *user=0;LPWSTR sid=0;WCHAR text[512];DWORD e=current_user(&user);
    if(e)return e;
    if(!ConvertSidToStringSidW(user->User.Sid,&sid)){e=GetLastError();LocalFree(user);return e;}
    swprintf_s(text,512,L"O:%sD:P(A;OICI;GA;;;%s)",sid,sid);
    if(!ConvertStringSecurityDescriptorToSecurityDescriptorW(text,SDDL_REVISION_1,out,0))e=GetLastError();
    LocalFree(sid);LocalFree(user);return e;
}
DWORD tk_storage_validate_descriptor(PSECURITY_DESCRIPTOR sd) {
    TOKEN_USER *user=0;PSID owner=0;PACL acl=0;BOOL defaulted=FALSE,present=FALSE;DWORD e=current_user(&user);
    if(e)return e;
    if(!GetSecurityDescriptorOwner(sd,&owner,&defaulted)||!owner||!EqualSid(owner,user->User.Sid)){e=ERROR_ACCESS_DENIED;goto done;}
    if(!GetSecurityDescriptorDacl(sd,&present,&acl,&defaulted)||!present||!acl){e=ERROR_ACCESS_DENIED;goto done;}
    for(DWORD i=0;i<acl->AceCount;i++){
        ACE_HEADER *ace=0;
        if(!GetAce(acl,i,(void**)&ace)){e=GetLastError();goto done;}
        if(ace->AceType==ACCESS_DENIED_ACE_TYPE)continue;
        if(ace->AceType!=ACCESS_ALLOWED_ACE_TYPE||!EqualSid(&((ACCESS_ALLOWED_ACE*)ace)->SidStart,user->User.Sid)){e=ERROR_ACCESS_DENIED;goto done;}
    }
done:
    LocalFree(user);return e;
}
static BOOL trusted_ancestor_sid(PSID sid,PSID user) {
    BYTE value[SECURITY_MAX_SID_SIZE];DWORD size=sizeof value;
    if(EqualSid(sid,user))return TRUE;
    if(CreateWellKnownSid(WinLocalSystemSid,0,value,&size)&&EqualSid(sid,value))return TRUE;
    size=sizeof value;
    if(CreateWellKnownSid(WinBuiltinAdministratorsSid,0,value,&size)&&EqualSid(sid,value))return TRUE;
    /* Windows' TrustedInstaller owns volume roots on supported installations. */
    PSID installer=0;BOOL same=FALSE;
    if(ConvertStringSidToSidW(L"S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464",&installer)){same=EqualSid(sid,installer);LocalFree(installer);}
    return same;
}
DWORD tk_storage_validate_ancestor(PSECURITY_DESCRIPTOR sd) {
    TOKEN_USER *user=0;PSID owner=0;PACL acl=0;BOOL defaulted=FALSE,present=FALSE;DWORD e=current_user(&user);
    if(e)return e;
    if(!GetSecurityDescriptorOwner(sd,&owner,&defaulted)||!owner||!trusted_ancestor_sid(owner,user->User.Sid)){e=ERROR_ACCESS_DENIED;goto done;}
    if(!GetSecurityDescriptorDacl(sd,&present,&acl,&defaulted)||!present||!acl){e=ERROR_ACCESS_DENIED;goto done;}
    for(DWORD i=0;i<acl->AceCount;i++){
        ACE_HEADER *ace=0;if(!GetAce(acl,i,(void**)&ace)){e=GetLastError();goto done;}
        if(ace->AceFlags&INHERIT_ONLY_ACE)continue;
        if(ace->AceType==ACCESS_DENIED_ACE_TYPE)continue;
        if(ace->AceType!=ACCESS_ALLOWED_ACE_TYPE){e=ERROR_ACCESS_DENIED;goto done;}
        ACCESS_ALLOWED_ACE *allow=(ACCESS_ALLOWED_ACE*)ace;
        if(trusted_ancestor_sid(&allow->SidStart,user->User.Sid))continue;
        DWORD unsafe=GENERIC_ALL|GENERIC_WRITE|WRITE_DAC|WRITE_OWNER|DELETE|FILE_DELETE_CHILD|FILE_WRITE_DATA|FILE_WRITE_ATTRIBUTES;
        if(allow->Mask&unsafe){e=ERROR_ACCESS_DENIED;goto done;}
    }
done:
    LocalFree(user);return e;
}
/* flags: directory=1, create-private=2, check-owner/DACL=4, share-delete=8,
 * attributes-only=16 (inspection only), trusted-ancestor=32.
 * Open the reparse point itself, reject it, and retain handles to prevent path
 * replacement. Files must be single-link disk files, never an alias or device. */
DWORD tk_storage_open(LPCWSTR path,DWORD flags,HANDLE *out) {
    PSECURITY_DESCRIPTOR created=0,actual=0;DWORD e=0;*out=INVALID_HANDLE_VALUE;
    if(flags&2){
        e=storage_descriptor(&created);if(e)return e;
        if((flags&1)&&GetFileAttributesW(path)==INVALID_FILE_ATTRIBUTES){
            SECURITY_ATTRIBUTES sa={sizeof sa,created,FALSE};
            if(!CreateDirectoryW(path,&sa)&&GetLastError()!=ERROR_ALREADY_EXISTS){e=GetLastError();goto done;}
        }
    }
    SECURITY_ATTRIBUTES sa={sizeof sa,created,FALSE};
    /* Attribute-only opens do not participate in Windows sharing checks. Read
     * data/list-directory access makes the no-share-delete pin effective. */
    DWORD access=FILE_READ_ATTRIBUTES|((flags&16)?0:FILE_READ_DATA)|((flags&(4|32))?READ_CONTROL:0);
    /* Directory write handles can mutate reparse metadata without a rename.
     * Deny those too while the namespace is pinned. Child file IO is separate. */
    DWORD share=FILE_SHARE_READ|((flags&1)?0:FILE_SHARE_WRITE)|((flags&8)?FILE_SHARE_DELETE:0);
    *out=CreateFileW(path,access,share,created?&sa:0,
        (flags&2)&&!(flags&1)?OPEN_ALWAYS:OPEN_EXISTING,FILE_FLAG_OPEN_REPARSE_POINT|FILE_FLAG_BACKUP_SEMANTICS,0);
    if(*out==INVALID_HANDLE_VALUE){e=GetLastError();goto done;}
    BY_HANDLE_FILE_INFORMATION info;
    if(!GetFileInformationByHandle(*out,&info)){e=GetLastError();goto done;}
    if((info.dwFileAttributes&FILE_ATTRIBUTE_REPARSE_POINT)||!!(info.dwFileAttributes&FILE_ATTRIBUTE_DIRECTORY)!=!!(flags&1)||(!(flags&1)&&(GetFileType(*out)!=FILE_TYPE_DISK||info.nNumberOfLinks!=1))){e=ERROR_ACCESS_DENIED;goto done;}
    if(flags&(4|32)){
        e=GetSecurityInfo(*out,SE_FILE_OBJECT,OWNER_SECURITY_INFORMATION|DACL_SECURITY_INFORMATION,0,0,0,0,&actual);
        if(e)goto done;
        e=(flags&4)?tk_storage_validate_descriptor(actual):tk_storage_validate_ancestor(actual);if(e)goto done;
        if((flags&1)&&(flags&4)){SECURITY_DESCRIPTOR_CONTROL control;DWORD revision;
            if(!GetSecurityDescriptorControl(actual,&control,&revision)||!(control&SE_DACL_PROTECTED))e=ERROR_ACCESS_DENIED;
        }
    }
done:
    LocalFree(created);LocalFree(actual);
    if(e&&*out!=INVALID_HANDLE_VALUE){CloseHandle(*out);*out=INVALID_HANDLE_VALUE;}
    return e;
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
        if(WaitForSingleObject(op->hEvent,timeout)!=WAIT_OBJECT_0){CancelIoEx(pipe,op);if(!GetOverlappedResult(pipe,op,count,TRUE)){e=GetLastError();if(e==ERROR_OPERATION_ABORTED)e=ERROR_SEM_TIMEOUT;}}
        else if(!GetOverlappedResult(pipe,op,count,FALSE))e=GetLastError();
    } else if(!done)e=initial;
    CloseHandle(op->hEvent);return e;
}
DWORD tk_pipe_accept(HANDLE pipe) {
    OVERLAPPED op={0};DWORD count=0;op.hEvent=CreateEventW(0,TRUE,FALSE,0);
    if(!op.hEvent)return GetLastError();
    BOOL done=ConnectNamedPipe(pipe,&op);DWORD e=done?0:GetLastError();
    if(e==ERROR_PIPE_CONNECTED){CloseHandle(op.hEvent);return 0;}
    return finish_io(pipe,&op,done,e,250,&count);
}
DWORD tk_pipe_disconnect(HANDLE pipe) {
    if(DisconnectNamedPipe(pipe))return 0;
    DWORD e=GetLastError();return e==ERROR_PIPE_NOT_CONNECTED?0:e;
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
    ULONGLONG deadline=GetTickCount64()+5000;
    for(;;){
        ULONGLONG now=GetTickCount64();
        if(now>=deadline)return ERROR_SEM_TIMEOUT;
        if(!WaitNamedPipeW(name,(DWORD)(deadline-now)))return GetLastError();
        *out=CreateFileW(name,GENERIC_READ|GENERIC_WRITE,0,0,OPEN_EXISTING,FILE_FLAG_OVERLAPPED|SECURITY_SQOS_PRESENT|SECURITY_IDENTIFICATION,0);
        if(*out!=INVALID_HANDLE_VALUE)break;
        e=GetLastError();
        // WaitNamedPipe does not reserve the instance. Another authorized client
        // can win between wait and open; retry only that race, within the bound.
        if(e!=ERROR_PIPE_BUSY)return e;
    }
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
