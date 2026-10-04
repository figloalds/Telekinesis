/* Native WinFsp ABI shim. Storage, handles and conflict policy belong to Rust. */
#include <winfsp/winfsp.h>
#include <sddl.h>
#include <stdlib.h>
#include <wchar.h>
typedef struct {UINT64 size, creation_time, access_time, write_time, change_time, index; UINT32 attributes;} TK_INFO;
typedef struct {UINT64 token;} TK_HANDLE;
extern UINT32 tk_call(UINT32,const WCHAR*,UINT64*,UINT64,void*,UINT32,UINT32,TK_INFO*,UINT32*);
static PSECURITY_DESCRIPTOR Security;
static ULONG SecuritySize;
static void fill(FSP_FSCTL_FILE_INFO *out, TK_INFO *in) {
    if (!out) return;
    memset(out,0,sizeof *out);out->FileAttributes=in->attributes;out->FileSize=in->size;
    out->AllocationSize=(in->size+4095)/4096*4096;
    out->CreationTime=in->creation_time;out->LastAccessTime=in->access_time;
    out->LastWriteTime=in->write_time;out->ChangeTime=in->change_time;
    out->IndexNumber=in->index;
}
static NTSTATUS call(UINT32 op,PWSTR name,PVOID context,UINT64 offset,PVOID buffer,ULONG length,UINT32 flags,FSP_FSCTL_FILE_INFO *fi,PULONG count) {
    TK_INFO info={0};UINT64 token=context?((TK_HANDLE*)context)->token:0;
    NTSTATUS result=tk_call(op,name,&token,offset,buffer,length,flags,&info,count);
    if(NT_SUCCESS(result))fill(fi,&info);return result;
}
static NTSTATUS sec(PSECURITY_DESCRIPTOR sd,SIZE_T *size) {
    if(size){if(*size<SecuritySize){*size=SecuritySize;return STATUS_BUFFER_OVERFLOW;}
        *size=SecuritySize;if(sd)memcpy(sd,Security,SecuritySize);}return STATUS_SUCCESS;
}
static NTSTATUS volume(FSP_FILE_SYSTEM *fs,FSP_FSCTL_VOLUME_INFO *out) {
    memset(out,0,sizeof *out);out->TotalSize=1024ULL*1024*1024;out->FreeSize=512ULL*1024*1024;
    wcscpy_s(out->VolumeLabel,32,L"TKFS PoC");out->VolumeLabelLength=16;return STATUS_SUCCESS;
}
static NTSTATUS security_name(FSP_FILE_SYSTEM *fs,PWSTR name,PUINT32 attrs,PSECURITY_DESCRIPTOR sd,SIZE_T *size) {
    TK_INFO info={0};UINT64 token=0;NTSTATUS result=tk_call(1,name,&token,0,0,0,0,&info,0);
    if(!NT_SUCCESS(result))return result;if(attrs)*attrs=info.attributes;return sec(sd,size);
}
static NTSTATUS create_open(PWSTR name,UINT32 options,UINT32 access,UINT32 attrs,UINT32 op,PVOID *context,FSP_FSCTL_FILE_INFO *fi) {
    TK_HANDLE *handle=calloc(1,sizeof *handle);if(!handle)return STATUS_INSUFFICIENT_RESOURCES;
    TK_INFO info={0};UINT32 flags=((options&FILE_DIRECTORY_FILE)?1:0)|((access&(FILE_WRITE_DATA|FILE_APPEND_DATA))?2:0);
    NTSTATUS result=tk_call(op,name,&handle->token,0,op==2?&attrs:0,op==2?sizeof attrs:0,flags,&info,0);
    if(!NT_SUCCESS(result)){free(handle);return result;}*context=handle;fill(fi,&info);
    /* Force ordinary reads/writes through Rust; kernel data caching would hide
       remotely updated bytes behind an existing file node. Public SDK wire flag. */
    FspFileSystemGetOperationContext()->Response->Rsp.Create.Opened.DisableCache=1;
    return result;
}
static NTSTATUS create(FSP_FILE_SYSTEM *fs,PWSTR name,UINT32 options,UINT32 access,UINT32 attrs,PSECURITY_DESCRIPTOR sd,UINT64 allocation,PVOID *context,FSP_FSCTL_FILE_INFO *fi) {return create_open(name,options,access,attrs,2,context,fi);}
static NTSTATUS open_file(FSP_FILE_SYSTEM *fs,PWSTR name,UINT32 options,UINT32 access,PVOID *context,FSP_FSCTL_FILE_INFO *fi) {return create_open(name,options,access,0,3,context,fi);}
static NTSTATUS overwrite(FSP_FILE_SYSTEM *fs,PVOID context,UINT32 attrs,BOOLEAN replace,UINT64 allocation,FSP_FSCTL_FILE_INFO *fi) {return call(6,0,context,0,0,0,0,fi,0);}
static void cleanup(FSP_FILE_SYSTEM *fs,PVOID context,PWSTR name,ULONG flags) {call(9,0,context,0,0,0,(flags&FspCleanupDelete)?1:0,0,0);}
static void close_file(FSP_FILE_SYSTEM *fs,PVOID context) {call(10,0,context,0,0,0,0,0,0);free(context);}
static NTSTATUS read_file(FSP_FILE_SYSTEM *fs,PVOID context,PVOID buffer,UINT64 offset,ULONG length,PULONG count) {return call(4,0,context,offset,buffer,length,0,0,count);}
static NTSTATUS write_file(FSP_FILE_SYSTEM *fs,PVOID context,PVOID buffer,UINT64 offset,ULONG length,BOOLEAN append,BOOLEAN constrained,PULONG count,FSP_FSCTL_FILE_INFO *fi) {return call(5,0,context,offset,buffer,length,(append?1:0)|(constrained?2:0),fi,count);}
static NTSTATUS flush(FSP_FILE_SYSTEM *fs,PVOID context,FSP_FSCTL_FILE_INFO *fi) {return call(7,0,context,0,0,0,0,fi,0);}
static NTSTATUS get_info(FSP_FILE_SYSTEM *fs,PVOID context,FSP_FSCTL_FILE_INFO *fi) {return call(8,0,context,0,0,0,0,fi,0);}
static NTSTATUS basic(FSP_FILE_SYSTEM *fs,PVOID context,UINT32 attrs,UINT64 ct,UINT64 at,UINT64 wt,UINT64 cht,FSP_FSCTL_FILE_INFO *fi) {
    struct {UINT64 times[4];UINT32 attributes;} update={{ct,at,wt,cht},attrs};
    return call(14,0,context,0,&update,sizeof update,0,fi,0);
}
static NTSTATUS size_file(FSP_FILE_SYSTEM *fs,PVOID context,UINT64 size,BOOLEAN allocation,FSP_FSCTL_FILE_INFO *fi) {return call(6,0,context,size,0,0,allocation?1:0,fi,0);}
static NTSTATUS can_delete(FSP_FILE_SYSTEM *fs,PVOID context,PWSTR name) {return call(11,0,context,0,0,0,0,0,0);}
static NTSTATUS rename_file(FSP_FILE_SYSTEM *fs,PVOID context,PWSTR old_name,PWSTR new_name,BOOLEAN replace) {return call(12,new_name,context,0,0,0,replace?1:0,0,0);}
static NTSTATUS get_security(FSP_FILE_SYSTEM *fs,PVOID context,PSECURITY_DESCRIPTOR sd,SIZE_T *size) {return sec(sd,size);}
static NTSTATUS read_dir(FSP_FILE_SYSTEM *fs,PVOID context,PWSTR pattern,PWSTR marker,PVOID buffer,ULONG length,PULONG count) {
    UINT64 ordinal;*count=0;
    for(ordinal=0;;ordinal++) {
        union {UINT8 bytes[104+512];FSP_FSCTL_DIR_INFO info;} item;
        WCHAR name[256]={0};TK_INFO info={0};UINT64 token=((TK_HANDLE*)context)->token;
        NTSTATUS status=tk_call(13,0,&token,ordinal,name,256,0,&info,0);
        if(status==STATUS_NO_MORE_FILES)break;if(!NT_SUCCESS(status))return status;
        if(marker&&_wcsicmp(name,marker)<=0)continue;
        memset(&item,0,sizeof item);fill(&item.info.FileInfo,&info);
        wcscpy_s(item.info.FileNameBuf,256,name);
        item.info.Size=(UINT16)(sizeof(FSP_FSCTL_DIR_INFO)+wcslen(name)*sizeof(WCHAR));
        if(!FspFileSystemAddDirInfo(&item.info,buffer,length,count))return STATUS_SUCCESS;
    }
    FspFileSystemAddDirInfo(0,buffer,length,count);return STATUS_SUCCESS;
}
static FSP_FILE_SYSTEM_INTERFACE Interface;
UINT32 tk_mount_start(PWSTR path,FSP_FILE_SYSTEM **out) {
    if(!Security) {
        /* Mount ACL for the current user and SYSTEM. No host ACL changes. */
        HANDLE token;DWORD length=0;
        if(!OpenProcessToken(GetCurrentProcess(),TOKEN_QUERY,&token))return FspNtStatusFromWin32(GetLastError());
        GetTokenInformation(token,TokenUser,0,0,&length);TOKEN_USER *user=malloc(length);
        if(!user){CloseHandle(token);return STATUS_INSUFFICIENT_RESOURCES;}
        if(!GetTokenInformation(token,TokenUser,user,length,&length)){free(user);CloseHandle(token);return STATUS_ACCESS_DENIED;}
        PWSTR sid=0;ConvertSidToStringSidW(user->User.Sid,&sid);WCHAR sddl[512];
        swprintf_s(sddl,512,L"O:%sG:%sD:P(A;;FA;;;%s)(A;;FA;;;SY)",sid,sid,sid);
        BOOL ok=ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl,SDDL_REVISION_1,&Security,&SecuritySize);
        LocalFree(sid);free(user);CloseHandle(token);if(!ok)return FspNtStatusFromWin32(GetLastError());
    }
    memset(&Interface,0,sizeof Interface);
    Interface.GetVolumeInfo=volume;Interface.GetSecurityByName=security_name;
    Interface.Create=create;Interface.Open=open_file;Interface.Overwrite=overwrite;
    Interface.Cleanup=cleanup;Interface.Close=close_file;Interface.Read=read_file;Interface.Write=write_file;
    Interface.Flush=flush;Interface.GetFileInfo=get_info;Interface.SetBasicInfo=basic;Interface.SetFileSize=size_file;
    Interface.CanDelete=can_delete;Interface.Rename=rename_file;Interface.GetSecurity=get_security;Interface.ReadDirectory=read_dir;
    FSP_FSCTL_VOLUME_PARAMS p={0};p.Version=sizeof p;p.SectorSize=4096;p.SectorsPerAllocationUnit=1;
    p.MaxComponentLength=255;p.FileInfoTimeout=0;p.CaseSensitiveSearch=0;p.CasePreservedNames=1;
    p.UnicodeOnDisk=1;p.PersistentAcls=0;p.FlushAndPurgeOnCleanup=1;p.UmFileContextIsUserContext2=1;
    wcscpy_s(p.FileSystemName,sizeof p.FileSystemName/sizeof(WCHAR),L"TKFS");
    NTSTATUS status=FspFileSystemCreate(L"WinFsp.Disk",&p,&Interface,out);
    if(!NT_SUCCESS(status))return status;
    FspFileSystemSetOperationGuardStrategy(*out,FSP_FILE_SYSTEM_OPERATION_GUARD_STRATEGY_COARSE);
    status=FspFileSystemSetMountPoint(*out,path);
    if(NT_SUCCESS(status))status=FspFileSystemStartDispatcher(*out,0);
    if(!NT_SUCCESS(status)){FspFileSystemDelete(*out);*out=0;}return status;
}
void tk_mount_stop(FSP_FILE_SYSTEM *fs) {FspFileSystemStopDispatcher(fs);FspFileSystemDelete(fs);}
UINT32 tk_mount_notify(FSP_FILE_SYSTEM *fs,PWSTR path,UINT32 action) {
    SIZE_T bytes=sizeof(FSP_FSCTL_NOTIFY_INFO)+wcslen(path)*sizeof(WCHAR);
    if(bytes>65535)return STATUS_NAME_TOO_LONG;
    FSP_FSCTL_NOTIFY_INFO *info=calloc(1,bytes);if(!info)return STATUS_INSUFFICIENT_RESOURCES;
    info->Size=(UINT16)bytes;info->Action=action;
    info->Filter=FILE_NOTIFY_CHANGE_FILE_NAME|FILE_NOTIFY_CHANGE_DIR_NAME|FILE_NOTIFY_CHANGE_SIZE|FILE_NOTIFY_CHANGE_LAST_WRITE|FILE_NOTIFY_CHANGE_ATTRIBUTES|FILE_NOTIFY_CHANGE_CREATION|FILE_NOTIFY_CHANGE_LAST_ACCESS;
    memcpy(info->FileNameBuf,path,wcslen(path)*sizeof(WCHAR));
    NTSTATUS result=FspFileSystemNotifyBegin(fs,100);
    if(NT_SUCCESS(result)){result=FspFileSystemNotify(fs,info,bytes);FspFileSystemNotifyEnd(fs);}
    free(info);return result;
}
