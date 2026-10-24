"""Build inert DLLs and test actual delay imports without starting a driver/mount.

Each case runs in a bounded child because the verified binding is process-wide.
Every stub export exits 88 if invoked; only loading/resolving symbols is allowed.
"""
from pathlib import Path
import argparse, json, os, re, struct, subprocess, time
from mount_lifecycle import run_bounded

def imports(path):
    data=path.read_bytes()
    u32=lambda at:struct.unpack_from('<I',data,at)[0]
    pe=u32(0x3c);count=struct.unpack_from('<H',data,pe+6)[0]
    optional=pe+24;sections=optional+struct.unpack_from('<H',data,pe+20)[0]
    def offset(rva):
        for index in range(count):
            at=sections+index*40;size,start,raw_size,raw=struct.unpack_from('<IIII',data,at+8)
            if start<=rva<start+max(size,raw_size):return raw+rva-start
        raise ValueError(f'RVA outside image: {rva}')
    def string(rva):
        at=offset(rva);return data[at:data.index(b'\0',at)].decode('ascii')
    directory=offset(u32(optional+112+13*8))
    for index in range(64):
        fields=struct.unpack_from('<IIIIIIII',data,directory+index*32)
        if not fields[1]:break
        if string(fields[1]).lower()!='winfsp-x64.dll':continue
        at=offset(fields[4]);names=[]
        while True:
            value=struct.unpack_from('<Q',data,at)[0];at+=8
            if not value:return names
            assert not value>>63,'ordinal fixture import needs an explicit export definition'
            name=string(value+2);assert re.fullmatch(r'Fsp[A-Za-z0-9_]+',name),name
            names.append(name)
    raise RuntimeError('test executable has no WinFsp delay imports')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home',type=Path,default=Path('test-runs/winfsp-binding')/str(time.time_ns()))
    args=parser.parse_args();home=args.home.resolve();home.mkdir(parents=True,exist_ok=False)
    built=run_bounded(['cargo','test','--test','winfsp_binding','--no-run','--message-format=json'],timeout=300,text=True)
    (home/'build-console.txt').write_text(built.stdout+built.stderr)
    built.check_returncode()
    artifacts=[json.loads(line) for line in built.stdout.splitlines() if line.startswith('{')]
    exe=Path(next(item['executable'] for item in artifacts if item.get('reason')=='compiler-artifact'
                  and item.get('executable') and item.get('target',{}).get('name')=='winfsp_binding'))
    names=imports(exe)
    vswhere=Path(os.environ.get('ProgramFiles(x86)','C:/Program Files (x86)'))/'Microsoft Visual Studio/Installer/vswhere.exe'
    install=Path(run_bounded([str(vswhere),'-prerelease','-latest','-products','*','-property','installationPath'],timeout=10,text=True).stdout.strip())
    tools=install/'VC/Tools/MSVC';version=max(tools.iterdir(),key=lambda p:tuple(map(int,p.name.split('.'))))
    compiler=version/'bin/Hostx64/x64/cl.exe'
    sdk=Path(os.environ.get('ProgramFiles(x86)','C:/Program Files (x86)'))/'Windows Kits/10'
    sdk_version=max((p.name for p in (sdk/'Include').iterdir() if (p/'um/Windows.h').exists()),key=lambda value:tuple(map(int,value.split('.'))))
    env={**os.environ,'PATH':str(compiler.parent)+';'+os.environ['PATH'],
         'INCLUDE':';'.join(map(str,[version/'include',*[sdk/'Include'/sdk_version/p for p in ('ucrt','shared','um','winrt')]])),
         'LIB':';'.join(map(str,[version/'lib/x64',sdk/'Lib'/sdk_version/'ucrt/x64',sdk/'Lib'/sdk_version/'um/x64']))}
    source=home/'stub.c'
    source.write_text('#include <windows.h>\n'+''.join('__declspec(dllexport) void '+name+'(void){ExitProcess(88);}\n' for name in names))
    dlls=[]
    for label in ('a','b'):
        folder=home/label;folder.mkdir();dll=folder/'winfsp-x64.dll';dlls.append(dll)
        result=run_bounded([str(compiler),'/nologo','/LD','/MT','/O2',str(source),'/Fo'+str(folder/'stub.obj'),'/Fe'+str(dll),'/link','/IMPLIB:'+str(folder/'stub.lib')],timeout=30,text=True,env=env,cwd=folder)
        (home/(label+'-compiler.txt')).write_text(result.stdout+result.stderr);result.check_returncode()
    result={'scope':'no driver/service/mount; inert exports only','test_executable':str(exe),'imports':names,'cases':[]}
    for mode in ('default-helper-baseline','preloaded','prebound','mixed-iat','freeze'):
        child_env={**os.environ,'TKFS_BINDING_CASE':mode,'TKFS_BINDING_DLL_A':str(dlls[0]),'TKFS_BINDING_DLL_B':str(dlls[1])}
        child=run_bounded([str(exe),'--ignored','--exact','binding_fixture','--nocapture'],timeout=10,text=True,env=child_env)
        (home/(mode+'.txt')).write_text(child.stdout+child.stderr)
        result['cases'].append({'name':mode,'returncode':child.returncode})
        (home/'result.json').write_text(json.dumps(result,indent=2))
        child.check_returncode()
    result['passed']=True;(home/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))

if __name__=='__main__':main()
