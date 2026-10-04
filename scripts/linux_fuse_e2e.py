#!/usr/bin/env python3
"""Bounded real Linux mount qualification. Creates only its own state/mounts.
Retains JSON/logs/state under linux-evidence; no network or live project access.
"""
import argparse, errno, json, os, pathlib, signal, stat, subprocess, tempfile, time, traceback

def main():
    args=argparse.ArgumentParser()
    args.add_argument('--binary',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parents[1]/'target/debug/tkfs')
    args.add_argument('--evidence',type=pathlib.Path,default=pathlib.Path('linux-evidence'))
    args=args.parse_args(); binary=args.binary.resolve(); args.evidence.mkdir(exist_ok=True)
    root=pathlib.Path(tempfile.mkdtemp(prefix='mounted-',dir=args.evidence.resolve()))
    state=root/'state'; mount=root/'mount'; daemon=None; report={'root':str(root),'checks':[]}; log=(root/'daemon.log').open('w')
    def run(argv,cwd=None,ok=True):
        result=subprocess.run([str(v) for v in argv],cwd=cwd,text=True,capture_output=True,timeout=20)
        if ok and result.returncode: raise RuntimeError(f'{argv}: {result.stderr}')
        return result
    def cli(*argv,ok=True): return run([binary,'--runtime',state/'runtime.json',*argv],ok=ok)
    def passed(name,**facts): report['checks'].append({'name':name,'passed':True,**facts}); print('PASS '+name,flush=True)
    def alarm(*_): raise TimeoutError('mount harness deadline')
    signal.signal(signal.SIGALRM,alarm);signal.alarm(100)
    try:
        init=json.loads(run([binary,'init','--state',state]).stdout)
        daemon=subprocess.Popen([str(binary),'daemon','--state',str(state),'--mount',str(mount)],stdout=log,stderr=log)
        for _ in range(100):
            if daemon.poll() is not None: raise RuntimeError('daemon exited: '+(root/'daemon.log').read_text())
            if (state/'runtime.json').exists() and mount.exists() and os.path.ismount(mount): break
            time.sleep(.05)
        else: raise RuntimeError('mount did not become ready')
        assert stat.S_IMODE((state/'control.sock').stat().st_mode)==0o600
        assert json.loads(cli('status').stdout)['health'] is None
        run([binary,'--runtime',state/'runtime.json','status'],cwd=mount)
        competing=run([binary,'daemon','--state',state],ok=False)
        assert competing.returncode and 'STATE_ALREADY_OWNED' in competing.stderr
        passed('real_mount_owner_socket_and_exclusive_state_lock')
        folder=mount/'docs'; folder.mkdir(); data=b'0123456789'*70000
        file=folder/'File.txt'
        with file.open('wb') as out: out.write(data);out.flush();os.fsync(out.fileno())
        assert file.read_bytes()==data
        with file.open('r+b') as out: out.seek(100);out.write(b'EDIT');out.truncate(400000);out.flush();os.fsync(out.fileno())
        assert file.stat().st_size==400000 and file.read_bytes()[100:104]==b'EDIT'
        assert (folder/'file.TXT').read_bytes()==file.read_bytes()
        try: os.open(folder/'FILE.txt',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o644)
        except OSError as error: assert error.errno==errno.EEXIST
        else: raise AssertionError('case collision allowed')
        os.chmod(file,0o444);assert stat.S_IMODE(file.stat().st_mode)==0o444
        try: file.open('wb')
        except OSError as error: assert error.errno==errno.EACCES
        else: raise AssertionError('readonly file accepted writable open')
        os.chmod(file,0o755);assert stat.S_IMODE(file.stat().st_mode)==0o755
        timestamp=1_700_000_000_123_456_700
        os.utime(file,ns=(timestamp,timestamp));assert file.stat().st_mtime_ns==timestamp
        tmp=folder/'replace.tmp';tmp.write_bytes(b'replaced');os.replace(tmp,file);assert file.read_bytes()==b'replaced'
        os.chmod(file,0o755)
        assert sorted(p.name for p in folder.iterdir())==['File.txt']
        passed('write_fsync_disk_staging_truncate_case_contract_chmod_utime_atomic_replace')
        for name,operation in [('symlink',lambda:os.symlink('File.txt',folder/'link')),('hardlink',lambda:os.link(file,folder/'hard')),('setuid',lambda:os.chmod(file,0o4755))]:
            try:operation()
            except OSError as error:assert error.errno==errno.EOPNOTSUPP,(name,error)
            else:raise AssertionError(name+' silently accepted')
        passed('unsupported_symlink_hardlink_privileged_mode_explicit_errors')
        branches=json.loads(cli('branches').stdout); initial=next(b['name'] for b in branches if b['id']==init['branch']['id']) if isinstance(init.get('branch'),dict) else branches[0]['name']
        cli('branch','private-linux')
        with file.open('rb') as live:
            busy=cli('checkout','private-linux',ok=False);assert busy.returncode and 'BUSY_VIEW' in busy.stderr
            assert live.read()==b'replaced'
        cwd_holder=subprocess.Popen(['python3','-c','import time; time.sleep(15)'],cwd=folder)
        try:
            busy=cli('checkout','private-linux',ok=False);assert busy.returncode and 'BUSY_VIEW' in busy.stderr
        finally:cwd_holder.terminate();cwd_holder.wait(timeout=3)
        cli('checkout','private-linux');file.write_bytes(b'private change');(mount/'secret.txt').write_text('test-only private data')
        private=json.loads(cli('status').stdout);assert private['upload_state']=='local-only'
        cli('checkout',initial);assert file.read_bytes()==b'replaced';assert not (mount/'secret.txt').exists();assert stat.S_IMODE(file.stat().st_mode)==0o755
        cli('checkout','private-linux');assert (mount/'secret.txt').read_text()=='test-only private data'
        cli('checkout',initial)
        passed('busy_handle_and_cwd_checkout_remount_private_branch_isolation_and_modes')
        seed=root/'seed';seed.mkdir();run(['git','init',seed]);(seed/'hello.c').write_text('int main(void) { return 0; }\n');(seed/'run.sh').write_text('#!/bin/sh\nexit 0\n');os.chmod(seed/'run.sh',0o755)
        run(['git','add','.'],cwd=seed);run(['git','-c','user.name=TKFS Qualification','-c','user.email=tkfs-test@example.invalid','commit','-m','fixture'],cwd=seed)
        checkout=mount/'git-work';run(['git','clone','--no-local',seed,checkout])
        assert run(['git','status','--porcelain=v1'],cwd=checkout).stdout==''
        assert stat.S_IMODE((checkout/'run.sh').stat().st_mode)&0o111
        (checkout/'hello.c').write_text('int main(void) { return 7; }\n')
        assert 'hello.c' in run(['git','status','--porcelain=v1'],cwd=checkout).stdout
        run(['cc','hello.c','-o','hello'],cwd=checkout); assert run([checkout/'hello'],ok=False).returncode==7
        run(['git','add','hello.c'],cwd=checkout);run(['git','-c','user.name=TKFS Qualification','-c','user.email=tkfs-test@example.invalid','commit','-m','mounted edit'],cwd=checkout)
        passed('git_clone_status_edit_commit_and_compiler_executable_on_mount')
        cli('checkpoint','-m','linux mounted qualification')
        cli('stop');daemon.wait(timeout=5);assert daemon.returncode==0 and not os.path.ismount(mount) and not mount.exists()
        daemon=None;passed('checkpoint_and_controlled_unmount_exit')
        # Recover the same state and requalify persisted data and executable bits.
        daemon=subprocess.Popen([str(binary),'daemon','--state',str(state),'--mount',str(mount)],stdout=log,stderr=log)
        for _ in range(100):
            if os.path.ismount(mount):break
            if daemon.poll() is not None:raise RuntimeError('restart failed')
            time.sleep(.05)
        assert file.read_bytes()==b'replaced' and stat.S_IMODE(file.stat().st_mode)==0o755
        assert run([mount/'git-work/hello'],ok=False).returncode==7
        with file.open('rb') as live:
            daemon.send_signal(signal.SIGTERM);time.sleep(.3)
            assert daemon.poll() is None and live.read()==b'replaced'
        daemon.send_signal(signal.SIGTERM);daemon.wait(timeout=5)
        assert daemon.returncode==0 and not os.path.ismount(mount) and not mount.exists()
        daemon=None
        passed('restart_persistence_content_posix_mode_and_binary')
        passed('sigterm_busy_refusal_then_graceful_owned_shutdown')
        report['passed']=True
    except BaseException as error:
        report['passed']=False;report['error']=str(error);report['traceback']=traceback.format_exc();print(report['traceback'],flush=True)
    finally:
        signal.alarm(0)
        if daemon is not None:
            try:cli('stop',ok=False)
            except Exception:pass
            if daemon.poll() is None:
                # Only this child is signalled; only its uniquely scoped mount is detached.
                run(['fusermount3','-u','--',mount],ok=False)
                daemon.terminate()
                try:daemon.wait(timeout=3)
                except subprocess.TimeoutExpired:daemon.kill();daemon.wait(timeout=3)
        log.close();report['cleanup_mount_absent']=not os.path.ismount(mount)
        (root/'report.json').write_text(json.dumps(report,indent=2));(args.evidence/'mounted-latest.json').write_text(json.dumps(report,indent=2))
        print('EVIDENCE '+str(root),flush=True)
    return 0 if report.get('passed') else 1

if __name__=='__main__':raise SystemExit(main())
