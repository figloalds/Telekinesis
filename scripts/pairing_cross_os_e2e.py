"""Foreground Windows/Ubuntu acceptance using owned disposable loopback fixtures.

The Linux --node controller uses pipes, not another network listener. Invitation
tokens stay in memory/stdin. No systemd activation or production keys are used.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import time
import uuid


class Node:
    def __init__(self, root):
        self.root = Path(root).absolute()
        assert self.root.name.startswith('.tkfs-pairing-test-')
        self.children = {}
        self.env = dict(os.environ, CREDENTIALS_DIRECTORY=str(self.root / 'credentials'))

    def path(self, name):
        path = self.root / name
        assert path.absolute().is_relative_to(self.root) and '..' not in path.parts
        return path

    def call(self, request):
        op = request['op']
        if op == 'run':
            result = subprocess.run(request['args'], input=request.get('input'), capture_output=True,
                                    text=True, env=self.env, timeout=request.get('timeout', 40))
            return dict(code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        if op == 'spawn':
            name = request['name']
            assert name not in self.children or self.children[name].poll() is not None
            with self.path(name + '.log').open('ab') as log:
                child = subprocess.Popen(request['args'], stdin=subprocess.DEVNULL,
                                         stdout=log, stderr=log, env=self.env)
            self.children[name] = child
            return dict(pid=child.pid)
        if op == 'stop':
            child = self.children[request['name']]
            if child.poll() is None:
                if request.get('kill'): child.kill()
                else: child.terminate()
                try: child.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    child.kill(); child.wait(timeout=5)
                    raise RuntimeError('Owned process exceeded stop budget')
            return dict(code=child.returncode)
        if op == 'port':
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                return sock.getsockname()[1]
        if op == 'connections':
            child = self.children['service']
            assert child.poll() is None
            if os.name == 'nt':
                script = ('ConvertTo-Json -Compress -InputObject @(' +
                          f'Get-NetTCPConnection -OwningProcess {child.pid} -State Established | ' +
                          'Select-Object LocalAddress,LocalPort,RemoteAddress,RemotePort)')
                result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', script],
                                        capture_output=True, text=True, timeout=15)
                assert result.returncode == 0, result.stderr
                return dict(pid=child.pid, sockets=json.loads(result.stdout))
            inodes = set()
            for descriptor in Path(f'/proc/{child.pid}/fd').iterdir():
                try: target = os.readlink(descriptor)
                except FileNotFoundError: continue
                if target.startswith('socket:['): inodes.add(target[8:-1])
            entries = []
            for table, family in (('tcp', socket.AF_INET), ('tcp6', socket.AF_INET6)):
                for line in Path(f'/proc/{child.pid}/net/{table}').read_text().splitlines()[1:]:
                    fields = line.split()
                    if fields[9] not in inodes or fields[3] != '01': continue
                    def address(field):
                        ip, port = field.split(':')
                        raw = bytes.fromhex(ip)
                        raw = b''.join(raw[index:index+4][::-1] for index in range(0, len(raw), 4))
                        return dict(address=socket.inet_ntop(family, raw), port=int(port, 16))
                    entries.append(dict(local=address(fields[1]), peer=address(fields[2])))
            return dict(pid=child.pid, sockets=entries)
        if op == 'write':
            data = bytes([request.get('byte', 90)]) * request.get('size', 100)
            assert len(data) <= 16 * 1024 * 1024
            with self.path(request['path']).open('wb') as output:
                output.write(data); output.flush(); os.fsync(output.fileno())
            return dict(sha256=hashlib.sha256(data).hexdigest(), size=len(data))
        if op == 'read':
            path = self.path(request['path'])
            if not path.exists(): return None
            data = path.read_bytes()
            return dict(sha256=hashlib.sha256(data).hexdigest(), size=len(data),
                        mode=stat.S_IMODE(path.stat().st_mode))
        if op == 'rename':
            self.path(request['path']).rename(self.path(request['target'])); return True
        if op == 'delete':
            self.path(request['path']).unlink(); return True
        if op == 'chmod':
            self.path(request['path']).chmod(request['mode']); return True
        if op == 'credentials-remove':
            for name in ('tkfs.identity', 'identity.dpapi'):
                self.path('credentials/' + name).unlink(missing_ok=True)
            return True
        if op == 'cleanup':
            return self.close(remove=request.get('remove', False), keep=request.get('keep', False))
        if op == 'reject-config':
            source = self.path('pairing.toml').read_text()
            if 'inbound = false' in source:
                values = {'inbound': 'true', 'listen': '"127.0.0.1:43180"',
                          'advertise': '"ws://127.0.0.1:43180/tkfs/sync"',
                          'enrollment_advertise': '"ws://127.0.0.1:43180/tkfs/enroll"'}
                source = '\n'.join(key + ' = ' + values[key] if key in values else line
                                   for line in source.splitlines() for key in [line.partition(' =')[0]])
            path = self.path('rejected.toml')
            results = []
            variants = [(host, source.replace('127.0.0.1', host)) for host in
                        ('172.20.0.1', '0.0.0.0', 'localhost', '[::ffff:172.20.0.1]')]
            # Public bind must also fail when both advertisements remain loopback.
            lines = source.splitlines()
            variants.append(('nonloopback-bind', '\n'.join(
                line.replace('127.0.0.1', '0.0.0.0') if line.startswith('listen =') else line for line in lines)))
            try:
                for name, text in variants:
                    path.write_text(text)
                    result = subprocess.run([request['binary'], 'pairing', '-f', str(path), 'status'],
                                            capture_output=True, text=True, env=self.env, timeout=10)
                    assert result.returncode != 0
                    assert any(code in result.stderr for code in ('WS_LOOPBACK', 'NUMERIC_ADDRESS_REQUIRED', 'INVALID_LISTEN_ADDRESS'))
                    results.append(dict(case=name, code=result.returncode, error=result.stderr.strip()))
            finally: path.unlink(missing_ok=True)
            return results
        raise ValueError('Unknown fixture operation')

    def close(self, remove=False, keep=False):
        errors = []
        for name in reversed(self.children):
            try: self.call(dict(op='stop', name=name))
            except Exception as error: errors.append(str(error))
        try: self.call(dict(op='credentials-remove'))
        except Exception as error: errors.append(str(error))
        if errors: raise RuntimeError('; '.join(errors))
        if self.root.exists():
            (self.root / '.test-artifacts.json').write_text(json.dumps(
                dict(version=1, stopped=True, keep=keep, completed_at=time.time())))
            if remove:
                from test_artifacts import remove_run
                remove_run(self.root, self.root.parent)
        return dict(stopped=True, disposable_credentials_removed=True, fixtures_removed=not self.root.exists())


class Remote:
    def __init__(self, root, distro):
        script = '/mnt/' + Path(__file__).drive[0].lower() + str(Path(__file__).absolute())[2:].replace('\\', '/')
        self.process = subprocess.Popen(['wsl.exe', '-d', distro, '--exec', 'python3', '-u', script,
                                         '--node', '--linux-root', root], stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def call(self, request):
        self.process.stdin.write(json.dumps(request) + '\n'); self.process.stdin.flush()
        result = json.loads(self.process.stdout.readline())
        if not result['ok']: raise RuntimeError(result['error'])
        return result['value']

    def close(self, remove=False, keep=False):
        result = self.call(dict(op='cleanup', remove=remove, keep=keep))
        self.process.stdin.close()
        self.process.wait(timeout=45)
        assert self.process.returncode == 0
        return result


def node_loop(root):
    node = Node(root)
    try:
        for line in sys.stdin:
            try: reply = dict(ok=True, value=node.call(json.loads(line)))
            except Exception as error: reply = dict(ok=False, error=str(error))
            print(json.dumps(reply), flush=True)
    finally: node.close()


def acceptance(options):
    evidence = Path(options.evidence).absolute()
    evidence.parent.mkdir(parents=True, exist_ok=True)
    repo = str(uuid.uuid4())
    win = Node(options.windows_root)
    linux = Remote(options.linux_root, options.distro)
    checks = []
    metadata = {}

    def passed(name, **facts):
        checks.append(dict(name=name, passed=True, **facts)); print('PASS ' + name, flush=True)

    def run(node, args, input=None, ok=True):
        result = node.call(dict(op='run', args=list(map(str, args)), input=input))
        if ok and result['code'] != 0:
            raise RuntimeError('Fixture command failed: ' + result['stderr'][:1000])
        return result

    def pair(node, binary, info, *args, input=None, ok=True):
        return run(node, [binary, 'pairing', '-f', info['config'], *args], input, ok)

    def cli(node, binary, info, *args):
        return json.loads(run(node, [binary, '--runtime', info['runtime'], *args])['stdout'])

    def wait(predicate, description):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if predicate(): return
            except (OSError, RuntimeError, json.JSONDecodeError): pass
            time.sleep(.1)
        raise TimeoutError(description)

    def read(node, path): return node.call(dict(op='read', path='mount/' + path))
    def write(node, path, size=100, byte=90):
        return node.call(dict(op='write', path='mount/' + path, size=size, byte=byte))
    def matches(node, path, value):
        actual = read(node, path)
        return actual is not None and actual['sha256'] == value['sha256']

    error = None
    try:
        port = linux.call(dict(op='port'))
        url = f'{options.scheme}://127.0.0.1:{port}/tkfs/sync'
        li = json.loads(run(linux, [options.linux_helper, '--disposable-test-fixture', options.linux_root, repo, url])['stdout'])
        wi = json.loads(run(win, [options.windows_helper, '--disposable-test-fixture', options.windows_root, repo, 'outbound-only'])['stdout'])
        metadata = dict(repo=repo, windows=wi, ubuntu=li, scheme=options.scheme, endpoint=url,
                        topology='Windows outbound-only client -> Ubuntu 127.0.0.1 listener with dial=false; upload/download on client-initiated sessions',
                        credential_delivery='Fresh test-only Windows CurrentUser DPAPI and Linux 0600 credential file with CREDENTIALS_DIRECTORY; no systemd activation')
        if options.scheme == 'ws':
            for label, node, binary in (('windows', win, options.windows_binary), ('ubuntu', linux, options.linux_binary)):
                cases = node.call(dict(op='reject-config', binary=binary))
                passed(label + '_actual_cli_rejects_nonloopback_dns_mapped_address_and_public_bind_before_network', cases=cases)
        for node, binary, root in ((win, options.windows_binary, options.windows_root), (linux, options.linux_binary, options.linux_root)):
            node.call(dict(op='spawn', name='worker', args=[binary, 'daemon', '--state', root + '/state', '--mount', root + '/mount']))
        wait(lambda: cli(win, options.windows_binary, wi, 'status') and cli(linux, options.linux_binary, li, 'status'), 'mounted workers unavailable')
        passed('fresh_windows_winfsp_and_ubuntu_fuse_workers', windows_replica=wi['replica'], linux_replica=li['replica'])
        for node, binary, info in ((linux, options.linux_binary, li), (win, options.windows_binary, wi)):
            node.call(dict(op='spawn', name='service', args=[binary, 'pairing', '-f', info['config'], 'run']))
        invitation = pair(linux, options.linux_binary, li, 'invite')['stdout'].strip()
        # Public invitation ID is extracted in memory; token itself never logged.
        invitation_id = json.loads(bytes.fromhex(invitation).decode())['id']
        wait(lambda: pair(win, options.windows_binary, wi, 'join', input=invitation+'\n', ok=False)['code'] == 0, 'loopback enrollment failed')
        invitation = None
        denied = pair(win, options.windows_binary, wi, 'remote-list', li['installation'], ok=False)
        assert denied['code'] != 0
        pair(linux, options.linux_binary, li, 'approve', invitation_id, '--installation', wi['installation'], '--fingerprint', wi['fingerprint'])
        empty = json.loads(pair(win, options.windows_binary, wi, 'remote-list', li['installation'])['stdout'])
        assert empty == [] or empty.get('result') == []
        passed('cli_enrollment_exact_key_local_approval_and_no_access_before_grants')
        for node, binary, info, other in ((linux, options.linux_binary, li, wi), (win, options.windows_binary, wi, li)):
            pair(node, binary, info, 'grant', other['installation'], '--repo', repo, '--runtime', info['runtime'], '--remote-replica', other['replica'])
        first = write(win, 'windows.txt', byte=65)
        wait(lambda: matches(linux, 'windows.txt', first), 'Windows mounted write not synchronized')
        reverse = write(linux, 'linux.txt', byte=66)
        wait(lambda: matches(win, 'linux.txt', reverse), 'Ubuntu mounted write not synchronized')
        passed('mounted_file_writes_both_directions_over_windows_initiated_sessions')
        for label, node in (('windows', win), ('ubuntu', linux)):
            connections = node.call(dict(op='connections'))
            assert connections['sockets'], 'No owned established transport connection observed'
            passed(label + '_owned_transport_socket_addresses', **connections)
        large = write(win, 'large.bin', size=12 * 1024 * 1024, byte=91)
        wait(lambda: matches(linux, 'large.bin', large), '12 MiB transfer not synchronized')
        passed('large_resumable_transfer_sha256', **large)
        executable = write(linux, 'run.sh', byte=67)
        linux.call(dict(op='chmod', path='mount/run.sh', mode=0o755))
        wait(lambda: matches(win, 'run.sh', executable), 'executable not synchronized')
        # Windows content edits must preserve Linux executable bits.
        edited = write(win, 'run.sh', byte=68)
        wait(lambda: matches(linux, 'run.sh', edited) and read(linux, 'run.sh')['mode'] == 0o755, 'executable mode lost across Windows edit')
        passed('linux_executable_mode_survives_windows_content_edit', mode=read(linux, 'run.sh')['mode'])
        win.call(dict(op='rename', path='mount/windows.txt', target='mount/renamed.txt'))
        wait(lambda: matches(linux, 'renamed.txt', first) and read(linux, 'windows.txt') is None, 'rename not synchronized')
        linux.call(dict(op='delete', path='mount/linux.txt'))
        wait(lambda: read(win, 'linux.txt') is None, 'delete not synchronized')
        passed('mounted_rename_and_delete_cross_os')
        cli(linux, options.linux_binary, li, 'branch', 'private-test')
        cli(linux, options.linux_binary, li, 'checkout', 'private-test')
        secret = write(linux, 'private-only.txt', byte=69)
        private_hash = secret['sha256']
        time.sleep(1)
        assert read(win, 'private-only.txt') is None
        cli(linux, options.linux_binary, li, 'checkout', 'main')
        denied_object = run(win, [options.windows_binary, '--runtime', wi['runtime'], 'cat-object', private_hash], ok=False)
        assert denied_object['code'] != 0
        passed('private_branch_and_private_cas_stay_local')
        linux.call(dict(op='stop', name='service', kill=True))
        queued = write(win, 'queued-offline.txt', byte=70)
        linux.call(dict(op='spawn', name='service', args=[options.linux_binary, 'pairing', '-f', li['config'], 'run']))
        wait(lambda: matches(linux, 'queued-offline.txt', queued), 'stale pooled session did not reconnect')
        passed('listener_crash_restart_and_pooled_session_reconnect')
        for node, binary, info, root in ((win, options.windows_binary, wi, options.windows_root), (linux, options.linux_binary, li, options.linux_root)):
            node.call(dict(op='stop', name='service', kill=True))
            node.call(dict(op='stop', name='worker'))
            node.call(dict(op='spawn', name='worker', args=[binary, 'daemon', '--state', root + '/state', '--mount', root + '/mount']))
        wait(lambda: cli(win, options.windows_binary, wi, 'status') and cli(linux, options.linux_binary, li, 'status'), 'worker restart failed')
        for node, binary, info in ((linux, options.linux_binary, li), (win, options.windows_binary, wi)):
            node.call(dict(op='spawn', name='service', args=[binary, 'pairing', '-f', info['config'], 'run']))
        resumed = write(linux, 'resumed.txt', byte=71)
        wait(lambda: matches(win, 'resumed.txt', resumed), 'restart synchronization failed')
        passed('both_workers_and_services_restart_with_durable_grants_and_credentials')
        pair(linux, options.linux_binary, li, 'revoke', wi['installation'])
        revoked = write(linux, 'revoked.txt', byte=72)
        result = pair(win, options.windows_binary, wi, 'sync', li['installation'], '--repo', repo, ok=False)
        assert result['code'] != 0
        assert read(win, 'revoked.txt') is None
        passed('revocation_blocks_existing_and_fresh_sync', denied_exit=result['code'])
    except Exception as exc:
        error = str(exc)
        raise
    finally:
        cleanup = []
        for name, node in (('windows', win), ('ubuntu', linux)):
            try:
                result = node.close(remove=error is None and not options.keep_artifacts, keep=options.keep_artifacts)
                cleanup.append(dict(os=name, **result))
            except Exception as exc: cleanup.append(dict(os=name, stopped=False, error=str(exc)))
        evidence.write_text(json.dumps(dict(metadata=metadata, checks=checks, error=error, cleanup=cleanup,
                                            qualification='Foreground fresh disposable fixtures only; no systemd install/enable, no-login boot, firewall changes, VPS, or production daemon state'), indent=2))
        if any(not entry['stopped'] for entry in cleanup): raise RuntimeError('Fixture cleanup failed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node', action='store_true')
    parser.add_argument('--distro', default='Ubuntu')
    parser.add_argument('--scheme', choices=('ws', 'wss'), default='ws')
    parser.add_argument('--keep-artifacts', action='store_true')
    for name in ('windows-binary', 'linux-binary', 'windows-helper', 'linux-helper', 'windows-root', 'linux-root', 'evidence'):
        parser.add_argument('--' + name)
    args = parser.parse_args()
    if args.node: node_loop(args.linux_root)
    else: acceptance(args)
