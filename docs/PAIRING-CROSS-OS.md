# Foreground Windows–Linux pairing acceptance

`scripts/pairing_cross_os_e2e.py` exercises fresh Windows WinFsp and Ubuntu FUSE
workers through the public pairing CLI and actual mounted file operations. The
Windows installation is outbound-only; Ubuntu binds `127.0.0.1` with `dial=false`.
The Windows-initiated exchanges carry both upload and download. The Linux control
helper runs through a WSL stdin/stdout pipe; it opens no extra network listener.

The default endpoint is explicit loopback `ws`, with the certificate-authenticated
TLS carrier described in [PAIRING-WSS.md](PAIRING-WSS.md#explicit-loopback-ws).
This is a native TKFS protocol. The plain HTTP upgrade is not authenticated and
does not expose pairing secrets or data. Non-loopback `ws` is rejected; the
harness never substitutes a WSL gateway/private IP or changes firewall settings.
If localhost forwarding does not work on another machine, report that limitation
and qualify cross-OS WSS separately, keeping local WS boundary tests.

## Build and invoke

Prerequisites are installed WinFsp on Windows, Ubuntu with Rust/Cargo and a C
compiler, `/dev/fuse` access and `fusermount3`, and Python on both OSes. Build the
same reviewed source on each OS; use a native Linux filesystem for Linux builds
and state. No systemd installation or activation is required by this harness.

```powershell
# Windows, from the repository root:
cargo build --locked --bin tkfs --example pairing_fixture
# Ubuntu, from a native Linux checkout of the same source:
wsl.exe -d Ubuntu --exec bash --noprofile --norc -c 'cd /native/checkout; cargo build --locked --bin tkfs --example pairing_fixture'

# Choose NEW fixture paths; the helper refuses an existing fixture root.
# Windows parent must have safe owner-controlled storage ancestors.
python scripts/pairing_cross_os_e2e.py `
  --windows-binary C:\checkout\target\debug\tkfs.exe `
  --windows-helper C:\checkout\target\debug\examples\pairing_fixture.exe `
  --linux-binary /native/checkout/target/debug/tkfs `
  --linux-helper /native/checkout/target/debug/examples/pairing_fixture `
  --windows-root C:\private-tests\.tkfs-pairing-test-new-run `
  --linux-root /home/your-user/.tkfs-pairing-test-new-run `
  --evidence test-evidence/loopback-ws/CHECKS.json
```

Replace the paths with the actual checkouts, binaries and private test parent.
`--distro` selects an explicitly installed WSL distro; `--scheme wss` selects TLS
before HTTP for a separate WSS run. Neither changes distro defaults or networking.

The example `pairing_fixture` is a test utility, not a production credential
provider. It creates a fresh identity, replica, config and state in a NEW directory
whose name begins `.tkfs-pairing-test-`. Windows uses CurrentUser DPAPI. Linux uses
a newly generated test-only mode-0600 credential under an owner-private directory,
delivered to the unchanged credential reader through `CREDENTIALS_DIRECTORY`.
It never prints key bytes. The product CLI retains its encrypted systemd credential
provisioning and has no new plaintext production fallback. Do not reuse this
fixture delivery as a production deployment procedure.

## Command and operation flow

The harness performs the following public command sequence, using the fresh IDs
and paths returned by the fixture utility. Invitation output remains in memory
and is piped to `join` stdin, never placed in argv, a URL or a log.

1. On both OSes, `tkfs daemon --state STATE --mount MOUNT`; wait for local status.
2. On both OSes, `tkfs pairing -f CONFIG run` as owned foreground processes.
3. Ubuntu `pairing invite`; Windows `pairing join`; verify data access is refused
   before approval. Ubuntu `pairing approve INVITATION_ID --installation WINDOWS_ID
   --fingerprint WINDOWS_FINGERPRINT`; verify approval alone exposes no repos.
4. Each OS `pairing grant OTHER_ID --repo REPO --runtime RUNTIME --remote-replica
   OTHER_REPLICA`, binding its own local worker to the other replica.
5. Write/fsync files through both mounts and compare hashes; transfer a 12 MiB
   file; set Linux mode 0755 and verify that a Windows content edit preserves it;
   propagate a Windows rename and Linux delete.
6. Create/check out a Linux private branch, write private data, verify neither
   the Windows mount nor its local object lookup can access that content, then
   return to the shared branch.
7. Kill/restart the owned listener process while Windows has a queued mounted
   write. Verify stale pooled sessions reconnect. Restart both workers and
   services; verify durable grants/credentials and new reverse-direction sync.
8. Ubuntu `pairing revoke WINDOWS_ID`; a fresh explicit Windows `pairing sync`
   fails and the existing scheduler does not deliver newly revoked data.

Actual CLI config rejection checks cover private/gateway, unspecified, DNS,
non-loopback mapped IPv6 and a non-loopback bind with loopback advertisements
on both OSes. These commands fail during config validation, before binding or
connecting. Unit tests additionally cover `::1`, mapped loopback, numeric/DNS
pitfalls, forged keys with an approved UUID, server-key mismatch, plaintext
identity claims, live revocation, quotas and absolute trickle deadlines.

All child processes are owned, stopped and waited. Credentials are removed even
after failure. Successful bulk fixtures are removed after checking for remaining
mounts/reparse points and verifying deletion stays inside the fixture parent.
`--keep-artifacts` retains stopped fixtures for inspection, still removing keys;
failed stopped fixtures carry the existing retention marker. JSON summaries live
outside the fixture directory under `test-evidence`; build logs belong in
`test-runs`. These directories are ignored by Git.

## Qualification limits

The observed foreground run verifies program behavior, not systemd unit ordering,
encrypted credential delivery at boot, unattended/no-login startup or reboot
recovery. No installed service, production identity, existing user daemon state,
VPS, public listener or firewall/network setting is used or changed. Localhost
forwarding is an observed property of the tested machine, not a promise for every
WSL installation. The existing two-origin/one-active-peer-per-repository scope
and O1 transport-supervision limitations remain unchanged.
