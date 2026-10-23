# Configuration

`tkfs --state DIRECTORY` selects one greenfield folder store. Commands use the
running daemon's token-authenticated local control endpoint when it exists.
`init --repo REPOSITORY_ID` can give two new stores the same repository identity.
Without `--repo`, initialization generates an identity.

```powershell
tkfs --state C:/tkfs/a daemon --mount X: --listen 127.0.0.1:43180
tkfs --state C:/tkfs/b daemon --mount Y: --listen 127.0.0.1:43181
tkfs --state C:/tkfs/a pairing-code
tkfs --state C:/tkfs/b pair ws://127.0.0.1:43180 CODE
```

Pairing enrolls persistent credentials on both peers. `--advertise URL` specifies
the endpoint to give the peer when it differs from the listener's address.
Peer listeners speak WS; use a VPN or terminate WSS at a TLS proxy as described
in the authority scope. WSS clients use the platform certificate trust store.
`peers` lists paired endpoints and `unpair INSTALLATION` removes a pairing.
Credentials and control tokens are not included in status responses.

On Linux, `--mount /absolute/path` uses `fusermount3` and `/dev/fuse`.
On Windows the WinFsp SDK/runtime must be installed; `WINFSP_DIR` selects a
nonstandard SDK path at build time.

`checkpoint` returns a snapshot ID; `branch NAME --snapshot ID` forks it;
`checkout NAME_OR_ID` selects a branch. `conflicts --history` includes retained
alternatives; `resolve ENTITY_ID VERSION_ID` resolves current competing heads.
`import PATH SOURCE`, `cat PATH`, `mkdir PATH`, `rename PATH TARGET`, `delete PATH`,
`flush`, `gc` and `stop` use the same local control operations as the desktop.
The desktop currently exposes folder status and the entity conflicts view.

Defaults: 64 KiB chunks, 64 MiB pack target, 256 MiB verified cache, one-second
write-back delay, 64 MiB pending budget, and 32 retained snapshot roots for GC.
State consists of `metadata.sqlite`, `objects/index.sqlite`, `objects/packs/`,
and recoverable scratch files under `scratch/`.
