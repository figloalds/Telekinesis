# One-port WSS published-data transport

This portable transport uses one inbound TCP port for enrollment and published
sync, regardless of repository count. Homes can initiate outbound connections
without binding any TCP listener. Windows workers use local named pipes; Linux
workers use local Unix sockets. The transport process remains separate from O1;
Windows O1 can own workers, but does not yet supervise this transport process.
Linux O1, relay-only VPS operation and several homes sharing the same repository
remain separate work. One active paired peer and two replica origins per repo
remain enforced. Different repositories can share the installation's listener.

## Configuration

Use the normal distinct installation IDs and DPAPI/systemd credentials described
in [PAIRING.md](PAIRING.md). Never copy a live key or worker state to create a new
replica. Select private storage with safe ancestors: foreign write/delete/security
grants on any ancestor are rejected even if the final directory is private.

VPS-style listener configuration (numeric loopback example, no deployment implied):

```toml
format_version = 1
installation = "11111111-1111-4111-8111-111111111111" # replace with new-id
transport = "wss"
inbound = true
dial = false
data_directory = "pairing-data"
listen = "127.0.0.1:43180"
advertise = "wss://127.0.0.1:43180/tkfs/sync"
enrollment_advertise = "wss://127.0.0.1:43180/tkfs/enroll"
[credential]
backend = "systemd"
name = "tkfs.identity"
```

Home configuration:

```toml
format_version = 1
installation = "22222222-2222-4222-8222-222222222222" # replace with new-id
transport = "wss"
inbound = false
dial = true
data_directory = "pairing-data"
advertise = "outbound-only"
[credential]
backend = "windows-dpapi"
file = "pairing-data/identity.dpapi"
```

Credential backend follows the OS, independently of listener/dial role. For WSS,
omit `enrollment_listen`: both paths share `listen`. An outbound-only config must
omit `listen` and `enrollment_advertise`. This first slice requires an explicit
numeric IP and port in WSS URLs, including bracketed IPv6; DNS/redirect/proxy
discovery is not implemented. The synthetic certificate name is the installation
UUID, verified with Rustls and the exact approved certificate. These are native
TKFS client connections, not browser/public-PKI web endpoints.

### Explicit loopback `ws`

For local native clients, keep `transport = "wss"` and explicitly use matching
`ws://127.0.0.1:PORT/tkfs/sync` and `ws://127.0.0.1:PORT/tkfs/enroll` URLs.
The listener must bind a numeric loopback address. `::1` and IPv4-mapped IPv6
loopback are accepted; unspecified, private/gateway and other non-loopback
addresses are rejected. DNS names (including `localhost`), implicit ports,
userinfo and URL query parameters are rejected. No DNS resolution, redirect or
automatic downgrade occurs. The client verifies its connected local and peer
addresses; the server checks the actual bound address and both accepted socket
addresses. WSL gateway/private IPs are not loopback.

This mode is an RFC 6455 binary carrier for the existing Rustls TLS 1.3 session:
HTTP `GET /tkfs/tunnel` with subprotocol `tkfs-loopback-tls-v1`, then TLS records
inside WS binary messages, then the existing authenticated `tkfs-wss-v1` exchange
inside TLS. An HTTP upgrade grants no identity or data access. Exact server-key
verification, proof of possession of the approved client key, anonymous enrollment
only, invitation approval, per-operation grants and revocation remain identical
to WSS. UUID/certificate claims and forwarded headers confer no access. Keys or
invitation secrets never enter a URL. There is no plaintext data/authentication
fallback. This preserves the established authentication instead of adding a new
challenge/signature protocol; it adds framing overhead and retains TLS cost.

Outer WS messages/frames are bounded at 16 KiB, with at most 32 control messages
per phase and the existing absolute byte/time/cancellation budgets. The initial
WS upgrade, TLS handshake and inner upgrade share one handshake deadline.
Non-loopback WebSocket endpoints require `wss`.

See [foreground Windows–Linux acceptance](PAIRING-CROSS-OS.md) for a reproducible
WSL test with mounted workers, fresh test credentials and no systemd activation.

The layering uses [RFC 6455 binary frames](https://www.rfc-editor.org/rfc/rfc6455)
and Rustls's existing [Read/Write stream interface](https://docs.rs/rustls/0.23.45/rustls/struct.StreamOwned.html).

`dial` controls automatic outgoing sync only. It does not change repository
authorization: VPS grants remain enabled with `dial=false`. Explicit local
`sync`/`remote-list` commands may still dial. Peers advertising `outbound-only`
are never automatically dialed by the WSS scheduler.

## Commands and restart behavior

Start initialized local workers independently, then start `tkfs pairing run` on
both configurations (use `-f CONFIG` or the existing cwd `pairing.toml` default).
The listener owner issues `tkfs pairing invite`; the home runs `tkfs pairing join`
and supplies the invitation on masked/piped stdin. The listener owner checks the
public installation/fingerprint and runs the existing `approve` command. Both
owners use the existing `grant --repo --runtime --remote-replica` commands. Approval
alone grants no repository access. An outbound-only installation cannot issue an
invitation because it has no enrollment listener.

The home sends its page and receives the listener's page over the same outbound
connection. Both upload and download work with the listener's `dial=false`.
Persisted grants, inventories and partial object offsets survive worker/service
restarts. Idle or broken connections are discarded, then exponential retry with
jitter reconnects; responses must match both the WSS operation ID and core sync
request binding. Objects and events are acknowledged only after verification
and durable activation. Neither Pong nor successful WebSocket delivery is a
durable acknowledgment.

## Authentication and limits

TKFS terminates TLS 1.3 directly. The shared listener offers optional verified
client certificates: enrollment clients omit their certificate, while sync
upgrades require an actual TLS certificate from the approved registry. Fresh
registry authorization occurs before accepting a payload and again during every
data operation, including on reused connections. Missing/unapproved/revoked
certificates cannot reach sync. Anonymous TLS has access solely to enrollment;
invitations remain expiring, single-use and subject to local approval. HTTP
forwarded-certificate/identity headers confer no access. There is no implicit
reverse-proxy TLS termination trust, session resumption or 0-RTT.

Wire subprotocol is `tkfs-wss-v1`, with binary JSON messages and bounded two-phase
begin/ready/payload/response exchanges. `/tkfs/sync/control` permits published
listing; `/tkfs/sync/bulk` permits exchange only. Neither route reaches the local
management dispatcher. Unknown operations, worker paths, private catalog/object
requests and wrong repository/replica bindings fail closed.

Per authenticated device, the transport allows one control and two bulk
connections in total across incoming and outgoing sessions. Limits aggregate
across that device's connections and operation starts, with up to
eight active devices and eight concurrent background jobs. No unbounded request
queue exists: pool exhaustion reports backpressure and background work retries.
Per-device start rates are eight control and four bulk operations per second;
reopening a connection does not reset that window.
Recently closed device admissions remain briefly reserved to retain their rate
windows; a ninth newly active device can retry after the reservation expires.
Enrollment has two active slots; up to eight TLS/HTTP handshakes may be pending.
The shared pre-auth
handshake capacity is bounded but does not promise availability under a sustained
unauthenticated handshake flood.

Messages/pages are limited to 8 MiB; begin/control requests to 8 KiB, control
responses to 256 KiB, and enrollment to 64 KiB. Published offers use smaller
metadata/object budgets and existing resumable parts, rather than expanding
message limits for large files. Oversized inventories/metadata fail explicitly;
arbitrarily large catalogs are not qualified. Read buffers are 8 KiB and write
buffers are bounded. Each IO phase also has a byte ceiling and at most 32
Ping/Pong controls, preventing control traffic from evading operation budgets.

TLS and HTTP upgrade share an absolute five-second budget; enrollment has ten
seconds, and begin-approved payload/response has 45 seconds. Byte/fragment/Ping
progress never extends those deadlines. Server idle waits expire after 60
seconds. The client retains idle pool entries for at most 30 seconds, probes a
reused entry idle at least ten seconds with a three-second Ping/Pong heartbeat,
and rotates entries after ten minutes. No periodic connection is maintained
solely to keep an unused slot alive. Network cancellation is polled every 100 ms,
and shutdown drain is bounded at 35 seconds. Local worker RPC/storage scheduling
retain their separate limits; these are not hard realtime guarantees.

Each service tick prunes idle connections independently of outgoing requests.
Idle/rotation expiry, certificate or endpoint changes, revocation, outbound-only
advertisements, disabled dialing and loss of enabled outgoing grants release the
shared quota guards. A lease completing after a policy change is discarded rather
than recached. Changes take effect on the next completed service iteration
(normally about 50 ms); config-file changes still require the existing restart
workflow. Active already-authorized operations retain their existing deadlines.

Reserved lanes, per-device quotas and earliest-eligible job scheduling prevent
bulk payload reception from taking the control connection slot. Worker locks
and SQLite authorization transactions can still delay control operations;
this slice does not promise strict latency isolation during storage work.

The existing raw TLS mode remains the default for older configurations, with its
two listeners and unchanged wire schema. WSS endpoints choose WSS explicitly;
there is no automatic fallback to raw/unauthenticated transport. New WSS
configuration fields/endpoints require the new binary on participating devices.

Implementation uses pinned Tungstenite 0.28.0 (`handshake` only), over the existing
Rustls 0.23.45/ring stack. Its archive declares Rust 1.63; the locked dependency
set was qualified with Rust 1.88 on Ubuntu and Rust 1.96 on Windows. These are
recorded test versions; Cargo.toml and Cargo.lock define the build dependencies.
The callback
also validates that Sec-WebSocket-Key decodes to exactly 16 bytes. SHA-1 is used
only for the standard WebSocket upgrade, never TLS or peer authentication.

No VPS access, public listener, firewall/Tailscale change, production credential,
service installation/enablement or no-login boot qualification was performed.
