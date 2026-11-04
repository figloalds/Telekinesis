# Two-way content over one pairing

Status: draft for the owner's approval, 2026-11-04.

Implements the prerequisite in section 7 of
[the desktop orchestrator client design](2026-10-29-desktop-orchestrator-client-design.md).
The Android client and Send to host come after this and have their own designs.

## Why

Today file contents move only from listener to dialer: the dialer calls
`objects.fetch` and nothing lets it put objects on the listener. Metadata already
moves both ways through `meta.exchange`. So after a one-way pairing, the listener
sees a file the dialer created but can never read its bytes.

This breaks ordinary two-way editing between PCs and blocks Send to host. It also
blocks the Android client, because a phone can only dial and the PC cannot dial it
back. The phone's use as a transfer buffer needs its files to reach the PC.

The owner's decisions:

- **2026-10-29:** content flows both ways over one pairing, on the connection the
  dialer opened. This uses WebSocket as full duplex and "half NAT piercing". Two-way
  pairing and dial-back were rejected.
- **2026-11-04:** the protocol supports two sync profiles. PCs synchronize as active
  peers, with instant notify plus a slow backup poll. A phone opts into a passive,
  client/server-looking strategy where it alone sets the pace.
- **2026-11-04:** "client/server" describes only who sets the pace. Neither side has
  authority over the other. Conflicts stay as they are today: both versions are kept
  for the user to resolve, and the PC never wins automatically.

## Goals

1. After one pairing, an edit on either side reaches the other with its bytes.
2. Active peers react to a change at once on both sides, without depending on a
   short poll, and a backup poll covers anything a notify missed.
3. A reader blocked on a file on the listener gets it ahead of background work,
   when the dialer is reachable.
4. A passive dialer is battery-friendly wherever it can be. It makes no sync requests
   while idle, and sends and receives only when asked. It downloads metadata and the
   files being read, not everything. The owner's framing: passive is a direction, not
   an absolute, so connection heartbeats while the app is awake are accepted.
5. A successful store on the listener means the bytes are durable there, so a later
   Android design can tell which local bytes are safe to free.
6. Every conflicting version's bytes move on their own, so a version stays readable
   and resolvable on a side after the device that wrote it has gone away.

## Non-goals

- The Android app, its scheduling, and freeing local storage.
- Send to host (`folder.offer`, `tkfs manage send`).
- Making `Conn` requests two-way, or giving the listener its own scheduler.
  Revamp decision 4 stands: only dialers originate requests. Nothing here prevents
  adding this later if it is ever needed.
- A relay or other NAT traversal. The dialer must still be able to reach the listener.
- Any change to conflict handling.

## Anti-goals

These would make it a failure even if it technically worked:

- A pushed object is accepted without checking its digest, or reported durable before
  it is.
- A paired peer can fill the listener's disk with objects it never asked for.
- A notify becomes something correctness depends on. It is only a hint.
- A passive dialer is woken, polled or made to transfer by the other side.
- Background traffic in either direction delays a reader who is waiting.

## Constraints

- Follow `AGENTS.md`: this work runs headless. No host filesystem mounts, and W6-P0
  stays open. Performance gates come last, so targets here are test bounds, not
  qualification results.
- All test waits are bounded, and a stranded process is a failure.
- Existing behaviours stay: authenticated sessions, enrolled credentials,
  `require_paired`, `REPO_NOT_PUBLISHED`, per-lane op policy, and no replay of an
  uncertain request (invariant 16).
- Pairing codes stay 64 hex.

## Design

### The shape

The dialer drives everything, as it does today. Three additions:

1. **Push:** two peer operations that let the dialer ask what the listener lacks and
   store objects there.
2. **Notify:** a one-way hint from listener to dialer on the existing control lane.
3. **A per-peer profile:** the dialer declares `active` or `passive` when it connects.

### Capability negotiation

`ClientHello` and `ServerHello` already carry an unused `features` list. Use it, with
no change to `PROTOCOL_VERSION`:

- `content-push`: the listener serves the push operations.
- `notify`: the listener may send notify frames, and the dialer accepts them.
- `profile-passive`: this dialer is passive. Its absence means active.

A dialer uses push only against a listener advertising `content-push`. A listener
sends notify only on a session where the dialer asked for `notify`. An older peer
keeps today's behaviour, including pull-only content and the short poll, and `manage
peers` shows that two-way content is unavailable with that peer.

### Push operations

Both are `objects.*` on bulk lanes, so `op_allowed` already places them correctly.
`PeerService` serves them, with `require_paired` and the same folder lookup as
`objects.fetch`, so `REPO_NOT_PUBLISHED` and pause behave identically.

- **`objects.wanted`** `{repo, limit, exclude, after}` returns
  `{demand, missing, cursor}`. `demand` is what the listener's readers are blocked on
  now. `missing` is a page of the worker's `objects.missing` scan, extended as described
  below.
- **`objects.store`** `{repo}` with an object batch as its body returns
  `{stored: [digest], refused: [{digest, code}]}`. `PeerService` checks each object
  and passes the accepted ones to the worker's `objects.write`, which already enforces
  `OBJECT_BATCH` and `MAX_BATCH_BYTES`, verifies digests, and flushes with fsync before
  replying. The reply names digests, not a count, so goal 5 can say exactly which bytes
  are durable. A digest in `stored` is durable on the listener.

**Admission.** The listener accepts an object only if its digest is one it listed to
that peer for that repo in an `objects.wanted` reply, or is currently in demand. A
digest that does not match its bytes is refused with `INVALID_PEER_OBJECT`, and an
unlisted one with `OBJECT_NOT_WANTED`. The set of listed digests is kept per
(installation, repo), not per session, because a digest can be listed on one bulk lane
and stored on the lane that owns it. It is bounded and held in memory, and a digest
evicted from it just gets listed again. This is the disk-fill protection. A refusal
covers only that object, and the session stays usable.

**Conflict versions.** The `objects.missing` scan lists only the visible head of each
entry today, so another conflicting version's bytes move only when something demands
them. Extend the scan and its resumable cursor to cover every current conflict head.
This serves both directions, since pull uses the same scan. GC already keeps conflict
alternatives, so these bytes are not collected. A passive dialer still does no
background pull, but its bursts push its own versions.

Requests that fail uncertainly are not replayed. The digest stays missing, so it gets
listed again, and storing a digest twice is harmless because storage is
content-addressed.

### The push pass in the object pump

The pump gets a fixed order per round, for each peer and folder:

1. Pull what this side's readers are blocked on (today's pass 1).
2. Push what the listener's readers are blocked on.
3. Pull missing objects in the background, if the profile allows it.
4. Push missing objects in the background, if the profile allows it.

Steps 3 and 4 alternate fairly and stop before their next call when demand arrives,
as today's pass 2 does.

A push reads objects with the link's existing `fetch_reply`, then calls
`objects.store` on the bulk lane that owns the digest. It reuses the pull path's rules:
lane sharding by `owns`, deadlines that grow with batch size, batch halving on timeout,
growth when answered, and per-digest backoff for what this side does not hold. A dialer
holding only part of what the listener wants sends just that part. Digests it lacks are
not an error.

### Notify

Add `FrameKind::Notify`, sent only by the server on a control session that negotiated
`notify`. The server already sends frames of its own (`Heartbeat`, `Goaway`), so this
adds no request and changes nothing about who originates requests.

The header is `{repo, reason}`, where reason is `metadata` or `demand`. Frames are a
coalesced hint per repo with a small bound, and dropping one is safe. Notify carries no
digests and no content.

A reader waits only 10 s (`Demand::FETCH_WAIT`), which is shorter than the backup poll.
So while demand is pending on a repo, the listener sends the `demand` notify again every
2 s. A dropped one then costs a short delay, not a failed read, without adding any
polling while nothing is waiting. A passive dialer does not subscribe, so a reader on
the listener still fails with `OBJECT_MISSING` until that dialer's next burst.

On the listener, the folder's existing watch detects new local events or new demand and
sends notify to every subscribed session of a peer syncing that repo. A dialer that
receives one wakes the exchange for that (peer, repo), or the object pump. The dialer
also wakes the exchange at once on its own local changes, using the same watch, so both
directions are immediate.

### Profiles

The dialer declares its profile, because only it knows its battery and data situation.

| | `active` (default) | `passive` |
|---|---|---|
| Notify | Subscribed | Not subscribed |
| Metadata exchange | On local change, on notify, and on a backup poll | Only when asked |
| Foreground pull | Yes | Yes, while running |
| Background pull | Yes | No |
| Push | Yes | Only during an asked-for burst, plus demand it learns of then |

**Configuration.** Add `network.sync_profile = "active" | "passive"`, default `active`.
My call: this is per orchestrator rather than per peer, because the phone case is a
property of the device. It can be split later without changing the protocol.

**Backup poll.** Add `network.backup_poll_ms`, default 30,000, 1,000 to 3,600,000. It
applies to a control session with `notify`. Without notify, the session keeps using
`network.sync_interval_ms`, so an older peer behaves as it does today.

**The burst.** Add a `sync-now` action to `Daemon::execute` and `tkfs manage sync-now
[FOLDER]`. It works in either profile. It:

- clears peer backoff so lanes redial at once, which addresses the spike's slow
  reconnection after the PC was away;
- runs metadata exchanges until neither side has more;
- pushes what the listener wants, demand first;
- returns `{folders: [{id, exchanged, pushed, remaining, error}]}` and a bound on its
  duration.

**Connections.** A passive orchestrator keeps its lanes open while its process is
running and makes no sync requests on its own. It only serves local readers and does
what it was asked to do. Heartbeats continue as on any open lane, which is accepted
because Android freezes the app soon after it goes to the background, and that stops
them too. Where a choice exists, take the battery-friendly one, such as longer redial
backoff while a passive peer is unreachable, since `sync-now` clears it. Closing lanes
between bursts is not part of this project. It can be added later if real battery
measurements from a phone call for it.

### Status

- `manage peers` shows the negotiated features and profile for each session.
- The sync view gains push counters and the last push error, next to the existing
  ones.
- New error codes go in `model::errors` and the README table: `OBJECT_NOT_WANTED`.

### Documentation

Update `docs/ARCHITECTURE.md`, `docs/CONFIGURATION.md`, `docs/CLI.md` and the README.
Add a note to revamp decision 4 that only dialers still originate requests, and that
content now moves both ways through dialer-driven push. The `tests/docs_cli.rs` checks
cover the new command.

## Acceptance

Headless tests in `tests/peer_sync.rs`, built on its `shared_pair` harness, plus unit
tests where they fit. All waits bounded. A sets long poll intervals where a test must
prove notify, rather than poll, delivered the change.

1. **The gap is closed.** A dials B, one pairing. A file imported on A becomes readable
   on B with identical bytes, and the reverse still works.
2. **Two-way editing.** Edits on both sides converge. Concurrent edits to one file
   appear as a conflict with both versions, as today. Once background transfer settles,
   stop the side that wrote the other version: both versions stay readable on the
   remaining side, and resolving to either one leaves the file readable.
3. **Notify delivers.** With a 60 s backup poll, a change on either side reaches the
   other well inside that interval.
4. **Lost notifies are covered.** With notify suppressed by a fault injection, the
   backup poll still delivers a change. With only the first `demand` notify dropped, a
   blocked reader still gets its file within `FETCH_WAIT`.
5. **Demand goes first.** While a large background transfer runs in either direction, a
   reader blocked on B gets its small file from A within a bound.
6. **Dialer away.** B's read of a file only A holds fails cleanly with today's bounded
   `OBJECT_MISSING`, then succeeds after A returns.
7. **Interrupted push.** Killing either side mid-push leaves no corrupt state, and the
   transfer completes after restart.
8. **Admission.** A digest mismatch, an unwanted digest, an unpaired peer and a repo
   that is not published are each refused with their own code, and the session stays
   usable where the code says it should.
9. **Passive idle.** Over a bounded idle window, a passive A makes no sync requests and
   is sent no notify. Heartbeats do not count. Measure by request counters, or by a lane
   byte counter bound that leaves room for heartbeats alone.
10. **Passive burst.** `sync-now` on a passive A exchanges metadata and pushes its new
    file so B can read it, and does not download A's unread files. Reading one on A
    still fetches it.
11. **Older peer.** A peer without these features still syncs pull-only, and `manage
    peers` says so. Simulate this with a feature override in tests.
12. **Existing suites still pass.** This includes `tests/orchestrator.rs` and
    `tests/session_core.rs`, with the frame-kind tests updated for the new kind.

## Left to the builder

- The order of work and how it splits into commits.
- The exact bound on the per-session listed-digest set and on queued notifies.
- Whether mutual pairs skip the push pass when a peer also dials back. Transferring an
  object twice is wasteful but harmless, so this is an optimization.
- How `remaining` in `sync-now` is computed, as long as it is honest about being an
  estimate if it is one.
- The fault-injection mechanics for the tests.

## Calls I made that the owner should check

- Notify is a new frame kind instead of a parked long-poll request. A long poll would
  hold a handler slot per session and needs no frame change, but a frame costs less and
  fits "hint, may be dropped".
- `sync_profile` is per orchestrator, not per peer.
- The backup poll defaults to 30 s, and the existing 500 ms interval remains only for
  peers without notify.
- Feature flags, not a protocol version bump, gate all of this.
- `sync-now` is added now, not with Android, because goal 4 cannot be tested without it
  and it also fixes the reconnection delay.
- Push admission is tracked per (installation, repo), because digests are listed and
  stored on different lanes.
- `objects.store` reports per digest, with a code for each refusal, rather than failing
  a whole batch.
- While a reader waits, the listener repeats the `demand` notify every 2 s, so a lost
  one cannot fail a read that the dialer could have served.
