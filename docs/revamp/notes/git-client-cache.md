# Git client cache isolation

The isolated guest's exact Portable Git 2.56.0.windows.3 has no `core.fscache`
setting, and no `GIT_TEST_FSCACHE` override. Its
[configuration parser](https://github.com/git-for-windows/git/blob/v2.56.0.windows.3/compat/mingw.c)
leaves that setting disabled unless configured. In this mode `mingw_lstat`
calls `GetFileAttributesExW` for each file. When enabled, the
[client cache](https://github.com/git-for-windows/git/blob/v2.56.0.windows.3/compat/win32/fscache.c)
uses directory listings to answer those attribute queries; the
[index preloader](https://github.com/git-for-windows/git/blob/v2.56.0.windows.3/preload-index.c)
still processes the tracked entries.

The diagnostic changes only this client option. All runs disable fsmonitor
and untracked-cache acceleration, use `--no-optional-locks`, and keep the same
read-only, mutation-rejecting binary `2facfa4e...`, 16 dispatcher workers,
purge, access checks and cache settings. The native/mounted 50,000-file trees
have the same Git tree id `f212d710d6a082c6786ff6762a389910ef133fee`.
The native index receives an ordinary untimed warmup to adopt native file
timestamps before the comparison. Each configuration then gets one warmup
and three measured samples.

| Client configuration | Native median | Mounted median | Ratio |
|---|---:|---:|---:|
| Default, first control | 725.5 ms | 7,484.1 ms | 10.32x |
| `core.fscache=true` | 186.0 ms | 380.6 ms | 2.05x |
| `core.fscache=false` | 729.9 ms | 7,315.5 ms | 10.02x |
| Default, repeated control | 729.7 ms | 7,662.9 ms | 10.50x |

Git trace2 still reports 50,000 index `lstat` operations with the cache enabled.
In the first measured mounted sample, actual Open callbacks fall from 51,090
to 610 and security-by-name callbacks from 51,124 to 630. The separate,
untimed client-cache trace reports 50,521 `lstat` requests and 526 cache misses.
This distinguishes logical file checks from requests crossing the driver
boundary. Directory-entry callbacks remain near 52,000. Their aggregate
callback time and GetFileInfo time are concurrent/inclusive observations and
must not be subtracted from elapsed wall time.

All status outputs are clean, client writes are rejected, and the daemon
stops normally in 0.387 s without a kill or stranded process. The namespace
head is unchanged. The 50,057-member export passes CRC, SHA-256 and timestamp
size checks. The [retained diagnostic](../../../test-evidence/wave-6/git-fscache-isolation-sandbox-20261024.json)
includes counters per repetition, Git trace2, client-cache traces, configuration
and controller sources. No host compilation overlaps timed phases. An earlier
controller preparation error occurred before any server/volume start; that
[failed attempt](../../../test-evidence/wave-6/git-fscache-setup-failure-20261024.json)
is retained separately.

The original acceptance command and its 1.3x target remain unchanged. Even
the optional cached-client configuration misses its matched native target.
This is not a replacement acceptance profile or a fix for the other workloads;
it establishes the cost of individual attribute-open roundtrips and the
remaining directory-listing path. P0 stays open.
