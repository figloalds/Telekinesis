# Documentation

The [repository README](../README.md) describes the current implementation and
build/run commands. These guides add platform and operational detail.

| Guide | Contents |
|---|---|
| [Linux headless/FUSE](LINUX.md) | Build, owner control, POSIX metadata, mount contract and qualification |
| [Persistent pairing](PAIRING.md) | Credentials, enrollment/approval, published repository grants, restart and revocation |
| [One-port WSS](PAIRING-WSS.md) | Listener/outbound-only configurations, connection pools, deadlines and topology limits |
| [Orchestrator CLI](ORCHESTRATOR-CLI.md) | Windows local management commands and retry semantics |
| [Validation notes](VALIDATION.md) | Historical implementation checks, physical-machine findings and acceptance gaps |

Design documents contain proposed work as well as implemented slices; use the
code, repository tests and dated [validation notes](VALIDATION.md) to establish
status. `test-evidence/` contains optional ignored local reports and is absent
from a fresh clone. Paths in this documentation are relative to the repository
root unless stated otherwise; example absolute paths are operator-selected
placeholders or documented platform defaults. Commit lasting findings here and
reusable acceptance harnesses under `scripts/`, keeping generated output local.

| Design/history | Contents |
|---|---|
| [TKFS plan](TKFS-PLAN.md) | Causal filesystem contract and remaining scope |
| [Orchestrator plan](ORCHESTRATOR-PLAN.md) | Local O1/desktop slices and later distribution design |
| [Pairing assessment](PAIRING-ASSESSMENT.md) | Historical starting-point assessment and architecture choices |
| [Pairing implementation contract](PAIRING-IMPLEMENTATION.md) | Approved scope and implemented security/transport follow-ups |
| [Legacy TKFS plan](TKFS-PLAN-2026-10-02-LEGACY.md) | Archived earlier design; not a description of current implementation |

Run guide commands from the repository root unless a guide specifies otherwise.
License texts and third-party notices remain at the repository root.
