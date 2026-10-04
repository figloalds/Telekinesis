# Directory-local orchestrator commands

In the directory containing `orchestrator.toml`, use:

```powershell
tkfs orchestrator
tkfs manage hello
tkfs manage list
```

`orchestrator` and `manage` default `--defaults-file` to `./orchestrator.toml` in
the shell's current working directory. Relative data/mount paths inside that file
remain relative to its own directory. There is no parent-directory search,
global fallback, daemon auto-selection or automatic configuration creation.
Missing/invalid files fail before a daemon is contacted or launched.

An explicit path wins; `-f` is the short alias:

```powershell
tkfs orchestrator -f "other directory/orchestrator.toml"
tkfs manage --defaults-file "other directory/orchestrator.toml" list
```

Existing commands with explicit `--defaults-file` keep working. Local mutations
still need their expected `--generation` and support exact `--request-id` retry.
The GUI keeps resolving its configuration beside the executable; this convenience
does not change GUI startup, shell profiles or running daemon configuration.
The O1 supervisor remains Windows-only until its separate Linux portability work.
