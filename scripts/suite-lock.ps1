# Machine-wide lock so only one full `cargo test` suite runs at a time across
# worktrees (REVAMP_PLAN §13 rule 7). Timing-sensitive daemon tests are flaky
# when several full suites share the CPU.
#
# Usage (from a worktree, with its own CARGO_TARGET_DIR):
#   powershell -NoProfile -File scripts/suite-lock.ps1 cargo test --all-targets
#
# Two named mutexes, always taken in this order:
# - "Local\tkfs-full-suite": the name C:/tkfs-build/suite-lock.ps1 uses, so the
#   two scripts exclude each other. "Local\" objects are per logon session.
# - "Global\tkfs-full-suite": machine-wide, so a suite started from another
#   logon session (SSH, a service, another user) is excluded too. (Creating a
#   mutex in "Global\" needs no privilege; SeCreateGlobalPrivilege only governs
#   file mappings.) If it cannot be opened (for example created by an elevated
#   process with a DACL this process may not use), the script warns and runs
#   with the session lock only.
# The fixed order (Local, then Global) cannot deadlock with the Local-only script.
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Command)
if (-not $Command -or $Command.Length -eq 0) {
    Write-Error "usage: suite-lock.ps1 <command> [args...]"
    exit 2
}

function Enter-Mutex([string]$Name) {
    try {
        $m = New-Object System.Threading.Mutex($false, $Name)
    } catch {
        Write-Warning "suite-lock: cannot open $Name ($($_.Exception.Message)); continuing without it"
        return $null
    }
    try { [void]$m.WaitOne() } catch [System.Threading.AbandonedMutexException] { }
    return $m
}

$waited = [System.Diagnostics.Stopwatch]::StartNew()
$held = @()
foreach ($name in @("Local\tkfs-full-suite", "Global\tkfs-full-suite")) {
    $m = Enter-Mutex $name
    if ($m) { $held += $m }
}
if ($waited.Elapsed.TotalSeconds -ge 1) {
    Write-Host ("suite-lock: acquired after {0:N0} s" -f $waited.Elapsed.TotalSeconds)
}
$code = 1
try {
    $exe = $Command[0]
    $rest = @()
    if ($Command.Length -gt 1) { $rest = $Command[1..($Command.Length - 1)] }
    & $exe @rest
    $code = $LASTEXITCODE
} finally {
    [array]::Reverse($held)
    foreach ($m in $held) { $m.ReleaseMutex() }
}
exit $code
