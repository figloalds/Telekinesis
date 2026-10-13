# Machine-wide lock so only one full `cargo test` suite runs at a time across
# worktrees (REVAMP_PLAN §13 rule 7). Timing-sensitive daemon tests are flaky
# when several full suites share the CPU.
#
# Usage (from a worktree, with its own CARGO_TARGET_DIR):
#   powershell -NoProfile -File scripts/suite-lock.ps1 cargo test --all-targets
#
# The mutex name matches C:/tkfs-build/suite-lock.ps1, so both scripts exclude
# each other. "Local\" is shared by every process of the interactive session
# (all agents and terminals); "Global\" would need SeCreateGlobalPrivilege.
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Command)
if (-not $Command -or $Command.Length -eq 0) {
    Write-Error "usage: suite-lock.ps1 <command> [args...]"
    exit 2
}
$mutex = New-Object System.Threading.Mutex($false, "Local\tkfs-full-suite")
$waited = [System.Diagnostics.Stopwatch]::StartNew()
try { [void]$mutex.WaitOne() } catch [System.Threading.AbandonedMutexException] { }
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
} finally { $mutex.ReleaseMutex() }
exit $code
