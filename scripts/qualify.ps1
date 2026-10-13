<#
.SYNOPSIS
  Release qualification runner (REVAMP_PLAN §13 rule 7, §14 WP0.5 task 10, §15).

.DESCRIPTION
  Builds the release test binaries, runs the ignored qualification scenarios
  (tests/qual_*.rs) one file at a time with `--ignored --test-threads=1`,
  collects their `QUAL {json}` records (through TKFS_QUAL_OUT), compares the
  metrics with tests/qualification-floors.json and writes evidence JSON with
  the hardware it ran on.

  -Wave N    run the qual files owned by waves <= N; enforce floors tagged <= N.
  -All       every qual file and every floor (same as -Wave 4).
  -Baseline  every qual file; floors are evaluated and recorded, never enforced
             (exit 0); writes test-evidence/qualification-baseline.json.

  Exit code: 0 when every enforced floor holds and every selected scenario
  passed (or skipped); 1 otherwise. -Baseline always exits 0 unless the build fails.

.EXAMPLE
  $env:CARGO_TARGET_DIR = "C:\tkfs-target\wp1-6"
  powershell -NoProfile -File scripts\qualify.ps1 -Wave 1 -Files qual_wp1_6_activation

.EXAMPLE
  powershell -NoProfile -File scripts\qualify.ps1 -Baseline -BudgetS 60
#>
param(
    [int]$Wave = 0,
    [switch]$All,
    [switch]$Baseline,
    [string[]]$Files,
    [string]$Filter = "",
    [int]$BudgetS = 120,
    [int]$FileTimeoutMin = 90,
    [string]$Evidence = "",
    [switch]$NoBuild,
    [ValidateSet("Idle", "BelowNormal", "Normal")][string]$Priority = "BelowNormal"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Owner wave of each qualification file (REVAMP_PLAN §14 WP0.5).
$qualFiles = [ordered]@{
    "qual_wp1_4_worker"     = 1
    "qual_wp1_5_mount"      = 1
    "qual_wp1_6_activation" = 1
    "qual_wp1_7_derive"     = 1
    "qual_wp1_8_objects"    = 1
    "qual_wp1_9_session"    = 1
    "qual_wp1_12_legacy"    = 1
    "qual_wp2_1_lifecycle"  = 2
    "qual_wp2_3_worker"     = 2
    "qual_wp2_4_offlock"    = 2
    "qual_wp2_5_feed"       = 2
    "qual_wp3_1_transport"  = 3
    "qual_wp3_2_objects"    = 3
    "qual_wp3_int_e2e"      = 3
    "qual_scale"            = 4
}

if ($All -or $Baseline) { $Wave = 4 }
if ($Wave -lt 1 -or $Wave -gt 4) { throw "use -Wave 1..4, -All or -Baseline" }
$selected = @($qualFiles.Keys | Where-Object { $qualFiles[$_] -le $Wave })
if ($Files) { $selected = @($selected | Where-Object { $Files -contains $_ }) }
if ($selected.Count -eq 0) { throw "no qualification files selected" }

if ($Baseline) { $kind = "baseline" } elseif ($All) { $kind = "all" } else { $kind = "wave-$Wave" }
if (-not $Evidence) { $Evidence = Join-Path $root "test-evidence/qualification-$kind.json" }
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $root "test-runs/qualify-$kind-$stamp"
New-Item -ItemType Directory -Force $runDir | Out-Null
New-Item -ItemType Directory -Force (Split-Path -Parent $Evidence) | Out-Null

function Get-Git([string[]]$GitArgs) {
    try { (& git @GitArgs 2>$null | Out-String).Trim() } catch { "" }
}

$hardware = [ordered]@{}
try {
    $cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
    $os = Get-CimInstance Win32_OperatingSystem
    $hardware = [ordered]@{
        cpu       = $cpu.Name
        cores     = $cpu.NumberOfCores
        threads   = $cpu.NumberOfLogicalProcessors
        memory_gb = [math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
        os        = "$($os.Caption) $($os.Version)"
        machine   = $env:COMPUTERNAME
    }
} catch { $hardware = [ordered]@{ error = "$_" } }

$started = Get-Date
Write-Host "qualify: $kind, files: $($selected -join ', ')"
if (-not $NoBuild) {
    $buildArgs = @("test", "--release", "--no-run")
    foreach ($f in $selected) { $buildArgs += @("--test", $f) }
    & cargo @buildArgs
    if ($LASTEXITCODE -ne 0) { throw "release build failed" }
}

$floorsDoc = Get-Content (Join-Path $root "tests/qualification-floors.json") -Raw | ConvertFrom-Json
$fileResults = @()
$records = @()
foreach ($file in $selected) {
    $qualOut = Join-Path $runDir "$file.qual.jsonl"
    $log = Join-Path $runDir "$file.log"
    $env:TKFS_QUAL_OUT = $qualOut
    $env:TKFS_QUAL_BUDGET_S = "$BudgetS"
    $cargoArgs = "test --release --test $file -- --ignored --test-threads=1 --nocapture $Filter"
    Write-Host "qualify: running $file (budget $BudgetS s per scenario)"
    $t0 = Get-Date
    $p = Start-Process -FilePath "cargo" -ArgumentList $cargoArgs -NoNewWindow -PassThru `
        -RedirectStandardOutput $log -RedirectStandardError "$log.err"
    try { $p.PriorityClass = $Priority } catch { }
    $done = $p.WaitForExit($FileTimeoutMin * 60 * 1000)
    if (-not $done) {
        & taskkill /PID $p.Id /T /F | Out-Null
        $fileStatus = "timeout"
    } else {
        $p.WaitForExit()
        if ($p.ExitCode -eq 0) { $fileStatus = "pass" } else { $fileStatus = "fail" }
    }
    $summary = Select-String -Path $log -Pattern "^test result:" -ErrorAction SilentlyContinue |
        Select-Object -Last 1
    $fileRecords = @()
    if (Test-Path $qualOut) {
        foreach ($line in Get-Content $qualOut) {
            if ($line.Trim()) { $fileRecords += ($line | ConvertFrom-Json) }
        }
    }
    $records += $fileRecords
    $scenarios = @()
    $names = @($fileRecords | Where-Object { $_.status -eq "started" } | ForEach-Object { $_.scenario } | Select-Object -Unique)
    foreach ($name in $names) {
        $final = $fileRecords | Where-Object { $_.scenario -eq $name -and $_.status -and $_.status -ne "started" } |
            Select-Object -Last 1
        $metrics = @($fileRecords | Where-Object { $_.scenario -eq $name -and $_.metric } |
            ForEach-Object { [ordered]@{ metric = $_.metric; value = $_.value; unit = $_.unit } })
        if ($final) { $st = $final.status } elseif ($fileStatus -eq "timeout") { $st = "killed" } else { $st = "incomplete" }
        $elapsed = $null; $failures = @(); $notes = @()
        if ($final) {
            $elapsed = $final.elapsed_ms
            $failures = @($final.failures | Where-Object { $null -ne $_ })
            $notes = @($final.notes | Where-Object { $null -ne $_ })
        }
        $unsupported = @($fileRecords | Where-Object { $_.scenario -eq $name -and $_.unsupported } | ForEach-Object { $_.unsupported })
        $scenarios += [ordered]@{
            scenario    = $name
            status      = $st
            elapsed_ms  = $elapsed
            failures    = $failures
            notes       = $notes
            unsupported = $unsupported
            metrics     = $metrics
        }
    }
    $fileResults += [ordered]@{
        file       = $file
        wave       = $qualFiles[$file]
        status     = $fileStatus
        duration_s = [math]::Round(((Get-Date) - $t0).TotalSeconds, 1)
        result     = if ($summary) { $summary.Line } else { $null }
        log        = $log
        scenarios  = $scenarios
    }
    Write-Host "qualify: $file -> $fileStatus ($([math]::Round(((Get-Date) - $t0).TotalSeconds)) s)"
}
Remove-Item Env:TKFS_QUAL_OUT -ErrorAction SilentlyContinue
Remove-Item Env:TKFS_QUAL_BUDGET_S -ErrorAction SilentlyContinue

function Test-Floor($floor, $value) {
    if ($null -eq $value) { return $false }
    $v = [double]$value; $limit = [double]$floor.value
    switch ($floor.op) {
        "<" { return $v -lt $limit }
        "<=" { return $v -le $limit }
        ">" { return $v -gt $limit }
        ">=" { return $v -ge $limit }
        "==" { return $v -eq $limit }
        default { return $false }
    }
}

$floorResults = @()
foreach ($floor in $floorsDoc.floors) {
    $file = $floor.scenario.Split(":")[0]
    if (-not ($selected -contains $file)) { continue }
    $metric = $records | Where-Object { $_.scenario -eq $floor.scenario -and $_.metric -eq $floor.metric } |
        Select-Object -Last 1
    $measured = $null
    if ($metric) { $measured = $metric.value }
    $ran = [bool]($records | Where-Object { $_.scenario -eq $floor.scenario -and $_.status -eq "started" })
    $enforced = (-not $Baseline) -and ($floor.wave -le $Wave) -and ($ran -or -not $Filter)
    if (-not $ran) { $verdict = "not_run" }
    elseif ($null -eq $metric) { $verdict = "missing" }
    elseif (Test-Floor $floor $measured) { $verdict = "pass" }
    else { $verdict = "fail" }
    $floorResults += [ordered]@{
        id       = $floor.id
        wave     = $floor.wave
        scenario = $floor.scenario
        metric   = $floor.metric
        op       = $floor.op
        floor    = $floor.value
        unit     = $floor.unit
        measured = $measured
        verdict  = $verdict
        enforced = $enforced
        baseline = $floor.baseline
    }
}

$allScenarios = @($fileResults | ForEach-Object { $_.scenarios })
$counts = [ordered]@{}
foreach ($s in $allScenarios) {
    $k = $s.status
    if ($counts.Contains($k)) { $counts[$k] += 1 } else { $counts[$k] = 1 }
}
$enforcedFailures = @($floorResults | Where-Object { $_.enforced -and $_.verdict -ne "pass" })
$scenarioFailures = @($allScenarios | Where-Object { $_.status -notin @("pass", "skipped") })
$fileFailures = @($fileResults | Where-Object { $_.status -ne "pass" })
$ok = $enforcedFailures.Count -eq 0 -and $scenarioFailures.Count -eq 0 -and $fileFailures.Count -eq 0

$evidenceDoc = [ordered]@{
    schema       = 1
    kind         = $kind
    wave         = $Wave
    generated_at = (Get-Date).ToString("o")
    duration_s   = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
    git          = [ordered]@{
        commit = Get-Git @("rev-parse", "HEAD")
        branch = Get-Git @("rev-parse", "--abbrev-ref", "HEAD")
        dirty  = [bool](Get-Git @("status", "--porcelain"))
    }
    toolchain    = (& rustc -V 2>$null | Out-String).Trim()
    hardware     = $hardware
    settings     = [ordered]@{
        budget_s         = $BudgetS
        file_timeout_min = $FileTimeoutMin
        priority         = $Priority
        filter           = $Filter
        profile          = "release"
    }
    summary      = [ordered]@{
        ok                = if ($Baseline) { $null } else { $ok }
        scenarios         = $allScenarios.Count
        by_status         = $counts
        floors_evaluated  = $floorResults.Count
        floors_passed     = @($floorResults | Where-Object { $_.verdict -eq "pass" }).Count
        floors_failed     = @($floorResults | Where-Object { $_.verdict -eq "fail" }).Count
        floors_missing    = @($floorResults | Where-Object { $_.verdict -eq "missing" }).Count
        floors_not_run    = @($floorResults | Where-Object { $_.verdict -eq "not_run" }).Count
        enforced_failures = @($enforcedFailures | ForEach-Object { $_.id })
    }
    files        = $fileResults
    floors       = $floorResults
    run_dir      = $runDir
}
$evidenceDoc | ConvertTo-Json -Depth 12 | Set-Content -Path $Evidence -Encoding UTF8
Write-Host "qualify: evidence written to $Evidence"
Write-Host ("qualify: scenarios {0}; floors pass {1}, fail {2}, missing {3}" -f `
        $allScenarios.Count, $evidenceDoc.summary.floors_passed, $evidenceDoc.summary.floors_failed, `
        $evidenceDoc.summary.floors_missing)
if ($Baseline) { exit 0 }
if ($ok) { exit 0 } else { exit 1 }
