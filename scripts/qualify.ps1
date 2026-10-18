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
  -All       every qual file and every floor (same as -Wave 5), with the
             budgets the long scenarios need (150K-event feeds, 1 GiB
             objects): -BudgetS 2400 and -FileTimeoutMin 150 unless given.
             Evidence: test-evidence/qualification-all.json (WP4.2).
  -Heavy     also run the heavy scenarios (WP5.INT): the two-hour WP5.1
             daemon workloads (qual_wp5_1_concurrent::conflict_storm and
             ::fast_two_way_editing, about 2.5 h each), which a run skips
             otherwise (each file then gets -FileTimeoutMin 200 unless
             given). A floor marked `heavy` in
             tests/qualification-floors.json is enforced only when its
             scenario ran (a run without -Heavy records it as not_run). They
             are usually run on their own, beside the soak:
               qualify.ps1 -Wave 5 -Files qual_wp5_1_concurrent -Heavy
                   -Filter conflict_storm -Evidence <file>
  -Baseline  every qual file; floors are evaluated and recorded, never enforced
             (exit 0); writes test-evidence/qualification-baseline.json.
  -Merge     keep the other files' results from the existing evidence file and
             replace only the files run now (floors and summary are recomputed).

  -SelfTest  check the process runner only (no build, no qual file): commands
             with known exit codes and `test result:` lines must get the
             expected file status. Exit 0 when every case holds, 1 otherwise.
             tests/harness_selftest.rs runs it under `powershell` (5.1).

  File status: pass only when cargo exited 0 AND the last `test result:` line
  of its output says `ok.`; timeout when the per-file limit killed it; fail
  otherwise (including an unknown exit code). The exit code and the result
  line are recorded per file.

  Floor verdicts: pass, fail, missing (no metric), incomplete (the scenario
  timed out, so its metric covers a partial run), not_run. A floor whose
  `platforms` list (tests/qualification-floors.json) does not name "windows"
  is evaluated and recorded but not enforced here.

  Evidence records the hardware it ran on (CPU, memory, OS), the disks of the
  temporary directory and of the build (model, media, bus, free space: the
  fsync-bound floors depend on them), and the machine's load when the run
  started (CPU load, other build and tkfs processes): the dev machine is
  shared, so a floor read must be read with its context.

  Exit code: 0 when every enforced floor holds and every selected file and
  scenario passed (or skipped); 1 otherwise. -Baseline always exits 0 unless
  the build fails.

  Encoding: every file is read as UTF-8 and the evidence is written as UTF-8
  without a BOM and with LF line endings (strict JSON readers such as
  serde_json reject a BOM; Windows PowerShell 5.1 `Set-Content -Encoding UTF8`
  would add one, and its default read encoding is the ANSI code page).

.EXAMPLE
  $env:CARGO_TARGET_DIR = "C:\tkfs-target\wp1-6"
  powershell -NoProfile -File scripts\qualify.ps1 -Wave 1 -Files qual_wp1_6_activation

.EXAMPLE
  powershell -NoProfile -File scripts\qualify.ps1 -Baseline -BudgetS 60

.EXAMPLE
  powershell -NoProfile -File scripts\qualify.ps1 -SelfTest
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
    [switch]$Merge,
    [switch]$SelfTest,
    [switch]$Heavy,
    [ValidateSet("Idle", "BelowNormal", "Normal")][string]$Priority = "BelowNormal"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
# -All runs the 150K-event and 1 GiB scenarios: give them their budget
# unless the caller chose one (WP4.2).
if ($All -and -not $PSBoundParameters.ContainsKey("BudgetS")) { $BudgetS = 2400 }
if ($All -and -not $PSBoundParameters.ContainsKey("FileTimeoutMin")) { $FileTimeoutMin = 150 }
# -Heavy runs two-hour workloads (2 h of edits, up to 30 min to settle, the
# oracles): a file gets 200 minutes unless the caller chose (WP5.INT).
if ($Heavy -and -not $PSBoundParameters.ContainsKey("FileTimeoutMin")) { $FileTimeoutMin = 200 }

# File status from the exit code and the last `test result:` line. A pass
# needs both: exit code 0 and `test result: ok.`. An unknown ($null) exit code
# is never a pass.
function Get-QualStatus($ExitCode, $Result) {
    if ($null -eq $ExitCode -or $ExitCode -ne 0) { return "fail" }
    if ($Result -and $Result -match '^test result: ok\.') { return "pass" }
    return "fail"
}

# Run one command line with stdout in $Log and stderr in "$Log.err", at
# $ProcessPriority, for at most $TimeoutMs (then the process tree is killed).
# Returns status (pass|fail|timeout), exit_code and result (the last
# `test result:` line, or $null).
function Invoke-QualProcess([string]$FilePath, [string]$Arguments, [string]$Log, [int]$TimeoutMs,
    [string]$ProcessPriority) {
    $p = Start-Process -FilePath $FilePath -ArgumentList $Arguments -NoNewWindow -PassThru `
        -RedirectStandardOutput $Log -RedirectStandardError "$Log.err"
    # Windows PowerShell 5.1: the Process object Start-Process returns holds no
    # handle, so ExitCode reads $null after the process exits (every file would
    # be recorded as failed). Reading Handle while the process runs caches it.
    $null = $p.Handle
    try { $p.PriorityClass = $ProcessPriority } catch { }
    $exitCode = $null
    $timedOut = -not $p.WaitForExit($TimeoutMs)
    if ($timedOut) {
        & taskkill /PID $p.Id /T /F | Out-Null
        $null = $p.WaitForExit(30000)
    } else {
        $p.WaitForExit()
        $exitCode = $p.ExitCode
    }
    $summary = Select-String -Path $Log -Pattern "^test result:" -ErrorAction SilentlyContinue |
        Select-Object -Last 1
    $result = if ($summary) { $summary.Line.Trim() } else { $null }
    if ($timedOut) { $status = "timeout" } else { $status = Get-QualStatus $exitCode $result }
    return [ordered]@{ status = $status; exit_code = $exitCode; result = $result }
}

if ($SelfTest) {
    # Commands with known outcomes through the same runner as the qual files.
    $dir = Join-Path ([System.IO.Path]::GetTempPath()) ("tkfs-qualify-selftest-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force $dir | Out-Null
    $cases = @(
        @{ name = "pass"; args = '/c "echo test result: ok. 1 passed; 0 failed& exit 0"'; status = "pass"; exit = "0"; ms = 60000 },
        @{ name = "failed"; args = '/c "echo test result: FAILED. 0 passed; 1 failed& exit 101"'; status = "fail"; exit = "101"; ms = 60000 },
        @{ name = "exit3"; args = '/c exit 3'; status = "fail"; exit = "3"; ms = 60000 },
        @{ name = "exit0-no-result"; args = '/c exit 0'; status = "fail"; exit = "0"; ms = 60000 },
        @{ name = "exit0-failed-result"; args = '/c "echo test result: FAILED. 0 passed; 1 failed& exit 0"'; status = "fail"; exit = "0"; ms = 60000 },
        @{ name = "timeout"; args = '/c "ping -n 30 127.0.0.1 >nul"'; status = "timeout"; exit = ""; ms = 1000 }
    )
    $bad = 0
    foreach ($c in $cases) {
        $r = Invoke-QualProcess "cmd.exe" $c.args (Join-Path $dir "$($c.name).log") $c.ms $Priority
        $got = "status=$($r.status) exit=$($r.exit_code)"
        $want = "status=$($c.status) exit=$($c.exit)"
        if ($got -eq $want) { Write-Host "qualify selftest: $($c.name): $got" }
        else { $bad += 1; Write-Host "qualify selftest: $($c.name): got $got, want $want (result: $($r.result))" }
    }
    Remove-Item -Recurse -Force $dir -ErrorAction SilentlyContinue
    if ($bad -gt 0) {
        Write-Host "qualify selftest: FAILED ($bad of $($cases.Count) cases, PowerShell $($PSVersionTable.PSVersion))"
        exit 1
    }
    Write-Host "qualify selftest: ok ($($cases.Count) cases, PowerShell $($PSVersionTable.PSVersion))"
    exit 0
}

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
    # CCR-WP4.4-4 (applied by WP4.INT): WP4.4's seal-path scenarios.
    "qual_wp4_4_seal"       = 4
    # Wave 5 (WP5.INT; CCR-WP5.1-4, CCR-WP5.2-6, CCR-WP5.3-4).
    "qual_wp5_1_concurrent" = 5
    "qual_wp5_2_local"      = 5
    "qual_wp5_3_recovery"   = 5
}
# Scenarios a run skips unless -Heavy (WP5.INT): the two-hour WP5.1 daemon
# workloads, run on their own (each about 2.5 h).
$heavySkips = @{
    "qual_wp5_1_concurrent" = "--skip conflict_storm --skip fast_two_way_editing"
}

if ($All -or $Baseline) { $Wave = 5 }
if ($Wave -lt 1 -or $Wave -gt 5) { throw "use -Wave 1..5, -All or -Baseline" }
$selected = @($qualFiles.Keys | Where-Object { $qualFiles[$_] -le $Wave })
if ($Files) {
    # `powershell -File` passes "a,b" as one string: accept commas as separators.
    $Files = @($Files | ForEach-Object { $_ -split "," } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    $unknown = @($Files | Where-Object { -not $qualFiles.Contains($_) })
    if ($unknown.Count -gt 0) { throw "unknown qualification files: $($unknown -join ', ')" }
    $selected = @($selected | Where-Object { $Files -contains $_ })
}
if ($selected.Count -eq 0) { throw "no qualification files selected" }

if ($Baseline) { $kind = "baseline" } elseif ($All) { $kind = "all" } else { $kind = "wave-$Wave" }
if (-not $Evidence) { $Evidence = Join-Path $root "test-evidence/qualification-$kind.json" }
# Absolute (relative to the repository root, as before): .NET file APIs below
# resolve relative paths against the process directory, not Set-Location.
$Evidence = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Evidence)
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $root "test-runs/qualify-$kind-$stamp"
New-Item -ItemType Directory -Force $runDir | Out-Null
New-Item -ItemType Directory -Force (Split-Path -Parent $Evidence) | Out-Null

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
function Read-Utf8([string]$Path) { [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::UTF8) }

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

# The disk of a directory: drive, free space and the physical disk behind it
# (best effort; WP4.2).
function Get-DiskInfo([string]$Role, [string]$Path) {
    $info = [ordered]@{ role = $Role; path = $Path }
    try {
        $full = [System.IO.Path]::GetFullPath($Path)
        $letter = $full.Substring(0, 1)
        $volume = Get-Volume -DriveLetter $letter -ErrorAction Stop
        $info.drive = "$($letter):"
        $info.free_gb = [math]::Round($volume.SizeRemaining / 1GB, 1)
        $info.size_gb = [math]::Round($volume.Size / 1GB, 1)
        $info.file_system = $volume.FileSystem
        $partition = Get-Partition -DriveLetter $letter -ErrorAction Stop
        $disk = Get-PhysicalDisk | Where-Object { "$($_.DeviceId)" -eq "$($partition.DiskNumber)" } |
            Select-Object -First 1
        if ($disk) {
            $info.model = $disk.FriendlyName
            $info.media = "$($disk.MediaType)"
            $info.bus = "$($disk.BusType)"
        }
    } catch { $info.error = "$_" }
    return $info
}
$targetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $root "target" }
$tempDir = [System.IO.Path]::GetTempPath()
$hardware.disks = @((Get-DiskInfo "temp" $tempDir), (Get-DiskInfo "target" $targetDir))

# What else ran when this run started (the dev machine is shared; WP4.2).
$load = [ordered]@{}
try {
    $load.cpu_load_percent = [math]::Round((Get-CimInstance Win32_Processor |
                Measure-Object -Property LoadPercentage -Average).Average, 1)
    $others = @(Get-Process -ErrorAction SilentlyContinue |
            Where-Object { $_.ProcessName -match '^(cargo|rustc|tkfs|link)$' -or $_.ProcessName -like 'qual_*' })
    $load.build_processes = @($others | Where-Object { $_.ProcessName -match '^(cargo|rustc|link)$' }).Count
    $load.tkfs_processes = @($others | Where-Object { $_.ProcessName -eq 'tkfs' }).Count
    $load.qual_processes = @($others | Where-Object { $_.ProcessName -like 'qual_*' }).Count
} catch { $load.error = "$_" }

$started = Get-Date
# Git state when the run starts (the build and the binaries under test), not
# when the evidence is written: edits made while a long run is in progress do
# not belong to it.
try {
    $porcelain = @(& git status --porcelain 2>$null | ForEach-Object { "$_".TrimEnd() } | Where-Object { $_ })
} catch { $porcelain = @() }
$gitState = [ordered]@{
    commit      = Get-Git @("rev-parse", "HEAD")
    branch      = Get-Git @("rev-parse", "--abbrev-ref", "HEAD")
    dirty       = $porcelain.Count -gt 0
    dirty_paths = @($porcelain | Select-Object -First 50)
}
Write-Host "qualify: $kind, files: $($selected -join ', ')"
if (-not $NoBuild) {
    $buildArgs = @("test", "--release", "--no-run")
    foreach ($f in $selected) { $buildArgs += @("--test", $f) }
    & cargo @buildArgs
    if ($LASTEXITCODE -ne 0) { throw "release build failed" }
}

$floorsDoc = Read-Utf8 (Join-Path $root "tests/qualification-floors.json") | ConvertFrom-Json
$fileResults = @()
$records = @()
foreach ($file in $selected) {
    $qualOut = Join-Path $runDir "$file.qual.jsonl"
    $log = Join-Path $runDir "$file.log"
    $env:TKFS_QUAL_OUT = $qualOut
    $env:TKFS_QUAL_BUDGET_S = "$BudgetS"
    $cargoArgs = "test --release --test $file -- --ignored --test-threads=1 --nocapture $Filter"
    if (-not $Heavy -and $heavySkips.Contains($file)) { $cargoArgs += " " + $heavySkips[$file] }
    Write-Host "qualify: running $file (budget $BudgetS s per scenario)"
    $t0 = Get-Date
    $run = Invoke-QualProcess "cargo" $cargoArgs $log ($FileTimeoutMin * 60 * 1000) $Priority
    $fileStatus = $run.status
    $fileRecords = @()
    if (Test-Path $qualOut) {
        foreach ($line in Get-Content $qualOut -Encoding UTF8) {
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
        exit_code  = $run.exit_code
        duration_s = [math]::Round(((Get-Date) - $t0).TotalSeconds, 1)
        result     = $run.result
        log        = $log
        scenarios  = $scenarios
    }
    Write-Host "qualify: $file -> $fileStatus (exit $($run.exit_code), $([math]::Round(((Get-Date) - $t0).TotalSeconds)) s)"
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

$mergedFrom = $null
if ($Merge -and (Test-Path $Evidence)) {
    # Carry the other files' results (and their metrics, for the floors) forward.
    $old = Read-Utf8 $Evidence | ConvertFrom-Json
    $mergedFrom = [ordered]@{ generated_at = $old.generated_at; git = $old.git; run_dir = $old.run_dir }
    $kept = @($old.files | Where-Object { -not ($selected -contains $_.file) })
    foreach ($f in $kept) {
        foreach ($sc in @($f.scenarios)) {
            $records += [pscustomobject]@{ scenario = $sc.scenario; status = "started" }
            foreach ($m in @($sc.metrics)) {
                $records += [pscustomobject]@{ scenario = $sc.scenario; metric = $m.metric; value = $m.value }
            }
        }
    }
    $fileResults = @($kept) + @($fileResults)
    $fileResults = @($fileResults | Sort-Object { [array]::IndexOf(@($qualFiles.Keys), $_.file) })
}
$evaluated = @($fileResults | ForEach-Object { $_.file })

$floorResults = @()
foreach ($floor in $floorsDoc.floors) {
    $file = $floor.scenario.Split(":")[0]
    if (-not ($evaluated -contains $file)) { continue }
    $metric = $records | Where-Object { $_.scenario -eq $floor.scenario -and $_.metric -eq $floor.metric } |
        Select-Object -Last 1
    $measured = $null
    if ($metric) { $measured = $metric.value }
    $ran = [bool]($records | Where-Object { $_.scenario -eq $floor.scenario -and $_.status -eq "started" })
    $scenarioStatus = ($fileResults | ForEach-Object { $_.scenarios } |
        Where-Object { $_.scenario -eq $floor.scenario } | Select-Object -First 1).status
    $platformOk = (-not $floor.platforms) -or (@($floor.platforms) -contains "windows")
    # A heavy floor (WP5.INT) is enforced only when its scenario ran.
    $heavyOk = $ran -or -not $floor.heavy
    $enforced = (-not $Baseline) -and ($floor.wave -le $Wave) -and ($ran -or -not $Filter) -and $platformOk -and $heavyOk
    if (-not $ran) { $verdict = "not_run" }
    elseif ($null -eq $metric) { $verdict = "missing" }
    elseif ($scenarioStatus -in @("timeout", "killed", "incomplete")) { $verdict = "incomplete" }
    elseif (Test-Floor $floor $measured) { $verdict = "pass" }
    else { $verdict = "fail" }
    $floorResults += [ordered]@{
        id       = $floor.id
        wave     = $floor.wave
        platforms = $floor.platforms
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
    platform     = "windows"
    wave         = $Wave
    generated_at = (Get-Date).ToString("o")
    duration_s   = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
    git          = $gitState
    toolchain    = (& rustc -V 2>$null | Out-String).Trim()
    hardware     = $hardware
    load_at_start = $load
    settings     = [ordered]@{
        budget_s         = $BudgetS
        file_timeout_min = $FileTimeoutMin
        priority         = $Priority
        filter           = $Filter
        heavy            = [bool]$Heavy
        profile          = "release"
        temp_dir         = $tempDir
        target_dir       = $targetDir
        baseline_bin     = $env:TKFS_BASELINE_BIN
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
        floors_incomplete = @($floorResults | Where-Object { $_.verdict -eq "incomplete" }).Count
        enforced_failures = @($enforcedFailures | ForEach-Object { $_.id })
    }
    files        = $fileResults
    floors       = $floorResults
    run_dir      = $runDir
    merged_from  = $mergedFrom
}
$json = ($evidenceDoc | ConvertTo-Json -Depth 12) -replace "`r`n", "`n"
[System.IO.File]::WriteAllText($Evidence, $json + "`n", $utf8NoBom)
Write-Host "qualify: evidence written to $Evidence"
Write-Host ("qualify: scenarios {0}; floors pass {1}, fail {2}, missing {3}" -f `
        $allScenarios.Count, $evidenceDoc.summary.floors_passed, $evidenceDoc.summary.floors_failed, `
        $evidenceDoc.summary.floors_missing)
if ($Baseline) { exit 0 }
if ($ok) { exit 0 } else { exit 1 }
