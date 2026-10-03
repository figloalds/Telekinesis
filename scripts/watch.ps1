param([string]$Mount,[string]$Ready,[string]$Events,[string]$StopPath)
$ErrorActionPreference='Stop'
$watcher=[IO.FileSystemWatcher]::new($Mount)
$watcher.IncludeSubdirectories=$true
$watcher.NotifyFilter=[IO.NotifyFilters]::FileName -bor [IO.NotifyFilters]::DirectoryName -bor [IO.NotifyFilters]::LastWrite -bor [IO.NotifyFilters]::Size
try {
    foreach($kind in @('Changed','Created','Deleted','Renamed')) {
        Register-ObjectEvent -InputObject $watcher -EventName $kind -SourceIdentifier "tkfs-$kind" | Out-Null
    }
    $watcher.EnableRaisingEvents=$true
    Set-Content -LiteralPath $Ready -Value 'ready'
    while(!(Test-Path -LiteralPath $StopPath)) {
        $event=Wait-Event -Timeout 1
        if($event) {
            Add-Content -LiteralPath $Events -Value ($event.SourceEventArgs.ChangeType.ToString()+':'+$event.SourceEventArgs.FullPath)
            Remove-Event -EventIdentifier $event.EventIdentifier
        }
    }
} finally {
    $watcher.EnableRaisingEvents=$false
    $watcher.Dispose()
    foreach($kind in @('Changed','Created','Deleted','Renamed')) {Unregister-Event -SourceIdentifier "tkfs-$kind" -ErrorAction SilentlyContinue}
}
