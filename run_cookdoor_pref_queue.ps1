param(
    [Parameter(Mandatory=$true)]
    [string]$Prefs,
    [string]$Destination = "",
    [int]$PauseSeconds = 60,
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Runner = Join-Path $RepoRoot "run_cookdoor_to_share.ps1"
$NetHarvestRoot = Join-Path (Split-Path -Parent $RepoRoot) "NetHarvest"
$OutputDir = Join-Path $NetHarvestRoot "output"

if (-not $Destination) {
    $jpFolder = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String("5Y+W5b6X44OH44O844K/"))
    $month = Get-Date -Format "yyyyMM"
    $day = Get-Date -Format "yyyyMMdd"
    $Destination = "\\STREAM06\Share\Scraping\$jpFolder\$month\$day"
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
New-Item -ItemType Directory -Force -Path $Destination | Out-Null

$QueueTag = Get-Date -Format "yyyyMMdd_HHmmss"
$QueueLog = Join-Path $OutputDir "cookdoor_pref_queue_$QueueTag.log"
$PrefList = $Prefs.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }

function Write-QueueLog {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    $line | Tee-Object -FilePath $QueueLog -Append
}

Write-QueueLog "Cookdoor prefecture queue started. prefs=$($PrefList -join ',') destination=$Destination"

foreach ($pref in $PrefList) {
    Write-QueueLog "pref=$pref start"
    try {
        & $Runner -Prefs $pref -Destination $Destination -Python $Python
        $exitCode = $LASTEXITCODE
        Write-QueueLog "pref=$pref finished exit=$exitCode"
    } catch {
        Write-QueueLog "pref=$pref failed error=$($_.Exception.Message)"
    }

    if ($PauseSeconds -gt 0) {
        Start-Sleep -Seconds $PauseSeconds
    }
}

Write-QueueLog "Cookdoor prefecture queue finished."
