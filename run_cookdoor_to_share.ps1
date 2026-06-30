param(
    [string]$Destination = "",
    [string]$Prefs = "",
    [int]$MaxItems = 0,
    [int]$MaxPages = 0,
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
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

$RunTag = Get-Date -Format "yyyyMMdd_HHmmss"
$MainLog = Join-Path $OutputDir "cookdoor_to_share_$RunTag.log"
$DoneLog = Join-Path $OutputDir "cookdoor_to_share_$RunTag.done.txt"
$attemptStart = Get-Date

function Write-RunLog {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    $line | Tee-Object -FilePath $MainLog -Append
}

function Get-FinalCsv {
    param([datetime]$After)
    Get-ChildItem -Path $OutputDir -Filter "*.csv" |
        Where-Object {
            $_.LastWriteTime -ge $After -and
            $_.Name -notlike "pipeline_*" -and
            ($_.Name -like "*cookdoor*" -or $_.Name -like "*クックドア*")
        } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
}

Write-RunLog "Cookdoor scraping started. destination=$Destination repo=$RepoRoot"

$env:PYTHONIOENCODING = "utf-8"
if ($Prefs) {
    $env:COOKDOOR_PREFS = $Prefs
    if ($Prefs -notmatch ",") {
        $env:COOKDOOR_OUTPUT_LABEL = $Prefs
    } else {
        Remove-Item Env:\COOKDOOR_OUTPUT_LABEL -ErrorAction SilentlyContinue
    }
} else {
    Remove-Item Env:\COOKDOOR_PREFS -ErrorAction SilentlyContinue
    Remove-Item Env:\COOKDOOR_OUTPUT_LABEL -ErrorAction SilentlyContinue
}
if ($MaxItems -gt 0) {
    $env:COOKDOOR_MAX_ITEMS = [string]$MaxItems
} else {
    Remove-Item Env:\COOKDOOR_MAX_ITEMS -ErrorAction SilentlyContinue
}
if ($MaxPages -gt 0) {
    $env:COOKDOOR_MAX_PAGES = [string]$MaxPages
} else {
    Remove-Item Env:\COOKDOOR_MAX_PAGES -ErrorAction SilentlyContinue
}

$attemptLog = Join-Path $OutputDir "cookdoor_to_share_$RunTag.attempt.log"
$attemptOutLog = Join-Path $OutputDir "cookdoor_to_share_$RunTag.stdout.log"
$attemptErrLog = Join-Path $OutputDir "cookdoor_to_share_$RunTag.stderr.log"
$process = Start-Process -FilePath $Python `
    -ArgumentList @("sites/food/cookdoor.py") `
    -WorkingDirectory $RepoRoot `
    -PassThru `
    -Wait `
    -WindowStyle Hidden `
    -RedirectStandardOutput $attemptOutLog `
    -RedirectStandardError $attemptErrLog
$exitCode = $process.ExitCode
Get-Content -LiteralPath $attemptOutLog, $attemptErrLog -ErrorAction SilentlyContinue |
    Set-Content -LiteralPath $attemptLog -Encoding UTF8

Write-RunLog "Cookdoor scraper finished. exit=$exitCode attempt_log=$attemptLog"

$csv = Get-FinalCsv -After $attemptStart
if ($null -eq $csv) {
    Write-RunLog "No final Cookdoor CSV was produced."
    exit 1
}

$target = Join-Path $Destination $csv.Name
Copy-Item -LiteralPath $csv.FullName -Destination $target -Force
Write-RunLog "Copied final csv. source=$($csv.FullName) target=$target"

"Copied: $($csv.FullName)`nTo: $target`nAt: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" |
    Set-Content -LiteralPath $DoneLog -Encoding UTF8

exit $exitCode
