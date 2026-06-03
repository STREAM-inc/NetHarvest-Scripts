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
} else {
    Remove-Item Env:\COOKDOOR_PREFS -ErrorAction SilentlyContinue
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
$processInfo = New-Object System.Diagnostics.ProcessStartInfo
$processInfo.FileName = $Python
$processInfo.Arguments = '"sites/food/cookdoor.py"'
$processInfo.WorkingDirectory = $RepoRoot
$processInfo.UseShellExecute = $false
$processInfo.RedirectStandardOutput = $true
$processInfo.RedirectStandardError = $true
$processInfo.CreateNoWindow = $true
$process = [System.Diagnostics.Process]::Start($processInfo)
$stdout = $process.StandardOutput.ReadToEnd()
$stderr = $process.StandardError.ReadToEnd()
$process.WaitForExit()
$exitCode = $process.ExitCode
($stdout + $stderr) | Set-Content -LiteralPath $attemptLog -Encoding UTF8

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
