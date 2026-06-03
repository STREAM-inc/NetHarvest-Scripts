param(
    [Parameter(Mandatory=$true)]
    [string]$Prefs,
    [string]$Destination = "",
    [int]$PauseSeconds = 60,
    [string]$Python = "python",
    [string]$MergedName = "",
    [switch]$SkipMerge
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
$QueueStart = Get-Date
$QueueLog = Join-Path $OutputDir "cookdoor_pref_queue_$QueueTag.log"
$PrefList = $Prefs.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }

function Write-QueueLog {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    $line | Tee-Object -FilePath $QueueLog -Append
}

function Get-CookdoorPrefCsv {
    param(
        [string]$Pref,
        [datetime]$After
    )

    Get-ChildItem -LiteralPath $Destination -Filter "*.csv" |
        Where-Object {
            $_.LastWriteTime -ge $After -and
            $_.Name -like "*cookdoor*" -and
            $_.Name -notlike "*merged*" -and
            $_.Name -notlike "pipeline_*" -and
            $_.Name -like "*_${Pref}_*"
        } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
}

function Merge-CookdoorCsv {
    param(
        [System.IO.FileInfo[]]$Files,
        [string]$OutputPath
    )

    $encoding = New-Object System.Text.UTF8Encoding($true)
    $writer = New-Object System.IO.StreamWriter($OutputPath, $false, $encoding)
    $rowCount = 0
    $wroteHeader = $false

    try {
        foreach ($file in $Files) {
            $reader = New-Object System.IO.StreamReader($file.FullName, [System.Text.Encoding]::UTF8)
            try {
                $header = $reader.ReadLine()
                if ([string]::IsNullOrWhiteSpace($header)) {
                    continue
                }
                if (-not $wroteHeader) {
                    $writer.WriteLine($header)
                    $wroteHeader = $true
                }

                while (($line = $reader.ReadLine()) -ne $null) {
                    if ($line.Length -eq 0) {
                        continue
                    }
                    $writer.WriteLine($line)
                    $rowCount += 1
                }
            } finally {
                $reader.Close()
            }
        }
    } finally {
        $writer.Close()
    }

    return $rowCount
}

function Invoke-CookdoorQueueMerge {
    if ($SkipMerge) {
        Write-QueueLog "Cookdoor merge skipped."
        return
    }

    $sourceFiles = @()
    $seenFiles = @{}
    foreach ($pref in $PrefList) {
        $file = Get-CookdoorPrefCsv -Pref $pref -After $QueueStart
        if ($null -eq $file) {
            Write-QueueLog "merge source missing. pref=$pref"
            continue
        }
        if (-not $seenFiles.ContainsKey($file.FullName)) {
            $sourceFiles += $file
            $seenFiles[$file.FullName] = $true
        }
    }

    if ($sourceFiles.Count -eq 0) {
        Write-QueueLog "Cookdoor merge skipped because no source csv was found."
        return
    }

    $providedName = -not [string]::IsNullOrWhiteSpace($MergedName)
    if ($providedName) {
        $targetName = $MergedName
        if (-not $targetName.EndsWith(".csv", [System.StringComparison]::OrdinalIgnoreCase)) {
            $targetName = "$targetName.csv"
        }
        $targetPath = Join-Path $Destination $targetName
        $rowCount = Merge-CookdoorCsv -Files $sourceFiles -OutputPath $targetPath
    } else {
        $date = Get-Date -Format "yyyyMMdd"
        $targetBase = "${date}_cookdoor_merged_queue_${QueueTag}_クックドア_merged"
        $tempPath = Join-Path $Destination "$targetBase.tmp"
        $rowCount = Merge-CookdoorCsv -Files $sourceFiles -OutputPath $tempPath
        $targetName = "${targetBase}_${rowCount}件.csv"
        $targetPath = Join-Path $Destination $targetName
        Move-Item -LiteralPath $tempPath -Destination $targetPath -Force
    }

    Write-QueueLog "Cookdoor merge finished. files=$($sourceFiles.Count) rows=$rowCount target=$targetPath"
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

Invoke-CookdoorQueueMerge
Write-QueueLog "Cookdoor prefecture queue finished."
