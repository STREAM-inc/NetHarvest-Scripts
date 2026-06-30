param(
    [string]$Destination = "",
    [string]$Prefs = "",
    [string]$Date = "",
    [string]$OutputName = "",
    [datetime]$After = [datetime]::MinValue
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$NetHarvestRoot = Join-Path (Split-Path -Parent $RepoRoot) "NetHarvest"
$OutputDir = Join-Path $NetHarvestRoot "output"

if (-not $Date) {
    $Date = Get-Date -Format "yyyyMMdd"
}

if (-not $Destination) {
    $jpFolder = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String("5Y+W5b6X44OH44O844K/"))
    $month = $Date.Substring(0, 6)
    $Destination = "\\STREAM06\Share\Scraping\$jpFolder\$month\$Date"
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
New-Item -ItemType Directory -Force -Path $Destination | Out-Null

$PrefSlugs = @(
    "hokkaido",
    "aomori",
    "iwate",
    "miyagi",
    "akita",
    "yamagata",
    "fukushima",
    "ibaraki",
    "tochigi",
    "gunma",
    "saitama",
    "chiba",
    "tokyo",
    "kanagawa",
    "niigata",
    "toyama",
    "ishikawa",
    "fukui",
    "yamanashi",
    "nagano",
    "gifu",
    "shizuoka",
    "aichi",
    "mie",
    "shiga",
    "kyoto",
    "osaka",
    "hyogo",
    "nara",
    "wakayama",
    "tottori",
    "shimane",
    "okayama",
    "hiroshima",
    "yamaguchi",
    "tokushima",
    "kagawa",
    "ehime",
    "kochi",
    "fukuoka",
    "saga",
    "nagasaki",
    "kumamoto",
    "oita",
    "miyazaki",
    "kagoshima",
    "okinawa"
)

if ($Prefs) {
    $PrefList = $Prefs.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
} else {
    $PrefList = $PrefSlugs
}

$RunTag = Get-Date -Format "yyyyMMdd_HHmmss"
$MergeLog = Join-Path $OutputDir "cookdoor_merge_$RunTag.log"

function Write-MergeLog {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    $line | Tee-Object -FilePath $MergeLog -Append
}

function Get-CookdoorPrefCsv {
    param([string]$Pref)

    Get-ChildItem -LiteralPath $Destination -Filter "$Date*cookdoor*.csv" |
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

Write-MergeLog "Cookdoor merge started. destination=$Destination prefs=$($PrefList -join ',') date=$Date after=$After"

$sourceFiles = @()
$seenFiles = @{}
foreach ($pref in $PrefList) {
    $file = Get-CookdoorPrefCsv -Pref $pref
    if ($null -eq $file) {
        Write-MergeLog "source missing. pref=$pref"
        continue
    }
    if (-not $seenFiles.ContainsKey($file.FullName)) {
        $sourceFiles += $file
        $seenFiles[$file.FullName] = $true
        Write-MergeLog "source added. pref=$pref file=$($file.FullName)"
    }
}

if ($sourceFiles.Count -eq 0) {
    Write-MergeLog "No source csv was found."
    exit 1
}

if ($OutputName) {
    if (-not $OutputName.EndsWith(".csv", [System.StringComparison]::OrdinalIgnoreCase)) {
        $OutputName = "$OutputName.csv"
    }
    $targetPath = Join-Path $Destination $OutputName
    $rowCount = Merge-CookdoorCsv -Files $sourceFiles -OutputPath $targetPath
} else {
    $targetBase = "${Date}_cookdoor_merged_all_クックドア_merged"
    $tempPath = Join-Path $Destination "$targetBase.tmp"
    $rowCount = Merge-CookdoorCsv -Files $sourceFiles -OutputPath $tempPath
    $targetPath = Join-Path $Destination "${targetBase}_${rowCount}件.csv"
    Move-Item -LiteralPath $tempPath -Destination $targetPath -Force
}

Write-MergeLog "Cookdoor merge finished. files=$($sourceFiles.Count) rows=$rowCount target=$targetPath"
