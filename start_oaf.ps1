param(
  [string]$ContainerName = "violin_oaf_server",
  [string]$PreferredImage = "violin/oaf:latest",   # leave as-is (we’ll fall back)
  [string]$FallbackImage  = "tensorflow/magenta",
  [int]   $HostPort       = 9077,
  [string]$ProjectRoot    = "$PSScriptRoot",
  [string]$CheckpointDir  = "$PSScriptRoot\onsets-frames"
)

$ErrorActionPreference = "Stop"
function Log($m){ Write-Host $m }

Write-Host "=== start_oaf.ps1 ==="
[pscustomobject]@{
  ContainerName  = $ContainerName
  PreferredImage = $PreferredImage
  FallbackImage  = $FallbackImage
  HostPort       = $HostPort
  ProjectRoot    = $ProjectRoot
  CheckpointDir  = $CheckpointDir
} | Format-List

# 0) Warm up WSL + confirm the project is visible (auto-check each run)
$wslProject = ""
try { $wslProject = (wsl.exe wslpath -a -u "$ProjectRoot" 2>$null).Trim() } catch {}
if (-not $wslProject) {
  Write-Warning "WSL could not convert ProjectRoot to a Linux path. Attempting a warm-up..."
  try { wsl.exe -e sh -lc "pwd" | Out-Null } catch {}
  try { $wslProject = (wsl.exe wslpath -a -u "$ProjectRoot" 2>$null).Trim() } catch {}
}
if ($wslProject) {
  try { wsl.exe -e sh -lc "ls -ld '$wslProject'" | Out-Null } catch {
    Write-Warning "WSL conversion still failed. Docker bind mounts may not work until file sharing is enabled."
  }
} else {
  Write-Warning "WSL conversion still failed. Docker bind mounts may not work until file sharing is enabled."
}

# 1) If already running, we’re done
$existing = (& docker ps --filter "name=$ContainerName" --format "{{.Names}}" 2>$null)
if ($LASTEXITCODE -eq 0 -and $existing) {
  Log "Container already running: $existing"
  exit 0
}

# Helper: run docker and surface its output clearly
function Invoke-Docker { param([string[]]$Args)
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = "docker"
  $psi.Arguments = ($Args -join ' ')
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute = $false
  $p = [System.Diagnostics.Process]::Start($psi)
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  if ($out) { Write-Host $out }
  if ($err) { Write-Host $err -ForegroundColor DarkYellow }
  return $p.ExitCode
}

# 2) Try preferred image (expects the image to already expose 9077)
Log "Starting container from preferred image: $PreferredImage on 127.0.0.1:$HostPort:9077 ..."
$rc = Invoke-Docker @("run","-d","--rm","--name",$ContainerName,"-p","127.0.0.1:$HostPort:9077",$PreferredImage)
if ($rc -eq 0) {
  Log "Preferred image started."
  Log "Tailing logs (Ctrl+C to stop tail; container keeps running)..."
  & docker logs -f $ContainerName
  exit 0
}
Write-Warning "Preferred image failed or not present. Trying fallback image: $FallbackImage"

# 3) Fallback image: mount your project + checkpoints and run the Python server
if (-not (Test-Path -LiteralPath $ProjectRoot))   { throw "ProjectRoot does not exist: $ProjectRoot" }
if (-not (Test-Path -LiteralPath $CheckpointDir)) { Write-Warning "Checkpoint dir not found: $CheckpointDir (O&F may fail until checkpoints are present)" }

# Use proper -v syntax with literal paths; Docker handles Windows paths fine.
$rc = Invoke-Docker @(
  "run","-d","--rm",
  "--name",$ContainerName,
  "-p","127.0.0.1:$HostPort:9077",
  "--mount","type=bind,source=$ProjectRoot,target=/data",
  "--mount","type=bind,source=$CheckpointDir,target=/model",
  "-w","/data",
  $FallbackImage, "python","DOCKER_ONSETS_AND_FRAMES_SERVER.py"
)
if ($rc -ne 0) { throw "Failed to start fallback container." }

Log "Fallback image started."
Log "Tailing logs (Ctrl+C to stop tail; container keeps running)..."
& docker logs -f $ContainerName
