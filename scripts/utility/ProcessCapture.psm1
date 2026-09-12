#requires -Version 7

# Captures native command output for callers such as scheduled validation without choosing
# command semantics. Arguments and inherited environment reach the same executable a user runs.
# Ref: docs/build-and-tooling.md#validating-changes.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Invoke-CapturedProcess {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [Parameter(Mandatory)][string] $FilePath,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $ArgumentList,
        [Parameter(Mandatory)][string] $WorkingDirectory,
        [Parameter(Mandatory)][string] $StandardOutputPath,
        [Parameter(Mandatory)][string] $StandardErrorPath
    )

    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $FilePath
    $start.WorkingDirectory = $WorkingDirectory
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $ArgumentList) { $start.ArgumentList.Add($argument) }
    $process = Get-CaptureProcess
    $process.StartInfo = $start
    $stdout = $null
    $stderr = $null
    $started = $false
    try {
        $stdout = [IO.File]::Create($StandardOutputPath)
        $stderr = [IO.File]::Create($StandardErrorPath)
        $started = $process.Start()
        if (-not $started) { throw 'Could not start the command.' }
        $copyOut = $process.StandardOutput.BaseStream.CopyToAsync($stdout)
        $copyErr = $process.StandardError.BaseStream.CopyToAsync($stderr)
        # A failed reader can leave its child blocked on a full pipe. Observe capture faults
        # while the process is running, and let finally terminate the process tree on error.
        $pending = [Collections.Generic.List[Threading.Tasks.Task]]::new(
            [Threading.Tasks.Task[]]@($copyOut, $copyErr, $process.WaitForExitAsync()))
        while ($pending.Count -gt 0) {
            $completed = [Threading.Tasks.Task]::WhenAny([Threading.Tasks.Task[]]$pending).GetAwaiter().GetResult()
            $null = $completed.GetAwaiter().GetResult()
            $null = $pending.Remove($completed)
        }
        return $process.ExitCode
    } finally {
        try {
            if ($started -and -not $process.HasExited) {
                try { $process.Kill($true) }
                catch [InvalidOperationException] {
                    # The child can exit between HasExited and Kill.
                    if (-not $process.HasExited) { throw }
                }
                $process.WaitForExit()
            }
        } finally {
            if ($null -ne $stdout) { $stdout.Dispose() }
            if ($null -ne $stderr) { $stderr.Dispose() }
            $process.Dispose()
        }
    }
}

function Get-CaptureProcess {
    # Isolate construction so lifetime/error handling can use deterministic process doubles.
    [CmdletBinding()]
    [OutputType([Diagnostics.Process])]
    param()
    return [Diagnostics.Process]::new()
}

Export-ModuleMember -Function Invoke-CapturedProcess
