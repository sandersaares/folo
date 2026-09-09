#requires -Version 7

# Native process boundaries for the hosted reporter: stream bounded GitHub artifacts/logs and
# exchange UTF-8 JSON with executables built from the trusted controller. PowerShell owns process
# setup and failure reporting; private Rust utilities own the structured evidence logic.
# Ref: ../../.github/workflows/implementation.md#immutable-execution and #serialized-reporting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Save-ScheduledGitHubResponse {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Endpoint,
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][long] $MaxBytes,
        [switch] $Truncate
    )
    if ($Endpoint -cnotmatch '^repos/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/' -or $MaxBytes -le 0) {
        throw [ArgumentException]::new('A repository endpoint and positive response size limit are required.')
    }
    Save-ScheduledProcessResponse -Executable (Get-Command gh -CommandType Application | Select-Object -First 1).Source `
        -Arguments @('api', $Endpoint, '--allow-escape-sequences') -Path $Path -MaxBytes $MaxBytes -Truncate:$Truncate
}

function Save-ScheduledProcessResponse {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Executable,
        [Parameter(Mandatory)][string[]] $Arguments,
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][long] $MaxBytes,
        [switch] $Truncate
    )
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $Executable
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        $start.ArgumentList.Add($argument)
    }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $file = [IO.File]::Open($Path, [IO.FileMode]::CreateNew)
    $started = $false
    try {
        $started = $process.Start()
        if (-not $started) { throw [IO.IOException]::new('Could not start GitHub response download.') }
        $errorTask = $process.StandardError.ReadToEndAsync()
        $buffer = [byte[]]::new(81920) # .NET's normal stream-copy buffer size.
        [long]$length = 0
        $truncated = $false
        while (($count = $process.StandardOutput.BaseStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
            $remaining = $MaxBytes - $length
            if ($count -gt $remaining) {
                if (-not $Truncate) { throw [FormatException]::new('GitHub response exceeds the policy size limit.') }
                if ($remaining -gt 0) { $file.Write($buffer, 0, [int]$remaining) }
                $length = $MaxBytes
                $truncated = $true
                break
            }
            $file.Write($buffer, 0, $count)
            $length += $count
        }
        if ($truncated -and -not $process.HasExited) { $process.Kill($true) }
        $process.WaitForExit()
        $diagnostic = $errorTask.GetAwaiter().GetResult()
        if (-not $truncated -and $process.ExitCode -ne 0) {
            throw [IO.IOException]::new("GitHub response download failed: $diagnostic")
        }
        return @{ bytes = $length; truncated = $truncated }
    } finally {
        if ($started -and -not $process.HasExited) { $process.Kill($true) }
        $file.Dispose()
        $process.Dispose()
    }
}

function Invoke-ScheduledJsonExecutable {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][string] $Executable,
        [Parameter(Mandatory)][string] $Directory,
        [Parameter(Mandatory)][AllowEmptyString()][string] $InputText,
        [string[]] $Arguments = @(),
        [hashtable] $Environment = @{}
    )
    if (-not [IO.Path]::IsPathFullyQualified($Executable) -or
        -not [IO.Path]::IsPathFullyQualified($Directory)) {
        throw [ArgumentException]::new('Controller executable and working directory must be absolute paths.')
    }
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $Executable
    $start.WorkingDirectory = $Directory
    $start.UseShellExecute = $false
    $start.RedirectStandardInput = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    foreach ($key in $Environment.Keys) {
        if ($null -eq $Environment[$key]) { $null = $start.Environment.Remove($key) }
        else { $start.Environment[$key] = $Environment[$key] }
    }
    $start.StandardInputEncoding = [Text.UTF8Encoding]::new($false)
    $start.StandardOutputEncoding = [Text.UTF8Encoding]::new($false)
    $start.StandardErrorEncoding = [Text.UTF8Encoding]::new($false)
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    try {
        if (-not $process.Start()) { throw [IO.IOException]::new('Could not start the controller utility.') }
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $process.StandardInput.Write($InputText)
        $process.StandardInput.Close()
        $process.WaitForExit()
        $json = $stdout.GetAwaiter().GetResult()
        $diagnostic = $stderr.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) {
            throw [FormatException]::new("Controller utility rejected its input: $diagnostic")
        }
        return $json
    } finally {
        $process.Dispose()
    }
}

Export-ModuleMember -Function Save-ScheduledGitHubResponse, Invoke-ScheduledJsonExecutable
