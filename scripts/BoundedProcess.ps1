# Run a CLI with a deadline covering both exit and redirected pipe completion.
# Like tests/Shared/DockerCommand.cs, drain both pipes concurrently and bound cleanup.
function Invoke-BoundedProcess {
    param(
        [Parameter(Mandatory)] [string]$FilePath,
        [string[]]$Arguments = @(),
        [ValidateRange(1, 600)] [int]$TimeoutSeconds = 30
    )

    $start = [Diagnostics.ProcessStartInfo]::new($FilePath)
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $deadline = [Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds($TimeoutSeconds))
    try {
        if (-not $process.Start()) { throw 'Could not start subprocess' }
        $output = $process.StandardOutput.ReadToEndAsync($deadline.Token)
        $errorOutput = $process.StandardError.ReadToEndAsync($deadline.Token)
        $completion = [Threading.Tasks.Task]::WhenAll([Threading.Tasks.Task[]]@($process.WaitForExitAsync($deadline.Token), $output, $errorOutput))
        try { $completion.WaitAsync($deadline.Token).GetAwaiter().GetResult() }
        catch {
            if (-not $deadline.IsCancellationRequested) { throw }
            try { $process.Kill($true) } catch { } # May have exited concurrently.
            # A descendant can hold a pipe open after the CLI exits. Neither pipe
            # drainage nor process termination may turn a timeout into another hang.
            try { $process.WaitForExitAsync().WaitAsync([TimeSpan]::FromSeconds(5)).GetAwaiter().GetResult() } catch { }
            throw "subprocess exceeded ${TimeoutSeconds}s deadline"
        }
        return [pscustomobject]@{
            ExitCode = $process.ExitCode
            Output = $output.GetAwaiter().GetResult()
            ErrorOutput = $errorOutput.GetAwaiter().GetResult()
        }
    }
    finally {
        $deadline.Dispose()
        $process.Dispose()
    }
}
