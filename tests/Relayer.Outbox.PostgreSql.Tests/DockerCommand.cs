using System.Diagnostics;

namespace Relayer.Outbox.PostgreSql.Tests;

internal sealed class DockerCommand
{
    private readonly TimeSpan _timeout;
    private readonly Func<string[], ProcessStartInfo> _createStartInfo;

    internal DockerCommand(TimeSpan? timeout = null, Func<string[], ProcessStartInfo>? createStartInfo = null)
    {
        _timeout = timeout ?? TimeSpan.FromSeconds(60);
        _createStartInfo = createStartInfo ?? CreateStartInfo;
    }

    internal async Task<string> RunAsync(string[] args)
    {
        using var timeout = new CancellationTokenSource(_timeout);
        var start = _createStartInfo(args);
        start.UseShellExecute = false;
        start.CreateNoWindow = true;
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        using var process = Process.Start(start) ?? throw new InvalidOperationException($"Could not start docker {args[0]}.");
        var output = process.StandardOutput.ReadToEndAsync(timeout.Token);
        var error = process.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            // Include pipe reads in the deadline: a descendant can inherit a pipe after the CLI exits.
            await process.WaitForExitAsync(timeout.Token);
            await Task.WhenAll(output, error).WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            var failure = new TimeoutException($"docker {string.Join(' ', args)} exceeded {_timeout}.");
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException) { } // The process exited concurrently.
            catch (Exception exception)
            {
                failure.Data["ProcessTerminationFailure"] = exception;
            }
            // Never replace the timeout with a canceled pipe read or wait indefinitely for termination.
            try { await Task.WhenAll(output, error, process.WaitForExitAsync()).WaitAsync(TimeSpan.FromSeconds(5)); }
            catch (OperationCanceledException) { }
            catch (Exception exception) { failure.Data["ProcessShutdownFailure"] = exception; }
            throw failure;
        }
        var errorText = await error;
        if (process.ExitCode != 0) throw new InvalidOperationException($"docker {string.Join(' ', args)} failed ({process.ExitCode}): {errorText}");
        return await output;
    }

    private static ProcessStartInfo CreateStartInfo(string[] args)
    {
        var start = new ProcessStartInfo("docker");
        foreach (var arg in args) start.ArgumentList.Add(arg);
        return start;
    }
}
