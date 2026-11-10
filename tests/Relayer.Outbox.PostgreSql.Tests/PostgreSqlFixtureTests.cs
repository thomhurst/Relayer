using System.Diagnostics;

namespace Relayer.Outbox.PostgreSql.Tests;

public class PostgreSqlFixtureTests
{
    [Test]
    [Arguments("run")]
    [Arguments("port")]
    [Arguments("rm")]
    public async Task Blocked_commands_time_out_and_stop_the_owned_process_tree(string command)
    {
        using var shim = new BlockingCommandShim();
        var docker = new DockerCommand(TimeSpan.FromSeconds(3), _ => shim.StartInfo());
        var exception = await Assert.ThrowsAsync<TimeoutException>(() =>
            // Only a backstop against a hang: killing the process tree alone can take over ten seconds on macOS.
            docker.RunAsync([command, "owned-container"]).WaitAsync(TimeSpan.FromSeconds(60)));

        await Assert.That(exception!.Message).Contains($"docker {command} owned-container");
        var ids = File.ReadAllLines(shim.PidFile).Select(int.Parse).ToArray();
        await Assert.That(ids.Length).IsEqualTo(2);
        foreach (var id in ids)
        {
            try
            {
                using var process = Process.GetProcessById(id);
                await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (ArgumentException) { } // Already reaped.
        }
    }

    [Test]
    [Arguments("run")]
    [Arguments("port")]
    public async Task Failed_startup_retains_ownership_and_cleanup_does_not_replace_the_failure(string blockedCommand)
    {
        var primary = new TimeoutException($"docker {blockedCommand} timed out");
        var cleanup = new TimeoutException("docker rm timed out");
        var calls = new List<string[]>();
        var failures = new List<Exception>();
        var fixture = new PostgreSqlServer(args =>
        {
            calls.Add(args);
            if (args[0] == "rm") return Task.FromException<string>(cleanup);
            return args[0] == blockedCommand ? Task.FromException<string>(primary) : Task.FromResult("container-id");
        }, failures.Add);

        var observed = await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await using (fixture) await fixture.StartAsync();
        });

        await Assert.That(observed).IsSameReferenceAs(primary);
        var name = (string)primary.Data["DockerContainerName"]!;
        await Assert.That(name).StartsWith("relayer-schema-test-");
        await Assert.That(calls[^1]).IsEquivalentTo(new[] { "rm", "--force", "--volumes", name });
        await Assert.That(failures.Count).IsEqualTo(1);
        await Assert.That(failures[0].Message).Contains($"docker rm --force --volumes {name}");
        await Assert.That(failures[0].InnerException).IsSameReferenceAs(cleanup);
        await fixture.DisposeAsync();
        await Assert.That(calls.Count(args => args[0] == "rm")).IsEqualTo(1);
    }

    [Test]
    public async Task Cleanup_failure_does_not_replace_an_exception_from_the_test_body()
    {
        var primary = new InvalidOperationException("test body failed");
        var failures = new List<Exception>();
        var fixture = new PostgreSqlServer(args => args[0] == "rm"
            ? Task.FromException<string>(new TimeoutException("docker rm timed out"))
            : Task.FromException<string>(new TimeoutException("docker run timed out")), failures.Add);
        // A failed run may still have created the named container.
        await Assert.ThrowsAsync<TimeoutException>(() => fixture.StartAsync());
        var observed = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using (fixture) throw primary;
        });
        await Assert.That(observed).IsSameReferenceAs(primary);
        await Assert.That(failures.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Dispose_drops_the_database_and_those_created_under_its_name()
    {
        var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        var admin = (await PostgreSqlSession.Current).Admin;
        await using (var create = admin.CreateCommand($"CREATE DATABASE {fixture.Database}_tenant")) await create.ExecuteNonQueryAsync();
        await using var count = admin.CreateCommand("SELECT count(*) FROM pg_database WHERE starts_with(datname, $1)");
        count.Parameters.AddWithValue(fixture.Database);
        await Assert.That((long)(await count.ExecuteScalarAsync())!).IsEqualTo(2L);

        await fixture.DisposeAsync();

        await Assert.That((long)(await count.ExecuteScalarAsync())!).IsEqualTo(0L);
    }

    [Test]
    public async Task Disposing_an_unused_fixture_does_not_invoke_Docker()
    {
        await using var fixture = new PostgreSqlServer(_ => throw new InvalidOperationException("Unexpected Docker invocation"), _ => { });
    }

    [Test]
    public async Task Successful_cleanup_removes_only_the_owned_container()
    {
        var calls = new List<string[]>();
        var fixture = new PostgreSqlServer(args =>
        {
            calls.Add(args);
            return args[0] == "run" ? Task.FromException<string>(new TimeoutException("startup")) : Task.FromResult("");
        }, _ => throw new InvalidOperationException("Unexpected cleanup failure"));
        await Assert.ThrowsAsync<TimeoutException>(() => fixture.StartAsync());
        await fixture.DisposeAsync();
        var nameIndex = Array.IndexOf(calls[0], "--name") + 1;
        await Assert.That(calls[1]).IsEquivalentTo(new[] { "rm", "--force", "--volumes", calls[0][nameIndex] });
    }

    [Test]
    public async Task Cleanup_failure_is_reported_by_the_TUnit_hook()
    {
        var context = TestContext.Current!;
        var failure = new TimeoutException("docker rm --force owned-container");
        context.StateBag.GetOrAdd("PostgreSqlFixture.CleanupFailures", static _ => new List<Exception>()).Add(failure);
        try
        {
            var observed = Assert.Throws<AggregateException>(() => PostgreSqlFixtureCleanup.ReportFailures(context));
            await Assert.That(observed!.InnerExceptions[0]).IsSameReferenceAs(failure);
        }
        finally
        {
            context.StateBag.TryRemove("PostgreSqlFixture.CleanupFailures", out _);
        }
    }
    [Test]
    [Arguments(0)]
    [Arguments(17)]
    public async Task Command_output_and_nonzero_exit_diagnostics_are_preserved(int exitCode)
    {
        var docker = new DockerCommand(createStartInfo: _ =>
        {
            var start = new ProcessStartInfo(OperatingSystem.IsWindows() ? "powershell.exe" : "/bin/sh");
            if (OperatingSystem.IsWindows())
            {
                start.ArgumentList.Add("-NoProfile");
                start.ArgumentList.Add("-NonInteractive");
                start.ArgumentList.Add("-Command");
                start.ArgumentList.Add($"[Console]::Out.Write('command-output'); [Console]::Error.Write('command-error'); exit {exitCode}");
            }
            else
            {
                start.ArgumentList.Add("-c");
                start.ArgumentList.Add($"printf command-output; printf command-error >&2; exit {exitCode}");
            }
            return start;
        });
        if (exitCode == 0)
            await Assert.That(await docker.RunAsync(["port", "owned-container", "5432/tcp"])).IsEqualTo("command-output");
        else
        {
            var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => docker.RunAsync(["port", "owned-container", "5432/tcp"]));
            await Assert.That(failure!.Message).Contains("docker port owned-container 5432/tcp failed (17): command-error");
        }
    }
    private sealed class BlockingCommandShim : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "relayer-docker-shim-" + Guid.NewGuid().ToString("N"));
        internal string PidFile => Path.Combine(_directory, "pids");

        internal BlockingCommandShim() => Directory.CreateDirectory(_directory);

        internal ProcessStartInfo StartInfo()
        {
            var start = new ProcessStartInfo(OperatingSystem.IsWindows() ? "powershell.exe" : "/bin/sh");
            if (OperatingSystem.IsWindows())
            {
                start.ArgumentList.Add("-NoProfile");
                start.ArgumentList.Add("-NonInteractive");
                start.ArgumentList.Add("-Command");
                start.ArgumentList.Add("$child = Start-Process powershell.exe -ArgumentList '-NoProfile -NonInteractive -Command [Threading.Thread]::Sleep(-1)' -WindowStyle Hidden -PassThru; @($PID, $child.Id) | Set-Content -LiteralPath $env:RELAYER_SHIM_PIDS; [Console]::Out.WriteLine('output'); [Console]::Error.WriteLine('error'); [Threading.Thread]::Sleep(-1)");
            }
            else
            {
                start.ArgumentList.Add("-c");
                start.ArgumentList.Add("sleep 600 & child=$!; printf '%s\\n%s\\n' $$ $child > \"$RELAYER_SHIM_PIDS\"; echo output; echo error >&2; wait");
            }
            start.Environment["RELAYER_SHIM_PIDS"] = PidFile;
            return start;
        }

        public void Dispose()
        {
            if (File.Exists(PidFile))
            {
                foreach (var line in File.ReadAllLines(PidFile))
                {
                    try
                    {
                        using var process = Process.GetProcessById(int.Parse(line));
                        process.Kill(entireProcessTree: true);
                    }
                    catch (ArgumentException) { }
                    catch (InvalidOperationException) { }
                }
            }
            Directory.Delete(_directory, recursive: true);
        }
    }
}
