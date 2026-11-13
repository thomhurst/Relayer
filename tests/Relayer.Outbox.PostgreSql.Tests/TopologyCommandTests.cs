using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AmazonSqs;
using Relayer.AzureServiceBus;
using Relayer.Tool;

namespace Relayer.Outbox.PostgreSql.Tests;

public sealed class TopologyCommandTests
{
    private static string[] Command(Type factory, params string[] options) =>
        ["topology", "export", "--assembly", typeof(TopologyCommandTests).Assembly.Location, "--type", factory.FullName!, .. options];

    [Test]
    [Arguments("json")]
    [Arguments("bicep")]
    [Arguments("terraform")]
    public async Task Service_bus_command_preserves_exporter_output_and_is_deterministic(string format)
    {
        var args = Command(typeof(ServiceBusDeclarations), "--format", format);
        if (format != "json") args = [.. args, "--transport", "events"];
        using var provider = (ServiceProvider)ServiceBusDeclarations.CreateTopologyServices();
        using var expected = new MemoryStream();
        if (format == "json") provider.GetRequiredService<ITopology>().ExportJson(expected);
        else provider.GetRequiredService<ServiceBusTopologyExporter>().Export("events", expected,
            format == "bicep" ? ServiceBusTopologyFormat.Bicep : ServiceBusTopologyFormat.Terraform);
        for (var i = 0; i < 2; i++)
        {
            using var output = new StringWriter();
            using var error = new StringWriter();
            var result = await RelayerCommand.RunAsync(args, output, error);
            await Assert.That(error.ToString()).IsEqualTo("");
            await Assert.That(result).IsEqualTo(0);
            await Assert.That(output.ToString()).IsEqualTo(Encoding.UTF8.GetString(expected.ToArray()));
        }
    }

    [Test]
    public async Task Aws_command_preserves_native_queue_topic_and_subscription_export()
    {
        using var provider = (ServiceProvider)AwsDeclarations.CreateTopologyServices();
        using var expected = new MemoryStream();
        provider.GetRequiredService<ITopology>().ExportAmazonTerraform(provider, expected,
            new() { AccountId = "123456789012", Region = "eu-west-1" });
        using var output = new StringWriter();
        using var error = new StringWriter();
        var result = RelayerCommand.Run(Command(typeof(AwsDeclarations), "--format", "terraform", "--account-id", "123456789012", "--region", "eu-west-1"), output, error);
        await Assert.That(error.ToString()).IsEqualTo("");
        await Assert.That(result).IsEqualTo(0);
        await Assert.That(output.ToString()).IsEqualTo(Encoding.UTF8.GetString(expected.ToArray()));
        await Assert.That(output.ToString()).Contains("aws_sns_topic_subscription");
    }

    [Test]
    public async Task Invalid_options_emit_no_artifact()
    {
        string[][] cases = [[], ["--format"], ["--format", "xml"], ["--format", "json", "--format", "json"],
            ["--format", "json", "--transport", "events"], ["--format", "bicep"],
            ["--format", "terraform", "--account-id", "123456789012"],
            ["--format", "bicep", "--account-id", "123456789012", "--region", "eu-west-1"],
            ["--format", "terraform", "--transport", "events", "--region", "eu-west-1", "--account-id", "123456789012"],
            ["--format", "json", "--unknown", "x"], ["--format", "--type"], ["--format", ""],
            ["--format", "json", "--assembly", "repeated.dll"]];
        foreach (var options in cases)
            await Failure(Command(typeof(ServiceBusDeclarations), options));
        await Failure(["topology", "export", "--format", "json"]);
    }

    [Test]
    public async Task Invalid_declarations_fail_without_partial_output()
    {
        await Failure(Command(typeof(ServiceBusDeclarations), "--format", "bicep", "--transport", "missing"));
        await Failure(Command(typeof(InvalidDeclarations), "--format", "bicep", "--transport", "events"));
        await Failure(Command(typeof(ThrowingDeclarations), "--format", "json"), "factory failed");
        await Failure(Command(typeof(WrongFactory), "--format", "json"), "CreateTopologyServices");
        await Failure(Command(typeof(DisposalFailureDeclarations), "--format", "json"), "dispose failed");
        await Failure(Command(typeof(MissingFactory), "--format", "json"), "CreateTopologyServices");
        await Failure(Command(typeof(NullDeclarations), "--format", "json"), "returned null");
        await Failure(Command(typeof(EmptyDeclarations), "--format", "json"), "ITopology");
        await Failure(Command(typeof(ServiceBusDeclarations), "--format", "terraform", "--account-id", "123456789012", "--region", "eu-west-1"), "not SQS/SNS");
        await Failure(Command(typeof(AwsDeclarations), "--format", "terraform", "--account-id", "invalid", "--region", "eu-west-1"), "twelve digits");
        await Failure(["topology", "export", "--assembly", "missing.dll", "--type", "Missing", "--format", "json"]);
        await Failure(["topology", "export", "--assembly", typeof(TopologyCommandTests).Assembly.Location, "--type", "Missing", "--format", "json"]);
    }

    [Test]
    public async Task Managed_tool_runs_the_documented_contract_in_a_separate_process()
    {
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(typeof(RelayerCommand).Assembly.Location);
        foreach (var arg in Command(typeof(ServiceBusDeclarations), "--format", "bicep", "--transport", "events")) start.ArgumentList.Add(arg);
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        await Assert.That(await error).IsEqualTo("");
        await Assert.That(process.ExitCode).IsEqualTo(0);
        await Assert.That(await output).Contains("Microsoft.ServiceBus/namespaces/topics/subscriptions");
    }

    private static async Task Failure(string[] args, string? message = null)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        await Assert.That(RelayerCommand.Run(args, output, error)).IsEqualTo(1);
        await Assert.That(output.ToString()).IsEqualTo("");
        await Assert.That(error.ToString().Length).IsGreaterThan(0);
        if (message is not null) await Assert.That(error.ToString()).Contains(message);
    }
}

// No factory starts a host or resolves a broker client. Invalid connection settings make accidental use fail.
public static class ServiceBusDeclarations
{
    public static IServiceProvider CreateTopologyServices() => Create(false);
    internal static ServiceProvider Create(bool invalid)
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.ServiceBus("events", t => t.Options.Configure(o =>
        {
            o.ConnectionString = "not a connection string";
            o.Queues.Add(new("orders"));
            if (invalid) o.Queues.Add(new("orders"));
            o.Topics.Add(new("order-events"));
            o.Subscriptions.Add(new("order-events", "billing") { MaxDeliveryCount = 5 });
        })));
        return services.BuildServiceProvider();
    }
}
public static class AwsDeclarations
{
    public static IServiceProvider CreateTopologyServices()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.AmazonSns("events", t => t.Options.Configure(o =>
        {
            o.QueueTopology.Queues.Add(new() { QueueName = "orders" });
            o.Topics.Add(new() { Name = "order-events" });
            o.Subscriptions.Add(new()
            {
                TopicArn = "arn:aws:sns:eu-west-1:123456789012:order-events", Protocol = "sqs",
                Endpoint = "arn:aws:sqs:eu-west-1:123456789012:orders",
                Attributes = new() { ["RawMessageDelivery"] = "true" }
            });
        })));
        return services.BuildServiceProvider();
    }
}
public static class InvalidDeclarations { public static IServiceProvider CreateTopologyServices() => ServiceBusDeclarations.Create(true); }
public static class ThrowingDeclarations { public static IServiceProvider CreateTopologyServices() => throw new InvalidOperationException("factory failed"); }
public static class MissingFactory { }
public static class NullDeclarations { public static IServiceProvider CreateTopologyServices() => null!; }
public static class EmptyDeclarations { public static IServiceProvider CreateTopologyServices() => new ServiceCollection().BuildServiceProvider(); }

public static class WrongFactory { public static ServiceProvider CreateTopologyServices() => new ServiceCollection().BuildServiceProvider(); }
public static class DisposalFailureDeclarations
{
    public static IServiceProvider CreateTopologyServices() => new FailingProvider();
    private sealed class FailingProvider : IServiceProvider, IDisposable
    {
        private readonly IServiceProvider _inner = ServiceBusDeclarations.CreateTopologyServices();
        public object? GetService(Type serviceType) => _inner.GetService(serviceType);
        public void Dispose()
        {
            ((IDisposable)_inner).Dispose();
            throw new InvalidOperationException("dispose failed");
        }
    }
}
