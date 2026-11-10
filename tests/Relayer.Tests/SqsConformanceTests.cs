using System.Diagnostics;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging;
using Relayer.AmazonSqs;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class SqsConformanceTests
{
    [Test]
    public async Task LocalStack_passes_standard_and_FIFO_transport_contracts()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(8));
        await using var broker = await SqsBroker.StartAsync(deadline.Token);
        foreach (var scenario in Enum.GetValues<TransportConformanceCase>())
        {
            var fifo = scenario == TransportConformanceCase.OrderingByKey;
            var suite = new TransportConformanceSuite<SqsDelivery, SqsMessageBinding>(
                (tracing, token) => SqsConformanceFixture.CreateAsync(broker, tracing, fifo, token), TimeSpan.FromMinutes(1));
            var result = await suite.RunAsync(scenario, deadline.Token);
            // Specific-message dead-lettering is a core durable-copy fallback, tested in SqsReceiveLocalStackTests.
            await Assert.That(result.Skipped).IsEqualTo(scenario == TransportConformanceCase.DeadLetter);
        }
    }
}

internal sealed class SqsBroker(IContainer container) : IAsyncDisposable
{
    internal string Url => $"http://{container.Hostname}:{container.GetMappedPublicPort(4566)}";
    private bool _paused;
    internal bool IsPaused => _paused;
    internal static async Task<SqsBroker> StartAsync(CancellationToken token)
    {
        var builder = new ContainerBuilder("localstack/localstack:4.10.0").WithEnvironment("SERVICES", "sqs")
            .WithPortBinding(4566, true).WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Ready\\."));
        if (OperatingSystem.IsLinux()) builder = builder.WithPortBinding("20000-29999", "4566");
        var container = builder.Build();
        try { await container.StartAsync(token); return new(container); }
        catch { await container.DisposeAsync(); throw; }
    }
    internal AmazonSQSClient Client() => new(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
    { ServiceURL = Url, AuthenticationRegion = "us-east-1", MaxErrorRetry = 0, Timeout = TimeSpan.FromSeconds(3) });
    internal async Task PauseAsync(CancellationToken token) { await CommandAsync("pause", token); _paused = true; }
    internal async Task ResumeAsync(CancellationToken token) { await CommandAsync("unpause", token); _paused = false; }
    private async Task CommandAsync(string command, CancellationToken token)
    {
        var start = new ProcessStartInfo("docker") { RedirectStandardError = true, RedirectStandardOutput = true };
        start.ArgumentList.Add(command); start.ArgumentList.Add(container.Id);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Cannot start Docker CLI.");
        await process.WaitForExitAsync(token);
        if (process.ExitCode != 0) throw new InvalidOperationException(await process.StandardError.ReadToEndAsync(token));
    }
    public async ValueTask DisposeAsync()
    {
        try { if (_paused) await ResumeAsync(CancellationToken.None); }
        finally { await container.DisposeAsync(); }
    }
}

internal sealed class SqsConformanceFixture : ITransportConformanceFixture<SqsDelivery>, ITransportShutdownConformanceFixture
{
    private readonly SqsBroker _broker;
    private readonly AmazonSQSClient _client;
    private readonly SqsSendTransport _transport;
    private readonly SqsSettlement _settlement;
    private readonly ILoggerFactory _logs;
    private string _url = "";
    private int _settlements;
    private int _closed;
    public ConformanceLogCapture Logs { get; } = new();
    public string Destination { get; }
    public ITransport Transport => _transport;
    public string MessagingSystem => "aws_sqs";
    public string SettlementOperation => "delete";
    public HeaderNames HeaderNames { get; } = new();
    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(5);
    public bool IsExpectedOutageFailure(Exception error) => error is AmazonServiceException or HttpRequestException or IOException or TimeoutException or TaskCanceledException;

    private SqsConformanceFixture(SqsBroker broker, MessageTracing tracing, bool fifo)
    {
        _broker = broker;
        _client = broker.Client();
        _logs = LoggerFactory.Create(builder => builder.AddProvider(Logs));
        Destination = TestContext.Current!.Isolation.GetIsolatedName("conformance-" + Guid.NewGuid().ToString("N")[..8]) + (fifo ? ".fifo" : "");
        var options = new AmazonSqsTransportOptions { SqsClient = _client };
        options.Queues.Add(new(Destination) { Attributes = fifo ? new() { ["FifoQueue"] = "true" } : [] });
        _transport = new("sqs", _client, false, options, tracing, _logs);
        _settlement = new(tracing, new() { System = "aws_sqs", Destination = Destination });
    }
    internal static async ValueTask<ITransportConformanceFixture<SqsDelivery>> CreateAsync(SqsBroker broker, MessageTracing tracing, bool fifo, CancellationToken token)
    {
        var fixture = new SqsConformanceFixture(broker, tracing, fifo);
        try
        {
            await fixture._transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            fixture._url = fixture._transport.GetUrl(fixture.Destination);
            return fixture;
        }
        catch { await fixture.DisposeAsync(); throw; }
    }
    public void RegisterConsumer(Func<SqsDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        var declaration = new ConsumerDefinition<AmazonSqsConsumerOptions>(Destination,
            new() { ConfigureReceive = request => request.WaitTimeSeconds = 1 }, [], 2, 1, TimeSpan.Zero);
        _transport.Consumers.Add(new(declaration, _client, _url,
            async (delivery, token) => { await receive(delivery, token); }, logger: _logs.CreateLogger<SqsConsumer>()));
    }
    public async ValueTask SettleAsync(SqsDelivery delivery, Disposition disposition, CancellationToken token)
    {
        if (Volatile.Read(ref _closed) != 0) throw new InvalidOperationException("Settlement after shutdown.");
        await _settlement.SettleAsync(delivery, disposition, token);
        Interlocked.Increment(ref _settlements);
    }
    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token) => throw new NotSupportedException();
    public Task DisconnectAsync(CancellationToken token) => _broker.PauseAsync(token);
    public async Task ReconnectAsync(CancellationToken token)
    {
        await _broker.ResumeAsync(token);
        await _client.GetQueueAttributesAsync(_url, ["QueueArn"], token);
    }
    public async ValueTask VerifyShutdownAsync(CancellationToken token)
    {
        await Assert.That(Volatile.Read(ref _settlements)).IsEqualTo(1);
        Volatile.Write(ref _closed, 1);
        var attributes = await _client.GetQueueAttributesAsync(_url, ["ApproximateNumberOfMessagesNotVisible"], token);
        await Assert.That(attributes.Attributes["ApproximateNumberOfMessagesNotVisible"]).IsEqualTo("0");
    }
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_broker.IsPaused) await _broker.ResumeAsync(CancellationToken.None);
            await _transport.DisposeAsync();
            if (_url.Length != 0) await _client.DeleteQueueAsync(_url);
        }
        finally { _client.Dispose(); _logs.Dispose(); }
    }
}
