using System.Diagnostics;
using System.Net;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging;
using Relayer.AmazonSqs;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class SqsConformanceTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Floci_requires_native_deletion_after_consumer_restart(bool suppressSecondDelete)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        // The session owns this broker. This case only changes its own queue.
        var broker = new SqsBroker(await SharedBrokers.Floci.Value.WaitAsync(deadline.Token));
        var suite = new TransportConformanceSuite<SqsDelivery, SqsMessageBinding>(
            (tracing, token) => SqsConformanceFixture.CreateAsync(broker, tracing, false, token, suppressSecondDelete));

        if (suppressSecondDelete)
        {
            var failure = await Assert.That(async () =>
            {
                await suite.RunAsync(TransportConformanceCase.SendReceiveSettle, deadline.Token);
            }).Throws<InvalidOperationException>();
            await Assert.That(failure!.Message).Contains("native settlement verification");
            await Assert.That(failure.InnerException!.Message).Contains("SQS queue still contains unsettled messages");
        }
        else
        {
            await Assert.That((await suite.RunAsync(TransportConformanceCase.SendReceiveSettle, deadline.Token)).Skipped).IsFalse();
        }
    }

    [Test]
    public async Task Floci_passes_standard_and_FIFO_transport_contracts()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(8));
        await using var broker = await SqsBroker.StartAsync(deadline.Token);

        var fifo = new TransportConformanceSuite<SqsDelivery, SqsMessageBinding>(
            (tracing, token) => SqsConformanceFixture.CreateAsync(broker, tracing, true, token), TimeSpan.FromMinutes(1));
        await Assert.That((await fifo.RunAsync(TransportConformanceCase.SendReceiveSettle, deadline.Token)).Skipped).IsFalse();
        await Assert.That((await fifo.RunAsync(TransportConformanceCase.SendDuringOutage, deadline.Token)).Skipped).IsFalse();
        await Assert.That((await fifo.RunAsync(TransportConformanceCase.OrderingByKey, deadline.Token)).Skipped).IsFalse();

        var standard = new TransportConformanceSuite<SqsDelivery, SqsMessageBinding>(
            (tracing, token) => SqsConformanceFixture.CreateAsync(broker, tracing, false, token), TimeSpan.FromMinutes(1));

        foreach (var scenario in Enum.GetValues<TransportConformanceCase>())
        {
            var result = await standard.RunAsync(scenario, deadline.Token);
            // Specific-message dead-lettering is a core durable-copy fallback, tested in SqsReceiveFlociTests.
            await Assert.That(result.Skipped).IsEqualTo(scenario is TransportConformanceCase.DeadLetter or TransportConformanceCase.OrderingByKey);
        }
    }
}

internal sealed class SqsBroker(IContainer container) : IAsyncDisposable
{
    internal string Url => SharedBrokers.GetFlociUrl(container);
    private bool _paused;

    internal bool IsPaused => _paused;

    internal static async Task<SqsBroker> StartAsync(CancellationToken token)
    {
        var container = SharedBrokers.CreateFlociContainer();

        try
        {
            await container.StartAsync(token);
            return new(container);
        }
        catch
        {
            await container.DisposeAsync();
            throw;
        }
    }

    internal AmazonSQSClient Client(bool suppressSecondDelete = false)
    {
        var credentials = new BasicAWSCredentials("test", "test");
        var config = new AmazonSQSConfig
        {
            ServiceURL = Url,
            AuthenticationRegion = "us-east-1",
            MaxErrorRetry = 0,
            Timeout = TimeSpan.FromSeconds(3)
        };
        return suppressSecondDelete ? new MissingDeleteClient(credentials, config) : new AmazonSQSClient(credentials, config);
    }

    private sealed class MissingDeleteClient(AWSCredentials credentials, AmazonSQSConfig config) : AmazonSQSClient(credentials, config)
    {
        private int _deletes;

        public override Task<DeleteMessageResponse> DeleteMessageAsync(DeleteMessageRequest request, CancellationToken cancellationToken = default)
        {
            return Interlocked.Increment(ref _deletes) == 2
                ? Task.FromResult(new DeleteMessageResponse { HttpStatusCode = HttpStatusCode.OK })
                : base.DeleteMessageAsync(request, cancellationToken);
        }
    }

    internal async Task PauseAsync(CancellationToken token)
    {
        await CommandAsync("pause", token);
        _paused = true;
    }

    internal async Task ResumeAsync(CancellationToken token)
    {
        await CommandAsync("unpause", token);
        _paused = false;
    }

    private async Task CommandAsync(string command, CancellationToken token)
    {
        var start = new ProcessStartInfo("docker") { RedirectStandardError = true, RedirectStandardOutput = true };
        start.ArgumentList.Add(command);
        start.ArgumentList.Add(container.Id);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Cannot start Docker CLI.");
        await process.WaitForExitAsync(token);

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(await process.StandardError.ReadToEndAsync(token));
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_paused)
            {
                await ResumeAsync(CancellationToken.None);
            }
        }
        finally
        {
            await container.DisposeAsync();
        }
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

    public ConformanceLogCapture Logs
    {
        get;
    } = new();

    public string Destination
    {
        get;
    }

    public ITransport Transport => _transport;

    public string MessagingSystem => "aws_sqs";

    public string SettlementOperation => "delete";

    public HeaderNames HeaderNames
    {
        get;
    } = new();

    public WireString? DefaultOrderingKey
    {
        get;
    }

    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(5);

    public bool IsExpectedOutageFailure(Exception error)
    {
        return error is AmazonServiceException or HttpRequestException or IOException or TimeoutException or TaskCanceledException;
    }

    private SqsConformanceFixture(SqsBroker broker, MessageTracing tracing, bool fifo, bool suppressSecondDelete)
    {
        _broker = broker;
        _client = broker.Client(suppressSecondDelete);
        _logs = LoggerFactory.Create(builder => builder.AddProvider(Logs));
        Destination = TestContext.Current!.Isolation.GetIsolatedName("conformance-" + Guid.NewGuid().ToString("N")[..8]) + (fifo ? ".fifo" : "");
        DefaultOrderingKey = fifo ? new WireString("conformance") : null;
        var options = new AmazonSqsTransportOptions { SqsClient = _client };
        // A short visibility timeout keeps the redelivery scenarios from waiting out SQS's 30 s default.
        options.Queues.Add(new(Destination)
        {
            Attributes = fifo
            ? new()
            {
                ["FifoQueue"] = "true",
                ["VisibilityTimeout"] = "3"
            }
            : new()
            {
                ["VisibilityTimeout"] = "3"
            }
        });
        _transport = new("sqs", _client, false, options, tracing, _logs);
        _settlement = new(tracing, new()
        {
            System = "aws_sqs",
            Destination = Destination
        });
    }

    internal static async ValueTask<ITransportConformanceFixture<SqsDelivery>> CreateAsync(SqsBroker broker, MessageTracing tracing, bool fifo, CancellationToken token, bool suppressSecondDelete = false)
    {
        var fixture = new SqsConformanceFixture(broker, tracing, fifo, suppressSecondDelete);

        try
        {
            await fixture._transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            fixture._url = fixture._transport.GetUrl(fixture.Destination);
            return fixture;
        }
        catch
        {
            await fixture.DisposeAsync();
            throw;
        }
    }

    public void RegisterConsumer(Func<SqsDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        var declaration = new ConsumerDefinition<AmazonSqsConsumerOptions>(Destination,
            new()
            {
                ConfigureReceive = request => request.WaitTimeSeconds = 1
            }, [], 2, 1, TimeSpan.Zero);
        _transport.Consumers.Add(new(declaration, _client, _url,
            async (delivery, token) =>
            await receive(delivery, token), logger: _logs.CreateLogger<SqsConsumer>()));
    }

    public async ValueTask SettleAsync(SqsDelivery delivery, Disposition disposition, CancellationToken token)
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new InvalidOperationException("Settlement after shutdown.");
        }

        await _settlement.SettleAsync(delivery, disposition, token);
        Interlocked.Increment(ref _settlements);
    }

    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token)
    {
        throw new NotSupportedException();
    }

    public Task DisconnectAsync(CancellationToken token)
    {
        return _broker.PauseAsync(token);
    }

    public async Task ReconnectAsync(CancellationToken token)
    {
        await _broker.ResumeAsync(token);
        await _client.GetQueueAttributesAsync(_url, ["QueueArn"], token);
    }

    public async ValueTask VerifyShutdownAsync(CancellationToken token)
    {
        await Assert.That(Volatile.Read(ref _settlements)).IsEqualTo(1);
        Volatile.Write(ref _closed, 1);
        await VerifySettlementAsync(token);
    }

    public async ValueTask VerifySettlementAsync(CancellationToken token)
    {
        const string Visible = "ApproximateNumberOfMessages";
        const string InFlight = "ApproximateNumberOfMessagesNotVisible";
        var response = await _client.GetQueueAttributesAsync(_url, [Visible, InFlight], token);
        var ready = response.Attributes[Visible];
        var leased = response.Attributes[InFlight];

        if (ready != "0" || leased != "0")
        {
            throw new InvalidOperationException($"SQS queue still contains unsettled messages: {ready} visible, {leased} in flight.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_broker.IsPaused)
            {
                await _broker.ResumeAsync(CancellationToken.None);
            }

            await _transport.DisposeAsync();

            if (_url.Length != 0)
            {
                await _client.DeleteQueueAsync(_url);
            }
        }
        finally
        {
            _client.Dispose();
            _logs.Dispose();
        }
    }
}
