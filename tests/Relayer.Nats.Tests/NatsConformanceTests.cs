using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public class NatsConformanceTests
{
    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.Retry)]
    [Arguments(TransportConformanceCase.DeadLetter)]
    [Arguments(TransportConformanceCase.Delay)]
    [Arguments(TransportConformanceCase.RedeliveryDelay)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.Reconnect)]
    [Arguments(TransportConformanceCase.SendDuringOutage)]
    [Arguments(TransportConformanceCase.CleanShutdown)]
    public async Task Nats_passes_or_explicitly_skips_unsupported_capability(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<NatsDelivery, NatsBinding>((tracing, token) => NatsFixture.CreateAsync(tracing, scenario is TransportConformanceCase.Reconnect or TransportConformanceCase.SendDuringOutage, token), TimeSpan.FromMinutes(2));
        var result = await suite.RunAsync(scenario);
        await Assert.That(result.Skipped).IsEqualTo(scenario is TransportConformanceCase.Retry or TransportConformanceCase.DeadLetter
            or TransportConformanceCase.Delay or TransportConformanceCase.RedeliveryDelay or TransportConformanceCase.OrderingByKey);
        if (result.Skipped) await Assert.That(result.SkipReason).IsNotNull();
    }
}

internal sealed class NatsFixture : ITransportConformanceFixture<NatsDelivery>, ITransportShutdownConformanceFixture
{
    private readonly NatsTopologyTests.Broker _broker;
    private readonly bool _ownsBroker;
    private readonly string _stream = TestContext.Current!.Isolation.GetIsolatedName("events");
    private readonly string _subject = TestContext.Current!.Isolation.GetIsolatedName("orders");
    private readonly ServiceProvider _provider;
    private readonly ILoggerFactory _logs;
    private readonly NatsConnection _admin;
    private readonly NatsJSContext _js;
    private readonly ActivityListener _nativeInstrumentation;
    private NatsConsumerWorker? _worker;
    private INatsConnection? _receiver;
    private readonly TaskCompletionSource _disconnected = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _reopened = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal NatsNativeTransport Adapter { get; }
    public ITransport Transport => Adapter;
    public string Destination => _subject;
    public string ProcessDestinationName => _stream;
    public string MessagingSystem => "nats";
    public string SettlementOperation => "ack";
    public HeaderNames HeaderNames { get; } = new("ce-");
    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(5);
    public ConformanceLogCapture Logs { get; } = new();
    public bool IsExpectedOutageFailure(Exception error) => error is NatsException or TimeoutException;

    private NatsFixture(NatsTopologyTests.Broker broker, MessageTracing tracing, bool ownsBroker)
    {
        _broker = broker;
        _ownsBroker = ownsBroker;
        _logs = LoggerFactory.Create(builder => builder.AddProvider(Logs).AddSimpleConsole().SetMinimumLevel(LogLevel.Debug));
        var options = broker.Options() with { LoggerFactory = _logs };
        _admin = new(options);
        _js = new(_admin);
        _admin.ConnectionDisconnected += (_, _) => { _disconnected.TrySetResult(); return default; };
        _admin.ConnectionOpened += (_, _) => { if (_disconnected.Task.IsCompleted) _reopened.TrySetResult(); return default; };
        _nativeInstrumentation = new()
        {
            ShouldListenTo = source => source.Name == NatsTelemetry.SourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(_nativeInstrumentation);
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(Logs));
        services.AddRelayer(builder => builder.Nats("conformance", transport =>
        {
            transport.Options.Configure(settings =>
            {
                settings.ConnectionOptions = options;
                settings.Streams.Add(new(_stream) { Configuration = new(_stream, [_subject]) });
                settings.Consumers.Add(new(_stream, "orders") { Configuration = new("orders") { FilterSubject = _subject } });
            });
            transport.Send<NatsConfigurationTests.Sample>(_subject);
        }));
        services.AddSingleton(tracing);
        _provider = services.BuildServiceProvider();
        Adapter = (NatsNativeTransport)_provider.GetRequiredService<ITransport>();
    }
    internal static async ValueTask<ITransportConformanceFixture<NatsDelivery>> CreateAsync(MessageTracing tracing, bool ownsBroker, CancellationToken token)
    {
        var broker = ownsBroker ? await NatsTopologyTests.Broker.StartAsync(token) : await NatsSessionBroker.Current.WaitAsync(token);
        NatsFixture? fixture = null;
        try
        {
            fixture = new(broker, tracing, ownsBroker);
            await fixture._admin.PingAsync(token);
            var report = await fixture.Adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
            if (report.Differences.Count != 0) throw new InvalidOperationException(string.Join("; ", report.Differences));
            return fixture;
        }
        catch
        {
            if (fixture is not null) await fixture.DisposeAsync();
            else if (ownsBroker) await broker.DisposeAsync();
            throw;
        }
    }
    public void RegisterConsumer(Func<NatsDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        _worker = Adapter.AddConsumer(_stream, "orders", new() { MaxMsgs = 32, DrainOnCancel = true }, 1);
        _worker.Receive = (delivery, token) =>
        {
            _receiver = delivery.Message.Connection;
            return receive(delivery, token);
        };
    }
    public ValueTask SettleAsync(NatsDelivery incoming, Disposition disposition, CancellationToken token) => _worker!.SettleAsync(incoming, disposition, token);
    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token) => throw new NotSupportedException("JetStream has no native dead-letter operation with error headers.");
    public async Task DisconnectAsync(CancellationToken token)
    {
        await _broker.SetConnectedAsync(false, token);
        await _disconnected.Task.WaitAsync(token);
    }
    public async Task ReconnectAsync(CancellationToken token)
    {
        await _broker.SetConnectedAsync(true, token);
        await _reopened.Task.WaitAsync(token);
        await _admin.PingAsync(token);
    }
    public async ValueTask VerifyShutdownAsync(CancellationToken token)
    {
        await _worker!.Completion.WaitAsync(token);
        var consumer = await _js.GetConsumerAsync(_stream, "orders", token);
        await Assert.That(consumer.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(consumer.Info.AckFloor.ConsumerSeq).IsGreaterThan(0UL);
    }
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_ownsBroker)
            {
                // Snapshot after scenario shutdown, before closing the admin connection.
                // A fresh token keeps diagnostics useful after the scenario deadline.
                Console.WriteLine($"NATS after scenario shutdown {_stream}/orders: worker={_worker?.Completion.Status}; receiver={_receiver?.ConnectionState}; admin={_admin.ConnectionState}; worker failure={_worker?.Completion.Exception}");
                using var diagnosticDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                try
                {
                    var consumer = await _js.GetConsumerAsync(_stream, "orders", diagnosticDeadline.Token);
                    var info = consumer.Info;
                    Console.WriteLine($"NATS consumer: pending={info.NumPending}; ack pending={info.NumAckPending}; waiting pulls={info.NumWaiting}; delivered={info.Delivered.ConsumerSeq}; ack floor={info.AckFloor.ConsumerSeq}");
                }
                catch (Exception error) { Console.WriteLine($"NATS consumer diagnostics failed: {error}"); }
            }
            await _provider.DisposeAsync();
        }
        finally
        {
            await _admin.DisposeAsync();
            _nativeInstrumentation.Dispose();
            _logs.Dispose();
            if (_ownsBroker) await _broker.DisposeAsync();
        }
    }
}
