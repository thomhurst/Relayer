using Relayer.Transports;
using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.SchemaRegistry;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Serialization;
using Testcontainers.Redpanda;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

public class KafkaSchemaRegistryTests
{
    internal const string Schema = """{"type":"object","properties":{"Number":{"type":"integer"}},"required":["Number"]}""";
    private const string EastSchema = """{"type":"object","properties":{"Number":{"type":"integer"}},"required":["Number"],"additionalProperties":false}""";

    [Test]
    public async Task Explicit_contract_handles_segmented_bodies_and_never_calls_registry_after_startup()
    {
        var http = new RegistryResponses();
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, http);
        await using var serializer = new KafkaJsonSchemaSerializer(client).Add("orders", Schema, SchemaJson.Default.SchemaOrder);
        var body = new ArrayBufferWriter<byte>();
        await Assert.That(() => serializer.Serialize(new SchemaOrder(42), body)).Throws<InvalidOperationException>();
        await Assert.That(async () => await serializer.InitializeAsync(new CancellationToken(canceled: true))).Throws<OperationCanceledException>();
        await serializer.InitializeAsync(default);
        await serializer.InitializeAsync(default);
        http.RejectRequests = true;
        serializer.Serialize(new SchemaOrder(42), body);
        await Assert.That(BinaryPrimitives.ReadInt32BigEndian(body.WrittenSpan[1..])).IsEqualTo(7);
        var first = new Segment(body.WrittenMemory[..2]);
        var last = first.Append(body.WrittenMemory[2..]);
        var segmented = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        await Assert.That(serializer.Deserialize<SchemaOrder>(segmented)).IsEqualTo(new SchemaOrder(42));
        await Assert.That(serializer.Deserialize<SchemaOrder>(new(body.WrittenMemory))).IsEqualTo(new SchemaOrder(42));
        await Assert.That(() => serializer.Add("other", Schema, SchemaJson.Default.EastSchemaOrder)).Throws<InvalidOperationException>();
        await Assert.That(() => serializer.Serialize(new EastSchemaOrder(1), body)).Throws<InvalidOperationException>();
        await Assert.That(http.Requests).IsEqualTo(1);

        var invalid = body.WrittenMemory.ToArray();
        invalid[4] = 8;
        await Assert.That(() => serializer.Deserialize<SchemaOrder>(new(invalid))).Throws<JsonException>();
        invalid[4] = 7;
        invalid[0] = 1;
        await Assert.That(() => serializer.Deserialize<SchemaOrder>(new(invalid))).Throws<JsonException>();
        await Assert.That(() => serializer.Deserialize<SchemaOrder>(new(invalid.AsMemory(0, 3)))).Throws<JsonException>();
        var prefix = body.WrittenMemory[..5].ToArray();
        await Assert.That(() => serializer.Deserialize<SchemaOrder>(new(prefix.Concat("null"u8.ToArray()).ToArray()))).Throws<JsonException>();
        await Assert.That(() => serializer.Deserialize<SchemaOrder>(new(body.WrittenMemory.ToArray().Concat(" {}"u8.ToArray()).ToArray()))).Throws<JsonException>();
    }

    [Test]
    public async Task Unsupported_dynamic_configuration_and_duplicate_contracts_fail_early()
    {
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, new RegistryResponses());
        await using var serializer = new KafkaJsonSchemaSerializer(client);
        await Assert.That(() => serializer.Add("orders", Schema, SchemaJson.Default.SchemaOrder,
            new() { UseLatestVersion = true, AutoRegisterSchemas = false })).Throws<NotSupportedException>();
        serializer.Add("orders", Schema, SchemaJson.Default.SchemaOrder);
        await Assert.That(() => serializer.Add("orders", Schema, SchemaJson.Default.SchemaOrder)).Throws<InvalidOperationException>();
        await serializer.DisposeAsync();
        await Assert.That(() => serializer.Serialize(new SchemaOrder(1), new ArrayBufferWriter<byte>())).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Registry_registration_does_not_reuse_a_lookup_connection_that_can_close_without_warning()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = timeout.Token;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var handler = CreateRegistryHandler();
        using var client = new SchemaRegistryClient(new()
        { Url = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}" }, handler);
        var server = ServeAsync();
        var registration = client.GetOrRegisterSchemaAsync("orders-value", new() { SchemaType = SchemaType.Json, SchemaString = Schema }, token);
        await Task.WhenAll(server, registration);
        await Assert.That(await registration).IsEqualTo(7);

        async Task ServeAsync()
        {
            using (var lookup = await listener.AcceptTcpClientAsync(token))
            {
                await ReplyAsync(lookup, "POST /subjects/orders-value HTTP/1.1", "404 Not Found", "{\"error_code\":40401,\"message\":\"Subject not found\"}");
                // Keep the socket open until the client acts: this deterministically exposes reuse
                // before Redpanda's unannounced FIN, without relying on scheduler timing or sleeps.
                var read = await lookup.GetStream().ReadAsync(new byte[1], token);
                await Assert.That(read).IsEqualTo(0);
            }
            using var register = await listener.AcceptTcpClientAsync(token);
            await ReplyAsync(register, "POST /subjects/orders-value/versions HTTP/1.1", "200 OK", "{\"id\":7}");
        }

        async Task ReplyAsync(TcpClient connection, string expectedRequest, string status, string body)
        {
            var stream = connection.GetStream();
            using var reader = new StreamReader(stream, Encoding.ASCII, leaveOpen: true);
            await Assert.That(await reader.ReadLineAsync(token)).IsEqualTo(expectedRequest);
            // Dekaf's JsonContent uses chunked HTTP/1.1 requests. Consume their framing without
            // buffering or rewriting the request in the client handler under test.
            var chunked = false;
            while (await reader.ReadLineAsync(token) is { Length: > 0 } header)
                chunked |= header.Equals("Transfer-Encoding: chunked", StringComparison.OrdinalIgnoreCase);
            await Assert.That(chunked).IsTrue();
            while (true)
            {
                var length = int.Parse((await reader.ReadLineAsync(token))!, System.Globalization.NumberStyles.HexNumber);
                if (length == 0) break;
                var buffer = new char[length];
                await Assert.That(await reader.ReadBlockAsync(buffer, token)).IsEqualTo(length);
                await Assert.That(await reader.ReadLineAsync(token)).IsEqualTo("");
            }
            await Assert.That(await reader.ReadLineAsync(token)).IsEqualTo("");
            var response = Encoding.ASCII.GetBytes($"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {body.Length}\r\n\r\n{body}");
            await stream.WriteAsync(response, token);
        }
    }

    [Test]
    public async Task Named_clusters_use_independent_registries_credentials_and_contracts()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var west = Broker();
        await using var east = Broker();
        // Dekaf group consumers require Kafka 4.0+. Redpanda supplies only the independent registry services.
        await using var westRegistry = Registry();
        await using var eastRegistry = Registry();
        await Task.WhenAll(west.StartAsync(token), east.StartAsync(token), westRegistry.StartAsync(token), eastRegistry.StartAsync(token));
        await CreateTopic(west, token);
        await CreateTopic(east, token);
        using var westHttp = new CredentialCheck("west:secret", westRegistry);
        using var eastHttp = new CredentialCheck("east:other-secret", eastRegistry);
        using var westClient = new SchemaRegistryClient(new()
        { Url = westRegistry.GetSchemaRegistryAddress(), BasicAuthUserInfo = "west:secret" }, westHttp);
        using var eastClient = new SchemaRegistryClient(new()
        { Url = eastRegistry.GetSchemaRegistryAddress(), BasicAuthUserInfo = "east:other-secret", NormalizeSchemas = true }, eastHttp);
        // Shift west's ID allocation. An ID from one registry must never select the other registry's schema.
        await westClient.RegisterSchemaAsync("seed", new() { SchemaType = SchemaType.Json, SchemaString = "{\"type\":\"string\"}" }, token);
        var received = Channel.CreateUnbounded<(string Identity, int Number)>();
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Logging.ClearProviders();
        hostBuilder.Services.Configure<HostOptions>(options => options.ServicesStartConcurrently = true);
        hostBuilder.Services.AddSingleton(received);
        hostBuilder.Services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<SchemaOrder>("tests.schema.west");
            relayer.Infrastructure().MessageTypes.Add<EastSchemaOrder>("tests.schema.east");
            relayer.Kafka("west", kafka =>
            {
                Configure(kafka, west.GetBootstrapAddress());
                kafka.UseSerializer(_ => new KafkaJsonSchemaSerializer(westClient).Add("orders", Schema, SchemaJson.Default.SchemaOrder));
                kafka.Send<SchemaOrder>("orders");
                kafka.Consume("orders", consumer => { consumer.Options.Group = "west"; consumer.Handler<SchemaOrderHandler>(); });
            });
            relayer.Kafka("east", kafka =>
            {
                Configure(kafka, east.GetBootstrapAddress());
                kafka.UseSerializer(_ => new KafkaJsonSchemaSerializer(eastClient)
                    .Add("orders", EastSchema, SchemaJson.Default.SchemaOrder)
                    .Add("orders", EastSchema, SchemaJson.Default.EastSchemaOrder));
                kafka.Send<EastSchemaOrder>("orders");
                kafka.Consume("orders", consumer => { consumer.Options.Group = "east"; consumer.Handler<EastSchemaOrderHandler>(); });
            });
        });
        using var host = hostBuilder.Build();
        // No global JSON context: named serializers must never resolve the unused application default.
        await host.StartAsync(token);
        using (var scope = host.Services.CreateScope())
        {
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendAsync(new SchemaOrder(11), token);
            await sender.SendAsync(new EastSchemaOrder(22), token);
        }
        var delivery = ReadBoth();
        var completions = host.Services.GetServices<Relayer.Transports.ITransport>().Cast<KafkaProducerTransport>()
            .SelectMany(transport => transport.Consumers).Select(consumer => consumer.Completion).ToArray();
        var finished = await Task.WhenAny(completions.Append(delivery));
        if (finished != delivery) await finished;
        var seen = await delivery;
        await Assert.That(seen).IsEquivalentTo(new[] { ("tests.schema.west;v=1", 11), ("tests.schema.east;v=1", 22) });
        var westRecord = await Read(west, token);
        var eastRecord = await Read(east, token);
        var westId = BinaryPrimitives.ReadInt32BigEndian(westRecord[1..]);
        var eastId = BinaryPrimitives.ReadInt32BigEndian(eastRecord[1..]);
        await Assert.That(westId).IsNotEqualTo(eastId);
        using var westSchema = JsonDocument.Parse((await westClient.GetSchemaAsync(westId, token)).SchemaString);
        using var eastSchema = JsonDocument.Parse((await eastClient.GetSchemaAsync(eastId, token)).SchemaString);
        await Assert.That(westSchema.RootElement.TryGetProperty("additionalProperties", out _)).IsFalse();
        await Assert.That(eastSchema.RootElement.GetProperty("additionalProperties").GetBoolean()).IsFalse();
        var westSerializer = host.Services.GetMessageSerializer("west");
        var eastSerializer = host.Services.GetMessageSerializer("east");
        await Assert.That(eastSerializer.Deserialize<SchemaOrder>(new(eastRecord))).IsEqualTo(new SchemaOrder(22));
        await Assert.That(() => westSerializer.Deserialize<SchemaOrder>(new(eastRecord))).Throws<JsonException>();
        await Assert.That(() => eastSerializer.Deserialize<SchemaOrder>(new(westRecord))).Throws<JsonException>();
        await Assert.That((await westClient.GetAllSubjectsAsync(token)).Order().ToArray()).IsEquivalentTo(new[] { "orders-value", "seed" });
        await Assert.That((await eastClient.GetAllSubjectsAsync(token)).ToArray()).IsEquivalentTo(new[] { "orders-value" });
        await Assert.That(westHttp.Requests > 0 && eastHttp.Requests > 0).IsTrue();
        await host.StopAsync(token);

        async Task<(string Identity, int Number)[]> ReadBoth() =>
            [await received.Reader.ReadAsync(token), await received.Reader.ReadAsync(token)];
    }

    private static KafkaContainer Broker()
    {
        var builder = new KafkaBuilder("apache/kafka:4.0.2").WithVendor(KafkaVendor.ApacheSoftwareFoundation);
        if (OperatingSystem.IsLinux())
            builder = builder.WithPortBinding("20000-29999", "9092").WithPortBinding("20000-29999", "9093");
        return builder.AfterStartupCopy().Build();
    }

    private static RedpandaContainer Registry()
    {
        var builder = new RedpandaBuilder("redpandadata/redpanda:v25.1.9");
        if (OperatingSystem.IsLinux())
            builder = builder.WithPortBinding("20000-29999", "8081").WithPortBinding("20000-29999", "9092");
        return builder
            .WithCommand(new OverwriteEnumerable<string>([$"while [ ! -f /tmp/relayer-ready ]; do sleep 0.1; done; rm /tmp/relayer-ready; exec {RedpandaBuilder.StartupScriptFilePath}"]))
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("touch", "/tmp/relayer-ready")
                // Kafka readiness does not establish that the independent HTTP registry is ready.
                .UntilHttpRequestIsSucceeded(request => request.ForPort(RedpandaBuilder.SchemaRegistryPort).ForPath("/subjects")))
            .Build();
    }

    private static void Configure(TransportBuilder<KafkaTransportOptions, KafkaConsumerOptions> kafka, string bootstrap)
    {
        kafka.TopologyMode = TopologyMode.None;
        kafka.Options.Configure(options =>
        {
            options.BootstrapServers.Add(bootstrap);
            options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
        });
    }

    private static async Task CreateTopic(KafkaContainer broker, CancellationToken token)
    {
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.GetBootstrapAddress()).Build();
        await admin.CreateTopicsAsync([new() { Name = "orders", NumPartitions = 1, ReplicationFactor = 1 },
            new() { Name = "orders.dead-letter", NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
    }

    private static async Task<byte[]> Read(KafkaContainer broker, CancellationToken token)
    {
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.GetBootstrapAddress())
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithOffsetCommitMode(OffsetCommitMode.Manual).Build();
        reader.Assign(new TopicPartition("orders", 0));
        await reader.InitializeAsync(token);
        return (await reader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value.Value;
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;
        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var segment = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = segment;
            return segment;
        }
    }

    private sealed class RegistryResponses : HttpMessageHandler
    {
        public int Requests;
        public bool RejectRequests;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (RejectRequests) throw new InvalidOperationException("Registry I/O after startup.");
            Interlocked.Increment(ref Requests);
            var json = "{\"id\":7,\"subject\":\"orders-value\",\"version\":1,\"schemaType\":\"JSON\",\"schema\":" + JsonSerializer.Serialize(Schema) + "}";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") });
        }
    }

    private static SocketsHttpHandler CreateRegistryHandler() => new()
    {
        // Redpanda v25.1.9 rejects unknown subjects before reading the lookup body, then
        // closes without advertising it. Prevent a pooled registration POST racing that FIN.
        // https://github.com/redpanda-data/redpanda/issues/31683
        PooledConnectionLifetime = TimeSpan.Zero,
    };

    private sealed class CredentialCheck(string credentials, RedpandaContainer registry) : DelegatingHandler(CreateRegistryHandler())
    {
        public int Requests;
        private string? _lastResponse;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var expected = Convert.ToBase64String(Encoding.UTF8.GetBytes(credentials));
            if (request.Headers.Authorization?.ToString() != "Basic " + expected)
                throw new InvalidOperationException("Registry credentials crossed transport boundaries.");
            Interlocked.Increment(ref Requests);
            try
            {
                var response = await base.SendAsync(request, cancellationToken);
                _lastResponse = $"{request.Method} {request.RequestUri}: HTTP/{response.Version} {(int)response.StatusCode}; Content-Length={response.Content.Headers.ContentLength}, Transfer-Encoding={response.Headers.TransferEncoding}, Connection={response.Headers.Connection}";
                return response;
            }
            catch (Exception error) when (error is HttpRequestException or OperationCanceledException)
            {
                Console.Error.WriteLine($"Registry request failed: {request.Method} {request.RequestUri}; container {registry.Id}, state {registry.State}.");
                Console.Error.WriteLine($"Request framing: HTTP/{request.Version}; Content-Length={request.Content?.Headers.ContentLength}, Transfer-Encoding={request.Headers.TransferEncoding}, Connection={request.Headers.Connection}. Previous response: {_lastResponse}");
                try
                {
                    // The test token may have expired. Bound diagnostics independently and retain the original failure.
                    using var diagnostics = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    var (stdout, stderr) = await registry.GetLogsAsync(ct: diagnostics.Token);
                    Console.Error.WriteLine(stdout);
                    Console.Error.WriteLine(stderr);
                }
                catch (Exception diagnosticError)
                {
                    Console.Error.WriteLine($"Registry diagnostics failed: {diagnosticError}");
                }
                throw;
            }
        }
    }
}

[Message("tests.schema.west")]
public sealed record SchemaOrder(int Number);
[Message("tests.schema.east")]
public sealed record EastSchemaOrder(int Number);
public sealed class SchemaOrderHandler(Channel<(string Identity, int Number)> received) : IHandler<SchemaOrder>
{
    public ValueTask HandleAsync(SchemaOrder message, MessageContext context, CancellationToken cancellationToken) =>
        received.Writer.WriteAsync((context.MessageType.ToString(), message.Number), cancellationToken);
}
public sealed class EastSchemaOrderHandler(Channel<(string Identity, int Number)> received) : IHandler<EastSchemaOrder>
{
    public ValueTask HandleAsync(EastSchemaOrder message, MessageContext context, CancellationToken cancellationToken) =>
        received.Writer.WriteAsync((context.MessageType.ToString(), message.Number), cancellationToken);
}
[JsonSerializable(typeof(SchemaOrder))]
[JsonSerializable(typeof(EastSchemaOrder))]
internal partial class SchemaJson : JsonSerializerContext;
