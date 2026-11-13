using Relayer.Serialization;
using System.Buffers;
using Dekaf.SchemaRegistry;
using Dekaf.SchemaRegistry.Avro;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.TestFixtures;

namespace Relayer.Kafka.Serialization.Avro.Tests;

public class AvroSerializerTests
{
    private static AvroMessageSerializer Create(SchemaRegistryClient client, AvroSerializerConfig? config = null) =>
        new AvroMessageSerializer(client).Add<AvroOrder, AvroOrder.AvroCodec>("orders", config);

    [Test]
    public async Task Matches_native_bytes_and_round_trips_without_registry_IO_after_startup()
    {
        using var responses = new AvroRegistryResponses();
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, responses);
        await using var serializer = Create(client);
        await serializer.InitializeAsync(default);
        await using var native = new Dekaf.SchemaRegistry.Avro.Poco.AvroPocoSchemaRegistrySerializer<AvroOrder, AvroOrder.AvroCodec>(client);
        await native.WarmupAsync("orders");
        var expected = new ArrayBufferWriter<byte>();
        native.Serialize(new AvroOrder { Number = 42, Customer = "Zoë " + new string('x', 1000) }, ref expected,
            new() { Topic = "orders", Component = SerializationComponent.Value });
        var requests = responses.Requests;
        responses.Fail = true;
        var destination = new ArrayBufferWriter<byte>();
        serializer.Serialize(new AvroOrder { Number = 42, Customer = "Zoë " + new string('x', 1000) }, destination);
        await Assert.That(destination.WrittenSpan.SequenceEqual(expected.WrittenSpan)).IsTrue();
        var source = new ReadOnlySequence<byte>(destination.WrittenMemory);
        var first = new Segment(destination.WrittenMemory[..1]);
        var last = first;
        for (var i = 1; i < destination.WrittenCount; i++) last = last.Append(destination.WrittenMemory.Slice(i, 1));
        var segmented = new ReadOnlySequence<byte>(first, 0, last, 1);
        await Assert.That(serializer.Deserialize<AvroOrder>(source).Customer).IsEqualTo("Zoë " + new string('x', 1000));
        await Assert.That(serializer.Deserialize<AvroOrder>(segmented).Number).IsEqualTo(42);
        await serializer.InitializeAsync(default);
        await Assert.That(responses.Requests).IsEqualTo(requests);
        await Assert.That(serializer.ContentType).IsEqualTo("avro/binary");
    }

    [Test]
    [Arguments(new byte[] { })]
    [Arguments(new byte[] { 0, 0, 0, 0 })]
    [Arguments(new byte[] { 1, 0, 0, 0, 7 })]
    [Arguments(new byte[] { 0, 0, 0, 0, 8 })]
    public async Task Rejects_invalid_prefix_and_unknown_schema_without_lookup(byte[] bytes)
    {
        using var responses = new AvroRegistryResponses();
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, responses);
        await using var serializer = Create(client);
        await serializer.InitializeAsync(default);
        var requests = responses.Requests;
        await Assert.That(() => serializer.Deserialize<AvroOrder>(new(bytes))).Throws<InvalidDataException>();
        await Assert.That(responses.Requests).IsEqualTo(requests);
    }

    [Test]
    public async Task Rejects_truncated_and_malformed_native_payloads()
    {
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, new AvroRegistryResponses());
        await using var serializer = Create(client);
        await serializer.InitializeAsync(default);
        await Assert.That(() => serializer.Deserialize<AvroOrder>(new(new byte[] { 0, 0, 0, 0, 7, 128 }))).Throws<EndOfStreamException>();
        await Assert.That(() => serializer.Deserialize<AvroOrder>(new(new byte[] { 0, 0, 0, 0, 7, 0, 1 }))).Throws<InvalidDataException>();
        // Follow the native codec's record parsing, which permits trailing bytes.
        await Assert.That(serializer.Deserialize<AvroOrder>(new(new byte[] { 0, 0, 0, 0, 7, 0, 0, 99 })).Number).IsEqualTo(0);
    }

    [Test]
    public async Task Uses_native_lookup_configuration_and_isolates_registry_IDs()
    {
        using var westResponses = new AvroRegistryResponses(7);
        using var eastResponses = new AvroRegistryResponses(9);
        using var westClient = new SchemaRegistryClient(new() { Url = "http://west" }, westResponses);
        using var eastClient = new SchemaRegistryClient(new() { Url = "http://east" }, eastResponses);
        await using var west = Create(westClient, new() { AutoRegisterSchemas = false, SubjectNameStrategy = SubjectNameStrategy.RecordName });
        await using var east = Create(eastClient);
        await west.InitializeAsync(default);
        await east.InitializeAsync(default);
        await Assert.That(westResponses.SubjectPath).IsEqualTo("/subjects/relayer.tests.Order");
        var body = new ArrayBufferWriter<byte>();
        west.Serialize(new AvroOrder { Number = 2 }, body);
        await Assert.That(() => east.Deserialize<AvroOrder>(new(body.WrittenMemory))).Throws<InvalidDataException>();
    }

    [Test]
    public async Task Fixed_schema_ID_looks_up_native_schema_at_startup()
    {
        using var responses = new AvroRegistryResponses();
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, responses);
        await using var serializer = Create(client, new() { UseSchemaId = 7, AutoRegisterSchemas = false });
        await serializer.InitializeAsync(default);
        await Assert.That(responses.Requests).IsGreaterThan(0);
        await Assert.That(responses.SubjectPath).IsNull();
        responses.Fail = true;
        var body = new ArrayBufferWriter<byte>();
        serializer.Serialize(new AvroOrder { Number = 12 }, body);
        await Assert.That(serializer.Deserialize<AvroOrder>(new(body.WrittenMemory)).Number).IsEqualTo(12);
    }

    [Test]
    public async Task Prepared_serializer_supports_concurrent_operations()
    {
        using var responses = new AvroRegistryResponses();
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, responses);
        await using var serializer = Create(client);
        await serializer.InitializeAsync(default);
        var requests = responses.Requests;
        responses.Fail = true;
        var workers = new Task[8];
        for (var worker = 0; worker < workers.Length; worker++)
            workers[worker] = Task.Run(() =>
            {
                var body = new ArrayBufferWriter<byte>();
                for (var i = 0; i < 100; i++)
                {
                    body.Clear();
                    serializer.Serialize(new AvroOrder { Number = i }, body);
                    if (serializer.Deserialize<AvroOrder>(new(body.WrittenMemory)).Number != i)
                        throw new InvalidOperationException("Concurrent Avro operations shared mutable message state.");
                }
            });
        await Task.WhenAll(workers);
        await Assert.That(responses.Requests).IsEqualTo(requests);
    }

    [Test]
    public async Task Host_prepares_application_serializer_and_propagates_registry_failure()
    {
        using var responses = new AvroRegistryResponses { Fail = true };
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, responses);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IMessageSerializer>(_ => Create(client));
        builder.Services.AddRelayer(_ => { });
        using var host = builder.Build();
        await Assert.That(async () => await host.StartAsync()).Throws<HttpRequestException>();
    }

    [Test]
    public async Task Host_prepares_application_serializer_before_use()
    {
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, new AvroRegistryResponses());
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IMessageSerializer>(_ => Create(client));
        builder.Services.AddRelayer(_ => { });
        using var host = builder.Build();
        await host.StartAsync();
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var body = new ArrayBufferWriter<byte>();
        serializer.Serialize(new AvroOrder { Number = 31 }, body);
        await Assert.That(serializer.Deserialize<AvroOrder>(new(body.WrittenMemory)).Number).IsEqualTo(31);
        await host.StopAsync();
    }

    [Test]
    public async Task Fails_startup_for_incompatible_registry_schema_and_cancellation()
    {
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, new AvroRegistryResponses { Incompatible = true });
        await using var serializer = Create(client);
        await Assert.That(async () => await serializer.InitializeAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await serializer.InitializeAsync(default)).Throws<InvalidOperationException>();
        await using var cancelled = Create(client);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await cancelled.InitializeAsync(cancellation.Token)).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Validates_declarations_lifecycle_and_disposal_without_owning_client()
    {
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, new AvroRegistryResponses());
        var serializer = Create(client);
        await Assert.That(() => Create(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => serializer.Add<AvroOrder, AvroOrder.AvroCodec>("orders")).Throws<InvalidOperationException>();
        await Assert.That(() => serializer.Serialize(new AvroOrder(), new ArrayBufferWriter<byte>())).Throws<InvalidOperationException>();
        await Assert.That(() => Create(client, new() { UseLatestVersion = true })).Throws<NotSupportedException>();
        await Assert.That(() => Create(client, new() { SchemaIdStrategy = SchemaIdSerializerStrategy.Header })).Throws<NotSupportedException>();
        await Assert.That(() => Create(client, new() { ValidationRulesExecution = ValidationRulesExecution.BeforeDomainRules })).Throws<NotSupportedException>();
        await serializer.InitializeAsync(default);
        await Assert.That(() => serializer.Add<AvroOrder, AvroOrder.AvroCodec>("orders")).Throws<InvalidOperationException>();
        await Assert.That(() => serializer.Serialize("missing", new ArrayBufferWriter<byte>())).Throws<InvalidOperationException>();
        await Assert.That(() => serializer.Serialize<AvroOrder>(null!, new ArrayBufferWriter<byte>())).Throws<ArgumentNullException>();
        await Assert.That(() => serializer.Serialize(new AvroOrder(), null!)).Throws<ArgumentNullException>();
        await serializer.DisposeAsync();
        await serializer.DisposeAsync();
        await Assert.That(() => serializer.Deserialize<AvroOrder>(ReadOnlySequence<byte>.Empty)).Throws<ObjectDisposedException>();
        await using var another = Create(client);
        await another.InitializeAsync(default);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;
        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
