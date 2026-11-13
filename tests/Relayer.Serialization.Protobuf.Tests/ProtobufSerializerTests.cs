using Relayer.Transports;
using System.Buffers;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StringValue = Google.Protobuf.WellKnownTypes.StringValue;

namespace Relayer.Serialization.Protobuf.Tests;

public class ProtobufSerializerTests
{
    private static ProtobufMessageSerializer CreateSerializer() => new(types => types.Add(StringValue.Parser).Add(Int32Value.Parser));

    [Test]
    public async Task Writes_native_bytes_and_round_trips_multiple_contracts()
    {
        var serializer = CreateSerializer();
        var message = new StringValue { Value = "Zoë " + new string('x', 1000) };
        var destination = new ArrayBufferWriter<byte>();
        serializer.Serialize(message, destination);
        await Assert.That(destination.WrittenSpan.SequenceEqual(message.ToByteArray())).IsTrue();
        await Assert.That(serializer.Deserialize<StringValue>(new(destination.WrittenMemory))).IsEqualTo(message);
        destination.Clear();
        serializer.Serialize(new Int32Value { Value = 12345 }, destination);
        await Assert.That(serializer.Deserialize<Int32Value>(new(destination.WrittenMemory)).Value).IsEqualTo(12345);
        await Assert.That(serializer.ContentType).IsEqualTo("application/protobuf");
    }

    [Test]
    public async Task Reads_every_byte_from_separate_segments()
    {
        var message = new StringValue { Value = "Zoë " + new string('x', 300) };
        var bytes = message.ToByteArray();
        var first = new Segment(bytes.AsMemory(0, 1));
        var last = first;
        for (var i = 1; i < bytes.Length; i++) last = last.Append(bytes.AsMemory(i, 1));
        var source = new ReadOnlySequence<byte>(first, 0, last, 1);
        await Assert.That(CreateSerializer().Deserialize<StringValue>(source)).IsEqualTo(message);
    }

    [Test]
    public async Task Writes_across_small_output_segments()
    {
        var message = new StringValue { Value = new string('x', 300) };
        var destination = new SmallBuffer();
        CreateSerializer().Serialize(message, destination);
        await Assert.That(destination.Bytes.SequenceEqual(message.ToByteArray())).IsTrue();
    }

    [Test]
    [Arguments(new byte[] { 0 })]
    [Arguments(new byte[] { 10, 5, 65 })]
    [Arguments(new byte[] { 10, 1, 255 })]
    [Arguments(new byte[] { 128 })]
    public async Task Rejects_malformed_payloads(byte[] bytes)
    {
        var serializer = CreateSerializer();
        await Assert.That(() => serializer.Deserialize<StringValue>(new(bytes))).Throws<InvalidProtocolBufferException>();
    }

    [Test]
    public async Task Empty_payload_is_valid_default_message_and_unknown_fields_follow_parser_settings()
    {
        var serializer = CreateSerializer();
        await Assert.That(serializer.Deserialize<StringValue>(ReadOnlySequence<byte>.Empty).Value).IsEqualTo("");
        byte[] payload = [10, 1, 65, 16, 42];
        var preserved = serializer.Deserialize<StringValue>(new(payload));
        var discarding = new ProtobufMessageSerializer(types => types.Add(StringValue.Parser.WithDiscardUnknownFields(true)));
        var discarded = discarding.Deserialize<StringValue>(new(payload));
        await Assert.That(preserved.ToByteArray().SequenceEqual(payload)).IsTrue();
        await Assert.That(discarded.ToByteArray().SequenceEqual(new byte[] { 10, 1, 65 })).IsTrue();
    }

    [Test]
    public async Task Missing_duplicate_and_null_registrations_fail_actionably()
    {
        var serializer = new ProtobufMessageSerializer(_ => { });
        await Assert.That(() => serializer.Deserialize<StringValue>(ReadOnlySequence<byte>.Empty)).Throws<InvalidOperationException>();
        await Assert.That(() => serializer.Serialize(new StringValue(), new ArrayBufferWriter<byte>())).Throws<InvalidOperationException>();
        await Assert.That(() => new ProtobufMessageSerializer(types => types.Add(StringValue.Parser).Add(StringValue.Parser))).Throws<InvalidOperationException>();
        await Assert.That(() => new ProtobufMessageSerializer(types => types.Add<StringValue>(null!))).Throws<ArgumentNullException>();
        await Assert.That(() => new ProtobufMessageSerializer(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => CreateSerializer().Serialize<StringValue>(null!, new ArrayBufferWriter<byte>())).Throws<ArgumentNullException>();
        await Assert.That(() => CreateSerializer().Serialize(new StringValue(), null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Captures_immutable_registry_and_isolates_instances()
    {
        ProtobufSerializerBuilder captured = null!;
        var first = new ProtobufMessageSerializer(types => { captured = types; types.Add(StringValue.Parser); });
        captured.Add(Int32Value.Parser);
        var second = new ProtobufMessageSerializer(types => types.Add(Int32Value.Parser));
        await Assert.That(() => first.Deserialize<Int32Value>(ReadOnlySequence<byte>.Empty)).Throws<InvalidOperationException>();
        await Assert.That(() => second.Deserialize<StringValue>(ReadOnlySequence<byte>.Empty)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Explicit_registration_starts_host_without_json_metadata()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IMessageSerializer>(CreateSerializer());
        builder.Services.AddRelayer(relayer => relayer.Infrastructure().MessageTypes.Add<StringValue>("tests.protobuf-string"));
        using var host = builder.Build();
        await host.StartAsync();
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var destination = new ArrayBufferWriter<byte>();
        serializer.Serialize(new StringValue { Value = "registered" }, destination);
        await Assert.That(serializer.Deserialize<StringValue>(new(destination.WrittenMemory)).Value).IsEqualTo("registered");
        await host.StopAsync();
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

    private sealed class SmallBuffer : IBufferWriter<byte>
    {
        private readonly byte[] _buffer = new byte[8];
        public List<byte> Bytes { get; } = [];
        public void Advance(int count) => Bytes.AddRange(_buffer.AsSpan(0, count));
        public Memory<byte> GetMemory(int sizeHint = 0) => sizeHint <= _buffer.Length ? _buffer : throw new InvalidOperationException("Unexpected size hint.");
        public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
    }
}
