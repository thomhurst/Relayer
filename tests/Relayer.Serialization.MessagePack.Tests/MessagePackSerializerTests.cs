using Relayer.Transports;
using System.Buffers;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Relayer.Serialization.MessagePack.Tests;

public class MessagePackSerializerTests
{
    private static MessagePackSerializerOptions Options => new MessagePackSerializerOptions(
        CompositeResolver.Create([NullableStringFormatter.Instance], [TestResolver.Instance]))
        .WithSecurity(MessagePackSecurity.UntrustedData);

    [Test]
    public async Task Writes_native_bytes_and_round_trips_generated_contracts()
    {
        var serializer = new MessagePackMessageSerializer(Options);
        var message = new TestMessage { Text = "Zoë " + new string('x', 1000), Number = 12345 };
        var destination = new ArrayBufferWriter<byte>();
        serializer.Serialize(message, destination);
        await Assert.That(destination.WrittenSpan.SequenceEqual(MessagePackSerializer.Serialize(message, Options))).IsTrue();
        var result = serializer.Deserialize<TestMessage>(new(destination.WrittenMemory));
        await Assert.That(result.Text).IsEqualTo(message.Text);
        await Assert.That(result.Number).IsEqualTo(message.Number);
        destination.Clear();
        serializer.Serialize(new SecondMessage { Flag = true }, destination);
        await Assert.That(serializer.Deserialize<SecondMessage>(new(destination.WrittenMemory)).Flag).IsTrue();
        await Assert.That(serializer.ContentType).IsEqualTo("application/x-msgpack");
    }

    [Test]
    public async Task Reads_every_byte_from_separate_segments()
    {
        var message = new TestMessage { Text = "Zoë " + new string('x', 300), Number = 12345 };
        var bytes = MessagePackSerializer.Serialize(message, Options);
        var first = new Segment(bytes.AsMemory(0, 1));
        var last = first;
        for (var i = 1; i < bytes.Length; i++) last = last.Append(bytes.AsMemory(i, 1));
        var result = new MessagePackMessageSerializer(Options).Deserialize<TestMessage>(new(first, 0, last, 1));
        await Assert.That(result.Text).IsEqualTo(message.Text);
        await Assert.That(result.Number).IsEqualTo(message.Number);
    }

    [Test]
    [Arguments(MessagePackCompression.Lz4Block)]
    [Arguments(MessagePackCompression.Lz4BlockArray)]
    public async Task Rejects_compression_at_configuration_time(MessagePackCompression compression)
    {
        var options = Options.WithCompression(compression).WithCompressionMinLength(1);
        await Assert.That(() => new MessagePackMessageSerializer(options)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Writes_across_output_segments()
    {
        var message = new TestMessage { Text = new string('x', 1000), Number = 12345 };
        var destination = new SegmentedBuffer();
        new MessagePackMessageSerializer(Options).Serialize(message, destination);
        await Assert.That(destination.Bytes.SequenceEqual(MessagePackSerializer.Serialize(message, Options))).IsTrue();
        await Assert.That(destination.Advances).IsGreaterThan(1);
    }

    [Test]
    public async Task Preserves_native_old_spec_options()
    {
        var options = Options.WithOldSpec(true);
        var message = new TestMessage { Text = new string('x', 40) };
        var destination = new ArrayBufferWriter<byte>();
        var serializer = new MessagePackMessageSerializer(options);
        serializer.Serialize(message, destination);
        await Assert.That(destination.WrittenSpan.SequenceEqual(MessagePackSerializer.Serialize(message, options))).IsTrue();
        await Assert.That(serializer.Deserialize<TestMessage>(new(destination.WrittenMemory)).Text).IsEqualTo(message.Text);
    }

    [Test]
    public async Task Preserves_security_depth_limit_and_isolates_options()
    {
        var message = new NestedMessage { Message = new TestMessage { Text = "nested" } };
        var bytes = MessagePackSerializer.Serialize(message, Options);
        var limited = new MessagePackMessageSerializer(Options.WithSecurity(
            MessagePackSecurity.UntrustedData.WithMaximumObjectGraphDepth(1)));
        await Assert.That(() => limited.Deserialize<NestedMessage>(new(bytes))).Throws<MessagePackSerializationException>();
        var normal = new MessagePackMessageSerializer(Options);
        await Assert.That(normal.Deserialize<NestedMessage>(new(bytes)).Message!.Text).IsEqualTo("nested");
    }

    [Test]
    [Arguments(new byte[] { })]
    [Arguments(new byte[] { 0xc1 })]
    [Arguments(new byte[] { 0x92, 0xd9, 5, 65 })]
    [Arguments(new byte[] { 0xc0 })]
    [Arguments(new byte[] { 0x92, 0xa1, 65, 1, 2 })]
    public async Task Rejects_malformed_nil_and_trailing_payloads(byte[] bytes)
    {
        var serializer = new MessagePackMessageSerializer(Options);
        await Assert.That(() => serializer.Deserialize<TestMessage>(new(bytes))).Throws<MessagePackSerializationException>();
    }

    [Test]
    public async Task Generated_only_resolver_rejects_missing_formatter_without_fallback()
    {
        var serializer = new MessagePackMessageSerializer(Options);
        await Assert.That(() => serializer.Serialize(new UnregisteredMessage(), new ArrayBufferWriter<byte>()))
            .Throws<MessagePackSerializationException>();
        await Assert.That(() => serializer.Deserialize<UnregisteredMessage>(new(new byte[] { 0x90 })))
            .Throws<MessagePackSerializationException>();
    }

    [Test]
    public async Task Rejects_null_options_message_and_destination()
    {
        await Assert.That(() => new MessagePackMessageSerializer(null!)).Throws<ArgumentNullException>();
        var serializer = new MessagePackMessageSerializer(Options);
        await Assert.That(() => serializer.Serialize<TestMessage>(null!, new ArrayBufferWriter<byte>())).Throws<ArgumentNullException>();
        await Assert.That(() => serializer.Serialize(new TestMessage(), null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Explicit_registration_starts_host_without_json_metadata()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IMessageSerializer>(new MessagePackMessageSerializer(Options));
        builder.Services.AddRelayer(relayer => relayer.Infrastructure().MessageTypes.Add<TestMessage>("tests.messagepack"));
        using var host = builder.Build();
        await host.StartAsync();
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var destination = new ArrayBufferWriter<byte>();
        serializer.Serialize(new TestMessage { Text = "registered" }, destination);
        await Assert.That(serializer.Deserialize<TestMessage>(new(destination.WrittenMemory)).Text).IsEqualTo("registered");
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

    private sealed class SegmentedBuffer : IBufferWriter<byte>
    {
        private byte[] _buffer = new byte[8];
        public List<byte> Bytes { get; } = [];
        public int Advances { get; private set; }
        public void Advance(int count)
        {
            Bytes.AddRange(_buffer.AsSpan(0, count));
            Advances++;
        }
        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            _buffer = new byte[Math.Max(8, sizeHint)];
            return _buffer;
        }
        public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
    }
}

[MessagePackObject]
public sealed class TestMessage
{
    [Key(0)] public string Text { get; set; } = "";
    [Key(1)] public int Number { get; set; }
}

[MessagePackObject]
public sealed class SecondMessage
{
    [Key(0)] public bool Flag { get; set; }
}

[MessagePackObject]
public sealed class NestedMessage
{
    [Key(0)] public TestMessage? Message { get; set; }
}

public sealed class UnregisteredMessage;

[GeneratedMessagePackResolver]
internal partial class TestResolver;
