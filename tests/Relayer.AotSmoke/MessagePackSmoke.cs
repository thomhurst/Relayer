using Relayer.Transports;
using System.Buffers;
using MessagePack;
using MessagePack.Resolvers;
using MessagePack.Formatters;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.Serialization;
using Relayer.Serialization.MessagePack;

internal static class MessagePackSmoke
{
    public static void Run()
    {
        var options = new MessagePackSerializerOptions(CompositeResolver.Create(
            [NullableStringFormatter.Instance], [SmokeMessagePackResolver.Instance]))
            .WithSecurity(MessagePackSecurity.UntrustedData);
        var services = new ServiceCollection();
        services.AddSingleton<IMessageSerializer>(new MessagePackMessageSerializer(options));
        services.AddRelayer(relayer => relayer.Infrastructure().MessageTypes.Add<MessagePackSmokeMessage>("aot.messagepack"));
        using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var message = new MessagePackSmokeMessage { Text = "Zoë", Number = 42 };
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(message, buffer);
        var split = buffer.WrittenCount / 2;
        var first = new Segment(buffer.WrittenMemory[..split]);
        var last = first.Append(buffer.WrittenMemory[split..]);
        var result = serializer.Deserialize<MessagePackSmokeMessage>(new(first, 0, last, last.Memory.Length));
        if (result.Text != message.Text || result.Number != message.Number ||
            !buffer.WrittenSpan.SequenceEqual(new byte[] { 0x92, 0xa4, 90, 111, 195, 171, 42 }) ||
            serializer.ContentType != "application/x-msgpack")
            throw new InvalidOperationException("MessagePack round trip failed under Native AOT.");
        try
        {
            serializer.Deserialize<MessagePackSmokeMessage>(new ReadOnlySequence<byte>(new byte[] { 0x92, 0xd9, 5, 65 }));
            throw new InvalidOperationException("Malformed MessagePack was accepted under Native AOT.");
        }
        catch (MessagePackSerializationException) { }
        try
        {
            serializer.Serialize(new MissingMessagePackFormatter(), buffer);
            throw new InvalidOperationException("Missing MessagePack formatter was accepted under Native AOT.");
        }
        catch (MessagePackSerializationException) { }
        Console.WriteLine("MessagePack generated formatters, native bytes, segmented reads and malformed payload rejection passed under Native AOT.");
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

[MessagePackObject]
public sealed class MessagePackSmokeMessage
{
    [Key(0)] public string Text { get; set; } = "";
    [Key(1)] public int Number { get; set; }
}

internal sealed class MissingMessagePackFormatter;

[GeneratedMessagePackResolver]
internal partial class SmokeMessagePackResolver;
