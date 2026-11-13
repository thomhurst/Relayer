using Relayer.Transports;
using System.Buffers;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.Serialization;
using Relayer.Serialization.Protobuf;

internal static class ProtobufSmoke
{
    public static void Run()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageSerializer>(new ProtobufMessageSerializer(types => types.Add(StringValue.Parser)));
        services.AddRelayer(relayer => relayer.Infrastructure().MessageTypes.Add<StringValue>("aot.protobuf"));
        using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var message = new StringValue { Value = "Protobuf under Native AOT: Zoë" };
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(message, buffer);
        var split = buffer.WrittenCount / 2;
        var first = new Segment(buffer.WrittenMemory[..split]);
        var last = new Segment(buffer.WrittenMemory[split..]);
        first.Append(last);
        var source = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        if (!serializer.Deserialize<StringValue>(source).Equals(message) ||
            !buffer.WrittenSpan.SequenceEqual(message.ToByteArray()) || serializer.ContentType != "application/protobuf")
            throw new InvalidOperationException("Protobuf round trip failed under Native AOT.");
        try
        {
            serializer.Deserialize<StringValue>(new ReadOnlySequence<byte>(new byte[] { 10, 5, 65 }));
            throw new InvalidOperationException("Malformed Protobuf was accepted under Native AOT.");
        }
        catch (InvalidProtocolBufferException) { }
        Console.WriteLine("Protobuf native bytes, segmented reads and malformed payload rejection passed under Native AOT.");
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;
        public void Append(Segment next)
        {
            next.RunningIndex = Memory.Length;
            Next = next;
        }
    }
}
