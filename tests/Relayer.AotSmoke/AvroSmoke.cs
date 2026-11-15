using System.Buffers;
using Dekaf.SchemaRegistry;
using Relayer.Kafka.Serialization.Avro;
using Relayer.TestFixtures;

// Dekaf turns Avro POCO rule support off under PublishAot, so registration, preparation and
// serialization stay warning-clean (D39).
internal static class AvroSmoke
{
    internal static async Task RunAsync()
    {
        using var responses = new AvroRegistryResponses();
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, responses);
        await using var serializer = new AvroMessageSerializer(client).Add<AvroOrder, AvroOrder.AvroCodec>("orders");
        await serializer.InitializeAsync(default);
        var body = new ArrayBufferWriter<byte>();
        serializer.Serialize(new AvroOrder { Number = 42, Customer = "smoke" }, body);
        var order = serializer.Deserialize<AvroOrder>(new ReadOnlySequence<byte>(body.WrittenMemory));
        if (order.Number != 42 || order.Customer != "smoke")
            throw new InvalidOperationException("Avro schema registry serialization failed under Native AOT.");
        Console.WriteLine("Avro schema registry serialization passed under Native AOT.");
    }
}
