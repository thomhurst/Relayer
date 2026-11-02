using System.Buffers;
using System.Net;
using System.Text;
using System.Text.Json.Serialization;
using Dekaf.SchemaRegistry;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.Kafka;

internal static class KafkaSchemaSmoke
{
    internal static async Task RunAsync()
    {
        using var client = new SchemaRegistryClient(new() { Url = "http://registry" }, new RegistryResponses());
        var services = new ServiceCollection().AddRelayer(relayer =>
        {
            relayer.MessageTypes.Add<SchemaSmokeOrder>("smoke.schema");
            relayer.Kafka("registry", kafka => kafka.UseSerializer(_ => new KafkaJsonSchemaSerializer(client)
                .Add("orders", "{\"type\":\"object\"}", SchemaSmokeJson.Default.SchemaSmokeOrder)));
        });
        await using var provider = services.BuildServiceProvider();
        var serializer = (KafkaJsonSchemaSerializer)provider.GetMessageSerializer("registry");
        await serializer.InitializeAsync(default);
        var body = new ArrayBufferWriter<byte>();
        serializer.Serialize(new SchemaSmokeOrder(42), body);
        if (serializer.Deserialize<SchemaSmokeOrder>(new(body.WrittenMemory)).Number != 42)
            throw new InvalidOperationException("Named Kafka registry serialization failed under Native AOT.");
        Console.WriteLine("Named Kafka registry serialization passed under Native AOT.");
    }

    private sealed class RegistryResponses : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"id":7,"subject":"orders-value","version":1,"schemaType":"JSON","schema":"{\"type\":\"object\"}"}""",
                    Encoding.UTF8, "application/json"),
            });
    }
}

public sealed record SchemaSmokeOrder(int Number);
[JsonSerializable(typeof(SchemaSmokeOrder))]
internal partial class SchemaSmokeJson : JsonSerializerContext;
