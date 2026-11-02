#pragma warning disable RLY001 // Export fixtures use explicit MessageTypeRegistry registrations.
using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Tests;

public class AsyncApiTests
{
    [Test]
    public async Task Export_preserves_endpoints_directions_identities_and_registered_unrouted_messages()
    {
        using var provider = Create();
        var topology = provider.GetRequiredService<ITopology>();
        using var output = new MemoryStream();
        topology.ExportAsyncApi(output, new() { Title = "Orders", Version = "1.2.3" });
        using var document = JsonDocument.Parse(output.ToArray());
        if (Environment.GetEnvironmentVariable("RELAYER_ASYNCAPI_TEST_OUTPUT") is { } path)
            File.WriteAllBytes(path, output.ToArray());
        var root = document.RootElement;
        await Assert.That(root.GetProperty("asyncapi").GetString()).IsEqualTo("3.0.0");
        await Assert.That(root.GetProperty("info").GetProperty("title").GetString()).IsEqualTo("Orders");
        await Assert.That(root.GetProperty("info").GetProperty("version").GetString()).IsEqualTo("1.2.3");
        var channels = root.GetProperty("channels");
        await Assert.That(channels.EnumerateObject().Count()).IsEqualTo(3);
        await Assert.That(channels.GetProperty("channel0").GetProperty("address").GetString()).IsEqualTo("orders/~é");
        await Assert.That(channels.GetProperty("channel0").GetProperty("x-relayer-transport").GetString()).IsEqualTo("first/~");
        await Assert.That(channels.GetProperty("channel0").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(1);
        await Assert.That(channels.GetProperty("channel2").GetProperty("address").GetString()).IsEqualTo("orders/~é");
        await Assert.That(channels.GetProperty("channel2").GetProperty("x-relayer-transport").GetString()).IsEqualTo("second");
        var operations = root.GetProperty("operations");
        await Assert.That(operations.EnumerateObject().Count()).IsEqualTo(4);
        await Assert.That(operations.GetProperty("operation0").GetProperty("action").GetString()).IsEqualTo("send");
        await Assert.That(operations.GetProperty("operation1").GetProperty("action").GetString()).IsEqualTo("receive");
        await Assert.That(operations.GetProperty("operation1").GetProperty("messages").GetArrayLength()).IsEqualTo(1);
        await Assert.That(operations.GetProperty("operation2").GetProperty("channel").GetProperty("$ref").GetString()).IsEqualTo("#/channels/channel1");
        var messages = root.GetProperty("components").GetProperty("messages");
        await Assert.That(messages.EnumerateObject().Count()).IsEqualTo(3);
        var order = messages.EnumerateObject().Single(item => item.Value.GetProperty("x-relayer-version").GetInt32() == 2).Value;
        await Assert.That(order.GetProperty("name").GetString()).IsEqualTo("orders/~é");
        await Assert.That(order.GetProperty("x-relayer-wire-type").GetString()).IsEqualTo("orders/~é;v=2");
        await Assert.That(root.TryGetProperty("servers", out _)).IsFalse();
        await Assert.That(output.CanWrite).IsTrue();
        await Assert.That(Encoding.UTF8.GetString(output.ToArray()).Contains(nameof(ExportOrder), StringComparison.Ordinal)).IsFalse();
    }

    [Test]
    public async Task Schemas_follow_last_registered_json_contract_and_keep_recursive_resources_local()
    {
        using var provider = Create();
        using var document = JsonDocument.Parse(Export(provider));
        var messages = document.RootElement.GetProperty("components").GetProperty("messages");
        var order = messages.EnumerateObject().First().Value.GetProperty("payload");
        await Assert.That(order.GetProperty("schemaFormat").GetString()).IsEqualTo("application/schema+json;version=draft-2020-12");
        var schema = order.GetProperty("schema");
        await Assert.That(schema.GetProperty("properties").TryGetProperty("customer_name", out _)).IsTrue();
        await Assert.That(schema.GetProperty("properties").TryGetProperty("CustomerName", out _)).IsFalse();
        var tree = messages.EnumerateObject().Last().Value.GetProperty("payload").GetProperty("schema");
        await Assert.That(tree.GetProperty("$id").GetString()!.StartsWith("urn:relayer:message:", StringComparison.Ordinal)).IsTrue();
        await Assert.That(tree.GetProperty("$schema").GetString()).IsEqualTo("https://json-schema.org/draft/2020-12/schema");
        await Assert.That(tree.GetProperty("properties").GetProperty("child").GetProperty("properties")
            .GetProperty("child").GetProperty("$ref").GetString()).IsEqualTo("#/properties/child");
    }

    [Test]
    public async Task Export_before_serializer_resolution_preserves_serialization_contract()
    {
        using var provider = Create();
        _ = Export(provider);
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var buffer = new ArrayBufferWriter<byte>();
        var message = new ExportOrder("Ada", [1, 2]);
        serializer.Serialize(message, buffer);
        using var document = JsonDocument.Parse(buffer.WrittenMemory);
        await Assert.That(document.RootElement.GetProperty("customer_name").GetString()).IsEqualTo("Ada");
        var roundTrip = serializer.Deserialize<ExportOrder>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        await Assert.That(roundTrip.CustomerName).IsEqualTo(message.CustomerName);
        await Assert.That(roundTrip.Items).IsEquivalentTo(message.Items);
    }

    [Test]
    public async Task Export_is_byte_deterministic_across_calls_providers_and_cultures()
    {
        using var first = Create();
        using var second = Create();
        var expected = Export(first);
        var culture = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ar-SA");
            await Assert.That(Export(first)).IsEqualTo(expected);
            await Assert.That(Export(second)).IsEqualTo(expected);
        }
        finally { CultureInfo.CurrentCulture = culture; }
    }

    [Test]
    public async Task Empty_topology_exports_without_json_metadata()
    {
        using var provider = new ServiceCollection().AddRelayer(_ => { }).BuildServiceProvider();
        var json = Export(provider);
        if (Environment.GetEnvironmentVariable("RELAYER_ASYNCAPI_TEST_OUTPUT") is { } path)
            File.WriteAllText(Path.Combine(Path.GetDirectoryName(path)!, "empty.json"), json);
        using var document = JsonDocument.Parse(json);
        await Assert.That(document.RootElement.GetProperty("channels").EnumerateObject().Count()).IsEqualTo(0);
        await Assert.That(document.RootElement.GetProperty("components").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(0);
    }

    [Test]
    public async Task Missing_metadata_fails_with_registration_guidance_before_writing()
    {
        using var provider = new ServiceCollection().AddRelayer(builder => builder.MessageTypes.Add<ExportOrder>("order")).BuildServiceProvider();
        using var output = new MemoryStream();
        var failure = await Assert.That(() => provider.GetRequiredService<ITopology>().ExportAsyncApi(output,
            new() { Title = "Orders", Version = "1" })).Throws<InvalidOperationException>();
        await Assert.That(failure!.Message).Contains("source-generated JSON metadata");
        await Assert.That(output.Length).IsEqualTo(0);
    }

    [Test]
    public async Task Invalid_document_identity_is_rejected_before_writing()
    {
        using var provider = Create();
        using var output = new MemoryStream();
        var topology = provider.GetRequiredService<ITopology>();
        await Assert.That(() => topology.ExportAsyncApi(output, new() { Title = " ", Version = "1" })).Throws<ArgumentException>();
        await Assert.That(() => topology.ExportAsyncApi(output, new() { Title = "Orders", Version = "" })).Throws<ArgumentException>();
        await Assert.That(output.Length).IsEqualTo(0);
    }

    [Test]
    public async Task Undeclared_message_identity_fails_before_writing()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("events",
                transport => transport.Send<ExportOrder>("orders"))).BuildServiceProvider();
        using var output = new MemoryStream();
        var failure = await Assert.That(() => provider.GetRequiredService<ITopology>().ExportAsyncApi(output,
            new() { Title = "Orders", Version = "1" })).Throws<InvalidOperationException>();
        await Assert.That(failure!.Message).Contains("stable message identity");
        await Assert.That(output.Length).IsEqualTo(0);
    }

    [Test]
    public async Task Tombstone_routes_reuse_original_message_identity()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.MessageTypes.Add<ExportOrder>("order");
            builder.UseJsonSerializer(ExportJsonContext.Default);
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("events",
                transport => transport.Send<Tombstone<ExportOrder>>("orders"));
        }).BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        await Assert.That(document.RootElement.GetProperty("components").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(1);
        await Assert.That(document.RootElement.GetProperty("channels").GetProperty("channel0").GetProperty("messages")
            .EnumerateObject().Count()).IsEqualTo(1);
    }

    private static string Export(ServiceProvider provider)
    {
        using var output = new MemoryStream();
        provider.GetRequiredService<ITopology>().ExportAsyncApi(output, new() { Title = "Orders", Version = "1.2.3" });
        return Encoding.UTF8.GetString(output.ToArray());
    }

    private static ServiceProvider Create()
    {
        return new ServiceCollection().AddRelayer(builder =>
        {
            builder.MessageTypes.Add<ExportOrder>("orders/~é", 2).Add<ExportOldOrder>("orders/~é", 1).Add<ExportTree>("tree");
            builder.UseJsonSerializer(ExportDefaultContext.Default);
            builder.UseJsonSerializer(ExportJsonContext.Default);
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("first/~", transport =>
            {
                transport.Send<ExportOrder>("orders/~é");
                transport.Consume("orders/~é", consumer => consumer.Handler<ExportHandler>().Handler<ExportOtherHandler>());
                transport.Consume("inbound-only", consumer => consumer.Handler<ExportHandler>());
            });
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("second",
                transport => transport.Send<ExportOldOrder>("orders/~é"));
        }).BuildServiceProvider();
    }
}

public sealed record ExportOrder(string CustomerName, int[] Items);
public sealed record ExportOldOrder(string Name);
public sealed record ExportTree(string Name, ExportTree? Child);
public sealed class ExportHandler : IHandler<ExportOrder>
{
    public ValueTask HandleAsync(ExportOrder message, MessageContext context, CancellationToken cancellationToken) => default;
}
public sealed class ExportOtherHandler : IHandler<ExportOrder>
{
    public ValueTask HandleAsync(ExportOrder message, MessageContext context, CancellationToken cancellationToken) => default;
}

[JsonSerializable(typeof(ExportOrder))]
internal partial class ExportDefaultContext : JsonSerializerContext;

[JsonSerializable(typeof(ExportOrder))]
[JsonSerializable(typeof(ExportOldOrder))]
[JsonSerializable(typeof(ExportTree))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower)]
internal partial class ExportJsonContext : JsonSerializerContext;
