using Relayer.Transports;
using Relayer.Pipeline;
#pragma warning disable RLY001 // Export fixtures use explicit MessageTypeRegistry registrations.
using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
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
        topology.ExportAsyncApi(output, new()
        {
            Title = "Orders",
            Version = "1.2.3"
        });
        using var document = JsonDocument.Parse(output.ToArray());

        if (Environment.GetEnvironmentVariable("RELAYER_ASYNCAPI_TEST_OUTPUT") is { } path)
        {
            File.WriteAllBytes(path, output.ToArray());
        }

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
    public async Task Receive_lists_only_versions_that_can_reach_its_selected_handler()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            var infrastructure = builder.Infrastructure();
            infrastructure.MessageTypes.Add<ExportOldOrder>("order", 1).Add<ExportMiddleOrder>("order", 2)
                .Add<ExportOrder>("order", 3).Add<ExportTree>("order", 4);
            infrastructure.Upcasters.Add(new ExportFirstUpgrade()).Add(new ExportSecondUpgrade());
            builder.UseJsonSerializer(ExportJsonContext.Default);
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("events", transport =>
            {
                transport.Send<ExportOrder>("orders");
                transport.Consume("orders", consumer => consumer.Handler<ExportHandler>());
                transport.Consume("another-version", consumer => consumer.Handler<ExportTreeHandler>());
                transport.Consume("tombstones", consumer => consumer.Handler<ExportTombstoneHandler>());
            });
        }).BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        var root = document.RootElement;
        var received = OperationMessages(root, "operation1");

        await Assert.That(received.Keys).IsEquivalentTo(new[] { "order;v=1", "order;v=2", "order;v=3" });
        await Assert.That(Schema(received["order;v=1"]).GetProperty("properties").EnumerateObject().Single().Name).IsEqualTo("name");
        await Assert.That(Schema(received["order;v=2"]).GetProperty("properties").EnumerateObject().Single().Name).IsEqualTo("order_number");
        await Assert.That(OperationMessages(root, "operation0").Keys).IsEquivalentTo(new[] { "order;v=3" });
        await Assert.That(OperationMessages(root, "operation2").Keys).IsEquivalentTo(new[] { "order;v=4" });
        await Assert.That(OperationMessages(root, "operation3").Keys).IsEquivalentTo(new[] { "order;v=3" });
        await Assert.That(root.GetProperty("channels").GetProperty("channel0").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(3);
    }

    [Test]
    public async Task Receive_aliases_use_the_selected_handlers_payload_only_on_the_declaring_consumer()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<ExportOrder>("order").Add<ExportOldOrder>("order.legacy");
            builder.UseJsonSerializer(ExportJsonContext.Default);
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("events", transport =>
            {
                transport.Send<ExportOldOrder>("import");
                transport.Consume("import", consumer => consumer.Handler<ExportHandler>()
                    .Accepts<ExportOrder>(new MessageType("order.legacy", 1), new MessageType("orders/~é", 2)));
                transport.Consume("normal", consumer => consumer.Handler<ExportHandler>().ConfigurePipeline(
                    _ => throw new InvalidOperationException("Offline export must not run pipeline callbacks.")));
            });
        }).BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        var root = document.RootElement;
        var imported = OperationMessages(root, "operation1");

        await Assert.That(imported.Keys).IsEquivalentTo(new[] { "order;v=1", "order.legacy;v=1", "orders/~é;v=2" });
        await Assert.That(imported["orders/~é;v=2"].GetProperty("name").GetString()).IsEqualTo("orders/~é");
        await Assert.That(imported["orders/~é;v=2"].GetProperty("x-relayer-version").GetInt32()).IsEqualTo(2);
        await Assert.That(Schema(imported["order.legacy;v=1"]).GetProperty("properties").TryGetProperty("customer_name", out _)).IsTrue();
        await Assert.That(Schema(imported["orders/~é;v=2"]).GetProperty("properties").TryGetProperty("customer_name", out _)).IsTrue();
        var sent = OperationMessages(root, "operation0");
        await Assert.That(Schema(sent["order.legacy;v=1"]).GetProperty("properties").EnumerateObject().Single().Name).IsEqualTo("name");
        await Assert.That(Schema(imported["order.legacy;v=1"]).GetProperty("$id").GetString())
            .IsNotEqualTo(Schema(imported["order;v=1"]).GetProperty("$id").GetString());
        await Assert.That(OperationMessages(root, "operation2").Keys).IsEquivalentTo(new[] { "order;v=1" });
        await Assert.That(root.GetProperty("channels").GetProperty("channel0").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(4);
        await Assert.That(root.GetProperty("channels").GetProperty("channel1").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Receive_matches_aliases_by_required_wire_type_headers()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<ExportOrder>("order.placed");
            builder.UseJsonSerializer(ExportJsonContext.Default);
            builder.InMemory("events", transport =>
            {
                transport.Consume("migration", consumer => consumer.Handler<ExportHandler>()
                    .Accepts<ExportOrder>(new MessageType("order.created", 2)));
                transport.Consume("canonical-only", consumer => consumer.Handler<ExportHandler>());
            });
        }).BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        var root = document.RootElement;
        var channels = root.GetProperty("channels");

        foreach (var (index, expectedTypes) in new[]
        {
            (0, new[] { "order.placed;v=1", "order.created;v=2" }),
            (1, new[] { "order.placed;v=1" })
        })
        {
            var operationMessages = OperationMessages(root, $"operation{index}");
            var channelMessages = channels.GetProperty($"channel{index}").GetProperty("messages").EnumerateObject()
                .Select(entry => ResolveReference(root, entry.Value.GetProperty("$ref").GetString()!));
            await Assert.That(operationMessages.Values.Select(RequiredWireType)).IsEquivalentTo(expectedTypes);
            await Assert.That(channelMessages.Select(RequiredWireType)).IsEquivalentTo(expectedTypes);
        }
    }

    [Test]
    public async Task Send_and_receive_alias_share_a_channel_message_when_the_contracts_match()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<LegacyExportOrder>("order.created")
                .Add<CurrentExportOrder>("order.placed");
            builder.UseJsonSerializer(ExportDefaultContext.Default);
            builder.InMemory("events", transport =>
            {
                transport.Send<LegacyExportOrder>("orders");
                transport.Consume("orders", consumer => consumer.Handler<CurrentExportOrderHandler>()
                    .Accepts<CurrentExportOrder>(new MessageType("order.created", 1)));
            });
        }).BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        var root = document.RootElement;
        var channel = root.GetProperty("channels").GetProperty("channel0");
        const string LegacyWireType = "order.created;v=1";
        var channelMatches = channel.GetProperty("messages").EnumerateObject()
            .Select(entry => ResolveReference(root, entry.Value.GetProperty("$ref").GetString()!))
            .Where(message => RequiredWireType(message) == LegacyWireType)
            .ToArray();

        await Assert.That(channel.GetProperty("address").GetString()).IsEqualTo("orders");
        await Assert.That(channelMatches.Length).IsEqualTo(1);
        await Assert.That(Schema(channelMatches[0]).GetProperty("properties").GetProperty("Number")
            .GetProperty("type").GetString()).IsEqualTo("string");
        var sent = OperationMessages(root, "operation0");
        var received = OperationMessages(root, "operation1");
        await Assert.That(sent.Keys).IsEquivalentTo(new[] { LegacyWireType });
        await Assert.That(received.Keys).IsEquivalentTo(new[] { LegacyWireType, "order.placed;v=1" });
        await Assert.That(sent[LegacyWireType]).IsEqualTo(channelMatches[0]);
        await Assert.That(received[LegacyWireType]).IsEqualTo(channelMatches[0]);
    }

    [Test]
    public async Task Different_schemas_for_the_same_wire_identity_get_distinct_resource_ids()
    {
        using var first = CreateWithSharedIdentity<ExportOrder>();
        using var second = CreateWithSharedIdentity<ExportOldOrder>();
        using var firstDocument = JsonDocument.Parse(Export(first));
        using var secondDocument = JsonDocument.Parse(Export(second));
        var firstComponent = firstDocument.RootElement.GetProperty("components").GetProperty("messages").EnumerateObject().Single();
        var secondComponent = secondDocument.RootElement.GetProperty("components").GetProperty("messages").EnumerateObject().Single();

        await Assert.That(firstComponent.Name).IsEqualTo(secondComponent.Name);
        await Assert.That(Schema(firstComponent.Value).GetProperty("$id").GetString())
            .IsNotEqualTo(Schema(secondComponent.Value).GetProperty("$id").GetString());
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
        finally
        {
            CultureInfo.CurrentCulture = culture;
        }
    }

    [Test]
    public async Task Empty_topology_exports_without_json_metadata()
    {
        using var provider = new ServiceCollection().AddRelayer(_ =>
        {
        }).BuildServiceProvider();
        var json = Export(provider);

        if (Environment.GetEnvironmentVariable("RELAYER_ASYNCAPI_TEST_OUTPUT") is { } path)
        {
            File.WriteAllText(Path.Combine(Path.GetDirectoryName(path)!, "empty.json"), json);
        }

        using var document = JsonDocument.Parse(json);
        await Assert.That(document.RootElement.GetProperty("channels").EnumerateObject().Count()).IsEqualTo(0);
        await Assert.That(document.RootElement.GetProperty("components").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(0);
    }

    [Test]
    public async Task Missing_metadata_fails_with_registration_guidance_before_writing()
    {
        using var provider = new ServiceCollection().AddRelayer(builder => builder.Infrastructure().MessageTypes.Add<ExportOrder>("order")).BuildServiceProvider();
        using var output = new MemoryStream();
        var failure = await Assert.That(() => provider.GetRequiredService<ITopology>().ExportAsyncApi(output,
            new()
            {
                Title = "Orders",
                Version = "1"
            })).Throws<InvalidOperationException>();
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
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("events",
                transport => transport.Send<ExportOrder>("orders"))).BuildServiceProvider();
        using var output = new MemoryStream();
        var failure = await Assert.That(() => provider.GetRequiredService<ITopology>().ExportAsyncApi(output,
            new()
            {
                Title = "Orders",
                Version = "1"
            })).Throws<InvalidOperationException>();
        await Assert.That(failure!.Message).Contains("stable message identity");
        await Assert.That(output.Length).IsEqualTo(0);
    }

    [Test]
    public async Task Export_uses_the_generated_context_for_each_message_root()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.AddOrdersContracts(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
            builder.AddBillingContracts();
        }).BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        var messages = document.RootElement.GetProperty("components").GetProperty("messages");
        var order = messages.EnumerateObject().Single(item => item.Value.GetProperty("name").GetString() == "orders.shared").Value;
        var properties = order.GetProperty("payload").GetProperty("schema").GetProperty("properties");
        await Assert.That(properties.EnumerateObject().Single().Name).IsEqualTo("messageId");
    }

    [Test]
    public async Task Tombstone_routes_reuse_original_message_identity()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<ExportOrder>("order");
            builder.UseJsonSerializer(ExportJsonContext.Default);
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("events",
                transport => transport.Send<Tombstone<ExportOrder>>("orders"));
        }).BuildServiceProvider();
        using var document = JsonDocument.Parse(Export(provider));
        await Assert.That(document.RootElement.GetProperty("components").GetProperty("messages").EnumerateObject().Count()).IsEqualTo(1);
        await Assert.That(document.RootElement.GetProperty("channels").GetProperty("channel0").GetProperty("messages")
            .EnumerateObject().Count()).IsEqualTo(1);
    }

    private static Dictionary<string, JsonElement> OperationMessages(JsonElement root, string operationName)
    {
        var result = new Dictionary<string, JsonElement>();
        var operation = root.GetProperty("operations").GetProperty(operationName);
        var channelPath = operation.GetProperty("channel").GetProperty("$ref").GetString()!;
        var channelMessages = ResolveReference(root, channelPath).GetProperty("messages");
        var messagePrefix = channelPath + "/messages/";

        foreach (var reference in operation.GetProperty("messages").EnumerateArray())
        {
            var path = reference.GetProperty("$ref").GetString()!;

            if (!path.StartsWith(messagePrefix, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Expected the operation to reference its channel's message.");
            }

            var channelMessage = channelMessages.GetProperty(path[messagePrefix.Length..]);
            var component = ResolveReference(root, channelMessage.GetProperty("$ref").GetString()!);
            result.Add(component.GetProperty("x-relayer-wire-type").GetString()!, component);
        }

        return result;
    }

    private static JsonElement ResolveReference(JsonElement root, string path)
    {
        foreach (var segment in path[2..].Split('/'))
        {
            root = root.GetProperty(segment);
        }

        return root;
    }

    private static string RequiredWireType(JsonElement message)
    {
        var headers = message.GetProperty("headers");

        if (headers.GetProperty("type").GetString() != "object" ||
            !headers.GetProperty("required").EnumerateArray().Any(header => header.GetString() == "ce_type"))
        {
            throw new InvalidOperationException("Expected a required ce_type application header.");
        }

        return headers.GetProperty("properties").GetProperty("ce_type").GetProperty("const").GetString()!;
    }

    private static JsonElement Schema(JsonElement message)
    {
        return message.GetProperty("payload").GetProperty("schema");
    }

    private static ServiceProvider CreateWithSharedIdentity<TMessage>()
    {
        return new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<TMessage>("shared");
            builder.UseJsonSerializer(ExportJsonContext.Default);
        }).BuildServiceProvider();
    }

    private static string Export(ServiceProvider provider)
    {
        using var output = new MemoryStream();
        provider.GetRequiredService<ITopology>().ExportAsyncApi(output, new()
        {
            Title = "Orders",
            Version = "1.2.3"
        });
        return Encoding.UTF8.GetString(output.ToArray());
    }

    private static ServiceProvider Create()
    {
        return new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<ExportOrder>("orders/~é", 2).Add<ExportOldOrder>("orders/~é", 1).Add<ExportTree>("tree");
            builder.UseJsonSerializer(ExportDefaultContext.Default);
            builder.UseJsonSerializer(ExportJsonContext.Default);
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("first/~", transport =>
            {
                transport.Send<ExportOrder>("orders/~é");
                transport.Consume("orders/~é", consumer => consumer.Handler<ExportHandler>().Handler<ExportOtherHandler>());
                transport.Consume("inbound-only", consumer => consumer.Handler<ExportHandler>());
            });
            builder.AddTransport<BuilderTests.BrokerOptions, BuilderTests.ConsumerOptions>("second",
                transport => transport.Send<ExportOldOrder>("orders/~é"));
        }).BuildServiceProvider();
    }
}

public sealed record ExportOrder(string CustomerName, int[] Items);

public sealed record ExportOldOrder(string Name);

public sealed record LegacyExportOrder(string Number);

public sealed record CurrentExportOrder(string Number);

public sealed record ExportTree(string Name, ExportTree? Child);

public sealed class CurrentExportOrderHandler : IHandler<CurrentExportOrder>
{
    public ValueTask HandleAsync(CurrentExportOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        return default;
    }
}

public sealed class ExportHandler : IHandler<ExportOrder>
{
    public ValueTask HandleAsync(ExportOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        return default;
    }
}

public sealed class ExportOtherHandler : IHandler<ExportOrder>
{
    public ValueTask HandleAsync(ExportOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        return default;
    }
}

public sealed record ExportMiddleOrder(int OrderNumber);

public sealed class ExportFirstUpgrade : IUpcaster<ExportOldOrder, ExportMiddleOrder>
{
    public ExportMiddleOrder Upcast(ExportOldOrder message)
    {
        return new(message.Name.Length);
    }
}

public sealed class ExportSecondUpgrade : IUpcaster<ExportMiddleOrder, ExportOrder>
{
    public ExportOrder Upcast(ExportMiddleOrder message)
    {
        return new("Customer", [message.OrderNumber]);
    }
}

public sealed class ExportTreeHandler : IHandler<ExportTree>
{
    public ValueTask HandleAsync(ExportTree message, MessageContext context, CancellationToken cancellationToken)
    {
        return default;
    }
}

public sealed class ExportTombstoneHandler : IHandler<Tombstone<ExportOrder>>
{
    public ValueTask HandleAsync(Tombstone<ExportOrder> message, MessageContext context, CancellationToken cancellationToken)
    {
        return default;
    }
}

[JsonSerializable(typeof(ExportOrder))]
[JsonSerializable(typeof(LegacyExportOrder))]
[JsonSerializable(typeof(CurrentExportOrder))]
internal partial class ExportDefaultContext : JsonSerializerContext;

[JsonSerializable(typeof(ExportOrder))]
[JsonSerializable(typeof(ExportOldOrder))]
[JsonSerializable(typeof(ExportMiddleOrder))]
[JsonSerializable(typeof(ExportTree))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower)]
internal partial class ExportJsonContext : JsonSerializerContext;
