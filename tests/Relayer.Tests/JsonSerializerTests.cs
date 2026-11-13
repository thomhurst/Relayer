using Relayer.Transports;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Serialization;

namespace Relayer.Tests;

[Message("tests.json-order")]
public sealed record JsonOrder(string CustomerName, decimal Total, int[] Items);

[Message("tests.json-value")]
public readonly record struct JsonValue(int Count);

[JsonSerializable(typeof(Relayer.TestContracts.Orders.Order))]
[JsonSerializable(typeof(JsonOrder))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower)]
internal partial class CustomJsonContext : JsonSerializerContext;

public class JsonSerializerTests
{
    [Test]
    public async Task Composed_reader_preserves_options_segmented_input_and_invalid_body_checks()
    {
        using var provider = CreateProvider(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip
        });
        var serializer = (JsonMessageSerializer)provider.GetRequiredService<IMessageSerializer>();
        var read = serializer.CreateDeserializer<JsonValue>();
        var first = new Segment("{ /* comment */ \"co"u8.ToArray());
        var last = first.Append("unt\":42,}"u8.ToArray());
        var source = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        await Assert.That(read(source)).IsEqualTo(new JsonValue(42));
        foreach (var invalid in new[] { "null", "{} {}", "{", "" })
        {
            await Assert.That(() => read(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(invalid))))
                .Throws<JsonException>();
        }
    }

    [Test]
    public async Task Exported_contexts_compose_and_allow_custom_context_override()
    {
        var services = new ServiceCollection().AddRelayer(builder =>
        {
            builder.AddOrdersContracts();
            builder.AddBillingContracts();
            builder.AddMessageTypesFromThisAssembly();
            builder.UseJsonSerializer(CustomJsonContext.Default);
        });
        using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new Relayer.TestContracts.Orders.Order(42), buffer);
        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).IsEqualTo("{\"message_id\":42}");
        buffer.Clear();
        serializer.Serialize(new Relayer.TestContracts.Billing.Payment(7), buffer);
        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).IsEqualTo("{\"MessageId\":7}");
        buffer.Clear();
        serializer.Serialize(new JsonValue(9), buffer);
        await Assert.That(serializer.Deserialize<JsonValue>(new ReadOnlySequence<byte>(buffer.WrittenMemory)))
            .IsEqualTo(new JsonValue(9));
    }

    [Test]
    public async Task Exported_contexts_preserve_custom_serializer()
    {
        var custom = new CustomSerializer();
        var services = new ServiceCollection().AddRelayer(builder =>
        {
            builder.AddOrdersContracts();
            builder.AddBillingContracts();
            builder.Services.AddSingleton<IMessageSerializer>(custom);
        });
        using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetRequiredService<IMessageSerializer>()).IsSameReferenceAs(custom);
    }

    [Test]
    public async Task Default_serializer_round_trips_declared_messages()
    {
        using var provider = CreateProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var message = new JsonOrder("Zoë \"Smith\"", 12.5m, [1, 2]);
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(message, buffer);
        var result = serializer.Deserialize<JsonOrder>(new ReadOnlySequence<byte>(buffer.WrittenMemory));

        await Assert.That(serializer.ContentType).IsEqualTo("application/json");
        await Assert.That(result.CustomerName).IsEqualTo(message.CustomerName);
        await Assert.That(result.Total).IsEqualTo(message.Total);
        await Assert.That(result.Items).IsEquivalentTo(message.Items);
    }

    [Test]
    public async Task Reads_segmented_utf8_without_copying_to_a_contiguous_buffer()
    {
        using var provider = CreateProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var bytes = Encoding.UTF8.GetBytes("{\"CustomerName\":\"Zoë\",\"Total\":12.5,\"Items\":[1,2]}");
        var split = Array.IndexOf(bytes, (byte)0xC3) + 1;
        var first = new Segment(bytes.AsMemory(0, split));
        var last = first.Append(bytes.AsMemory(split));
        var source = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);

        var result = serializer.Deserialize<JsonOrder>(source);
        await Assert.That(result.CustomerName).IsEqualTo("Zoë");
        await Assert.That(result.Total).IsEqualTo(12.5m);
    }

    [Test]
    public async Task Generated_context_honors_options()
    {
        using var provider = CreateProvider(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        var buffer = new ArrayBufferWriter<byte>();
        provider.GetRequiredService<IMessageSerializer>().Serialize(new JsonValue(42), buffer);

        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).IsEqualTo("{\"count\":42}");
    }

    [Test]
    public async Task Custom_context_overrides_generated_metadata_and_preserves_other_messages()
    {
        var services = new ServiceCollection().AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.UseJsonSerializer(CustomJsonContext.Default);
        });
        using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new JsonOrder("Ada", 1, []), buffer);
        var result = serializer.Deserialize<JsonOrder>(new ReadOnlySequence<byte>(buffer.WrittenMemory));

        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).Contains("customer_name");
        await Assert.That(result.CustomerName).IsEqualTo("Ada");
        buffer.Clear();
        serializer.Serialize(new JsonValue(7), buffer);
        await Assert.That(serializer.Deserialize<JsonValue>(new ReadOnlySequence<byte>(buffer.WrittenMemory))).IsEqualTo(new JsonValue(7));
    }

    [Test]
    public async Task Rejects_malformed_empty_trailing_and_null_payloads()
    {
        using var provider = CreateProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        foreach (var json in new[] { "", "{", "{} {}", "null" })
        {
            var source = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(json));
            await Assert.That(() => serializer.Deserialize<JsonOrder>(source)).Throws<JsonException>();
        }
    }

    [Test]
    public async Task Writer_is_reusable_after_a_failed_write()
    {
        using var provider = CreateProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        await Assert.That(() => serializer.Serialize(new JsonValue(1), new ThrowingBuffer())).Throws<InvalidOperationException>();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new JsonValue(2), buffer);
        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).IsEqualTo("{\"Count\":2}");
    }

    [Test]
    public async Task Pooled_writer_does_not_retain_the_callers_destination()
    {
        using var provider = CreateProvider();
        var destination = SerializeTemporary(provider.GetRequiredService<IMessageSerializer>());
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        await Assert.That(destination.IsAlive).IsFalse();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference SerializeTemporary(IMessageSerializer serializer)
    {
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new JsonValue(1), buffer);
        return new WeakReference(buffer);
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task Custom_serializer_replaces_json_without_requiring_metadata(int registrationOrder)
    {
        var serializer = new CustomSerializer();
        var services = new ServiceCollection();
        if (registrationOrder == 0)
            services.AddSingleton<IMessageSerializer>(serializer);
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<JsonValue>("manual");
            if (registrationOrder == 1)
                builder.Services.AddSingleton<IMessageSerializer>(serializer);
        });
        if (registrationOrder == 2)
            services.AddSingleton<IMessageSerializer>(serializer);
        using var provider = services.BuildServiceProvider();
        foreach (var service in provider.GetServices<IHostedService>())
            await service.StartAsync(CancellationToken.None);
        await Assert.That(provider.GetRequiredService<IMessageSerializer>()).IsSameReferenceAs(serializer);
    }

    [Test]
    public async Task Rejects_unregistered_types_without_reflection_fallback()
    {
        using var provider = CreateProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        await Assert.That(() => serializer.Serialize(DateTime.UtcNow, new ArrayBufferWriter<byte>()))
            .Throws<InvalidOperationException>().WithMessageContaining("UseJsonSerializer");
    }

    [Test]
    public async Task Generated_context_supports_same_simple_name_in_different_namespaces()
    {
        using var provider = CreateProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new OtherContracts.JsonOrder("reference"), buffer);
        await Assert.That(serializer.Deserialize<OtherContracts.JsonOrder>(new ReadOnlySequence<byte>(buffer.WrittenMemory)))
            .IsEqualTo(new OtherContracts.JsonOrder("reference"));
    }

    [Test]
    public async Task Missing_metadata_fails_at_host_startup()
    {
        using var provider = new ServiceCollection()
            .AddRelayer(builder => builder.Infrastructure().MessageTypes.Add<JsonValue>("manual"))
            .BuildServiceProvider();
        await Assert.That(async () =>
            {
                foreach (var service in provider.GetServices<IHostedService>())
                    await service.StartAsync(CancellationToken.None);
            })
            .Throws<InvalidOperationException>().WithMessageContaining("UseJsonSerializer");
    }

    [Test]
    public async Task Default_serializer_is_validated_at_host_startup()
    {
        using var provider = CreateProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();
        foreach (var service in hostedServices)
            await service.StartAsync(CancellationToken.None);
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        await Assert.That(serializer.ContentType).IsEqualTo("application/json");
        foreach (var service in hostedServices.Reverse())
            await service.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Custom_context_covers_explicitly_registered_types()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<JsonOrder>("manual");
            builder.UseJsonSerializer(CustomJsonContext.Default);
        }).BuildServiceProvider();
        var buffer = new ArrayBufferWriter<byte>();
        provider.GetRequiredService<IMessageSerializer>().Serialize(new JsonOrder("Ada", 1, []), buffer);
        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).Contains("customer_name");
    }

    [Test]
    public async Task Honors_reader_and_writer_options()
    {
        using var provider = CreateProvider(new JsonSerializerOptions
        {
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip,
            WriteIndented = true,
            IndentSize = 4
        });
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var source = new ReadOnlySequence<byte>("/*comment*/{\"Count\":42,} //trailing"u8.ToArray());
        await Assert.That(serializer.Deserialize<JsonValue>(source)).IsEqualTo(new JsonValue(42));
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new JsonValue(42), buffer);
        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).Contains("    \"Count\": 42");
    }

    [Test]
    public async Task Pooled_writer_preserves_custom_encoder_and_indentation_on_reuse()
    {
        using var provider = CreateProvider(new JsonSerializerOptions
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = true,
            IndentCharacter = '\t',
            IndentSize = 2,
            NewLine = "\n"
        });
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var message = new JsonOrder("<Ada>", 1, []);
        var buffer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < 2; i++)
        {
            buffer.Clear();
            serializer.Serialize(message, buffer);
            var json = Encoding.UTF8.GetString(buffer.WrittenSpan);
            await Assert.That(json).Contains("\n\t\t\"CustomerName\": \"<Ada>\"");
            var result = serializer.Deserialize<JsonOrder>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            await Assert.That(result.CustomerName).IsEqualTo(message.CustomerName);
        }
    }

    [Test]
    public async Task Serializer_instances_keep_options_and_writer_pools_isolated()
    {
        using var first = CreateProvider(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        using var second = CreateProvider();
        await Task.WhenAll(Enumerable.Range(0, 32).Select(index => Task.Run(() =>
        {
            var provider = index % 2 == 0 ? first : second;
            var serializer = provider.GetRequiredService<IMessageSerializer>();
            var buffer = new ArrayBufferWriter<byte>();
            for (var count = 0; count < 100; count++)
            {
                buffer.Clear();
                serializer.Serialize(new JsonValue(count), buffer);
                var expectedName = index % 2 == 0 ? "count" : "Count";
                if (Encoding.UTF8.GetString(buffer.WrittenSpan) != $"{{\"{expectedName}\":{count}}}")
                    throw new InvalidOperationException("Serializer options leaked across providers.");
                var result = serializer.Deserialize<JsonValue>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
                if (result.Count != count) throw new InvalidOperationException("Concurrent writer corruption.");
            }
        })));
    }

    [Test]
    public async Task Second_registration_fails_before_configuring_or_changing_the_serializer()
    {
        var services = new ServiceCollection().AddRelayer(builder => builder.AddMessageTypesFromThisAssembly());
        var configured = false;
        await Assert.That(() => services.AddRelayer(builder =>
        {
            configured = true;
            builder.AddMessageTypesFromThisAssembly(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        })).Throws<InvalidOperationException>().WithMessageContaining("single call");

        await Assert.That(configured).IsFalse();
        using var provider = services.BuildServiceProvider();
        var buffer = new ArrayBufferWriter<byte>();
        provider.GetRequiredService<IMessageSerializer>().Serialize(new JsonValue(42), buffer);
        await Assert.That(Encoding.UTF8.GetString(buffer.WrittenSpan)).IsEqualTo("{\"Count\":42}");
    }

    [Test]
    public async Task Context_metadata_does_not_register_additional_message_types()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<JsonValue>("manual");
            builder.UseJsonSerializer(new RelayerGeneratedJsonContext());
        }).BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new JsonValue(42), buffer);
        await Assert.That(serializer.Deserialize<JsonValue>(new ReadOnlySequence<byte>(buffer.WrittenMemory))).IsEqualTo(new JsonValue(42));
        await Assert.That(() => serializer.Serialize(new JsonOrder("Ada", 1, []), buffer))
            .Throws<InvalidOperationException>().WithMessageContaining("UseJsonSerializer");
        await Assert.That(() => serializer.Deserialize<JsonOrder>(new ReadOnlySequence<byte>(buffer.WrittenMemory)))
            .Throws<InvalidOperationException>().WithMessageContaining("UseJsonSerializer");
    }

    private static ServiceProvider CreateProvider(JsonSerializerOptions? options = null) =>
        new ServiceCollection().AddRelayer(builder => builder.AddMessageTypesFromThisAssembly(options)).BuildServiceProvider();

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

    private sealed class ThrowingBuffer : IBufferWriter<byte>
    {
        public void Advance(int count) => throw new InvalidOperationException("Test write failure.");
        public Memory<byte> GetMemory(int sizeHint = 0) => throw new InvalidOperationException("Test write failure.");
        public Span<byte> GetSpan(int sizeHint = 0) => throw new InvalidOperationException("Test write failure.");
    }

    private sealed class CustomSerializer : IMessageSerializer
    {
        public string ContentType => "custom";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }
}
