using System.Text.Json;
using System.Text.Json.Serialization;
using JsonPayload = System.Text.Json.Serialization.JsonSerializableAttribute;

namespace Relayer.Tests.JsonContextIsolation;

[JsonPayload(typeof(int)), JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
#if NET11_0_OR_GREATER
[JsonPayload(typeof(AuditPayload))]
#endif
internal partial class RelayerGeneratedJsonContext : JsonSerializerContext;

#if NET11_0_OR_GREATER
internal sealed class AuditPayload
{
    [JsonInclude]
    private int Number
    {
        get;
        set;
    }

    public int GetNumber()
    {
        return Number;
    }
}
#endif

public class JsonContextIsolationTests
{
    [Test]
    public async Task Same_named_contexts_both_supply_metadata()
    {
        int number = JsonSerializer.Deserialize("42", RelayerGeneratedJsonContext.Default.Int32);
        await Assert.That(number).IsEqualTo(42);

        var message = (JsonValue)JsonSerializer.Deserialize(
            """{"Count":42}""", typeof(JsonValue), global::Relayer.RelayerGeneratedJsonContext.Default)!;
        await Assert.That(message).IsEqualTo(new JsonValue(42));
    }

#if NET11_0_OR_GREATER
    [Test]
    public async Task Application_context_uses_the_sdk_generators_private_member_support()
    {
        var payload = JsonSerializer.Deserialize("""{"number":42}""", RelayerGeneratedJsonContext.Default.AuditPayload);
        await Assert.That(payload!.GetNumber()).IsEqualTo(42);
    }
#endif
}
