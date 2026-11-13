using Relayer.Transports;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Relayer;

internal static class AsyncApiSmoke
{
    internal static void Run()
    {
        using var provider = new ServiceCollection().AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<AsyncApiTree>("smoke.tree", 2);
            builder.UseJsonSerializer(AsyncApiSmokeContext.Default);
        }).BuildServiceProvider();
        using var output = new MemoryStream();
        provider.GetRequiredService<ITopology>().ExportAsyncApi(output, new() { Title = "Smoke", Version = "1" });
        using var document = JsonDocument.Parse(output.ToArray());
        var message = document.RootElement.GetProperty("components").GetProperty("messages").EnumerateObject().Single().Value;
        if (message.GetProperty("name").GetString() != "smoke.tree" ||
            !message.GetProperty("payload").GetProperty("schema").GetProperty("properties").TryGetProperty("Name", out _))
            throw new InvalidOperationException("AsyncAPI JSON contract export failed under Native AOT.");
        // Optional artifact for specification/schema validation outside the smoke process.
        if (Environment.GetEnvironmentVariable("RELAYER_ASYNCAPI_OUTPUT") is { } path)
            File.WriteAllBytes(path, output.ToArray());
        Console.WriteLine("AsyncAPI export passed under Native AOT.");
    }
}

internal sealed record AsyncApiTree(string Name, AsyncApiTree? Child);
[JsonSerializable(typeof(AsyncApiTree))]
internal partial class AsyncApiSmokeContext : JsonSerializerContext;
