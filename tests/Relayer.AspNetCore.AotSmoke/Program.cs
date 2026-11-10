using Relayer;
using Relayer.AspNetCore;

var builder = WebApplication.CreateSlimBuilder(args);
builder.WebHost.UseUrls("http://127.0.0.1:0");
builder.Services.AddKeyedSingleton<IDeadLetterManager>("bus", new SmokeManager());
await using var app = builder.Build();
app.MapRelayerDeadLetters("bus");
await app.StartAsync();
using var client = new HttpClient { BaseAddress = new Uri(app.Urls.Single()) };
var sources = await client.GetStringAsync("/relayer/dead-letters/");
if (!sources.Contains("orders", StringComparison.Ordinal)) throw new InvalidOperationException(sources);
var page = await client.GetStringAsync("/relayer/dead-letters/messages?source=orders");
if (!page.Contains("AQI=", StringComparison.Ordinal)) throw new InvalidOperationException(page);
foreach (var operation in new[] { "redrive", "purge" })
{
    using var body = new StringContent("""{"source":"orders","maxMessages":2}""", System.Text.Encoding.UTF8, "application/json");
    using var response = await client.PostAsync("/relayer/dead-letters/" + operation, body);
    response.EnsureSuccessStatusCode();
    if (await response.Content.ReadAsStringAsync() != """{"affectedMessages":1}""") throw new InvalidOperationException(operation);
}
using var invalid = await client.GetAsync("/relayer/dead-letters/messages?maxMessages=101");
if ((int)invalid.StatusCode != 400) throw new InvalidOperationException("Expected validation error");
await app.StopAsync();
Console.WriteLine("Dead-letter HTTP Native AOT smoke passed.");

sealed class SmokeManager : IDeadLetterManager
{
    public IReadOnlyList<DeadLetterSource> List() => [new("orders", "orders")];
    public ValueTask<DeadLetterPage> PeekAsync(string source, int maxMessages = 20, string? cursor = null, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(new DeadLetterPage([new() { MessageId = "id", Body = new byte[] { 1, 2 } }], null));
    public ValueTask<int> RedriveAsync(string source, Func<DeadLetterMessage, bool>? filter = null, int maxMessages = 20, CancellationToken cancellationToken = default) => ValueTask.FromResult(1);
    public ValueTask<int> PurgeAsync(string source, int maxMessages = 20, CancellationToken cancellationToken = default) => ValueTask.FromResult(1);
}
