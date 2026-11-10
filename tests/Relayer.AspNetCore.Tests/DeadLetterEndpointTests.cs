using System.Net;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Relayer.AspNetCore;

namespace Relayer.AspNetCore.Tests;

public sealed class DeadLetterEndpointTests
{
    [Test]
    public async Task Routes_round_trip_portable_snapshots_and_forward_bounds_cursor_and_filter()
    {
        await using var fixture = await Fixture.Start();
        var list = await fixture.Client.GetStringAsync("/manage/");
        await Assert.That(list).Contains("events/Subscriptions/audit");
        var page = await fixture.Client.GetStringAsync("/manage/messages?source=events%2FSubscriptions%2Faudit&maxMessages=3&cursor=42");
        using var json = JsonDocument.Parse(page);
        await Assert.That(json.RootElement.GetProperty("messages")[0].GetProperty("body").GetString()).IsEqualTo("AQI=");
        await Assert.That(page.Contains("features", StringComparison.OrdinalIgnoreCase)).IsFalse();
        await Assert.That(fixture.Manager.Source).IsEqualTo("events/Subscriptions/audit");
        await Assert.That(fixture.Manager.Count).IsEqualTo(3);
        await Assert.That(fixture.Manager.Cursor).IsEqualTo("42");
        var response = await fixture.Post("redrive", """{"source":"orders","maxMessages":7,"messageIds":["selected"]}""");
        await Assert.That(await response.Content.ReadAsStringAsync()).IsEqualTo("""{"affectedMessages":1}""");
        await Assert.That(fixture.Manager.Count).IsEqualTo(7);
        await Assert.That(fixture.Manager.Filter!(new() { MessageId = "selected" })).IsTrue();
        await Assert.That(fixture.Manager.Filter(new() { MessageId = "other" })).IsFalse();
        await Assert.That(fixture.Manager.Filter(new())).IsFalse();
        response = await fixture.Post("purge", """{"source":"orders"}""");
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(fixture.Manager.Count).IsEqualTo(20);
        await Assert.That(fixture.Manager.Purged).IsTrue();
    }

    [Test]
    [Arguments("redrive", "null")]
    [Arguments("redrive", """{"source":"orders","messageId":["x"]}""")]
    [Arguments("redrive", "{")]
    [Arguments("redrive", """{"source":"orders","maxMessages":101}""")]
    [Arguments("purge", """{"source":"orders","maxMessages":0}""")]
    [Arguments("redrive", """{"source":" "}""")]
    [Arguments("redrive", """{"source":"orders","messageIds":[]}""")]
    [Arguments("redrive", """{"source":"orders","messageIds":[null]}""")]
    [Arguments("purge", """{"source":"orders","messageIds":["x"]}""")]
    public async Task Invalid_bodies_are_actionable_and_do_not_call_manager(string route, string body)
    {
        await using var fixture = await Fixture.Start();
        var response = await fixture.Post(route, body);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(await response.Content.ReadAsStringAsync()).Contains("error");
        await Assert.That(fixture.Manager.Calls).IsEqualTo(0);
    }

    [Test]
    [Arguments("/manage/messages")]
    [Arguments("/manage/messages?source=orders&maxMessages=no")]
    [Arguments("/manage/messages?source=orders&maxMessages=0")]
    [Arguments("/manage/messages?source=a&source=b")]
    [Arguments("/manage/messages?source=orders&cursor=a&cursor=b")]
    [Arguments("/manage/messages?source=unknown")]
    public async Task Invalid_queries_and_manager_validation_return_bad_request(string path)
    {
        await using var fixture = await Fixture.Start();
        var response = await fixture.Client.GetAsync(path);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(await response.Content.ReadAsStringAsync()).Contains("error");
    }

    [Test]
    public async Task Wrong_content_type_is_rejected()
    {
        await using var fixture = await Fixture.Start();
        var response = await fixture.Client.PostAsync("/manage/purge", new StringContent("""{}"""));
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(fixture.Manager.Calls).IsEqualTo(0);
    }

    [Test]
    public async Task Application_policy_protects_all_routes_before_manager_calls()
    {
        await using var fixture = await Fixture.Start(protect: true);
        foreach (var path in new[] { "/manage/", "/manage/messages?source=orders", "/manage/redrive", "/manage/purge" })
        {
            using var request = new HttpRequestMessage(path.EndsWith("redrive") || path.EndsWith("purge") ? HttpMethod.Post : HttpMethod.Get, path);
            using var response = await fixture.Client.SendAsync(request);
            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        }
        await Assert.That(fixture.Manager.Calls).IsEqualTo(0);
        fixture.Client.DefaultRequestHeaders.Add("X-Operator", "true");
        await Assert.That((await fixture.Client.GetAsync("/manage/")).StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That((await fixture.Post("purge", """{"source":"orders"}""")).StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    [Test]
    public async Task Request_cancellation_reaches_manager()
    {
        await using var fixture = await Fixture.Start();
        fixture.Manager.WaitForCancellation = true;
        using var cancellation = new CancellationTokenSource();
        var request = fixture.Client.GetAsync("/manage/messages?source=orders", cancellation.Token);
        await fixture.Manager.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        cancellation.Cancel();
        try { await request; } catch (OperationCanceledException) { }
        await fixture.Manager.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Test]
    public async Task Missing_named_manager_fails_during_mapping()
    {
        var builder = WebApplication.CreateSlimBuilder();
        await using var app = builder.Build();
        await Assert.That(() => app.MapRelayerDeadLetters("missing")).Throws<InvalidOperationException>();
    }

    private sealed class Fixture(WebApplication app, HttpClient client, Manager manager) : IAsyncDisposable
    {
        public HttpClient Client => client;
        public Manager Manager => manager;
        public Task<HttpResponseMessage> Post(string route, string json) => client.PostAsync("/manage/" + route, new StringContent(json, Encoding.UTF8, "application/json"));
        public static async Task<Fixture> Start(bool protect = false)
        {
            var manager = new Manager();
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Services.AddKeyedSingleton<IDeadLetterManager>("bus", manager);
            builder.Services.AddAuthentication("test").AddScheme<AuthenticationSchemeOptions, TestAuthenticationHandler>("test", _ => { });
            builder.Services.AddAuthorization(options => options.AddPolicy("operator", policy => policy.RequireClaim("operator")));
            var app = builder.Build();
            app.UseAuthentication();
            app.UseAuthorization();
            var group = app.MapRelayerDeadLetters("bus", "/manage");
            if (protect) group.RequireAuthorization("operator");
            await app.StartAsync();
            return new(app, new HttpClient { BaseAddress = new Uri(app.Urls.Single()) }, manager);
        }
        public async ValueTask DisposeAsync() { client.Dispose(); await app.DisposeAsync(); }
    }

    private sealed class TestAuthenticationHandler(IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger, UrlEncoder encoder) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            Claim[] claims = Request.Headers.ContainsKey("X-Operator") ? [new("operator", "true")] : [];
            return Task.FromResult(AuthenticateResult.Success(new(new ClaimsPrincipal(new ClaimsIdentity(claims, "test")), "test")));
        }
    }

    private sealed class Manager : IDeadLetterManager
    {
        public string? Source;
        public string? Cursor;
        public int Count;
        public int Calls;
        public bool Purged;
        public bool WaitForCancellation;
        public Func<DeadLetterMessage, bool>? Filter;
        public TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public IReadOnlyList<DeadLetterSource> List() { Calls++; return [new("events/Subscriptions/audit", "events")]; }
        public async ValueTask<DeadLetterPage> PeekAsync(string source, int maxMessages = 20, string? cursor = null, CancellationToken cancellationToken = default)
        {
            Record(source, maxMessages); Cursor = cursor;
            if (WaitForCancellation)
            {
                Entered.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
                catch (OperationCanceledException) { Cancelled.TrySetResult(); throw; }
            }
            var message = new DeadLetterMessage { MessageId = "selected", Body = new byte[] { 1, 2 } };
            message.Features.Set(new object());
            return new([message], "43");
        }
        public ValueTask<int> RedriveAsync(string source, Func<DeadLetterMessage, bool>? filter = null, int maxMessages = 20, CancellationToken cancellationToken = default)
        { Record(source, maxMessages); Filter = filter; return ValueTask.FromResult(1); }
        public ValueTask<int> PurgeAsync(string source, int maxMessages = 20, CancellationToken cancellationToken = default)
        { Record(source, maxMessages); Purged = true; return ValueTask.FromResult(2); }
        private void Record(string source, int count) { if (source == "unknown") throw new ArgumentException("Unknown source; list configured sources."); Source = source; Count = count; Calls++; }
    }
}
