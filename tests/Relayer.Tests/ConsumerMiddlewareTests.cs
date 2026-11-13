using Relayer.Transports;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;

namespace Relayer.Tests;

public class ConsumerMiddlewareTests
{
    [Test]
    public async Task Hosted_consumers_apply_local_then_global_middleware_with_delivery_scopes()
    {
        var calls = new Calls();
        using var host = new HostBuilder().ConfigureServices(services =>
        {
            services.AddSingleton(calls);
            services.AddScoped<DeliveryService>();
            services.AddRelayer(relayer =>
            {
                relayer.AddMessageTypesFromThisAssembly();
                relayer.InMemory("local", transport =>
                {
                    transport.Send<MiddlewareMessage>("first");
                    transport.Send<OtherMiddlewareMessage>("second");
                    transport.Consume("first", consumer =>
                    {
                        consumer.Use<LocalMiddleware>();
                        consumer.Handler<MiddlewareHandler>();
                    });
                    transport.Consume("second", consumer => consumer.Handler<OtherMiddlewareHandler>());
                });
                // Applies even when registered after the transport declarations.
                relayer.UseConsumeMiddleware<GlobalMiddleware>();
            });
        }).Build();
        await host.StartAsync();
        try
        {
            using var scope = host.Services.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendAsync(new MiddlewareMessage());
            await calls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(string.Join(",", calls.Events)).IsEqualTo("local-before,global-before,handler,global-after,local-after");
            await Assert.That(calls.Scopes.Distinct().Count()).IsEqualTo(1);
            await sender.SendAsync(new OtherMiddlewareMessage());
            await calls.OtherCompleted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(string.Join(",", calls.Events)).IsEqualTo("local-before,global-before,handler,global-after,local-after,global-before,other-handler,global-after");
            await Assert.That(calls.Scopes.Distinct().Count()).IsEqualTo(2);
            await Assert.That(host.Services.GetRequiredService<LocalMiddleware>().Invocations).IsEqualTo(1);
        }
        finally { await host.StopAsync(); }
    }

    [Test]
    public async Task Middleware_registration_is_frozen_with_its_declaration()
    {
        IRelayerBuilder? saved = null;
        ConsumerBuilder<object>? consumer = null;
        new ServiceCollection().AddRelayer(relayer =>
        {
            saved = relayer;
            relayer.AddTransport<object, object>("test", transport => transport.Consume("source", c =>
            {
                consumer = c;
                c.Handler<MiddlewareHandler>();
            }));
        });
        await Assert.That(() => saved!.UseConsumeMiddleware<GlobalMiddleware>()).Throws<InvalidOperationException>();
        await Assert.That(() => consumer!.Use<LocalMiddleware>()).Throws<InvalidOperationException>();
        await Assert.That(() => consumer!.ConfigurePipeline(_ => { })).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Invalid_transaction_order_fails_during_host_startup()
    {
        using var host = new HostBuilder().ConfigureServices(services =>
        {
            services.AddSingleton(new Calls());
            services.AddRelayer(relayer =>
            {
                relayer.AddMessageTypesFromThisAssembly();
                relayer.InMemory("local", transport => transport.Consume("source", consumer =>
                {
                    consumer.Handler<MiddlewareHandler>();
                    consumer.Use<LocalMiddleware>();
                    consumer.ConfigurePipeline(pipeline => pipeline.Use(new Boundary()));
                }));
            });
        }).Build();
        await Assert.That(async () => await host.StartAsync()).Throws<InvalidOperationException>()
            .WithMessageContaining("transaction boundary");
    }

    public sealed class Calls
    {
        public ConcurrentQueue<string> Events { get; } = new();
        public ConcurrentQueue<Guid> Scopes { get; } = new();
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource OtherCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Add(string value, MessageContext context)
        {
            Events.Enqueue(value);
            Scopes.Enqueue(context.Services.GetRequiredService<DeliveryService>().Id);
        }
    }

    public sealed class DeliveryService { public Guid Id { get; } = Guid.NewGuid(); }
    public sealed class LocalMiddleware(Calls calls) : IConsumeMiddleware
    {
        public int Invocations;
        public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
        {
            Interlocked.Increment(ref Invocations);
            calls.Add("local-before", context);
            await next(context);
            calls.Add("local-after", context);
            calls.Completed.TrySetResult();
        }
    }
    public sealed class GlobalMiddleware(Calls calls) : IConsumeMiddleware
    {
        public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
        {
            calls.Add("global-before", context);
            await next(context);
            calls.Add("global-after", context);
            if (context.MessageType.Name == "tests.consumer-other") calls.OtherCompleted.TrySetResult();
        }
    }
    private sealed class Boundary : IConsumeTransactionMiddleware
    {
        public ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next) => next(context);
    }
}

[Message("tests.consumer-middleware")]
public sealed record MiddlewareMessage;
public sealed class MiddlewareHandler(ConsumerMiddlewareTests.Calls calls) : IHandler<MiddlewareMessage>
{
    public ValueTask HandleAsync(MiddlewareMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        calls.Add("handler", context);
        return default;
    }
}

[Message("tests.consumer-other")]
public sealed record OtherMiddlewareMessage;
public sealed class OtherMiddlewareHandler(ConsumerMiddlewareTests.Calls calls) : IHandler<OtherMiddlewareMessage>
{
    public ValueTask HandleAsync(OtherMiddlewareMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        calls.Add("other-handler", context);
        return default;
    }
}
