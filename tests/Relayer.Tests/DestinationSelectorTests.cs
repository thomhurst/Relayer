using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Tests;

[Message("tests.regional-order")]
public sealed record RegionalOrder(string Topic, int Number);

public class DestinationSelectorTests
{
    [Test]
    public async Task Each_message_reaches_the_destination_its_selector_returns()
    {
        using var host = Host(t =>
        {
            t.Send<RegionalOrder>(["regional-eu", "regional-us"], static order => order.Topic);
            t.Consume("regional-eu", c => c.Handler<EuHandler>());
            t.Consume("regional-us", c => c.Handler<UsHandler>());
        });
        await host.StartAsync();

        try
        {
            var seen = host.Services.GetRequiredService<Seen>();

            using (var scope = host.Services.CreateScope())
            {
                var sender = scope.ServiceProvider.GetRequiredService<ISender>();
                await sender.SendAsync(new RegionalOrder("regional-eu", 1));
                await sender.SendAsync(new RegionalOrder("regional-us", 2));
                await sender.SendAsync(new RegionalOrder("regional-eu", 3));
            }

            await seen.Done.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(seen.Sources.OrderBy(source => source)).IsEquivalentTo(new[] { "regional-eu:1", "regional-eu:3", "regional-us:2" });
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Test]
    public async Task Undeclared_selected_destination_fails_the_send()
    {
        using var host = Host(t => t.Send<RegionalOrder>(["regional-eu"], static order => order.Topic));
        await host.StartAsync();

        try
        {
            using var scope = host.Services.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await Assert.That(async () => await sender.SendAsync(new RegionalOrder("regional-apac", 1)))
                .Throws<InvalidOperationException>().WithMessageContaining("returned 'regional-apac', which the route does not declare");
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Test]
    public async Task A_null_selected_destination_fails_the_send()
    {
        using var host = Host(t => t.Send<RegionalOrder>(["regional-eu"], static _ => null!));
        await host.StartAsync();

        try
        {
            using var scope = host.Services.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await Assert.That(async () => await sender.SendAsync(new RegionalOrder("regional-eu", 1)))
                .Throws<InvalidOperationException>().WithMessageContaining("returned null, which the route does not declare");
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Test]
    public async Task Startup_validates_every_declared_destination()
    {
        using var host = Host(t => t.Send<RegionalOrder>(["regional-eu", "regional-us"], static order => order.Topic,
            route => route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromDays(1) }),
            options =>
            {
                options.Endpoints["regional-eu"] = new()
                {
                    Capabilities = new()
                    {
                        MaxDeliveryDelay = TimeSpan.FromHours(1)
                    }
                };
                options.Endpoints["regional-us"] = new()
                {
                    Capabilities = new()
                    {
                        MaxDeliveryDelay = TimeSpan.FromHours(1)
                    }
                };
            });
        OptionsValidationException? failure = null;

        try
        {
            await host.StartAsync();
        }
        catch (OptionsValidationException error)
        {
            failure = error;
        }

        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Message).Contains("'regional-eu'");
        await Assert.That(failure.Message).Contains("'regional-us'");
        await Assert.That(failure.Message).DoesNotContain("declared more than once");
    }

    [Test]
    public async Task A_destination_repeated_by_another_route_is_rejected()
    {
        using var host = Host(t =>
        {
            t.Send<RegionalOrder>(["regional-eu", "regional-us"], static order => order.Topic);
            t.Send<RegionalOrder>("regional-us");
        });
        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>()
            .WithMessageContaining("'regional-us' is declared more than once");
    }

    [Test]
    public async Task A_selector_route_fans_out_with_another_route_through_the_outbox()
    {
        var outbox = new RecordingOutbox();
        await using var provider = SinkProvider(services => services.AddSingleton<IOutboxEnlistment>(outbox),
            t => t.Send<RegionalOrder>("regional-audit"));
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new RegionalOrder("regional-us", 1));
        await Assert.That(outbox.Destinations).IsEquivalentTo(new[] { "regional-us", "regional-audit" });
    }

    [Test]
    public async Task Declaration_rejects_missing_or_repeated_destinations()
    {
        new ServiceCollection().AddRelayer(r => r.InMemory("local", t =>
        {
            Assert.Throws<ArgumentException>(() => t.Send<RegionalOrder>([], static order => order.Topic));
            Assert.Throws<ArgumentException>(() => t.Send<RegionalOrder>(["regional-eu", "regional-eu"], static order => order.Topic));
            Assert.Throws<ArgumentNullException>(() => t.Send<RegionalOrder>(["regional-eu", null!], static order => order.Topic));
        }));
        await Task.CompletedTask;
    }

    [Test]
    public async Task Outbox_rows_store_the_selected_destination()
    {
        var outbox = new RecordingOutbox();
        await using var provider = SinkProvider(services => services.AddSingleton<IOutboxEnlistment>(outbox));
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new RegionalOrder("regional-us", 1));
        await sender.SendAsync(new RegionalOrder("regional-eu", 2));
        await Assert.That(outbox.Destinations).IsEquivalentTo(new[] { "regional-us", "regional-eu" });
    }

    [Test]
    public async Task Selector_routes_carry_no_replies_or_tombstones()
    {
        await using var provider = SinkProvider();
        using var scope = provider.CreateScope();
        var route = provider.GetRequiredService<SendPipeline>().GetRoute<RegionalOrder>();
        await Assert.That(route.MatchesDestination(new WireString("regional-eu"))).IsFalse();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await Assert.That(async () => await sender.SendAsync(Tombstone.For<RegionalOrder>("key")))
            .Throws<InvalidOperationException>().WithMessageContaining("No send route");
    }

    private static ServiceProvider SinkProvider(Action<IServiceCollection>? configure = null,
        Action<TransportBuilder<SinkOptions, SinkOptions>>? routes = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(new Sink());
        configure?.Invoke(services);
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.AddTransport<SinkOptions, SinkOptions>("sink",
                t =>
                {
                    t.Send<RegionalOrder>(["regional-eu", "regional-us"], static order => order.Topic);
                    routes?.Invoke(t);
                });
        });
        return services.BuildServiceProvider();
    }

    public sealed class SinkOptions;

    private sealed class RecordingOutbox : IOutboxEnlistment
    {
        public List<string> Destinations
        {
            get;
        } = [];

        public bool IsActive => true;

        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            Destinations.Add(record.Message.Destination);
            return default;
        }
    }

    private sealed class Sink : ITransport
    {
        public string Name => "sink";

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new()
            {
                Tombstones = true
            };
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            return default;
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            return default;
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(new TopologyReport([]));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private static IHost Host(Action<TransportBuilder<InMemoryTransportOptions, InMemoryConsumerOptions>> configure,
        Action<InMemoryTransportOptions>? options = null)
    {
        return new HostBuilder().ConfigureServices(services =>
                {
                    services.AddSingleton(new Seen(expected: 3));
                    services.AddRelayer(r =>
                    {
                        r.AddMessageTypesFromThisAssembly();
                        r.InMemory("local", t =>
                        {
                            if (options is not null)
                            {
                                t.Options.Configure(options);
                            }

                            configure(t);
                        });
                    });
                }).Build();
    }

    public sealed class Seen(int expected)
    {
        public ConcurrentBag<string> Sources
        {
            get;
        } = [];

        public TaskCompletionSource Done
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Add(RegionalOrder order, string source)
        {
            Sources.Add($"{source}:{order.Number}");

            if (Sources.Count == expected)
            {
                Done.TrySetResult();
            }
        }
    }

    public sealed class EuHandler(Seen seen) : IHandler<RegionalOrder>
    {
        public ValueTask HandleAsync(RegionalOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            seen.Add(message, "regional-eu");
            return ValueTask.CompletedTask;
        }
    }

    public sealed class UsHandler(Seen seen) : IHandler<RegionalOrder>
    {
        public ValueTask HandleAsync(RegionalOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            seen.Add(message, "regional-us");
            return ValueTask.CompletedTask;
        }
    }
}
