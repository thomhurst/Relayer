using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public class NatsTopologyTests
{
    [Test]
    public async Task Hosted_provision_is_idempotent_native_and_non_destructive()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token);
        var nativeOptions = broker.Options();
        var stream = new StreamConfig("orders", ["orders.>"]) { Storage = StreamConfigStorage.Memory, MaxMsgs = 100, MaxAge = TimeSpan.FromMinutes(1) };
        var consumer = new ConsumerConfig("handler") { FilterSubject = "orders.placed", AckWait = TimeSpan.FromSeconds(5), Backoff = new[] { TimeSpan.FromSeconds(20) } };
        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder => builder.Nats("nats", nats =>
        {
            nats.TopologyMode = TopologyMode.Provision;
            nats.Options.Configure(options =>
            {
                options.ConnectionOptions = nativeOptions;
                options.Streams.Add(new("orders")
                {
                    Configuration = stream
                });
                options.Consumers.Add(new("orders", "handler")
                {
                    Configuration = consumer
                });
            });
        }))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredService<ITransport>();
        await Assert.That(host.Services.GetRequiredService<IOptionsMonitor<NatsTransportOptions>>().Get("nats").ConnectionOptions).IsSameReferenceAs(nativeOptions);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);

        await using var independent = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(independent);
        var actual = await js.GetStreamAsync("orders", cancellationToken: token);
        await Assert.That(actual.Info.Config.Storage).IsEqualTo(StreamConfigStorage.Memory);
        await Assert.That(actual.Info.Config.MaxMsgs).IsEqualTo(100L);
        var handler = await js.GetConsumerAsync("orders", "handler", token);
        await Assert.That(handler.Info.Config.AckWait).IsEqualTo(TimeSpan.FromSeconds(20));
        await Assert.That(handler.Info.Config.DurableName).IsEqualTo("handler");
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var drift = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Streams.Add(new("orders")
            {
                Configuration = stream with
                {
                    Storage = StreamConfigStorage.File
                }
            });
            options.Consumers.Add(new("orders", "handler")
            {
                Configuration = consumer with
                {
                    AckWait = TimeSpan.FromSeconds(5),
                    Backoff = new[] { TimeSpan.FromSeconds(5) }
                }
            });
            options.Streams.Add(new("after_error")
            {
                Configuration = new("after_error", ["after.error"])
            });
        });
        var report = await drift.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Provision, token);
        await Assert.That(report.Differences.Count).IsEqualTo(2);
        await Assert.That(report.Differences[0]).Contains("orders");
        await Assert.That(report.Differences[1]).Contains("handler");
        await js.GetStreamAsync("after_error", cancellationToken: token);
        await Assert.That((await js.GetStreamAsync("orders", cancellationToken: token)).Info.Config.Storage).IsEqualTo(StreamConfigStorage.Memory);
        await Assert.That((await js.GetConsumerAsync("orders", "handler", token)).Info.Config.AckWait).IsEqualTo(TimeSpan.FromSeconds(20));
        await Assert.That(stream.Storage).IsEqualTo(StreamConfigStorage.Memory);
        await Assert.That(consumer.AckWait).IsEqualTo(TimeSpan.FromSeconds(5));

        await host.StopAsync(token);
        await transport.StopAsync(token);
        await independent.PingAsync(token);
        await transport.StartAsync(token);
        await transport.StopAsync(token);
        await js.GetStreamAsync("orders", cancellationToken: token);
    }

    [Test]
    public async Task Validate_uses_live_stream_consumer_limits_for_omitted_settings()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("orders");
        var nativeOptions = broker.Options();
        const string ConsumerName = "handler";
        var consumer = new ConsumerConfig(ConsumerName);
        await using var connection = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(connection);

        // The pinned native StreamConfig cannot send consumer_limits.
        var createStream = Encoding.UTF8.GetBytes($$"""
            {
                "name": "{{stream}}",
                "subjects": ["{{stream}}.>"],
                "consumer_limits": { "max_ack_pending": 16, "inactive_threshold": 60000000000 }
            }
            """);
        var serializer = NatsRawSerializer<byte[]>.Default;
        await connection.RequestAsync<byte[], byte[]>($"{js.Opts.Prefix}.STREAM.CREATE.{stream}", createStream,
            requestSerializer: serializer, replySerializer: serializer, cancellationToken: token);

        await using var provisionProvider = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = nativeOptions;
            options.Streams.Add(new(stream));
            options.Consumers.Add(new(stream, ConsumerName)
            {
                Configuration = consumer
            });
        });
        var transport = provisionProvider.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();
        var actual = (await js.GetConsumerAsync(stream, ConsumerName, token)).Info.Config;
        await Assert.That(actual.MaxAckPending).IsEqualTo(16L);
        await Assert.That(actual.InactiveThreshold).IsEqualTo(TimeSpan.FromMinutes(1));
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var externalProvider = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = nativeOptions;
            options.Consumers.Add(new(stream, ConsumerName)
            {
                Configuration = consumer
            });
        });
        var external = externalProvider.GetRequiredService<ITransport>();
        await Assert.That((await external.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var driftProvider = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = nativeOptions;
            options.Consumers.Add(new(stream, ConsumerName)
            {
                Configuration = consumer with
                {
                    MaxAckPending = 8,
                    InactiveThreshold = TimeSpan.FromSeconds(30)
                }
            });
        });
        var drift = driftProvider.GetRequiredService<ITransport>();
        var differences = (await drift.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences;
        await Assert.That(differences.Count).IsEqualTo(2);
        await Assert.That(differences.Single(difference => difference.Contains("'max_ack_pending'", StringComparison.Ordinal)))
            .Contains("expected 8, actual 16");
        await Assert.That(differences.Single(difference => difference.Contains("'inactive_threshold'", StringComparison.Ordinal)))
            .Contains("expected 30000000000, actual 60000000000");
    }

    [Test]
    public async Task Validate_reports_missing_and_conflicting_configuration_without_creating_entities()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token);
        await using var connection = new NatsConnection(broker.Options());
        var js = new NatsJSContext(connection);
        await js.CreateStreamAsync(new("events", ["events.>"]), token);
        await js.CreateConsumerAsync("events", new("handler"), token);
        await using var names = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Streams.Add(new("events"));
            options.Consumers.Add(new("events", "handler"));
        });
        var transport = names.GetRequiredService<ITransport>();

        for (var attempt = 0; attempt < 2; attempt++)
        {
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        }

        await using var missing = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Streams.Add(new("missing"));
            options.Consumers.Add(new("events", "missing"));
            options.Streams.Add(new("events")
            {
                Configuration = new("events", ["different.>"])
            });
        });
        var differences = (await missing.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences;
        await Assert.That(differences.Count).IsEqualTo(3);
        await Assert.That(differences[0]).Contains("missing");
        await Assert.That(differences[1]).Contains("subjects");
        await Assert.That(differences[2]).Contains("missing");
        await Assert.That(async () => await js.GetStreamAsync("missing", cancellationToken: token)).Throws<NatsJSApiException>();
        await Assert.That(async () => await js.GetConsumerAsync("events", "missing", token)).Throws<NatsJSApiException>();
        await Assert.That((await js.GetStreamAsync("events", cancellationToken: token)).Info.State.Messages).IsEqualTo(0L);

        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder => builder.Nats("nats", nats =>
            nats.Options.Configure(options =>
            {
                options.ConnectionOptions = broker.Options();
                options.Streams.Add(new("missing"));
            })))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        var error = await Assert.That(async () => await host.StartAsync(token)).Throws<OptionsValidationException>();
        await Assert.That(error!.Message).Contains("missing");
    }

    [Test]
    public async Task Provision_reports_unsupported_replicas_and_creates_the_next_stream()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var replicated = isolation.GetIsolatedName("replicated");
        var ordinary = isolation.GetIsolatedName("ordinary");
        var nativeOptions = broker.Options();
        await using var provider = StreamProvider(nativeOptions, new StreamConfig(replicated, [replicated])
        {
            NumReplicas = 2
        }, new(ordinary, [ordinary]));
        var transport = provider.GetRequiredService<ITransport>();

        var report = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);

        var difference = report.Differences.Single();
        await Assert.That(difference).Contains($"Stream '{replicated}'");
        await Assert.That(difference).Contains("replicas > 1 not supported in non-clustered mode");
        await using var native = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(native);
        await js.GetStreamAsync(ordinary, cancellationToken: token);
    }

    [Test]
    public async Task Missing_stream_guidance_uses_the_mode_and_available_configuration()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("missing");
        var nativeOptions = broker.Options();
        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder => builder.Nats("nats", nats =>
            nats.Options.Configure(options =>
            {
                options.ConnectionOptions = nativeOptions;
                options.Streams.Add(new(name)
                {
                    Configuration = new(name, [name])
                });
            })))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;

        var error = await Assert.That(async () => await host.StartAsync(token)).Throws<OptionsValidationException>();
        await Assert.That(error!.Message).Contains($"Stream '{name}': stream not found");
        await Assert.That(error.Message).Contains("Create it externally or select Provision");
        await Assert.That(error.Message).Contains("Validate is read-only");
        await Assert.That(error.Message).DoesNotContain("supply native Configuration");

        await using var names = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = nativeOptions;
            options.Streams.Add(new(name));
        });
        var nameOnly = names.GetRequiredService<ITransport>();
        var report = await nameOnly.ApplyTopologyAsync(TopologyMode.Provision, token);
        await Assert.That(report.Differences.Single()).Contains("supply native Configuration for Provision");

        var configured = host.Services.GetRequiredService<ITransport>();
        await Assert.That((await configured.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();
        await Assert.That((await configured.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();
    }

    [Test]
    public async Task Validate_accepts_native_unlimited_stream_limits_and_reports_positive_drift()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var unlimitedName = isolation.GetIsolatedName("unlimited");
        var limitedName = isolation.GetIsolatedName("limited");
        var unlimited = new StreamConfig(unlimitedName, [unlimitedName])
        {
            MaxConsumers = -2,
            MaxMsgs = -2,
            MaxMsgsPerSubject = -2,
            MaxBytes = -2,
            MaxMsgSize = -2
        };
        var limited = new StreamConfig(limitedName, [limitedName])
        {
            MaxMsgs = 10
        };
        var nativeOptions = broker.Options();
        await using var provider = StreamProvider(nativeOptions, unlimited, limited);
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();

        await using var native = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(native);
        var actual = (await js.GetStreamAsync(unlimitedName, cancellationToken: token)).Info.Config;
        await Assert.That(actual.MaxConsumers).IsEqualTo(-1);
        await Assert.That(actual.MaxMsgs).IsEqualTo(-1L);
        await Assert.That(actual.MaxMsgsPerSubject).IsEqualTo(-1L);
        await Assert.That(actual.MaxBytes).IsEqualTo(-1L);
        await Assert.That(actual.MaxMsgSize).IsEqualTo(-1);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var driftProvider = StreamProvider(nativeOptions, limited with
        {
            MaxMsgs = 20
        });
        var drift = driftProvider.GetRequiredService<ITransport>();
        var report = await drift.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(report.Differences.Single()).Contains($"Stream '{limitedName}': setting 'max_msgs' expected 20, actual 10");
    }

    [Test]
    public async Task Validate_accepts_native_stream_subject_default_but_keeps_mirrors_and_sources_subjectless()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var ordinaryName = isolation.GetIsolatedName("ordinary");
        var mirrorName = isolation.GetIsolatedName("mirror");
        var sourcedName = isolation.GetIsolatedName("sourced");
        var ordinary = new StreamConfig(ordinaryName, [])
        {
            Storage = StreamConfigStorage.Memory
        };
        var mirror = new StreamConfig(mirrorName, [])
        {
            Storage = StreamConfigStorage.Memory,
            Mirror = new()
            {
                Name = ordinaryName
            }
        };
        var sourced = new StreamConfig(sourcedName, [])
        {
            Storage = StreamConfigStorage.Memory,
            Sources =
            [
                new()
                {
                    Name = ordinaryName
                }
            ]
        };
        await using var provider = StreamProvider(broker.Options(), ordinary, mirror, sourced);
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();

        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var actual = await js.GetStreamAsync(ordinaryName, cancellationToken: token);
        var actualMirror = await js.GetStreamAsync(mirrorName, cancellationToken: token);
        var actualSourced = await js.GetStreamAsync(sourcedName, cancellationToken: token);
        await Assert.That(actual.Info.Config.Subjects!.Single()).IsEqualTo(ordinaryName);
        await Assert.That(actualMirror.Info.Config.Subjects ?? []).IsEmpty();
        await Assert.That(actualSourced.Info.Config.Subjects ?? []).IsEmpty();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var explicitProvider = StreamProvider(broker.Options(), ordinary with
        {
            Subjects = [ordinaryName]
        });
        var explicitTransport = explicitProvider.GetRequiredService<ITransport>();
        await Assert.That((await explicitTransport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var driftProvider = StreamProvider(broker.Options(), ordinary with
        {
            Subjects = [isolation.GetIsolatedName("different")]
        });
        var drift = driftProvider.GetRequiredService<ITransport>();
        await Assert.That((await drift.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Single()).Contains("subjects");
    }

    [Test]
    public async Task Validate_accepts_default_persistence_and_reports_async_drift()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var defaultName = isolation.GetIsolatedName("persist_default");
        var asyncName = isolation.GetIsolatedName("persist_async");
        var defaultStream = new StreamConfig(defaultName, [defaultName])
        {
            PersistMode = StreamConfigPersistMode.Default
        };
        var asyncStream = new StreamConfig(asyncName, [asyncName])
        {
            PersistMode = StreamConfigPersistMode.Async
        };

        var nativeOptions = broker.Options();
        await using var provisionProvider = StreamProvider(nativeOptions, defaultStream, asyncStream);
        var provision = provisionProvider.GetRequiredService<ITransport>();
        await Assert.That((await provision.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();
        await Assert.That((await provision.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();

        await using var connection = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(connection);
        (await js.PublishAsync(defaultName, "value", cancellationToken: token)).EnsureSuccess();
        var stored = await js.GetStreamAsync(defaultName, cancellationToken: token);
        await Assert.That(stored.Info.State.Messages).IsEqualTo(1L);
        await Assert.That(stored.Info.Config.PersistMode).IsNull();

        await using var validateProvider = StreamProvider(nativeOptions, defaultStream, asyncStream);
        var validation = validateProvider.GetRequiredService<ITransport>();
        await Assert.That((await validation.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var omittedProvider = StreamProvider(nativeOptions, defaultStream with
        {
            PersistMode = null
        });
        var omitted = omittedProvider.GetRequiredService<ITransport>();
        await Assert.That((await omitted.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var driftProvider = StreamProvider(nativeOptions, asyncStream with
        {
            PersistMode = StreamConfigPersistMode.Default
        });
        var drift = driftProvider.GetRequiredService<ITransport>();
        var report = await drift.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(report.Differences.Single()).Contains("setting 'persist_mode' expected \"default\", actual \"async\"");
    }

    [Test]
    public async Task Validate_applies_the_duplicate_window_default_only_to_ordinary_streams()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var ordinaryName = isolation.GetIsolatedName("duplicates_ordinary");
        var mirrorName = isolation.GetIsolatedName("duplicates_mirror");
        var sourcedName = isolation.GetIsolatedName("duplicates_sourced");
        var ordinary = new StreamConfig(ordinaryName, [ordinaryName])
        {
            Storage = StreamConfigStorage.Memory
        };
        var mirror = new StreamConfig(mirrorName, [])
        {
            Storage = StreamConfigStorage.Memory,
            Mirror = new()
            {
                Name = ordinaryName
            }
        };
        var sourced = new StreamConfig(sourcedName, [])
        {
            Storage = StreamConfigStorage.Memory,
            Sources = [new() { Name = ordinaryName }]
        };

        var nativeOptions = broker.Options();
        await using var connection = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(connection);
        var actualOrdinary = await js.CreateStreamAsync(ordinary, token);
        var actualMirror = await js.CreateStreamAsync(mirror, token);
        var actualSourced = await js.CreateStreamAsync(sourced, token);
        var window = TimeSpan.FromMinutes(2);
        await Assert.That(actualOrdinary.Info.Config.DuplicateWindow).IsEqualTo(window);
        await Assert.That(actualMirror.Info.Config.DuplicateWindow).IsEqualTo(TimeSpan.Zero);
        await Assert.That(actualSourced.Info.Config.DuplicateWindow).IsEqualTo(TimeSpan.Zero);

        await using var matchingProvider = StreamProvider(nativeOptions, ordinary with
        {
            DuplicateWindow = window
        }, mirror, sourced);
        var matching = matchingProvider.GetRequiredService<ITransport>();
        await Assert.That((await matching.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var driftProvider = StreamProvider(nativeOptions, mirror with
        {
            DuplicateWindow = window
        }, sourced with
        {
            DuplicateWindow = window
        });
        var drift = driftProvider.GetRequiredService<ITransport>();
        var report = await drift.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(report.Differences.Count).IsEqualTo(2);
        await Assert.That(report.Differences[0]).Contains($"Stream '{mirrorName}': setting 'duplicate_window' expected 120000000000, actual null");
        await Assert.That(report.Differences[1]).Contains($"Stream '{sourcedName}': setting 'duplicate_window' expected 120000000000, actual null");
    }

    [Test]
    public async Task Validate_accepts_a_server_capped_duplicate_window_and_reports_explicit_drift()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token, """
            jetstream {
                limits {
                    duplicate_window: "30s"
                }
            }
            authorization {
                users: [
                    { user: admin, password: admin },
                    { user: reader, password: reader, permissions: {
                        publish: ["$JS.API.STREAM.INFO.*"],
                        subscribe: ["_INBOX.>"]
                    } }
                ]
            }
            """);
        var name = TestContext.Current!.Isolation.GetIsolatedName("duplicates_capped");
        var stream = new StreamConfig(name, [name]);
        var adminOptions = broker.Options("admin");
        var readerOptions = broker.Options("reader");
        await using var provisionProvider = StreamProvider(adminOptions, stream);
        var provision = provisionProvider.GetRequiredService<ITransport>();
        await Assert.That((await provision.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();
        await Assert.That((await provision.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();

        await using var native = new NatsConnection(adminOptions);
        var js = new NatsJSContext(native);
        var actual = await js.GetStreamAsync(name, cancellationToken: token);
        var configuredWindow = TimeSpan.FromSeconds(30);
        await Assert.That(actual.Info.Config.DuplicateWindow).IsEqualTo(configuredWindow);

        await using var omittedProvider = StreamProvider(readerOptions, stream);
        var omitted = omittedProvider.GetRequiredService<ITransport>();
        await Assert.That((await omitted.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var matchingProvider = StreamProvider(readerOptions, stream with
        {
            DuplicateWindow = configuredWindow
        });
        var matching = matchingProvider.GetRequiredService<ITransport>();
        await Assert.That((await matching.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        await using var driftProvider = StreamProvider(readerOptions, stream with
        {
            DuplicateWindow = TimeSpan.FromSeconds(20)
        });
        var drift = driftProvider.GetRequiredService<ITransport>();
        var report = await drift.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(report.Differences.Single()).Contains($"Stream '{name}': setting 'duplicate_window' expected 20000000000, actual 30000000000");
    }

    [Test]
    public async Task Validate_reports_a_duplicate_window_above_the_largest_possible_default()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("duplicates_long");
        var stream = new StreamConfig(name, [name])
        {
            DuplicateWindow = TimeSpan.FromMinutes(3)
        };
        var nativeOptions = broker.Options();
        await using var native = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(native);
        await js.CreateStreamAsync(stream, token);

        await using var provider = StreamProvider(nativeOptions, stream with
        {
            DuplicateWindow = TimeSpan.Zero
        });
        var transport = provider.GetRequiredService<ITransport>();
        var report = await transport.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(report.Differences.Single()).Contains($"Stream '{name}': setting 'duplicate_window'");
    }

    [Test]
    public async Task Existence_validation_uses_read_only_permissions_and_native_recovery_after_broker_restart()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token, """
            authorization {
                users: [
                    { user: admin, password: admin },
                    { user: reader, password: reader, permissions: {
                        publish: ["$JS.API.STREAM.INFO.>", "$JS.API.CONSUMER.INFO.>"],
                        subscribe: ["_INBOX.>"]
                    }}
                ]
            }
            """);
        await using var admin = new NatsConnection(broker.Options("admin"));
        var js = new NatsJSContext(admin);
        await js.CreateStreamAsync(new("events", ["events.>"]), token);
        await js.CreateConsumerAsync("events", new("handler"), token);
        await using var provider = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options("reader");
            options.Streams.Add(new("events")
            {
                Configuration = new("events", ["events.>"])
            });
            options.Consumers.Add(new("events", "handler")
            {
                Configuration = new("handler")
            });
        });
        var transport = (NatsNativeTransport)provider.GetRequiredService<ITransport>();
        await transport.StartAsync(token);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);

        var adminReopened = ObserveReconnect(admin);
        var readerReopened = ObserveReconnect(transport.Connection!);
        await broker.RestartAsync(token);
        await Task.WhenAll(adminReopened, readerReopened).WaitAsync(token);

        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        await Assert.That((await js.GetStreamAsync("events", cancellationToken: token)).Info.State.Messages).IsEqualTo(0L);

        static Task ObserveReconnect(NatsConnection connection)
        {
            var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var reopened = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            connection.ConnectionDisconnected += (_, _) =>
            {
                disconnected.TrySetResult();
                return default;
            };
            connection.ConnectionOpened += (_, _) =>
            {
                if (disconnected.Task.IsCompleted)
                {
                    reopened.TrySetResult();
                }

                return default;
            };
            return reopened.Task;
        }
    }

    private static ServiceProvider StreamProvider(NatsOpts nativeOptions, params StreamConfig[] streams)
    {
        return NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = nativeOptions;

            foreach (var stream in streams)
            {
                options.Streams.Add(new(stream.Name!)
                {
                    Configuration = stream
                });
            }
        });
    }

    internal sealed class Broker(IContainer container) : IAsyncDisposable
    {
        internal static async Task<Broker> StartAsync(CancellationToken token, string? configuration = null)
        {
            var builder = new ContainerBuilder("nats:2.15.2-alpine")
                .WithPortBinding(4222, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server is ready"));

            if (configuration is not null)
            {
                builder = builder.WithResourceMapping(Encoding.UTF8.GetBytes(configuration), FilePath.Of("/etc/nats.conf"));
            }

            var arguments = configuration is null ? "-js" : "-js -c /etc/nats.conf";
            // Docker stop/start reallocates random published ports. Restart only the broker process
            // so the recovery test preserves the endpoint every existing native client uses.
            // Raw string line endings follow the checkout; /bin/sh requires LF even on Windows.
            builder = builder.WithEntrypoint("/bin/sh", "-c").WithCommand($"""
                while true; do
                    while [ -f /tmp/relayer-offline ]; do sleep 0.1; done
                    nats-server {arguments} &
                    broker=$!
                    trap 'kill -TERM "$broker"; wait "$broker"; exit 0' TERM INT
                    wait "$broker"
                done
                """.ReplaceLineEndings("\n"));

            if (OperatingSystem.IsLinux())
            {
                builder = builder.WithPortBinding("20000-29999", "4222");
            }

            var container = builder.Build();

            try
            {
                await container.StartAsync(token);
                return new(container);
            }
            catch
            {
                await container.DisposeAsync();
                throw;
            }
        }

        internal NatsOpts Options(string? username = null)
        {
            return new()
            {
                Url = $"nats://{container.Hostname}:{container.GetMappedPublicPort(4222)}",
                ConnectTimeout = TimeSpan.FromSeconds(5),
                RequestTimeout = TimeSpan.FromSeconds(5),
                AuthOpts = username is null ? new() : new()
                {
                    Username = username,
                    Password = username
                },
            };
        }

        internal async Task RestartAsync(CancellationToken token)
        {
            var result = await container.ExecAsync(["killall", "-TERM", "nats-server"], token);

            if (result.ExitCode != 0)
            {
                throw new InvalidOperationException(result.Stderr);
            }
        }

        internal async Task SetConnectedAsync(bool connected, CancellationToken token)
        {
            var command = connected ? "rm -f /tmp/relayer-offline" : "touch /tmp/relayer-offline && killall -TERM nats-server";
            var result = await container.ExecAsync(["/bin/sh", "-c", command], token);

            if (result.ExitCode != 0)
            {
                throw new InvalidOperationException(result.Stderr);
            }
        }

        public ValueTask DisposeAsync()
        {
            return container.DisposeAsync();
        }
    }
}
