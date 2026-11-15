using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class RabbitMqConsumerControlTests
{
    [Test]
    public async Task Deferred_consumer_receives_only_after_the_keyed_control_starts_it()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var received = new RabbitMqReceiveControl();
        using var host = RabbitMqReceiveTests.Host(broker.Factory(), received, autoStart: false);
        var control = host.Services.GetRequiredKeyedService<IConsumerControl>("rabbit");
        await host.StartAsync(token);
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);

        // Sending works while the consumer waits; the message stays on the queue.
        await host.Services.GetRequiredService<ITransport>().SendAsync(
            new("orders", "{\"Value\":7}"u8.ToArray(), RabbitMqReceiveTests.Headers()), token);
        var queue = await channel.QueueDeclarePassiveAsync("orders", token);
        await Assert.That(queue.ConsumerCount).IsEqualTo(0u);
        await Assert.That(queue.MessageCount).IsEqualTo(1u);
        await Assert.That(received.Values.Reader.TryRead(out _)).IsFalse();

        await control.StartAsync(token);
        await control.StartAsync(token);
        await Assert.That(await received.Values.Reader.ReadAsync(token)).IsEqualTo(7);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).ConsumerCount).IsEqualTo(1u);

        await control.StopAsync(token);
        await control.StopAsync(token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).ConsumerCount).IsEqualTo(0u);
        await control.StartAsync(token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).ConsumerCount).IsEqualTo(1u);

        await host.StopAsync(token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).ConsumerCount).IsEqualTo(0u);
        await Assert.That(async () => await control.StartAsync(token)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Control_fails_before_the_host_starts_and_after_it_stops()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        using var host = RabbitMqReceiveTests.Host(broker.Factory(), new(), autoStart: false);
        var control = host.Services.GetRequiredKeyedService<IConsumerControl>("rabbit");

        await Assert.That(async () => await control.StartAsync(token)).Throws<InvalidOperationException>();
        await host.StartAsync(token);
        await host.StopAsync(token);
        await Assert.That(async () => await control.StartAsync(token)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Auto_started_consumer_can_be_paused_and_resumed()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        var received = new RabbitMqReceiveControl();
        using var host = RabbitMqReceiveTests.Host(broker.Factory(), received);
        var control = host.Services.GetRequiredKeyedService<IConsumerControl>("rabbit");
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredService<ITransport>();

        await control.StartAsync(token);
        await control.StopAsync(token);
        await transport.SendAsync(new("orders", "{\"Value\":1}"u8.ToArray(), RabbitMqReceiveTests.Headers()), token);
        await control.StartAsync(token);
        await Assert.That(await received.Values.Reader.ReadAsync(token)).IsEqualTo(1);
        await host.StopAsync(token);
    }

    [Test]
    public async Task Host_shutdown_with_an_expired_token_stops_a_control_started_consumer()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        using var host = RabbitMqReceiveTests.Host(broker.Factory(), new(), autoStart: false);
        var control = host.Services.GetRequiredKeyedService<IConsumerControl>("rabbit");
        await host.StartAsync(token);
        await control.StartAsync(token);
        var consumer = ((RabbitMqNativeTransport)host.Services.GetRequiredService<ITransport>()).Consumers.Single();

        try { await host.StopAsync(new CancellationToken(true)); }
        catch (OperationCanceledException) { }
        await Assert.That(consumer.Completion.IsCompleted).IsTrue();
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).ConsumerCount).IsEqualTo(0u);
    }

    [Test]
    public async Task Host_shutdown_with_an_expired_token_stops_every_consumer_and_closes_the_connection()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await RabbitMqTopologyTests.Broker.StartAsync(token);
        using var host = RabbitMqReceiveTests.Host(broker.Factory(), new(), secondConsumer: true);
        await host.StartAsync(token);
        var transport = (RabbitMqNativeTransport)host.Services.GetRequiredService<ITransport>();
        await Assert.That(transport.Consumers.Count).IsEqualTo(2);

        try { await host.StopAsync(new CancellationToken(true)); }
        catch (OperationCanceledException) { }
        foreach (var consumer in transport.Consumers) await Assert.That(consumer.Completion.IsCompleted).IsTrue();
        await Assert.That(transport.IsConnected).IsFalse();
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await Assert.That((await channel.QueueDeclarePassiveAsync("orders", token)).ConsumerCount).IsEqualTo(0u);
        await Assert.That((await channel.QueueDeclarePassiveAsync("audit", token)).ConsumerCount).IsEqualTo(0u);
    }
}
