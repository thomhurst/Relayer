using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class SqsStartupRollbackTests
{
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Cancelling_startup_aborts_rollback_and_preserves_the_receive_configuration_error(bool autoStart)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        await client.Deliveries.Writer.WriteAsync(SqsReceiveTests.Message("""{"Value":1}""", 1));

        await CancelStartupDuringRollbackAsync(client, "orders", "invalid", "dead", autoStart);

        await Assert.That(client.Polls).IsEqualTo(1);
        await Assert.That(client.Deletes).IsEqualTo(0);
    }

    [Test]
    public async Task Floci_redelivers_work_interrupted_by_startup_rollback()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
        {
            ServiceURL = await SharedBrokers.FlociUrlAsync(token),
            AuthenticationRegion = "us-east-1",
        });
        var isolation = TestContext.Current!.Isolation;
        var source = isolation.GetIsolatedName("rollback-orders");
        var invalid = isolation.GetIsolatedName("rollback-invalid");
        var dead = isolation.GetIsolatedName("rollback-dead");
        var url = (await client.CreateQueueAsync(source, token)).QueueUrl;
        await client.CreateQueueAsync(invalid, token);
        await client.CreateQueueAsync(dead, token);
        var message = SqsReceiveTests.Message("""{"Value":1}""", 1);
        var sent = await client.SendMessageAsync(new SendMessageRequest(url, message.Body)
        {
            MessageAttributes = message.MessageAttributes,
        }, token);

        await CancelStartupDuringRollbackAsync(client, source, invalid, dead, autoStart: false);

        var redelivered = await client.ReceiveMessageAsync(new ReceiveMessageRequest(url)
        {
            WaitTimeSeconds = 10,
        }, token);
        await Assert.That(redelivered.Messages.Single().MessageId).IsEqualTo(sent.MessageId);
    }

    private static async Task CancelStartupDuringRollbackAsync(IAmazonSQS client, string source, string invalid, string dead, bool autoStart)
    {
        using var startup = new CancellationTokenSource();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var entered = new ManualResetEventSlim();
        var probe = new RollbackProbe(entered);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(probe);
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AmazonSqs("sqs", sqs =>
            {
                sqs.TopologyMode = TopologyMode.None;
                sqs.Options.Configure(options =>
                {
                    options.SqsClient = client;
                    options.AutoStartConsumers = autoStart;
                    options.Queues.Add(new(source));
                    options.Queues.Add(new(invalid));
                    options.Queues.Add(new(dead));
                });
                sqs.Consume(source, consumer =>
                {
                    consumer.Handler<RollbackHandler>();
                    consumer.Options.ConfigureReceive = request => request.VisibilityTimeout = 0;
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = dead
                    });
                });
                sqs.Consume(invalid, consumer =>
                {
                    consumer.Handler<RollbackHandler>();
                    consumer.Options.ConfigureReceive = request =>
                    {
                        // The first delivery must be active before the second consumer fails validation.
                        entered.Wait(deadline.Token);
                        request.MaxNumberOfMessages = 11;
                    };
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = dead
                    });
                });
            });
        });
        await using var provider = services.BuildServiceProvider();

        if (!autoStart)
        {
            await ServiceBusReceiveTests.Start(provider, deadline.Token);
        }

        var control = provider.GetRequiredKeyedService<IConsumerControl>("sqs");
        var startInvoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = Task.Run(async () =>
        {
            var pending = autoStart ? ServiceBusReceiveTests.Start(provider, startup.Token) : control.StartAsync(startup.Token);
            startInvoked.SetResult();
            await pending;
        });

        try
        {
            await startInvoked.Task.WaitAsync(deadline.Token);
            await Assert.That(entered.IsSet).IsTrue();
            await Assert.That(start.IsCompleted).IsFalse();
            await startup.CancelAsync();

            await Assert.That(async () => await start.WaitAsync(deadline.Token))
                .Throws<InvalidOperationException>().WithMessageContaining("MaxNumberOfMessages");
            await Assert.That(probe.Cancelled.Task.IsCompletedSuccessfully).IsTrue();
        }
        finally
        {
            probe.Release.TrySetResult();

            try
            {
                await start;
            }
            catch (InvalidOperationException)
            {
                // The test observes the startup error above. Always finish rollback before disposal.
            }
        }
    }

    internal sealed class RollbackProbe(ManualResetEventSlim entered)
    {
        internal ManualResetEventSlim Entered
        {
            get;
        } = entered;

        internal TaskCompletionSource Cancelled
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Release
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    internal sealed class RollbackHandler(RollbackProbe probe) : IHandler<SqsReceiveFlociTests.Payload>
    {
        public async ValueTask HandleAsync(SqsReceiveFlociTests.Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(() => probe.Cancelled.SetResult());
            probe.Entered.Set();
            await probe.Release.Task;
            cancellationToken.ThrowIfCancellationRequested();
        }
    }
}
