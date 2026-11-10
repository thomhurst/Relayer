using System.Buffers;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Serialization;

namespace Relayer.AmazonSqs;

internal sealed class SqsHostedService(IServiceProvider provider, string name) : IHostedService
{
    private SqsSendTransport? _transport;
    private bool _composed;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _transport ??= provider.GetRequiredKeyedService<SqsSendTransport>(name);
        await _transport.ResolveUrlsAsync(cancellationToken).ConfigureAwait(false);
        if (!_composed)
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            var tracing = provider.GetRequiredService<MessageTracing>();
            var handlers = provider.GetRequiredService<HandlerRegistry>();
            var receivers = new List<SqsConsumer>(definition.Consumers.Count);
            foreach (var declaration in definition.Consumers)
            {
                var consumer = (ConsumerDefinition<AmazonSqsConsumerOptions>)declaration;
                await _transport.ValidateRedriveAsync(consumer, cancellationToken).ConfigureAwait(false);
                var endpoint = new MessagingEndpoint { System = "aws_sqs", Destination = consumer.Source };
                var settlement = new SqsSettlement(tracing, endpoint);
                var failures = new FailurePipeline(provider, name, consumer.Source, consumer.Source,
                    consumer.Options.Failure, context => Copy(context, _transport.GetIdentityHeader(consumer.Source),
                        _transport.GetIdentityHeader(consumer.Options.Failure.DeadLetterEndpoint!)), provider.GetService<IFailureScheduler>());
                var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                    provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name), failures,
                    provider.GetService<IFailureClassifier>());
                var telemetry = provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new() { System = "aws_sqs", Destination = consumer.Source });
                pipeline.Tracing(tracing, endpoint).Telemetry(telemetry).Settle(settlement.SettleAsync);
                foreach (var handler in handlers.Descriptors)
                    if (consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
                var receive = new ReceivePipeline<SqsDelivery, SqsMessageBinding>(pipeline.Build(consumer.Concurrency), new(), consumer.Concurrency);
                receivers.Add(new(consumer, _transport.Client, _transport.GetUrl(consumer.Source), receive.ProcessSettledAsync,
                    provider.GetService<TimeProvider>(), provider.GetService<ILoggerFactory>()?.CreateLogger<SqsConsumer>())
                    { TrackHealth = _transport.HealthTracking });
            }
            _transport.Consumers.AddRange(receivers);
            _composed = true;
        }
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static OutgoingMessage Copy(MessageContext context, string sourceIdentityHeader = "ce_id", string deadIdentityHeader = "ce_id")
    {
        var delivery = context.AmazonSqs();
        var attributes = delivery.Message.MessageAttributes;
        var headers = new List<KeyValuePair<string, WireString>>(attributes?.Count ?? 0);
        Dictionary<string, MessageAttributeValue>? native = null;
        if (attributes is not null)
            foreach (var attribute in attributes)
            {
                if (attribute.Value.DataType == "String" && attribute.Value.StringValue is { } text)
                    headers.Add(new(attribute.Key, new(text)));
                else (native ??= new(StringComparer.Ordinal)).Add(attribute.Key, attribute.Value);
            }
        SendOptions? options = null;
        // Normal envelope copies remain persistable. A malformed/differently bound FIFO
        // identity needs the original native fallback, which cannot be stored by an outbox.
        var hasSourceIdentity = attributes?.TryGetValue(sourceIdentityHeader, out var sourceIdentity) == true &&
            !string.IsNullOrWhiteSpace(sourceIdentity.StringValue);
        var hasDeadIdentity = attributes?.TryGetValue(deadIdentityHeader, out var deadIdentity) == true &&
            !string.IsNullOrWhiteSpace(deadIdentity.StringValue);
        var deduplication = hasSourceIdentity && hasDeadIdentity ? null :
            delivery.Message.Attributes?.GetValueOrDefault("MessageDeduplicationId");
        if (native is not null || deduplication is not null)
            options = new SendOptions().ConfigureNative<SendMessageRequest>(request =>
            {
                if (native is not null)
                    foreach (var attribute in native) request.MessageAttributes[attribute.Key] = attribute.Value;
                request.MessageDeduplicationId = deduplication;
            });
        WireString? group = delivery.Message.Attributes?.TryGetValue("MessageGroupId", out var key) == true ? new(key) : null;
        return new(delivery.QueueUrl, delivery.Body, headers.ToArray(), group, options: options);
    }

    public Task StopAsync(CancellationToken cancellationToken) => _transport?.StopAsync(cancellationToken) ?? Task.CompletedTask;
}
