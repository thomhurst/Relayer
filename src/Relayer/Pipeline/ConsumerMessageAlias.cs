namespace Relayer.Pipeline;

internal readonly record struct ConsumerMessageAlias(Type MessageType, Type TombstoneType, MessageType Identity)
{
    internal bool Matches(HandlerDescriptor descriptor)
    {
        return descriptor.MessageType == (descriptor.IsTombstone ? TombstoneType : MessageType);
    }
}
