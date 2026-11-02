namespace Relayer.Serialization;

/// <summary>Optional asynchronous serializer preparation. The host awaits it before starting any receiver.</summary>
public interface IMessageSerializerInitializer
{
    /// <summary>Prepares schemas and other immutable serializer state before message processing starts.</summary>
    ValueTask InitializeAsync(CancellationToken cancellationToken);
}
