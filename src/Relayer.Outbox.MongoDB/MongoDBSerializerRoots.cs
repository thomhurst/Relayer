using System.Diagnostics.CodeAnalysis;
using MongoDB.Bson.Serialization.Serializers;

namespace Relayer.Outbox.MongoDB;

// Driver 3.x creates these built-in serializers reflectively even for BsonDocument hello responses.
// Preserve their constructors without replacing any process-wide BSON configuration.
internal static class MongoDBSerializerRoots
{
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(EnumerableInterfaceImplementerSerializer<List<object>, object>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(DictionaryInterfaceImplementerSerializer<Dictionary<string, object>, string, object>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(ExpandoObjectSerializer))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(RawBsonDocumentSerializer))]
    internal static void Preserve() { }
}
