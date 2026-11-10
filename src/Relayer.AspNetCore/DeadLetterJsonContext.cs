using System.Text.Json.Serialization;

namespace Relayer.AspNetCore;

internal sealed record MessageSnapshot(string? MessageId, ReadOnlyMemory<byte> Body);
internal sealed record PageSnapshot(MessageSnapshot[] Messages, string? NextCursor);
internal sealed record OperationResult(int AffectedMessages);
internal sealed record ErrorResponse(string Error);
internal sealed record MaintenanceRequest(string Source, int MaxMessages = 20, string[]? MessageIds = null);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow)]
[JsonSerializable(typeof(DeadLetterSource[]))]
[JsonSerializable(typeof(PageSnapshot))]
[JsonSerializable(typeof(OperationResult))]
[JsonSerializable(typeof(ErrorResponse))]
[JsonSerializable(typeof(MaintenanceRequest))]
internal sealed partial class DeadLetterJsonContext : JsonSerializerContext;
