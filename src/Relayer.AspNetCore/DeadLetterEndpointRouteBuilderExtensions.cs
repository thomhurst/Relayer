using System.Globalization;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.AspNetCore;

/// <summary>Maps optional management endpoints over a named dead-letter manager.</summary>
public static class DeadLetterEndpointRouteBuilderExtensions
{
    /// <summary>Maps list, peek, redrive and purge endpoints. Apply application authorization to the returned group.</summary>
    /// <param name="endpoints">The application's route builder.</param>
    /// <param name="transportName">The keyed IDeadLetterManager registration to use.</param>
    /// <param name="prefix">The management route prefix.</param>
    /// <returns>A group to which the application can apply its authorization policy.</returns>
    public static RouteGroupBuilder MapRelayerDeadLetters(this IEndpointRouteBuilder endpoints,
        string transportName, string prefix = "/relayer/dead-letters")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(transportName);
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        // Resolve at mapping time so a misspelled registration fails during startup.
        endpoints.ServiceProvider.GetRequiredKeyedService<IDeadLetterManager>(transportName);
        var group = endpoints.MapGroup(prefix);
        group.MapGet("/", (RequestDelegate)(context => ExecuteAsync(context, transportName, Operation.List)));
        group.MapGet("/messages", (RequestDelegate)(context => ExecuteAsync(context, transportName, Operation.Peek)));
        group.MapPost("/redrive", (RequestDelegate)(context => ExecuteAsync(context, transportName, Operation.Redrive)));
        group.MapPost("/purge", (RequestDelegate)(context => ExecuteAsync(context, transportName, Operation.Purge)));
        return group;
    }

    private static async Task ExecuteAsync(HttpContext context, string name, Operation operation)
    {
        var cancellationToken = context.RequestAborted;
        cancellationToken.ThrowIfCancellationRequested();
        var manager = context.RequestServices.GetRequiredKeyedService<IDeadLetterManager>(name);
        try
        {
            if (operation == Operation.List)
            {
                var sources = manager.List();
                var snapshot = new DeadLetterSource[sources.Count];
                for (var i = 0; i < snapshot.Length; i++) snapshot[i] = sources[i];
                await context.Response.WriteAsJsonAsync(snapshot, DeadLetterJsonContext.Default.DeadLetterSourceArray,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }

            string? source;
            int maxMessages;
            MaintenanceRequest? request = null;
            if (operation == Operation.Peek)
            {
                source = SingleQuery(context, "source");
                var count = SingleQuery(context, "maxMessages");
                maxMessages = count is null ? 20 : int.TryParse(count, NumberStyles.None,
                    CultureInfo.InvariantCulture, out var parsed) ? parsed
                    : throw new ArgumentException("maxMessages must be an integer between 1 and 100.");
            }
            else
            {
                if (!context.Request.HasJsonContentType())
                {
                    context.Response.StatusCode = StatusCodes.Status415UnsupportedMediaType;
                    await WriteErrorAsync(context, "Send an application/json request body.").ConfigureAwait(false);
                    return;
                }
                request = await context.Request.ReadFromJsonAsync(DeadLetterJsonContext.Default.MaintenanceRequest,
                    cancellationToken).ConfigureAwait(false)
                    ?? throw new ArgumentException("A JSON request body is required.");
                source = request.Source;
                maxMessages = request.MaxMessages;
            }
            if (string.IsNullOrWhiteSpace(source)) throw new ArgumentException("source is required; use GET on the group root to list configured sources.");
            if (maxMessages is < 1 or > 100) throw new ArgumentException("maxMessages must be between 1 and 100.");
            if (request?.MessageIds is { } ids && (operation != Operation.Redrive || ids.Length is < 1 or > 100 || Array.Exists(ids, string.IsNullOrWhiteSpace)))
                throw new ArgumentException("messageIds is supported only for redrive and must contain 1 to 100 non-empty identifiers; omit it to redrive all scanned messages.");

            if (operation == Operation.Peek)
            {
                var page = await manager.PeekAsync(source, maxMessages, SingleQuery(context, "cursor"), cancellationToken).ConfigureAwait(false);
                var messages = new MessageSnapshot[page.Messages.Count];
                for (var i = 0; i < messages.Length; i++)
                    messages[i] = new(page.Messages[i].MessageId, page.Messages[i].Body);
                await context.Response.WriteAsJsonAsync(new PageSnapshot(messages, page.NextCursor),
                    DeadLetterJsonContext.Default.PageSnapshot, cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }

            Func<DeadLetterMessage, bool>? filter = null;
            if (request!.MessageIds is { } selected)
            {
                var set = new HashSet<string>(selected, StringComparer.Ordinal);
                filter = message => message.MessageId is { } id && set.Contains(id);
            }
            var affected = operation == Operation.Redrive
                ? await manager.RedriveAsync(source, filter, maxMessages, cancellationToken).ConfigureAwait(false)
                : await manager.PurgeAsync(source, maxMessages, cancellationToken).ConfigureAwait(false);
            await context.Response.WriteAsJsonAsync(new OperationResult(affected),
                DeadLetterJsonContext.Default.OperationResult, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (JsonException)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await WriteErrorAsync(context, "Invalid JSON body; provide source, maxMessages (1–100), and optional messageIds for redrive.").ConfigureAwait(false);
        }
        catch (ArgumentException exception)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await WriteErrorAsync(context, exception.Message).ConfigureAwait(false);
        }
    }

    private static string? SingleQuery(HttpContext context, string name)
    {
        var values = context.Request.Query[name];
        if (values.Count > 1) throw new ArgumentException($"Specify {name} once.");
        return values.Count == 0 ? null : values[0];
    }

    private static Task WriteErrorAsync(HttpContext context, string message) =>
        context.Response.WriteAsJsonAsync(new ErrorResponse(message), DeadLetterJsonContext.Default.ErrorResponse,
            cancellationToken: context.RequestAborted);

    private enum Operation { List, Peek, Redrive, Purge }
}
