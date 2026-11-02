using System.Net;
using System.Text;
using System.Text.Json;
using Dekaf.SchemaRegistry.Avro.Poco;

namespace Relayer.TestFixtures;

[AvroRecord(Name = "Order", Namespace = "relayer.tests")]
public sealed partial class AvroOrder
{
    public int Number { get; init; }
    public string Customer { get; init; } = "";
}

// Exercise the native registry HTTP client, including schema lookup during reader preparation.
public sealed class AvroRegistryResponses(int schemaId = 7) : HttpMessageHandler
{
    public int Requests { get; private set; }
    public bool Fail { get; set; }
    public bool Incompatible { get; set; }
    public string? SubjectPath { get; private set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Requests++;
        if (Fail) throw new HttpRequestException("Registry unavailable.");
        var path = request.RequestUri!.AbsolutePath;
        var schema = Incompatible ? "{\"type\":\"record\",\"name\":\"Wrong\",\"fields\":[]}" : AvroOrder.AvroCodec.SchemaJson;
        string body;
        if (path.StartsWith("/subjects/", StringComparison.Ordinal))
        {
            SubjectPath = path;
            using var document = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
            if (document.RootElement.GetProperty("schema").GetString() != AvroOrder.AvroCodec.SchemaJson)
                throw new InvalidOperationException("Unexpected registered schema.");
            body = $"{{\"id\":{schemaId},\"subject\":\"{path[10..]}\",\"version\":1,\"schema\":\"{JsonEncodedText.Encode(schema)}\",\"schemaType\":\"AVRO\"}}";
        }
        else if (path == $"/schemas/ids/{schemaId}")
        {
            body = "{\"schemaType\":\"AVRO\",\"schema\":\"" + JsonEncodedText.Encode(schema) + "\"}";
        }
        else throw new InvalidOperationException($"Unexpected registry request: {request.Method} {path}");
        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
    }
}
