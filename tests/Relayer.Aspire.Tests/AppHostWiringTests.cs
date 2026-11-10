using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Aspire.Tests;

public class AppHostWiringTests
{
    [Test]
    public async Task Sample_references_both_brokers_and_reports_application_health()
    {
        var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.RelayerSample_AppHost>();
        await using var app = await builder.BuildAsync();
        var model = app.Services.GetRequiredService<DistributedApplicationModel>();
        var api = model.Resources.Single(resource => resource.Name == "api");
        var references = api.Annotations.OfType<ResourceRelationshipAnnotation>()
            .Where(annotation => annotation.Type == "Reference")
            .Select(annotation => annotation.Resource.Name).ToArray();
        await Assert.That(references).Contains("kafka");
        await Assert.That(references).Contains("servicebus");
        await Assert.That(api.Annotations.OfType<HealthCheckAnnotation>().Any()).IsTrue();
    }
}
