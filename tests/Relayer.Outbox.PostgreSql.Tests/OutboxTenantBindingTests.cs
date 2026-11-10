using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.PostgreSql.Tests;

public class OutboxTenantBindingTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Authorized_binding_populates_metadata_and_rejects_ambiguous_or_conflicting_values(bool utf8)
    {
        var services = new ServiceCollection();
        services.AddRelayerTenancy();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var tenant = scope.ServiceProvider.GetRequiredService<TenantContext>();
        tenant.Id = utf8 ? new("ténant"u8.ToArray()) : new("ténant");
        var binding = new OutboxTenantBinding(tenant, "ténant");
        var header = new KeyValuePair<string, WireString>(TenantContext.HeaderName, tenant.Id);
        var record = new OutboxRecord(Guid.CreateVersion7(), "events", "probe;v=1",
            new OutgoingMessage("orders", new byte[] { 1 }, new[] { header }), DateTimeOffset.UtcNow);
        await Assert.That(binding.Bind(record).TenantId).IsEqualTo("ténant");
        await Assert.That(binding.Bind(record with { TenantId = "ténant" }).TenantId).IsEqualTo("ténant");
        await Assert.That(() => binding.Bind(record with { TenantId = "other" })).Throws<InvalidOperationException>();
        await Assert.That(() => binding.Bind(record with { Message = new("orders", new byte[] { 1 }) })).Throws<InvalidOperationException>();
        await Assert.That(() => binding.Bind(record with { Message = new("orders", new byte[] { 1 }, new[] { header, header }) }))
            .Throws<InvalidOperationException>();
        await Assert.That(() => binding.Bind(record with { Message = new("orders", new byte[] { 1 },
            new[] { new KeyValuePair<string, WireString>(TenantContext.HeaderName, new("other")) }) }))
            .Throws<InvalidOperationException>();
        tenant.Id = new("other");
        await Assert.That(() => binding.Bind(record)).Throws<InvalidOperationException>();
        await Assert.That(() => new OutboxTenantBinding(tenant, "ténant")).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Tenant_enabled_outbox_registrations_require_an_explicit_authorized_binding(bool ef)
    {
        var services = new ServiceCollection();
        services.AddRelayerTenancy();
        if (ef) services.AddRelayerEntityFrameworkCoreOutbox();
        else services.AddRelayerPostgreSqlOutbox();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        await Assert.That(() =>
        {
            if (ef) new DbContextOptionsBuilder().UseNpgsql("Host=localhost")
                .AddRelayerOutbox(scope.ServiceProvider);
            else scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        }).Throws<InvalidOperationException>().WithMessageContaining("TenantBinding");
    }

    [Test]
    public async Task Binding_is_not_accepted_from_another_scope_or_after_scope_disposal()
    {
        var services = new ServiceCollection();
        services.AddRelayerTenancy();
        await using var provider = services.BuildServiceProvider();
        var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        var tenant = first.ServiceProvider.GetRequiredService<TenantContext>();
        tenant.Id = new("one");
        var binding = new OutboxTenantBinding(tenant, "one");
        second.ServiceProvider.GetRequiredService<TenantContext>().Id = new("one");
        await Assert.That(() => binding.ValidateContext(second.ServiceProvider.GetRequiredService<TenantContext>()))
            .Throws<InvalidOperationException>();
        await first.DisposeAsync();
        await Assert.That(binding.Validate).Throws<ObjectDisposedException>();
        await using var reused = provider.CreateAsyncScope();
        var reusedTenant = reused.ServiceProvider.GetRequiredService<TenantContext>();
        await Assert.That(ReferenceEquals(tenant, reusedTenant)).IsTrue();
        reusedTenant.Id = new("one");
        await Assert.That(() => binding.ValidateContext(reusedTenant)).Throws<InvalidOperationException>();
    }
}
