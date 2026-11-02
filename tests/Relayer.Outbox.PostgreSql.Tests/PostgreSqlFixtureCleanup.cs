namespace Relayer.Outbox.PostgreSql.Tests;

public static class PostgreSqlFixtureCleanup
{
    // TUnit reports hook failures alongside the test exception, rather than replacing it.
    [AfterEvery(Test)]
    public static void ReportFailures(TestContext context)
    {
        if (context.StateBag.TryGetValue<List<Exception>>("PostgreSqlFixture.CleanupFailures", out var failures)
            && failures.Count > 0)
            throw new AggregateException("PostgreSQL fixture cleanup failed.", failures);
    }
}
