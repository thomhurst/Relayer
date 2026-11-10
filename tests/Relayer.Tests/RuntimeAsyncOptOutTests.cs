namespace Relayer.Tests;

public class RuntimeAsyncOptOutTests
{
    // Runtime async (net11.0) ignores [AsyncMethodBuilder]. A pooled builder without the opt-out still compiles,
    // stops pooling, and allocates on every suspension, so the source is checked here.
    [Test]
    public async Task Every_pooled_async_builder_opts_out_of_runtime_async()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "Relayer.slnx"))) root = root.Parent;
        await Assert.That(root).IsNotNull();

        var missing = new List<string>();
        var pooled = 0;
        foreach (var directory in new[] { "src", "tools" })
        foreach (var file in Directory.EnumerateFiles(Path.Combine(root!.FullName, directory), "*.cs", SearchOption.AllDirectories))
        {
            var separator = Path.DirectorySeparatorChar;
            if (file.Contains($"{separator}obj{separator}") || file.Contains($"{separator}bin{separator}")) continue;
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                if (!lines[i].Contains("[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder")) continue;
                pooled++;
                if (i == 0 || lines[i - 1].Trim() != "[RuntimeAsyncMethodGeneration(false)]")
                    missing.Add($"{Path.GetRelativePath(root.FullName, file)}:{i + 1}");
            }
        }

        await Assert.That(pooled).IsGreaterThan(0);
        await Assert.That(missing).IsEmpty();
    }
}
