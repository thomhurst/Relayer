using System.Runtime.Loader;
using System.Text.Json;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Parameters;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains;
using BenchmarkDotNet.Toolchains.DotNetCli;
using BenchmarkDotNet.Toolchains.Parameters;
using BenchmarkDotNet.Toolchains.Results;

if (args.Length is not (5 or 6))
{
    Console.Error.WriteLine("Usage: FrozenNativeHost <original-image> <benchmark-id> <dotnet-wrapper> <native-log> <receipt> [concurrent|scoped-dispatch|scope-only|scope-only-unscoped]");
    return 2;
}

var image = Path.GetFullPath(args[0]);
var directory = Path.GetDirectoryName(image)!;
var identity = int.Parse(args[1], System.Globalization.CultureInfo.InvariantCulture);
// The parent reads the original method identity; it never invokes its workload,
// setup, attributes, BenchmarkRunner or a builder. The child owns the native job.
var metadata = new FrozenMetadataContext(directory);
var assembly = metadata.LoadFromAssemblyPath(Path.Combine(directory, "Relayer.Benchmarks.dll"));
var selectedCase = args.Length == 6 ? args[5] : "concurrent";
var (owner, methodName, operations) = selectedCase switch
{
    "concurrent" => ("EndToEndBenchmarks", "ConcurrentRoundTrip", 8),
    "scope-only" or "scope-only-unscoped" => ("ConsumePipelineBenchmarks", "ScopeOnly", 1),
    "scoped-dispatch" => ("ConsumePipelineBenchmarks", "DirectScopedDispatch", 1),
    _ => throw new ArgumentException("Unsupported original benchmark case.")
};
var type = assembly.GetType("Relayer.Benchmarks." + owner, throwOnError: true)!;
var method = type.GetMethod(methodName) ?? throw new MissingMethodException(type.FullName, methodName);
// The inventoried generated runner ID selects the parameter and original job.
// The descriptor supplies only the method name required by the native executor.
var descriptor = new Descriptor(type, method, operationsPerInvoke: operations);
var scopeOnly = selectedCase is "scope-only" or "scope-only-unscoped";
var scopedHandler = selectedCase != "scope-only-unscoped";
ParameterInstance[] parameters = scopeOnly
    ? [new(new ParameterDefinition("ScopedHandler", false, [scopedHandler], false, typeof(bool), 0), scopedHandler, BenchmarkDotNet.Reports.SummaryStyle.Default)]
    : [];
using var benchmarkCase = BenchmarkCase.Create(descriptor, Job.Default.WithId("Default"),
    new ParameterInstances(parameters), ManualConfig.CreateEmpty().CreateImmutableConfig());
var benchmarkId = new BenchmarkId(identity, benchmarkCase);
var nameArgument = scopeOnly
    ? $"\"Relayer.Benchmarks.ConsumePipelineBenchmarks.ScopeOnly(ScopedHandler: {scopedHandler})\""
    : $"Relayer.Benchmarks.{owner}.{methodName}";
var expected = $"--benchmarkName {nameArgument} --job Default --benchmarkId {identity}";
if (benchmarkId.ToArguments() != expected)
    throw new InvalidOperationException("Native parent identity differs from the verified original runner.");
var paths = new ArtifactsPaths(directory, directory, directory, "", "", "", "", "", "", image, "", "");
var build = BuildResult.Success(GenerateResult.Success(paths, []));
using var writer = new StreamWriter(args[3]);
var logger = new StreamLogger(writer);
var result = new DotNetCliExecutor(Path.GetFullPath(args[2])).Execute(new ExecuteParameters(
    build, benchmarkCase, benchmarkId, logger, EnvironmentResolver.Instance, launchIndex: 1));
writer.Flush();
// Keep the native process output and broker results separately. Broker results
// include GC statistics sent over pipes, which need not appear on stdout.
File.WriteAllText(args[4], JsonSerializer.Serialize(new
{
    result.FoundExecutable, result.ExitCode, result.IsSuccess, result.Errors,
    result.Results, result.PrefixedLines, result.StandardOutput,
    parent = "BenchmarkDotNet DotNetCliExecutor (native anonymous pipes/Broker/priority)",
    parentAssembly = typeof(DotNetCliExecutor).Assembly.FullName,
    benchmarkArguments = benchmarkId.ToArguments(),
    selectedCase, operationsPerInvoke = operations
}, new JsonSerializerOptions { WriteIndented = true }));
return result.ExitCode == 0 && result.IsSuccess && result.Errors.Count == 0 ? 0 : 1;

sealed class FrozenMetadataContext(string directory) : AssemblyLoadContext
{
    protected override System.Reflection.Assembly? Load(System.Reflection.AssemblyName name)
    {
        if (name.Name == "BenchmarkDotNet.Annotations")
            return typeof(BenchmarkDotNet.Attributes.BenchmarkAttribute).Assembly;
        if (name.Name == "BenchmarkDotNet")
            return typeof(BenchmarkCase).Assembly;
        var path = Path.Combine(directory, name.Name + ".dll");
        return File.Exists(path) ? LoadFromAssemblyPath(path) : null;
    }
}
