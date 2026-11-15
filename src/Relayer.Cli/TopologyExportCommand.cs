using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AmazonSqs;
using Relayer.AzureServiceBus;

namespace Relayer.Tool;

internal static class TopologyExportCommand
{
    internal const string Usage = "dotnet relayer topology export --assembly <application.dll> --type <Namespace.Declarations> --format json|bicep|terraform [--transport <name>] [--account-id <12 digits> --region <aws-region> --partition aws]";

    public static int Run(string[] args, TextWriter output, TextWriter error)
    {
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 2; i < args.Length; i += 2)
        {
            if (args[i] is not ("--assembly" or "--type" or "--format" or "--transport" or "--account-id" or "--region" or "--partition")
                || i + 1 == args.Length || string.IsNullOrWhiteSpace(args[i + 1]) || args[i + 1].StartsWith("--", StringComparison.Ordinal)
                || !options.TryAdd(args[i], args[i + 1]))
                return Fail($"Unknown, repeated or incomplete option: {args[i]}. {Usage}");
        }
        if (!options.TryGetValue("--assembly", out var path) || !options.TryGetValue("--type", out var typeName)
            || !options.TryGetValue("--format", out var format))
            return Fail($"Provide --assembly, --type and --format. {Usage}");
        if (format is not ("json" or "bicep" or "terraform")) return Fail("Unsupported format. Choose json, bicep or terraform.");
        var aws = options.ContainsKey("--account-id") || options.ContainsKey("--region") || options.ContainsKey("--partition");
        if (format == "json" && (options.ContainsKey("--transport") || aws))
            return Fail("JSON exports the complete topology; omit --transport and AWS options.");
        if (aws && (format != "terraform" || options.ContainsKey("--transport") || !options.ContainsKey("--account-id") || !options.ContainsKey("--region")))
            return Fail("AWS export requires --format terraform, --account-id and --region; omit --transport. --partition is optional.");
        if (format != "json" && !aws && !options.ContainsKey("--transport"))
            return Fail("Service Bus export requires --transport <name>. For AWS Terraform provide --account-id and --region instead.");
        try
        {
            using var declarations = new DeclarationContext(Path.GetFullPath(path));
            var assembly = declarations.LoadFromAssemblyPath(Path.GetFullPath(path));
            var type = assembly.GetType(typeName, throwOnError: true)!;
            var factory = type.GetMethod("CreateTopologyServices", BindingFlags.Public | BindingFlags.Static, Type.EmptyTypes);
            if (factory is null || factory.ReturnType != typeof(IServiceProvider) || factory.ContainsGenericParameters)
                return Fail($"'{typeName}' must declare public static IServiceProvider CreateTopologyServices(). Return a declaration-only provider; do not start a host.");
            var services = (IServiceProvider?)factory.Invoke(null, null)
                ?? throw new InvalidOperationException("CreateTopologyServices returned null; return a declaration-only IServiceProvider.");
            string text;
            try
            {
                var topology = services.GetRequiredService<ITopology>();
                using var buffer = new MemoryStream();
                if (format == "json") topology.ExportJson(buffer);
                else if (aws)
                    topology.ExportAmazonTerraform(services, buffer, new AmazonTerraformOptions
                    {
                        AccountId = options["--account-id"], Region = options["--region"],
                        Partition = options.GetValueOrDefault("--partition", "aws")
                    });
                else
                    services.GetRequiredService<ServiceBusTopologyExporter>().Export(options["--transport"], buffer,
                        format == "bicep" ? ServiceBusTopologyFormat.Bicep : ServiceBusTopologyFormat.Terraform);
                text = Encoding.UTF8.GetString(buffer.ToArray());
            }
            finally { (services as IDisposable)?.Dispose(); }
            output.Write(text);
            return 0;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not StackOverflowException)
        {
            while (exception is TargetInvocationException { InnerException: { } inner }) exception = inner;
            return Fail($"Topology export failed: {exception.Message}");
        }

        int Fail(string message) { error.WriteLine(message); return 1; }
    }

    // Application code is explicitly selected, trusted code. Share Relayer and DI contracts with the tool;
    // resolve other application dependencies from its build/publish output, never the working directory.
    private sealed class DeclarationContext(string path) : AssemblyLoadContext(isCollectible: true), IDisposable
    {
        private readonly AssemblyDependencyResolver _resolver = new(path);
        protected override Assembly? Load(AssemblyName name)
        {
            if (name.Name is { } simple && (simple is "Relayer" or "Relayer.AzureServiceBus" or "Relayer.AmazonSqs" or "Azure.Core"
                || simple.StartsWith("Azure.Messaging.ServiceBus", StringComparison.Ordinal)
                || simple.StartsWith("AWSSDK.", StringComparison.Ordinal)
                || simple.StartsWith("Microsoft.Extensions.", StringComparison.Ordinal)))
            {
                try { return Default.LoadFromAssemblyName(name); }
                catch (FileNotFoundException) { /* Application-only Extensions implementations resolve below. */ }
            }
            var dependency = _resolver.ResolveAssemblyToPath(name);
            return dependency is null ? null : LoadFromAssemblyPath(dependency);
        }
        protected override nint LoadUnmanagedDll(string name)
        {
            var dependency = _resolver.ResolveUnmanagedDllToPath(name);
            return dependency is null ? 0 : LoadUnmanagedDllFromPath(dependency);
        }
        public void Dispose() => Unload();
    }
}
