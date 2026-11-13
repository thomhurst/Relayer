using System.Reflection;
using Microsoft.CodeAnalysis;

namespace Relayer.SourceGenerators;

/// <summary>Loads the pinned JSON generator independently of the compiler's analyzer dependencies.</summary>
internal static class JsonGeneratorLoader
{
    private static readonly Type GeneratorType = LoadGeneratorType();

    public static IIncrementalGenerator Create() =>
        (IIncrementalGenerator)Activator.CreateInstance(GeneratorType)!;

    private static Type LoadGeneratorType()
    {
        using var resource = typeof(JsonGeneratorLoader).Assembly.GetManifestResourceStream(
            "Relayer.System.Text.Json.SourceGeneration.dll")
            ?? throw new InvalidOperationException("The embedded System.Text.Json generator is missing. Rebuild or reinstall the Relayer analyzer package.");
        var bytes = new byte[resource.Length];
        var offset = 0;
        while (offset < bytes.Length)
        {
            var read = resource.Read(bytes, offset, bytes.Length - offset);
            if (read == 0)
            {
                throw new InvalidOperationException("The embedded System.Text.Json generator is truncated. Rebuild or reinstall the Relayer analyzer package.");
            }

            offset += read;
        }

        // Loading bytes gives this assembly its own load context. Loading by name or
        // path can unify with the SDK's newer generator in a long-lived compiler server.
        // Reflection is confined to build-time tooling, never the messaging pipeline.
#pragma warning disable RS1035 // Explicit isolation of a compiler-time dependency.
        var assembly = Assembly.Load(bytes);
#pragma warning restore RS1035
        return assembly.GetType("System.Text.Json.SourceGeneration.JsonSourceGenerator", throwOnError: true)!;
    }
}
