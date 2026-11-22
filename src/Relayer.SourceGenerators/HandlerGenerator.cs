using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Relayer.SourceGenerators;

/// <summary>Emits explicit registration and typed dispatch for concrete handlers.</summary>
[Generator(LanguageNames.CSharp)]
public sealed class HandlerGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor s_inaccessibleMessage = new(
        "RLYH001", "Handler message type is inaccessible",
        "Handler '{0}' uses message type '{1}', which generated dispatch cannot access. Make the message type assembly-accessible and non-file-local.",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyh001");

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // [Embedded] keeps the internal generated types out of friend assemblies (InternalsVisibleTo),
        // which would otherwise see two Handler<THandler>() extensions.
        context.RegisterPostInitializationOutput(static output => output.AddEmbeddedAttributeDefinition());
        var handlers = context.SyntaxProvider.CreateSyntaxProvider(
                static (node, _) => node is TypeDeclarationSyntax { BaseList: not null }
                    or GenericNameSyntax { Identifier.ValueText: "Handler" or "Consume", TypeArgumentList.Arguments.Count: 1 },
                static (ctx, ct) => Discover(ctx, ct))
            .SelectMany(static (models, _) => models)
            .WithTrackingName("HandlerModels")
            .Collect();

        context.RegisterSourceOutput(handlers, static (spc, models) => Emit(spc, models));
    }

    private static ImmutableArray<DispatchModel> Discover(GeneratorSyntaxContext context, CancellationToken ct)
    {
        INamedTypeSymbol? type;

        if (context.Node is TypeDeclarationSyntax declaration)
        {
            type = context.SemanticModel.GetDeclaredSymbol(declaration, ct) as INamedTypeSymbol;
        }
        else
        {
            var name = (GenericNameSyntax)context.Node;
            type = context.SemanticModel.GetTypeInfo(name.TypeArgumentList.Arguments[0], ct).Type as INamedTypeSymbol;
        }

        var compilation = context.SemanticModel.Compilation;

        if (type is null || type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsStatic
            || !CanReference(type, compilation))
        {
            return [];
        }

        var single = compilation.GetTypeByMetadataName("Relayer.IHandler`1");
        var batch = compilation.GetTypeByMetadataName("Relayer.IBatchHandler`1");
        var saga = compilation.GetTypeByMetadataName("Relayer.Sagas.Saga`2");
        INamedTypeSymbol? sagaBase = null;

        for (var current = type.BaseType; current is not null; current = current.BaseType)
        {
            if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, saga))
            {
                sagaBase = current;
                break;
            }
        }

        var result = ImmutableArray.CreateBuilder<DispatchModel>();

        foreach (var contract in type.AllInterfaces)
        {
            var isBatch = SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, batch);

            if (!isBatch && !SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, single))
            {
                continue;
            }

            var aliases = new SortedSet<string>(StringComparer.Ordinal);
            var message = contract.TypeArguments[0];
            var handlerName = FormatType(type, compilation, aliases);
            var messageName = message is IDynamicTypeSymbol ? "global::System.Object" : FormatType(message, compilation, aliases);
            var tombstoneMessage = !isBatch && message is INamedTypeSymbol named
                && SymbolEqualityComparer.Default.Equals(named.OriginalDefinition, compilation.GetTypeByMetadataName("Relayer.Tombstone`1"))
                ? FormatType(named.TypeArguments[0], compilation, aliases) : null;
            var sagaState = !isBatch && sagaBase is not null && SymbolEqualityComparer.Default.Equals(sagaBase.TypeArguments[1], message)
                ? FormatType(sagaBase.TypeArguments[0], compilation, aliases) : null;

            result.Add(new DispatchModel(handlerName, messageName, isBatch, CanReference(message, compilation),
                tombstoneMessage, sagaState, string.Join("\n", aliases)));
        }

        return result.ToImmutable();
    }

    private static string FormatType(ITypeSymbol type, Compilation compilation, SortedSet<string> aliases)
    {
        var builder = new StringBuilder();

        // Roslyn attaches the owning namespace to each global qualifier, including those inside type arguments.
        foreach (var part in type.ToDisplayParts(SymbolDisplayFormat.FullyQualifiedFormat))
        {
            if (part.Symbol is INamespaceSymbol { IsGlobalNamespace: true, ContainingAssembly: { } assembly }
                && compilation.GetMetadataReference(assembly) is { } reference
                && reference.Properties.Aliases is { IsDefaultOrEmpty: false } names
                && !names.Contains("global"))
            {
                var alias = names[0];

                if (names.Length > 1)
                {
                    // Prefer an alias that cannot also expose a same-named type from another reference.
                    alias = names.FirstOrDefault(name => !compilation.References.Any(
                        other => other != reference && other.Properties.Aliases.Contains(name))) ?? alias;
                }

                aliases.Add(alias);
                builder.Append('@').Append(alias);
            }
            else
            {
                builder.Append(part.ToString());
            }
        }

        return builder.ToString();
    }

    private static bool CanReference(ITypeSymbol type, Compilation compilation)
    {
        return type switch
        {
            ITypeParameterSymbol => false,
            IArrayTypeSymbol array => CanReference(array.ElementType, compilation),
            INamedTypeSymbol named => !named.IsUnboundGenericType && !named.IsFileLocal
                && compilation.IsSymbolAccessibleWithin(named, compilation.Assembly)
                && named.TypeArguments.All(argument => CanReference(argument, compilation))
                && (named.ContainingType is null || CanReference(named.ContainingType, compilation)),
            _ => true
        };
    }

    private static void Emit(SourceProductionContext context, ImmutableArray<DispatchModel> models)
    {
        foreach (var invalid in models.Where(static m => !m.IsAccessible)
                     .Select(static m => (m.Handler, m.Message)).Distinct())
        {
            context.ReportDiagnostic(Diagnostic.Create(s_inaccessibleMessage, Location.None, invalid.Handler, invalid.Message));
        }

        var contracts = models.Where(static m => m.IsAccessible).Distinct()
            .OrderBy(static m => m.Handler, StringComparer.Ordinal)
            .ThenBy(static m => m.Message, StringComparer.Ordinal).ThenBy(static m => m.IsBatch).ToArray();

        if (contracts.Length == 0)
        {
            return;
        }

        var builder = new StringBuilder("// <auto-generated/>\n#nullable enable\n\n");
        var aliases = contracts.SelectMany(static m => m.Aliases.Split(['\n'], StringSplitOptions.RemoveEmptyEntries))
            .Distinct(StringComparer.Ordinal).OrderBy(static alias => alias, StringComparer.Ordinal).ToArray();

        foreach (var alias in aliases)
        {
            builder.Append("extern alias @").Append(alias).AppendLine(";");
        }

        if (aliases.Length > 0)
        {
            builder.AppendLine();
        }

        builder.Append("namespace Relayer;\n\n[global::Microsoft.CodeAnalysis.EmbeddedAttribute]\ninternal static class RelayerGeneratedHandlers\n{\n");
        EmitRegistration(builder, contracts);

        for (var i = 0; i < contracts.Length; i++)
        {
            var model = contracts[i];
            builder.AppendLine();
            builder.Append("    private sealed class Dispatcher").Append(i).Append(" : global::Relayer.Pipeline.")
                .Append(model.IsBatch ? "BatchHandlerDescriptor<" : model.TombstoneMessage is not null ? "TombstoneHandlerDescriptor<" : "HandlerDescriptor<")
                .Append(model.TombstoneMessage ?? model.Message).AppendLine(">");
            builder.AppendLine("    {");
            builder.Append("        public Dispatcher").Append(i).Append("() : base(typeof(").Append(model.Handler).AppendLine(")) { }");
            builder.AppendLine("        public override global::Relayer.IHandlerMarker Resolve(global::System.IServiceProvider services)");
            builder.Append("            => global::Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService<")
                .Append(model.Handler).AppendLine(">(services);");
            builder.Append("        public override global::System.Threading.Tasks.ValueTask");

            if (model.IsBatch)
            {
                builder.Append("<global::Relayer.BatchResult>");
            }

            builder.Append(" DispatchAsync(global::Relayer.IHandlerMarker handler, ");

            if (model.IsBatch)
            {
                builder.Append("global::Relayer.Batch<").Append(model.Message).Append("> batch, ");
            }
            else
            {
                builder.Append(model.Message).Append(" message, global::Relayer.MessageContext context, ");
            }

            builder.AppendLine("global::System.Threading.CancellationToken cancellationToken)");
            builder.Append("            => ((global::Relayer.").Append(model.IsBatch ? "IBatchHandler<" : "IHandler<")
                .Append(model.Message).Append(">)(").Append(model.Handler).Append(")handler).HandleAsync(")
                .Append(model.IsBatch ? "batch, " : "message, context, ").AppendLine("cancellationToken);");
            builder.AppendLine("    }");
        }

        builder.AppendLine("}");
        context.AddSource("RelayerGeneratedHandlers.g.cs", SourceText.From(builder.ToString(), Encoding.UTF8));
    }

    private static void EmitRegistration(StringBuilder builder, DispatchModel[] contracts)
    {
        builder.AppendLine("    /// <summary>Consumes from the source using the named handler and default consumer settings.</summary>");
        builder.AppendLine("    public static void Consume<THandler>(this global::Relayer.ITransportConsumerBuilder transport, string source)");
        builder.AppendLine("        where THandler : class, global::Relayer.IHandlerMarker");
        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(transport);");
        builder.AppendLine("        transport.Consume(source, static consumer => consumer.Handler<THandler>());");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    public static global::Relayer.IConsumerBuilder Handler<THandler>(this global::Relayer.IConsumerBuilder builder)");
        builder.AppendLine("        where THandler : class, global::Relayer.IHandlerMarker");
        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(builder);");

        foreach (var handler in contracts.Select(static m => m.Handler).Distinct())
        {
            builder.Append("        if (typeof(THandler) == typeof(").Append(handler).AppendLine("))");
            builder.AppendLine("        {");

            for (var i = 0; i < contracts.Length; i++)
            {
                if (contracts[i].Handler == handler)
                {
                    builder.Append("            global::Relayer.Pipeline.ConsumerInfrastructureExtensions.AddHandler(builder, new Dispatcher")
                        .Append(i).AppendLine("());");

                    if (contracts[i].SagaState is { } state)
                    {
                        // The user's saga declaration supplies the opt-in; its pragma does not cover generated files.
                        builder.AppendLine("#pragma warning disable RLYEXP001");
                        builder.Append("            global::Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton<global::Relayer.Sagas.SagaExecution<")
                            .Append(state).Append(", ").Append(contracts[i].Message).AppendLine(">>(builder.Services);");
                        builder.AppendLine("#pragma warning restore RLYEXP001");
                    }
                }
            }

            builder.Append("            global::Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddScoped<")
                .Append(handler).AppendLine(">(builder.Services);");
            builder.AppendLine("            return builder;");
            builder.AppendLine("        }");
        }

        builder.AppendLine("        throw new global::System.InvalidOperationException($\"No generated descriptor for handler '{typeof(THandler)}'. Use a concrete, closed, assembly-accessible IHandler<T> or IBatchHandler<T> in Handler<THandler>(), or call Handler<THandler, TMessage>() from generic code.\");");
        builder.AppendLine("    }");
    }

    private sealed record DispatchModel(
        string Handler, string Message, bool IsBatch, bool IsAccessible, string? TombstoneMessage, string? SagaState, string Aliases);
}
