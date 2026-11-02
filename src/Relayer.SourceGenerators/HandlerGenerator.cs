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
    private static readonly DiagnosticDescriptor InaccessibleMessage = new(
        "RLYH001", "Handler message type is inaccessible",
        "Handler '{0}' uses message type '{1}', which generated dispatch cannot access. Make the message type assembly-accessible and non-file-local.",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var handlers = context.SyntaxProvider.CreateSyntaxProvider(
                static (node, _) => node is TypeDeclarationSyntax { BaseList: not null }
                    or GenericNameSyntax { Identifier.ValueText: "Handler", TypeArgumentList.Arguments.Count: 1 },
                static (ctx, ct) => Discover(ctx, ct))
            .SelectMany(static (models, _) => models)
            .WithTrackingName("HandlerModels")
            .Collect();

        context.RegisterSourceOutput(handlers, static (spc, models) => Emit(spc, models));
    }

    private static ImmutableArray<DispatchModel> Discover(GeneratorSyntaxContext context, CancellationToken ct)
    {
        INamedTypeSymbol? type;
        var isLocal = context.Node is TypeDeclarationSyntax;
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
            if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, saga)) { sagaBase = current; break; }
        var result = ImmutableArray.CreateBuilder<DispatchModel>();
        foreach (var contract in type.AllInterfaces)
        {
            var isBatch = SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, batch);
            if (isBatch || SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, single))
            {
                result.Add(new DispatchModel(
                    type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    contract.TypeArguments[0] is IDynamicTypeSymbol
                        ? "global::System.Object"
                        : contract.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), isBatch, isLocal,
                    CanReference(contract.TypeArguments[0], compilation),
                    !isBatch && contract.TypeArguments[0] is INamedTypeSymbol message
                        && SymbolEqualityComparer.Default.Equals(message.OriginalDefinition, compilation.GetTypeByMetadataName("Relayer.Tombstone`1"))
                        ? message.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) : null,
                    !isBatch && sagaBase is not null && SymbolEqualityComparer.Default.Equals(sagaBase.TypeArguments[1], contract.TypeArguments[0])
                        ? sagaBase.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) : null));
            }
        }

        return result.ToImmutable();
    }

    private static bool CanReference(ITypeSymbol type, Compilation compilation) => type switch
    {
        ITypeParameterSymbol => false,
        IArrayTypeSymbol array => CanReference(array.ElementType, compilation),
        INamedTypeSymbol named => !named.IsUnboundGenericType && !named.IsFileLocal
            && compilation.IsSymbolAccessibleWithin(named, compilation.Assembly)
            && named.TypeArguments.All(argument => CanReference(argument, compilation))
            && (named.ContainingType is null || CanReference(named.ContainingType, compilation)),
        _ => true
    };

    private static void Emit(SourceProductionContext context, ImmutableArray<DispatchModel> models)
    {
        foreach (var invalid in models.Where(static m => !m.IsAccessible)
                     .Select(static m => (m.Handler, m.Message)).Distinct())
        {
            context.ReportDiagnostic(Diagnostic.Create(InaccessibleMessage, Location.None, invalid.Handler, invalid.Message));
        }

        var descriptors = models.Where(static m => m.IsAccessible).Distinct()
            .OrderBy(static m => m.Handler, StringComparer.Ordinal)
            .ThenBy(static m => m.Message, StringComparer.Ordinal).ThenBy(static m => m.IsBatch).ToArray();
        if (descriptors.Length == 0)
        {
            return;
        }

        // Explicit requests for a local handler share the descriptors discovered from its declaration.
        var contracts = descriptors.GroupBy(static m => (m.Handler, m.Message, m.IsBatch))
            .Select(static group => group.First()).ToArray();
        var builder = new StringBuilder("// <auto-generated/>\n#nullable enable\n\nnamespace Relayer;\n\ninternal static class RelayerGeneratedHandlers\n{\n");
        builder.AppendLine("    public static global::Relayer.IRelayerBuilder AddHandlersFromThisAssembly(this global::Relayer.IRelayerBuilder builder)");
        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(builder);");
        foreach (var handler in descriptors.Where(static m => m.IsLocal).Select(static m => m.Handler).Distinct())
        {
            builder.Append("        Handler<").Append(handler).AppendLine(">(builder);");
        }
        builder.AppendLine("        return builder;");
        builder.AppendLine("    }");
        builder.AppendLine();
        EmitRegistration(builder, contracts, isConsumer: false);
        builder.AppendLine();
        EmitRegistration(builder, contracts, isConsumer: true);

        for (var i = 0; i < contracts.Length; i++)
        {
            var model = contracts[i];
            builder.AppendLine();
            builder.Append("    private sealed class Dispatcher").Append(i).Append(" : global::Relayer.")
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

    private static void EmitRegistration(StringBuilder builder, DispatchModel[] contracts, bool isConsumer)
    {
        var builderType = isConsumer ? "IConsumerBuilder" : "IRelayerBuilder";
        builder.Append("    public static global::Relayer.").Append(builderType)
            .Append(" Handler<THandler>(this global::Relayer.").Append(builderType).AppendLine(" builder)");
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
                    builder.Append(isConsumer ? "            builder.AddHandler(new Dispatcher" : "            builder.Handlers.Add(new Dispatcher")
                        .Append(i).AppendLine("());");
                    if (contracts[i].SagaState is { } state)
                        builder.Append("            global::Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton<global::Relayer.Sagas.SagaExecution<")
                            .Append(state).Append(", ").Append(contracts[i].Message).AppendLine(">>(builder.Services);");
                }
            }
            builder.Append("            global::Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddScoped<")
                .Append(handler).AppendLine(">(builder.Services);");
            builder.AppendLine("            return builder;");
            builder.AppendLine("        }");
        }
        builder.AppendLine("        throw new global::System.InvalidOperationException($\"No generated descriptor for handler '{typeof(THandler)}'. Use a concrete, closed, assembly-accessible IHandler<T> or IBatchHandler<T> in Handler<THandler>().\");");
        builder.AppendLine("    }");
    }

    private sealed record DispatchModel(string Handler, string Message, bool IsBatch, bool IsLocal, bool IsAccessible, string? TombstoneMessage, string? SagaState);
}
