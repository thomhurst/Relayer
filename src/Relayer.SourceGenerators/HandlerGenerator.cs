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
    private static readonly SymbolDisplayFormat s_nullableTypeFormat = SymbolDisplayFormat.FullyQualifiedFormat
        .AddMiscellaneousOptions(SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

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
            var handlerName = FormatDispatchType(type.WithNullableAnnotation(NullableAnnotation.NotAnnotated), compilation, aliases);
            var messageName = FormatDispatchType(message, compilation, aliases);
            var tombstoneMessage = !isBatch && message is INamedTypeSymbol named
                && SymbolEqualityComparer.Default.Equals(named.OriginalDefinition, compilation.GetTypeByMetadataName("Relayer.Tombstone`1"))
                ? FormatType(named.TypeArguments[0], compilation, aliases, s_nullableTypeFormat) : null;
            var sagaState = !isBatch && sagaBase is not null && SymbolEqualityComparer.Default.Equals(sagaBase.TypeArguments[1], message)
                ? FormatType(sagaBase.TypeArguments[0], compilation, aliases, s_nullableTypeFormat) : null;

            result.Add(new DispatchModel(handlerName, messageName, isBatch, CanReference(message, compilation),
                tombstoneMessage, sagaState, string.Join("\n", aliases)));
        }

        return result.ToImmutable();
    }

    private static TypeName FormatDispatchType(ITypeSymbol type, Compilation compilation, SortedSet<string> aliases)
    {
        if (type is IDynamicTypeSymbol)
        {
            const string ObjectType = "global::System.Object";
            var source = type.NullableAnnotation == NullableAnnotation.Annotated ? ObjectType + "?" : ObjectType;
            return new TypeName(source, ObjectType);
        }

        return new TypeName(FormatType(type, compilation, aliases, s_nullableTypeFormat),
            FormatType(type, compilation, aliases, SymbolDisplayFormat.FullyQualifiedFormat));
    }

    private static string FormatType(ITypeSymbol type, Compilation compilation, SortedSet<string> aliases, SymbolDisplayFormat format)
    {
        var builder = new StringBuilder();

        // Roslyn attaches the owning namespace to each global qualifier, including those inside type arguments.
        foreach (var part in type.ToDisplayParts(format))
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

    internal static bool CanReference(ITypeSymbol type, Compilation compilation)
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
        // Nullable reference annotations affect the source signature but not the registered CLR types.
        var contracts = models.GroupBy(static m => (m.Handler.Identity, m.Message.Identity, m.IsBatch))
            .Select(static group => group.OrderBy(static m => m.Handler.Source, StringComparer.Ordinal)
                .ThenBy(static m => m.Message.Source, StringComparer.Ordinal).First())
            .OrderBy(static m => m.Handler.Identity, StringComparer.Ordinal)
            .ThenBy(static m => m.Message.Identity, StringComparer.Ordinal).ThenBy(static m => m.IsBatch).ToArray();

        if (contracts.Length == 0)
        {
            return;
        }

        var builder = new StringBuilder("// <auto-generated/>\n#nullable enable\n#pragma warning disable CS0612, CS0618\n\n");
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
        var inaccessibleHandlers = new HashSet<string>(contracts.Where(static model => !model.IsAccessible)
            .Select(static model => model.Handler.Identity), StringComparer.Ordinal);

        for (var i = 0; i < contracts.Length; i++)
        {
            var model = contracts[i];

            if (inaccessibleHandlers.Contains(model.Handler.Identity))
            {
                continue;
            }

            builder.AppendLine();
            builder.Append("    private sealed class Dispatcher").Append(i).Append(" : global::Relayer.Pipeline.")
                .Append(model.IsBatch ? "BatchHandlerDescriptor<" : model.TombstoneMessage is not null ? "TombstoneHandlerDescriptor<" : "HandlerDescriptor<")
                .Append(model.TombstoneMessage ?? model.Message.Source).AppendLine(">");
            builder.AppendLine("    {");
            builder.Append("        public Dispatcher").Append(i).Append("() : base(typeof(").Append(model.Handler.Source).AppendLine(")) { }");
            builder.AppendLine("        public override global::Relayer.IHandlerMarker Resolve(global::System.IServiceProvider services)");
            builder.Append("            => global::Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService<")
                .Append(model.Handler.Source).AppendLine(">(services);");
            builder.Append("        public override global::System.Threading.Tasks.ValueTask");

            if (model.IsBatch)
            {
                builder.Append("<global::Relayer.BatchResult>");
            }

            builder.Append(" DispatchAsync(global::Relayer.IHandlerMarker handler, ");

            if (model.IsBatch)
            {
                builder.Append("global::Relayer.Batch<").Append(model.Message.Source).Append("> batch, ");
            }
            else
            {
                builder.Append(model.Message.Source).Append(" message, global::Relayer.MessageContext context, ");
            }

            builder.AppendLine("global::System.Threading.CancellationToken cancellationToken)");
            builder.Append("            => ((global::Relayer.").Append(model.IsBatch ? "IBatchHandler<" : "IHandler<")
                .Append(model.Message.Source).Append(">)(").Append(model.Handler.Source).Append(")handler).HandleAsync(")
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

        foreach (var handlerGroup in contracts.GroupBy(static m => m.Handler.Identity))
        {
            var handler = handlerGroup.First().Handler.Source;
            builder.Append("        if (typeof(THandler) == typeof(").Append(handler).AppendLine("))");
            builder.AppendLine("        {");
            var inaccessible = handlerGroup.FirstOrDefault(static model => !model.IsAccessible);

            if (inaccessible is not null)
            {
                // An indirect call with a handler type parameter may not be diagnosable until registration.
                var message = Microsoft.CodeAnalysis.CSharp.SymbolDisplay.FormatLiteral(
                    $"Handler '{handler}' uses message type '{inaccessible.Message.Identity}', which generated dispatch cannot access. "
                    + "Make the message type assembly-accessible and non-file-local, or select a single contract with Handler<THandler, TMessage>().", true);
                builder.Append("            throw new global::System.InvalidOperationException(").Append(message).AppendLine(");");
                builder.AppendLine("        }");
                continue;
            }

            for (var i = 0; i < contracts.Length; i++)
            {
                if (contracts[i].Handler.Identity == handlerGroup.Key)
                {
                    builder.Append("            global::Relayer.Pipeline.ConsumerInfrastructureExtensions.AddHandler(builder, new Dispatcher")
                        .Append(i).AppendLine("());");

                    if (contracts[i].SagaState is { } state)
                    {
                        // The user's saga declaration supplies the opt-in; its pragma does not cover generated files.
                        builder.AppendLine("#pragma warning disable RLYEXP001");
                        builder.Append("            global::Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton<global::Relayer.Sagas.SagaExecution<")
                            .Append(state).Append(", ").Append(contracts[i].Message.Source).AppendLine(">>(builder.Services);");
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

    private sealed record TypeName(string Source, string Identity);

    private sealed record DispatchModel(
        TypeName Handler, TypeName Message, bool IsBatch, bool IsAccessible, string? TombstoneMessage, string? SagaState, string Aliases);
}
