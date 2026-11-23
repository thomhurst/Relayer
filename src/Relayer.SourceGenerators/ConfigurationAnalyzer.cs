using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Relayer.SourceGenerators;

/// <summary>Reports statically visible mistakes in message and consumer declarations.</summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ConfigurationAnalyzer : DiagnosticAnalyzer
{
    private static readonly DiagnosticDescriptor s_missingMessage = Rule("RLY001", "Message has no stable identity",
        "Message type '{0}' has no stable identity. Add [Message(\"stable.name\")] or call MessageTypes.Add<T>(\"stable.name\") in this compilation.");
    private static readonly DiagnosticDescriptor s_duplicateMessage = Rule("RLY002", "Duplicate message identity",
        "Message type '{0}' duplicates message identity '{1}' version {2}. Choose a unique name and version.", DiagnosticSeverity.Error);
    private static readonly DiagnosticDescriptor s_unregisteredHandler = Rule("RLY003", "Handler is never registered",
        "Handler '{0}' is never registered in this compilation. Call consumer.Handler<{0}>() inside a consumer configuration callback.");
    private static readonly DiagnosticDescriptor s_emptyConsumer = Rule("RLY004", "Consumer has no handler",
        "Consumer has no handler. Call consumer.Handler<THandler>() inside its configuration callback.");
    private static readonly DiagnosticDescriptor s_inaccessibleMessage = Rule("RLYH001", "Handler message type is inaccessible",
        "Handler '{0}' uses message type '{1}', which generated dispatch cannot access. Make the message type assembly-accessible and non-file-local.",
        DiagnosticSeverity.Error);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [s_missingMessage, s_duplicateMessage, s_unregisteredHandler, s_emptyConsumer, s_inaccessibleMessage];

    private static DiagnosticDescriptor Rule(string id, string title, string message,
        DiagnosticSeverity severity = DiagnosticSeverity.Warning)
    {
        return new(id, title, message, "Relayer", severity,
                isEnabledByDefault: true, helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#" + id.ToLowerInvariant());
    }

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(static start =>
        {
            var compilation = start.Compilation;
            var attribute = compilation.GetTypeByMetadataName("Relayer.MessageAttribute");

            if (attribute is null)
            {
                return;
            }

            var single = compilation.GetTypeByMetadataName("Relayer.IHandler`1");
            var batch = compilation.GetTypeByMetadataName("Relayer.IBatchHandler`1");
            var transport = compilation.GetTypeByMetadataName("Relayer.TransportBuilder`2");
            var transportInterface = compilation.GetTypeByMetadataName("Relayer.ITransportConsumerBuilder");
            var consumer = compilation.GetTypeByMetadataName("Relayer.ConsumerBuilder`1");
            var consumerInterface = compilation.GetTypeByMetadataName("Relayer.IConsumerBuilder");
            var consumerExtensions = compilation.GetTypeByMetadataName("Relayer.ConsumeBuilderExtensions");
            var registration = compilation.GetTypeByMetadataName("Relayer.RelayerGeneratedHandlers");
            var typedRegistration = compilation.GetTypeByMetadataName("Relayer.HandlerRegistrationExtensions");
            var marker = compilation.GetTypeByMetadataName("Relayer.IHandlerMarker");
            var messageRegistry = compilation.GetTypeByMetadataName("Relayer.Pipeline.MessageTypeRegistry");
            var helperRegistration = compilation.GetTypeByMetadataName("Relayer.RegistersMessageIdentityAttribute");
            var startupAttribute = compilation.GetTypeByMetadataName("Relayer.MessageIdentitiesRegisteredAtStartupAttribute");
            var tombstone = compilation.GetTypeByMetadataName("Relayer.Tombstone`1");
            var identityComparer = new MessageIdentityTypeComparer(compilation);
            var registeredAtStartup = compilation.Assembly.GetAttributes()
                .Where(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, startupAttribute))
                .Select(static a => a.ConstructorArguments.Length == 1 ? a.ConstructorArguments[0].Value as ITypeSymbol : null)
                .Where(static t => t is not null)
                .Select(static t => t is INamedTypeSymbol { IsUnboundGenericType: true } generic ? generic.OriginalDefinition : t!)
                .ToImmutableHashSet<ITypeSymbol>(identityComparer);
            var explicitIdentities = new ConcurrentDictionary<ITypeSymbol, byte>(identityComparer);
            var declaredHandlers = new ConcurrentDictionary<INamedTypeSymbol, byte>(SymbolEqualityComparer.Default);
            var registered = new ConcurrentDictionary<INamedTypeSymbol, HandlerRegistration>(SymbolEqualityComparer.Default);
            var messages = new ConcurrentDictionary<INamedTypeSymbol, AttributeData>(SymbolEqualityComparer.Default);
            var missing = new ConcurrentDictionary<ITypeSymbol, ConcurrentDictionary<Location, byte>>(SymbolEqualityComparer.Default);

            void CheckMessage(ITypeSymbol message, Location use)
            {
                // An open contract (T, Envelope<T>, T[]) is checked where it is closed.
                if (message.TypeKind is TypeKind.Error || ContainsTypeParameter(message))
                {
                    return;
                }

                if (message is INamedTypeSymbol wrapper && SymbolEqualityComparer.Default.Equals(wrapper.OriginalDefinition, tombstone))
                {
                    message = wrapper.TypeArguments[0];
                }

                if (registeredAtStartup.Contains(message)
                    || (message is INamedTypeSymbol named && registeredAtStartup.Contains(named.OriginalDefinition)))
                {
                    return;
                }

                if (message.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, attribute)))
                {
                    return;
                }

                var location = message.Locations.FirstOrDefault(static l => l.IsInSource) ?? use;
                missing.GetOrAdd(message, static _ => new ConcurrentDictionary<Location, byte>()).TryAdd(location, 0);
            }

            bool CheckRelayerRegistration(OperationAnalysisContext context, IMethodSymbol method)
            {
                if (method.Name == "Handler" && method.TypeArguments.Length == 2
                    && SymbolEqualityComparer.Default.Equals(method.ContainingType, typedRegistration))
                {
                    RecordRegistration(registered, method.TypeArguments[0], HandlerRegistration.Selective);
                    CheckMessage(method.TypeArguments[1], context.Operation.Syntax.GetLocation());
                    return true;
                }

                if (method.Name is not ("Handler" or "Consume") || method.TypeArguments.Length != 1
                    || !SymbolEqualityComparer.Default.Equals(method.ContainingType, registration))
                {
                    return false;
                }

                var handler = method.TypeArguments[0];
                RecordRegistration(registered, handler, HandlerRegistration.Other);
                var location = context.Operation.Syntax.GetLocation();
                var canReferenceHandler = HandlerGenerator.CanReference(handler, compilation);

                foreach (var contract in handler.AllInterfaces)
                {
                    if (!IsHandlerContract(contract, single, batch))
                    {
                        continue;
                    }

                    var message = contract.TypeArguments[0];
                    CheckMessage(message, location);

                    if (canReferenceHandler && !HandlerGenerator.CanReference(message, compilation))
                    {
                        context.ReportDiagnostic(Diagnostic.Create(s_inaccessibleMessage, location,
                            handler.ToDisplayString(), message.ToDisplayString()));
                    }
                }

                return true;
            }

            start.RegisterSymbolAction(symbolContext =>
            {
                var type = (INamedTypeSymbol)symbolContext.Symbol;
                var identity = type.GetAttributes().FirstOrDefault(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, attribute));

                if (identity is not null)
                {
                    messages.TryAdd(type, identity);
                }

                foreach (var contract in type.AllInterfaces)
                {
                    if (IsHandlerContract(contract, single, batch))
                    {
                        declaredHandlers.TryAdd(type, 0);
                        break;
                    }
                }
            }, SymbolKind.NamedType);

            start.RegisterOperationAction(operationContext =>
            {
                var invocation = (IInvocationOperation)operationContext.Operation;
                var method = invocation.TargetMethod;

                if (method.Name == "Add" && method.TypeArguments.Length == 1
                    && SymbolEqualityComparer.Default.Equals(method.ContainingType, messageRegistry))
                {
                    explicitIdentities.TryAdd(method.TypeArguments[0], 0);
                }

                if (helperRegistration is not null && method.TypeArguments.Length > 0)
                {
                    AddHelperRegistrations(method, helperRegistration, explicitIdentities);
                }

                var declaringType = method.ContainingType.OriginalDefinition;
                var isTransportBuilder = SymbolEqualityComparer.Default.Equals(declaringType, transport);

                if (isTransportBuilder)
                {
                    if (method.Name == "Send" && method.TypeArguments.Length == 1)
                    {
                        CheckMessage(method.TypeArguments[0], invocation.Syntax.GetLocation());
                    }
                    else if (method.Name == "Request" && method.TypeArguments.Length == 2)
                    {
                        var location = invocation.Syntax.GetLocation();
                        CheckMessage(method.TypeArguments[0], location);
                        CheckMessage(method.TypeArguments[1], location);
                    }
                }

                if (method.Name == "Consume"
                    && (isTransportBuilder || SymbolEqualityComparer.Default.Equals(declaringType, transportInterface)))
                {
                    CheckConsumer(operationContext, invocation, consumer, consumerInterface, consumerExtensions);
                }

                if (method.TypeArguments.Length > 0 && !CheckRelayerRegistration(operationContext, method))
                {
                    foreach (var message in AddHandlerArguments(method, marker, single, batch, registered, operationContext.Compilation))
                    {
                        CheckMessage(message, invocation.Syntax.GetLocation());
                    }
                }
            }, OperationKind.Invocation);

            start.RegisterOperationAction(operationContext =>
            {
                var reference = (IMethodReferenceOperation)operationContext.Operation;
                CheckRelayerRegistration(operationContext, reference.Method);
            }, OperationKind.MethodReference);

            start.RegisterCompilationEndAction(end =>
            {
                foreach (var handler in declaredHandlers.Keys)
                {
                    registered.TryGetValue(handler, out var registrationKind);

                    // A typed registration checks its selected message at the use site. Keep the
                    // declaration checks when registration is absent or its full scope is unknown.
                    if (registrationKind != HandlerRegistration.Selective)
                    {
                        foreach (var contract in handler.AllInterfaces)
                        {
                            if (IsHandlerContract(contract, single, batch))
                            {
                                CheckMessage(contract.TypeArguments[0], handler.Locations[0]);
                            }
                        }
                    }

                    // A public handler can be registered by a referencing assembly.
                    if (registrationKind == HandlerRegistration.None && handler.TypeKind == TypeKind.Class && !handler.IsAbstract
                        && CanReference(handler, compilation) && !IsPublic(handler))
                    {
                        end.ReportDiagnostic(Diagnostic.Create(s_unregisteredHandler, handler.Locations[0], handler.ToDisplayString()));
                    }
                }

                foreach (var pair in missing)
                {
                    if (explicitIdentities.ContainsKey(pair.Key))
                    {
                        continue;
                    }

                    var declaration = pair.Key.DeclaringSyntaxReferences.FirstOrDefault();
                    var locations = declaration is null ? ImmutableArray<Location>.Empty
                        : ImmutableArray.Create(Location.Create(declaration.SyntaxTree, declaration.Span));

                    foreach (var location in pair.Value.Keys)
                    {
                        end.ReportDiagnostic(Diagnostic.Create(s_missingMessage, location, locations, null, pair.Key.ToDisplayString()));
                    }
                }

                foreach (var group in messages.GroupBy(static pair => Identity(pair.Value)))
                {
                    if (group.Key.Name is null || group.Count() < 2)
                    {
                        continue;
                    }

                    foreach (var pair in group)
                    {
                        end.ReportDiagnostic(Diagnostic.Create(s_duplicateMessage, pair.Key.Locations[0],
                            pair.Key.ToDisplayString(), group.Key.Name, group.Key.Version));
                    }
                }
            });
        });
    }

    [Flags]
    private enum HandlerRegistration
    {
        None = 0,
        Selective = 1,
        Other = 2,
    }

    private static bool IsHandlerContract(ITypeSymbol contract, INamedTypeSymbol? single, INamedTypeSymbol? batch)
    {
        return SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, single)
            || SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, batch);
    }

    private static void RecordRegistration(ConcurrentDictionary<INamedTypeSymbol, HandlerRegistration> registered,
        ITypeSymbol handler, HandlerRegistration kind)
    {
        if (handler is INamedTypeSymbol named)
        {
            registered.AddOrUpdate(named.OriginalDefinition, kind, (_, previous) => previous | kind);
        }
    }

    private sealed class MessageIdentityTypeComparer(Compilation compilation) : IEqualityComparer<ITypeSymbol>
    {
        public bool Equals(ITypeSymbol? x, ITypeSymbol? y)
        {
            // Roslyn's C# identity conversion ignores tuple element names and dynamic annotations,
            // including inside constructed types. Keep the original symbols for diagnostics.
            return SymbolEqualityComparer.Default.Equals(x, y)
                || (x is not null && y is not null && compilation.ClassifyCommonConversion(x, y).IsIdentity);
        }

        public int GetHashCode(ITypeSymbol type)
        {
            // Hash the metadata name without type arguments so CLR-equivalent spellings share a bucket.
            return type switch
            {
                IArrayTypeSymbol array => unchecked(GetHashCode(array.ElementType) * 31 + array.Rank),
                IDynamicTypeSymbol => StringComparer.Ordinal.GetHashCode(nameof(Object)),
                _ => StringComparer.Ordinal.GetHashCode(type.MetadataName),
            };
        }
    }

    // A helper marked [RegistersMessageIdentity(nameof(T))] registers its type argument, even
    // when the helper lives in a referenced assembly where its Add<T> call is not visible.
    private static void AddHelperRegistrations(IMethodSymbol method, INamedTypeSymbol helperRegistration,
        ConcurrentDictionary<ITypeSymbol, byte> explicitIdentities)
    {
        foreach (var attribute in method.OriginalDefinition.GetAttributes())
        {
            if (!SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, helperRegistration)
                || attribute.ConstructorArguments.Length != 1 || attribute.ConstructorArguments[0].Value is not string name)
            {
                continue;
            }

            for (var i = 0; i < method.TypeParameters.Length; i++)
            {
                if (method.TypeParameters[i].Name == name)
                {
                    explicitIdentities.TryAdd(method.TypeArguments[i], 0);
                }
            }
        }
    }

    // A generic helper whose type parameter must be a handler counts as a registration. Check
    // the messages its constraints select, while keeping the declaration checks for its handler.
    private static List<ITypeSymbol> AddHandlerArguments(IMethodSymbol method, INamedTypeSymbol? marker, INamedTypeSymbol? single,
        INamedTypeSymbol? batch, ConcurrentDictionary<INamedTypeSymbol, HandlerRegistration> registered, Compilation compilation)
    {
        var messages = new List<ITypeSymbol>();
        var parameters = method.OriginalDefinition.TypeParameters;

        for (var i = 0; i < parameters.Length; i++)
        {
            foreach (var constraint in parameters[i].ConstraintTypes)
            {
                var contract = IsHandlerContract(constraint, single, batch);

                var handlerConstraint = contract || SymbolEqualityComparer.Default.Equals(constraint.OriginalDefinition, marker)
                    || (marker is not null && constraint.AllInterfaces.Contains(marker, SymbolEqualityComparer.Default));

                if (!handlerConstraint)
                {
                    continue;
                }

                RecordRegistration(registered, method.TypeArguments[i], HandlerRegistration.Other);

                if (contract && constraint is INamedTypeSymbol { TypeArguments.Length: 1 } named)
                {
                    messages.Add(Substitute(named.TypeArguments[0], method, compilation));
                }
            }
        }

        return messages;
    }

    // Replaces the method's and its containing types' type parameters in a constraint type,
    // including inside constructed and array types (IHandler<Envelope<TMessage>> called with
    // TMessage = Order checks Envelope<Order>, IHandler<TMessage[]> checks Order[], and
    // Registrar<Order>.Register<H>() with IHandler<TMessage> on the class checks Order).
    private static ITypeSymbol Substitute(ITypeSymbol type, IMethodSymbol method, Compilation compilation)
    {
        if (type is ITypeParameterSymbol { TypeParameterKind: TypeParameterKind.Method } parameter)
        {
            // A local function's constraint can refer to a type parameter from an enclosing method.
            return SymbolEqualityComparer.Default.Equals(parameter.DeclaringMethod, method.OriginalDefinition)
                ? method.TypeArguments[parameter.Ordinal]
                : type;
        }

        if (type is ITypeParameterSymbol { TypeParameterKind: TypeParameterKind.Type } typeParameter)
        {
            for (var containing = method.ContainingType; containing is not null; containing = containing.ContainingType)
            {
                if (SymbolEqualityComparer.Default.Equals(containing.OriginalDefinition, typeParameter.ContainingSymbol))
                {
                    return containing.TypeArguments[typeParameter.Ordinal];
                }
            }

            return type;
        }

        if (type is IArrayTypeSymbol array)
        {
            var element = Substitute(array.ElementType, method, compilation);
            return SymbolEqualityComparer.Default.Equals(element, array.ElementType) ? array
                : compilation.CreateArrayTypeSymbol(element, array.Rank, array.ElementNullableAnnotation);
        }

        if (type is not INamedTypeSymbol { IsGenericType: true } named || named.TypeKind == TypeKind.Error)
        {
            return type;
        }

        var definition = named.ConstructedFrom;
        var changed = false;

        if (named.ContainingType is { } containingType
            && Substitute(containingType, method, compilation) is INamedTypeSymbol substitutedContaining
            && !SymbolEqualityComparer.Default.Equals(substitutedContaining, containingType))
        {
            // Construct only replaces the nested type's own arguments. Get its definition from the new container first.
            var nestedDefinition = substitutedContaining.GetTypeMembers(named.Name, named.Arity)
                .FirstOrDefault(member => SymbolEqualityComparer.Default.Equals(member.OriginalDefinition, named.OriginalDefinition));

            if (nestedDefinition is null)
            {
                return named;
            }

            definition = nestedDefinition;
            changed = true;
        }

        if (named.Arity == 0)
        {
            return definition;
        }

        var arguments = new ITypeSymbol[named.Arity];

        for (var i = 0; i < arguments.Length; i++)
        {
            arguments[i] = Substitute(named.TypeArguments[i], method, compilation);
            changed |= !SymbolEqualityComparer.Default.Equals(arguments[i], named.TypeArguments[i]);
        }

        return changed ? definition.Construct(arguments) : named;
    }

    private static (string? Name, int Version) Identity(AttributeData attribute)
    {
        var name = attribute.ConstructorArguments.Length == 1 ? attribute.ConstructorArguments[0].Value as string : null;
        var version = 1;

        foreach (var argument in attribute.NamedArguments)
        {
            if (argument.Key == "Version" && argument.Value.Value is int value)
            {
                version = value;
            }
        }

        return (name, version);
    }

    private static bool ContainsTypeParameter(ITypeSymbol type)
    {
        return type switch
        {
            ITypeParameterSymbol => true,
            IArrayTypeSymbol array => ContainsTypeParameter(array.ElementType),
            INamedTypeSymbol named => named.TypeArguments.Any(ContainsTypeParameter)
                || (named.ContainingType is { } containingType && ContainsTypeParameter(containingType)),
            _ => false,
        };
    }

    private static bool CanReference(INamedTypeSymbol type, Compilation compilation)
    {
        return !type.IsFileLocal && !type.IsUnboundGenericType && type.TypeArguments.All(static t => t.TypeKind != TypeKind.TypeParameter)
                && compilation.IsSymbolAccessibleWithin(type, compilation.Assembly)
                && (type.ContainingType is null || CanReference(type.ContainingType, compilation));
    }

    private static bool IsPublic(INamedTypeSymbol type)
    {
        return type.DeclaredAccessibility == Accessibility.Public && (type.ContainingType is null || IsPublic(type.ContainingType));
    }

    private static void CheckConsumer(OperationAnalysisContext context, IInvocationOperation invocation,
        INamedTypeSymbol? consumer, INamedTypeSymbol? consumerInterface, INamedTypeSymbol? consumerExtensions)
    {
        var configure = invocation.Arguments.FirstOrDefault(static a => a.Parameter?.Name == "configure")?.Value;

        while (configure is IConversionOperation conversion)
        {
            configure = conversion.Operand;
        }

        if (configure is IDelegateCreationOperation creation)
        {
            configure = creation.Target;
        }

        if (configure is not IAnonymousFunctionOperation callback || callback.Symbol.Parameters.Length != 1)
        {
            return;
        }

        // A helper, alias or capture can register the handler. Warn only when
        // the inline callback is visibly empty apart from native options/portable settings.
        var parameter = callback.Symbol.Parameters[0];

        foreach (var operation in callback.Body.DescendantsAndSelf())
        {
            if (operation is IInvocationOperation call)
            {
                var method = call.TargetMethod;

                var isBuilderSetting = method.Name is "Concurrency" or "Batch" or "Failures" or "Classify"
                    && (SymbolEqualityComparer.Default.Equals(method.ContainingType.OriginalDefinition, consumer)
                        || SymbolEqualityComparer.Default.Equals(method.ContainingType, consumerInterface));

                if (!isBuilderSetting && !IsConsumerSettingsExtension(method, consumerExtensions))
                {
                    return;
                }
            }

            if (operation is IParameterReferenceOperation reference
                && SymbolEqualityComparer.Default.Equals(reference.Parameter, parameter)
                && reference.Parent is not IPropertyReferenceOperation { Property.Name: "Options" }
                && reference.Parent is not IInvocationOperation
                && !IsConsumerSettingsReceiver(reference, consumerExtensions))
            {
                return;
            }
        }

        context.ReportDiagnostic(Diagnostic.Create(s_emptyConsumer, callback.Syntax.GetLocation()));
    }

    private static bool IsConsumerSettingsReceiver(IParameterReferenceOperation reference, INamedTypeSymbol? consumerExtensions)
    {
        // Extension receivers are represented as arguments, with an implicit conversion for concrete builders.
        var parent = reference.Parent;

        if (parent is IConversionOperation { IsImplicit: true, Conversion.IsReference: true } conversion)
        {
            parent = conversion.Parent;
        }

        return parent is IArgumentOperation { Parameter.Ordinal: 0, Parent: IInvocationOperation call }
            && IsConsumerSettingsExtension(call.TargetMethod, consumerExtensions);
    }

    private static bool IsConsumerSettingsExtension(IMethodSymbol method, INamedTypeSymbol? consumerExtensions)
    {
        return method.Name is "Use" or "Accepts"
            && SymbolEqualityComparer.Default.Equals(method.ContainingType, consumerExtensions);
    }
}
