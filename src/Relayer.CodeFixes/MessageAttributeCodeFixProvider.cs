using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Relayer.CodeFixes;

/// <summary>Adds an editable stable wire identity to a source message declaration.</summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(MessageAttributeCodeFixProvider)), Shared]
public sealed class MessageAttributeCodeFixProvider : CodeFixProvider
{
    private static readonly SymbolDisplayFormat s_identitySuggestionFormat = new(
        typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces);

    public override ImmutableArray<string> FixableDiagnosticIds => ["RLY001"];

    public override FixAllProvider GetFixAllProvider()
    {
        return WellKnownFixAllProviders.BatchFixer;
    }

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        foreach (var diagnostic in context.Diagnostics)
        {
            if (diagnostic.AdditionalLocations.Count != 1)
            {
                continue;
            }

            var location = diagnostic.AdditionalLocations[0];
            var document = context.Document.Project.Solution.GetDocument(location.SourceTree);

            if (document is null)
            {
                continue;
            }

            var root = await document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);

            if (root?.FindNode(location.SourceSpan) is not TypeDeclarationSyntax declaration)
            {
                continue;
            }

            var semanticModel = await document.GetSemanticModelAsync(context.CancellationToken).ConfigureAwait(false);

            if (semanticModel is null || semanticModel.GetDeclaredSymbol(declaration, context.CancellationToken) is not { } type
                || !CanAddAttribute(type, semanticModel.Compilation))
            {
                continue;
            }

            var target = await FindAttributeTargetAsync(document.Project.Solution, type, context.CancellationToken)
                .ConfigureAwait(false);

            if (target is not { } insertion)
            {
                continue;
            }

            var suggestedName = type.ToDisplayString(s_identitySuggestionFormat);
            context.RegisterCodeFix(CodeAction.Create("Add [Message] attribute (choose a stable wire name)",
                ct => AddAttributeAsync(insertion.Document, insertion.Position, suggestedName, ct), "Relayer.AddMessageAttribute"), diagnostic);
        }
    }

    private static bool CanAddAttribute(INamedTypeSymbol type, Compilation compilation)
    {
        if (type.TypeKind is not (TypeKind.Class or TypeKind.Struct) || type.IsStatic || type.IsRefLikeType
            || !compilation.IsSymbolAccessibleWithin(type, compilation.Assembly))
        {
            return false;
        }

        for (var current = type; current is not null; current = current.ContainingType)
        {
            if (current.IsFileLocal || current.Arity != 0)
            {
                return false;
            }
        }

        return true;
    }

    private static async Task<(Document Document, int Position)?> FindAttributeTargetAsync(Solution solution, INamedTypeSymbol type,
        CancellationToken cancellationToken)
    {
        var declarations = type.DeclaringSyntaxReferences;
        (Document Document, int Position)? conditionalTarget = null;
        var hasUnconditionalPart = false;

        foreach (var reference in declarations)
        {
            var document = solution.GetDocument(reference.SyntaxTree);

            if (document is null
                || await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false) is not CSharpSyntaxNode root
                || root.FindNode(reference.Span) is not TypeDeclarationSyntax declaration)
            {
                continue;
            }

            var conditional = declarations.Length > 1 && IsInConditionalGroup(root, declaration.Identifier.SpanStart);
            hasUnconditionalPart |= !conditional;

            if (FindInsertionPosition(root, declaration) is not { } position)
            {
                continue;
            }

            if (!conditional)
            {
                return (document, position);
            }

            conditionalTarget ??= (document, position);
        }

        // A conditional edit would leave some builds without an identity when a shared part exists.
        return hasUnconditionalPart ? null : conditionalTarget;
    }

    private static bool IsInConditionalGroup(CSharpSyntaxNode root, int position)
    {
        var directive = root.GetFirstDirective(static d => d is IfDirectiveTriviaSyntax);

        while (directive is not null && directive.SpanStart < position)
        {
            if (directive.GetRelatedDirectives().Last() is not EndIfDirectiveTriviaSyntax endIf || endIf.SpanStart > position)
            {
                return true;
            }

            directive = endIf.GetNextDirective(static d => d is IfDirectiveTriviaSyntax);
        }

        return false;
    }

    private static int? FindInsertionPosition(SyntaxNode root, TypeDeclarationSyntax declaration)
    {
        var keywordPosition = declaration.Keyword.SpanStart;
        var firstModifierPosition = declaration.Modifiers.Count > 0 ? declaration.Modifiers[0].SpanStart : keywordPosition;
        var position = firstModifierPosition;

        // Search only the declaration prefix. Directives in its body cannot change where its attributes belong.
        var prefix = TextSpan.FromBounds(declaration.FullSpan.Start, keywordPosition);
        var branchStart = prefix.Start;

        foreach (var trivia in declaration.DescendantTrivia(prefix))
        {
            var directive = trivia.GetStructure();

            if (directive is BranchingDirectiveTriviaSyntax branch && branch.GetRelatedDirectives().Last().SpanStart >= keywordPosition)
            {
                branchStart = branch.FullSpan.End;
            }

            if (directive is not EndIfDirectiveTriviaSyntax endIf
                || endIf.GetRelatedDirectives()[0] is not IfDirectiveTriviaSyntax startIf
                || startIf.SpanStart >= position)
            {
                continue;
            }

            var branches = TextSpan.FromBounds(startIf.FullSpan.End, endIf.FullSpan.Start);
            var (hasInactiveModifier, hasOnlyPrefixes) = InspectInactiveBranches(root, branches);

            if (firstModifierPosition >= endIf.SpanStart && !hasInactiveModifier)
            {
                continue;
            }

            // Moving before the group would affect another member or leave the type's conditional branch.
            if (!hasOnlyPrefixes || startIf.SpanStart < prefix.Start)
            {
                return null;
            }

            position = startIf.SpanStart;
        }

        return position >= branchStart ? position : null;
    }

    private static (bool HasModifier, bool HasOnlyPrefixes) InspectInactiveBranches(SyntaxNode root, TextSpan branches)
    {
        var hasModifier = false;
        var hasOnlyPrefixes = true;

        foreach (var trivia in root.DescendantTrivia(branches).Where(t => t.IsKind(SyntaxKind.DisabledTextTrivia)))
        {
            // Appending a declaration lets Roslyn distinguish inactive modifiers from other code.
            var inactive = trivia.ToString() + "\n";
            var parsed = SyntaxFactory.ParseCompilationUnit(inactive + "class Declaration {}");

            if (parsed.Members.LastOrDefault() is not ClassDeclarationSyntax type || type.Keyword.SpanStart != inactive.Length)
            {
                hasOnlyPrefixes = false;
                continue;
            }

            hasModifier |= type.Modifiers.Count > 0;
            hasOnlyPrefixes &= type.FullSpan.Start == 0 && !parsed.ContainsDiagnostics;
        }

        return (hasModifier, hasOnlyPrefixes);
    }

    private static async Task<Solution> AddAttributeAsync(Document document, int position, string suggestedName,
        CancellationToken cancellationToken)
    {
        var text = await document.GetTextAsync(cancellationToken).ConfigureAwait(false);
        var line = text.Lines.GetLineFromPosition(position);
        var prefix = text.ToString(TextSpan.FromBounds(line.Start, position));
        var separator = " ";

        if (string.IsNullOrWhiteSpace(prefix))
        {
            var lineWithEnding = line.End == line.EndIncludingLineBreak && line.LineNumber > 0
                ? text.Lines[line.LineNumber - 1]
                : line;
            var lineEnding = text.ToString(TextSpan.FromBounds(lineWithEnding.End, lineWithEnding.EndIncludingLineBreak));
            separator = (lineEnding.Length > 0 ? lineEnding : Environment.NewLine) + prefix;
        }

        // A single insertion lets Roslyn merge fixes for enclosing and nested declarations independently.
        var literal = SyntaxFactory.Literal(suggestedName).Text;
        var change = new TextChange(new TextSpan(position, 0), $"[global::Relayer.MessageAttribute({literal})]{separator}");
        return document.WithText(text.WithChanges(change)).Project.Solution;
    }
}
