// Runtime async (net11.0) ignores [AsyncMethodBuilder], so a pooled builder would silently stop pooling and
// every suspension would allocate. The compiler recognises this attribute by name: false keeps the
// compiler-generated state machine, and with it the pooled builder, for the annotated method.
namespace System.Runtime.CompilerServices;

[AttributeUsage(AttributeTargets.Method)]
internal sealed class RuntimeAsyncMethodGenerationAttribute(bool runtimeAsync) : Attribute
{
    public bool RuntimeAsync { get; } = runtimeAsync;
}
