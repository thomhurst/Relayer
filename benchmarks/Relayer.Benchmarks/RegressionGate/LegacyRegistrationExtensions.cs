#if !RELAYER_FOCUSED_SPI
namespace Relayer.Benchmarks;

// The performance workflow copies this harness onto the pre-namespace-split baseline,
// retaining its project file. No compatibility API ships in Relayer itself.
internal static class LegacyRegistrationExtensions
{
    internal static IRelayerBuilder Infrastructure(this IRelayerBuilder builder) => builder;
}
#endif
