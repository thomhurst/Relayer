namespace Relayer.Pipeline;

// A failure handler throws this to hand an already logged handler failure to its transport's own
// retry or poison handling. The pipeline and the transport do not log it again.
internal abstract class ConsumeFailureHandoff(Exception error) : Exception(error.Message, error);
