using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Relayer.Testing;

/// <summary>Fixture-local failure capture. Register with the fixture's ILoggerFactory before creating clients.</summary>
public sealed class ConformanceLogCapture : ILoggerProvider
{
    private readonly ConcurrentQueue<string> _failures = new();

    /// <summary>Throws if an error, critical entry or warning-level cancellation was logged.</summary>
    public void AssertClean()
    {
        if (!_failures.IsEmpty)
            throw new InvalidOperationException("Transport conformance: shutdown logged failures: " + string.Join(Environment.NewLine, _failures));
    }

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => new CaptureLogger(categoryName, _failures);
    /// <inheritdoc />
    public void Dispose() { } // Capture remains readable after the fixture's logger factory is disposed.

    private sealed class CaptureLogger(string category, ConcurrentQueue<string> failures) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error || logLevel >= LogLevel.Warning && exception is OperationCanceledException)
                failures.Enqueue($"{category} [{logLevel}] {formatter(state, exception)} {exception}");
        }
    }
}
