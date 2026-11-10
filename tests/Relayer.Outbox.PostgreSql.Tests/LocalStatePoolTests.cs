namespace Relayer.Outbox.PostgreSql.Tests;

public class LocalStatePoolTests
{
    [Test]
    [Arguments(16)]
    [Arguments(64)]
    public async Task Saturation_retains_every_state_and_disposes_overflow(int capacity)
    {
        var created = 0;
        var pool = new PostgreSqlLocalStatePool<State>(capacity, () => { created++; return new(); });
        var states = new State[capacity * 3 + 1];
        for (var i = 0; i < states.Length; i++) states[i] = pool.Rent();
        var original = new HashSet<State>(states);
        await Assert.That(original.Count).IsEqualTo(states.Length);
        // Concurrent returns contend for the segment links, including when the first segment is full.
        await Task.WhenAll(states.Select(state => Task.Run(() => pool.Return(state))));
        for (var round = 0; round < 3; round++)
        {
            for (var i = 0; i < states.Length; i++) states[i] = pool.Rent();
            await Assert.That(new HashSet<State>(states).SetEquals(original)).IsTrue();
            foreach (var state in states) pool.Return(state);
        }
        await Assert.That(created).IsEqualTo(states.Length);
        pool.Dispose();
        pool.Dispose();
        foreach (var state in states) await Assert.That(state.Disposals).IsEqualTo(1);
    }

    private sealed class State : IDisposable
    {
        internal int Disposals;
        public void Dispose() => Disposals++;
    }
}
