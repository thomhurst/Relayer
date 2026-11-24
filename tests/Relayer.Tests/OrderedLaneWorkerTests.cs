namespace Relayer.Tests;

public sealed class OrderedLaneWorkerTests
{
    [Test]
    [Arguments(10_000_000L, 500L)]
    [Arguments(10_000_001L, 501L)]
    [Arguments(10_018_000L, 501L)]
    [Arguments(10_020_000L, 501L)]
    [Arguments(36_000_000_000L, 1_800_000L)]
    public async Task Dispatch_and_release_schedule_the_half_lease_in_whole_milliseconds(long leaseTicks, long milliseconds)
    {
        var scheduled = new List<TimeSpan>();
        var worker = new Worker();
        worker.Send = _ =>
        {
            worker.StopRequested = true;
            return new(1);
        };

        await OrderedLaneWorker.RunAsync(worker, TimeSpan.FromTicks(leaseTicks), TimeSpan.FromMilliseconds(1),
            TimeProvider.System, default, default, scheduleTimeout: (source, delay) =>
            {
                scheduled.Add(delay);
                source.CancelAfter(delay);
            });

        var expected = TimeSpan.FromMilliseconds(milliseconds);
        await Assert.That(scheduled).IsEquivalentTo(new[] { expected, expected });
        await Assert.That(worker.Releases).IsEqualTo(1);
    }

    [Test]
    public async Task Slow_batch_refreshes_membership_and_does_not_dispatch_a_transferred_lane()
    {
        var clock = new Clock();
        var worker = new Worker();
        worker.Send = lane =>
        {
            worker.Sent.Add(lane);
            clock.Advance(TimeSpan.FromSeconds(11));
            return new(1);
        };
        worker.Rebalance = () =>
        {
            if (++worker.Rebalances == 2)
            {
                worker.Assigned[1] = false;
            }
        };
        worker.Scan = () =>
        {
            if (++worker.Scans == 2)
            {
                worker.StopRequested = true;
            }
        };
        await Run(worker, clock);
        await Assert.That(worker.Sent).IsEquivalentTo(new[] { 0 });
        await Assert.That(worker.Rebalances).IsEqualTo(2);
        await Assert.That(worker.Releases).IsEqualTo(1);
    }

    [Test]
    public async Task Failed_lane_does_not_prevent_the_next_lane_or_final_release()
    {
        var worker = new Worker();
        worker.Send = lane =>
        {
            worker.Sent.Add(lane);

            if (lane == 0)
            {
                return ValueTask.FromException<int>(new InvalidOperationException("send failed"));
            }

            worker.StopRequested = true;
            return new(1);
        };
        await Run(worker, TimeProvider.System);
        await Assert.That(worker.Sent).IsEquivalentTo(new[] { 0, 1 });
        await Assert.That(worker.Errors).IsEqualTo(1);
        await Assert.That(worker.Releases).IsEqualTo(1);
    }

    [Test]
    public async Task Local_stop_drains_active_batch_before_releasing_and_skips_later_lanes()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var idle = new CancellationTokenSource();
        var worker = new Worker();
        worker.Send = async lane =>
        {
            worker.Sent.Add(lane);
            entered.SetResult();
            await drain.Task;
            return 1;
        };
        var run = Run(worker, TimeProvider.System, idle.Token);
        await entered.Task;
        worker.StopRequested = true;
        await idle.CancelAsync();
        await Assert.That(worker.Releases).IsEqualTo(0);
        drain.SetResult();
        await run;
        await Assert.That(worker.Sent).IsEquivalentTo(new[] { 0 });
        await Assert.That(worker.Releases).IsEqualTo(1);
    }

    [Test]
    public async Task Idle_stop_wakes_poll_and_releases_with_an_uncancelled_cleanup_token()
    {
        using var idle = new CancellationTokenSource();
        var worker = new Worker();
        worker.Ready[0] = worker.Ready[1] = false;
        worker.Scan = () =>
        {
            worker.StopRequested = true;
            idle.Cancel();
        };
        await Run(worker, TimeProvider.System, idle.Token);
        await Assert.That(worker.Sent.Count).IsEqualTo(0);
        await Assert.That(worker.Releases).IsEqualTo(1);
    }

    [Test]
    public async Task Commit_signal_wakes_an_idle_poll_and_a_signal_during_a_scan_rescans_at_once()
    {
        var clock = new StalledClock();
        var wake = new OrderedLaneWake();
        using var idle = new CancellationTokenSource();
        var worker = new Worker();
        worker.Ready[0] = worker.Ready[1] = false;
        worker.Scan = () =>
        {
            // A commit that lands after the probe must not wait for the poll.
            if (++worker.Scans == 1)
            {
                wake.Signal();
            }

            if (worker.Scans == 3)
            {
                worker.StopRequested = true;
                idle.Cancel();
            }
        };
        var run = OrderedLaneWorker.RunAsync(worker, TimeSpan.FromSeconds(30), TimeSpan.FromHours(1), clock, default, idle.Token, wake);
        // The second scan finds nothing and parks on a poll timer that never fires.
        await clock.Polled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await Assert.That(worker.Scans).IsEqualTo(2);
        wake.Signal();
        await run.WaitAsync(TimeSpan.FromSeconds(30));
        await Assert.That(worker.Scans).IsEqualTo(3);
        await Assert.That(worker.Releases).IsEqualTo(1);
    }

    [Test]
    [Arguments(false, 19_000)]
    [Arguments(true, 19_000)]
    [Arguments(false, 20_000)]
    [Arguments(true, 20_000)]
    public async Task Idle_poll_timer_rounds_up_to_whole_milliseconds(bool localWake, int ticks)
    {
        var worker = new Worker();
        worker.Ready[0] = worker.Ready[1] = false;
        var clock = new StalledClock();
        using var idle = new CancellationTokenSource();
        var wake = localWake ? new OrderedLaneWake() : null;
        var run = OrderedLaneWorker.RunAsync(worker, TimeSpan.FromSeconds(30), TimeSpan.FromTicks(ticks), clock,
            default, idle.Token, wake);

        TimeSpan dueTime;

        try
        {
            dueTime = await clock.Polled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            await idle.CancelAsync();
            await run.WaitAsync(TimeSpan.FromSeconds(30));
        }

        await Assert.That(dueTime).IsEqualTo(TimeSpan.FromMilliseconds(2));
        await Assert.That(worker.Releases).IsEqualTo(1);
    }

    private static Task Run(Worker worker, TimeProvider clock, CancellationToken idle = default)
    {
        return OrderedLaneWorker.RunAsync(worker, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(1), clock, default, idle);
    }

    private sealed class StalledClock : TimeProvider
    {
        internal TaskCompletionSource<TimeSpan> Polled
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Polled.TrySetResult(dueTime);
            return new SilentTimer();
        }

        private sealed class SilentTimer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                return true;
            }

            public void Dispose()
            {
            }

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }
    }

    private sealed class Clock : TimeProvider
    {
        private long _ticks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp()
        {
            return _ticks;
        }

        internal void Advance(TimeSpan time)
        {
            _ticks += time.Ticks;
        }
    }

    private sealed class Worker : IOrderedLaneWorker
    {
        public int Lanes => 2;

        public bool StopRequested
        {
            get; set;
        }
        internal readonly bool[] Ready = [true, true];
        internal readonly bool[] Assigned = [true, true];
        internal readonly List<int> Sent = [];
        internal Func<int, ValueTask<int>> Send = _ => new(0);
        internal Action Scan = () =>
        {
        };
        internal Action Rebalance = () =>
        {
        };
        internal int Rebalances, Scans, Errors, Releases;

        public bool IsReady(int lane)
        {
            return Ready[lane];
        }

        public bool IsAssigned(int lane)
        {
            return Assigned[lane];
        }

        public ValueTask FindReadyAsync(CancellationToken token)
        {
            Scan();
            return ValueTask.CompletedTask;
        }

        public ValueTask RebalanceAsync(CancellationToken token)
        {
            Rebalance();
            return ValueTask.CompletedTask;
        }

        public ValueTask<int> DispatchLaneAsync(int lane, CancellationToken token)
        {
            return Send(lane);
        }

        public ValueTask ReleaseAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Releases++;
            return ValueTask.CompletedTask;
        }

        public void DispatchFailed(Exception error)
        {
            Errors++;
        }

        public void ReleaseFailed(Exception error)
        {
            throw error;
        }
    }
}
