using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace DC.Compute;

public sealed class IPSSolver : IPowSolver
{
    public const int TaskCount = 100;
    public const int BatchSize = 8192;
    public const int ClaimStep = 100_000;

    private sealed class ActiveTask
    {
        public readonly int TaskId;
        public readonly CancellationTokenSource LocalCts;
        public long NextNonce;
        public volatile bool Solved;
        public volatile int WorkersOnTask;

        public ActiveTask(int taskId)
        {
            TaskId = taskId;
            LocalCts = new CancellationTokenSource();
            NextNonce = 0;
        }
    }

    public Task<PowResult> SolveAsync(string seed, int difficultyBits, int workerCount,
        IProgress<PowProgress>? progress, CancellationToken ct, PowTelemetry? telemetry = null)
    {
        if (seed is null || seed.Length != 6 || seed.Any(c => c is < '0' or > '9'))
            throw new ArgumentException("The seed must contain exactly six decimal digits (0–9).", nameof(seed));
        if (difficultyBits is < 1 or > PowSolver.MaximumDifficultyBits)
            throw new ArgumentOutOfRangeException(nameof(difficultyBits));
        if (workerCount is < 1 or > PowSolver.MaximumWorkers)
            throw new ArgumentOutOfRangeException(nameof(workerCount));

        return Task.Run(() =>
        {
            var solutions = new PowSolution?[TaskCount];
            var activeTasks = Enumerable.Range(0, TaskCount).Select(i => new ActiveTask(i)).ToArray();
            var pendingQueue = new ConcurrentQueue<ActiveTask>(activeTasks);

            int completed = 0;
            long totalAttempts = 0;

            var sw = Stopwatch.StartNew();
            telemetry?.Start();

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            CancellationToken linkedToken = linkedCts.Token;

            try
            {
                var workers = new Task[workerCount];
                for (int w = 0; w < workerCount; w++)
                {
                    workers[w] = Task.Run(() => WorkerLoop(
                        seed, difficultyBits, activeTasks, pendingQueue,
                        solutions, ref completed, ref totalAttempts,
                        progress, linkedToken, telemetry), linkedToken);
                }

                Task.WaitAll(workers);
                ct.ThrowIfCancellationRequested();
                sw.Stop();

                var final = new PowSolution[TaskCount];
                for (int i = 0; i < TaskCount; i++)
                    final[i] = solutions[i] ?? throw new InvalidOperationException($"Task {i} has no solution.");

                return new PowResult(Array.AsReadOnly(final), sw.Elapsed,
                    completed, workerCount, totalAttempts);
            }
            finally
            {
                foreach (var t in activeTasks) t.LocalCts.Cancel();
                telemetry?.Finish();
            }
        }, ct);
    }

    private static void WorkerLoop(string seed, int difficultyBits,
        ActiveTask[] activeTasks, ConcurrentQueue<ActiveTask> pendingQueue,
        PowSolution?[] solutions, ref int completed, ref long totalAttempts,
        IProgress<PowProgress>? progress, CancellationToken ct, PowTelemetry? telemetry)
    {
        var sw = new Random();

        while (!ct.IsCancellationRequested)
        {
            ActiveTask? task = null;
            foreach (var t in activeTasks)
            {
                if (!t.Solved && !t.LocalCts.IsCancellationRequested)
                {
                    long next = Interlocked.Read(ref t.NextNonce);
                    if (next < long.MaxValue) { task = t; break; }
                }
            }

            if (task is null)
            {
                break;
            }

            Interlocked.Increment(ref task.WorkersOnTask);

            try
            {
                long startNonce = Interlocked.Add(ref task.NextNonce, ClaimStep) - ClaimStep;
                long endNonce = startNonce + ClaimStep;

                if (startNonce >= long.MaxValue) continue;

                int lane = telemetry?.BeginWork(task.TaskId) ?? -1;
                try
                {
                    progress?.Report(new PowProgress(task.TaskId, null,
                        Volatile.Read(ref completed), Interlocked.Read(ref totalAttempts)));

                    var result = SearchRange(seed, task.TaskId, startNonce, endNonce,
                        difficultyBits, ct, task.LocalCts.Token, telemetry, lane);

                    if (result.Found)
                    {
                        if (!task.Solved)
                        {
                            task.Solved = true;
                            var sol = new PowSolution(task.TaskId, result.Nonce);
                            solutions[task.TaskId] = sol;
                            telemetry?.Solved(task.TaskId);

                            long total = Interlocked.Add(ref totalAttempts, result.Attempts);
                            int done = Interlocked.Increment(ref completed);
                            progress?.Report(new PowProgress(task.TaskId, sol, done, total));

                            task.LocalCts.Cancel();
                        }
                        else
                        {
                            Interlocked.Add(ref totalAttempts, result.Attempts);
                        }
                    }
                    else
                    {
                        Interlocked.Add(ref totalAttempts, result.Attempts);
                    }
                }
                finally { telemetry?.EndWork(lane); }
            }
            finally
            {
                Interlocked.Decrement(ref task.WorkersOnTask);
            }
        }
    }

    private readonly struct SearchResult
    {
        public readonly bool Found;
        public readonly ulong Nonce;
        public readonly long Attempts;

        public SearchResult(bool found, ulong nonce, long attempts)
        {
            Found = found;
            Nonce = nonce;
            Attempts = attempts;
        }
    }

    private static SearchResult SearchRange(string seed, int taskId, long startNonce, long endNonce,
        int difficultyBits, CancellationToken globalCt, CancellationToken localCt,
        PowTelemetry? telemetry, int lane)
    {
        byte[] prefix = Encoding.UTF8.GetBytes($"BSU-POW-1|{seed}|{taskId}|");
        Span<byte> message = stackalloc byte[prefix.Length + 20];
        prefix.AsSpan().CopyTo(message);
        Span<byte> hash = stackalloc byte[32];

        long attempts = 0;
        long reported = 0;

        try
        {
            for (long n = startNonce; n < endNonce; n++)
            {
                if ((n & 255) == 0)
                {
                    globalCt.ThrowIfCancellationRequested();
                    if (localCt.IsCancellationRequested) break;
                }

                ulong nonce = (ulong)n;
                if (!System.Buffers.Text.Utf8Formatter.TryFormat(nonce, message[prefix.Length..], out int digits))
                    throw new InvalidOperationException("Could not encode the nonce.");

                SHA256.HashData(message[..(prefix.Length + digits)], hash);
                attempts++;

                if (telemetry is not null && (attempts & (BatchSize - 1)) == 0)
                {
                    telemetry.AddAttempts(lane, taskId, attempts - reported);
                    reported = attempts;
                }

                if (HasLeadingZeroBits(hash, difficultyBits))
                    return new SearchResult(true, nonce, attempts);
            }
        }
        finally
        {
            telemetry?.AddAttempts(lane, taskId, attempts - reported);
        }

        return new SearchResult(false, 0, attempts);
    }

    private static bool HasLeadingZeroBits(ReadOnlySpan<byte> hash, int bits)
    {
        int fullBytes = bits / 8;
        for (int i = 0; i < fullBytes; i++)
            if (hash[i] != 0) return false;
        int rem = bits % 8;
        return rem == 0 || (hash[fullBytes] >> (8 - rem)) == 0;
    }
}