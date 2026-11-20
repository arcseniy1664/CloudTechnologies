
using SPS.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SPS.Student;

public sealed class IS_HYBRID_Policy : ISystemPolicy
{
    private TimeSpan? _lowLoadSince;
    private int _lastDesiredWorkerCount = 2;

    // Популярность ключей, полученная через Observe.
    // Значение постепенно затухает со временем.
    private readonly Dictionary<string, (double Score, TimeSpan LastSeen)>
        _keyPopularity = new();

    private const double PopularityHalfLifeSeconds = 30.0;

    public IRequestPolicy RpcPolicy { get; } =
        new BirdAdaptiveRequestPolicy();

    // ---------------------------------------------------------
    // API ROUTING
    // ---------------------------------------------------------

    public EndpointSelectionDecision? SelectEndpoint(
        RoutingContext context,
        Random random)
    {
        if (context.Endpoints.Count == 0)
            return null;

        var selectedEndpoint = context.Endpoints
            .OrderBy(endpoint =>
                endpoint.OutstandingCalls / endpoint.Speed)
            .ThenByDescending(endpoint => endpoint.Speed)
            .ThenBy(endpoint => endpoint.Id)
            .First();

        return EndpointSelectionDecision.Assign(selectedEndpoint.Id);
    }

    public EndpointDecision ReviewEndpoint(
        EndpointContext context,
        Random random)
    {
        return EndpointDecision.Allow();
    }

    // ---------------------------------------------------------
    // CACHE READ
    // ---------------------------------------------------------

    public CacheReadDecision OnCacheRead(
        CacheReadContext context,
        Random random)
    {
        var entry = context.Entry;

        if (entry != null)
        {
            TimeSpan age = context.Now - entry.SourceObservedAt;

            bool isFresh =
                context.Now < entry.ExpiresAt &&
                age >= TimeSpan.Zero &&
                age <= context.Request.MaxDataAge;

            if (isFresh)
                return CacheReadDecision.ServeCached();
        }

        var pendingFetch = context.PendingFetches
            .FirstOrDefault(fetch =>
                fetch.Key == context.Request.ResourceKey);

        if (pendingFetch != null)
            return CacheReadDecision.JoinFetch(pendingFetch.Id);

        return CacheReadDecision.StartFetch();
    }

    // ---------------------------------------------------------
    // CACHE WRITE
    // ---------------------------------------------------------

    public CacheWriteDecision OnCacheFill(
        CacheFillContext context,
        Random random)
    {
        long newEntrySize = context.Value.SizeBytes;

        if (newEntrySize > context.CapacityBytes)
            return CacheWriteDecision.Skip();

        long retainedBytes = context.Entries
            .Where(entry => entry.Key != context.Value.Key)
            .Sum(entry => entry.SizeBytes);

        long bytesToFree =
            retainedBytes + newEntrySize - context.CapacityBytes;

        var keysToEvict = new List<string>();

        if (bytesToFree > 0)
        {
            // Сначала удаляем записи с наименьшей актуальной
            // популярностью. При равенстве удаляем более старые.
            var candidates = context.Entries
                .Where(entry => entry.Key != context.Value.Key)
                .OrderBy(entry =>
                    GetKeyPopularity(entry.Key, context.Now))
                .ThenBy(entry => entry.SourceObservedAt);

            foreach (var candidate in candidates)
            {
                keysToEvict.Add(candidate.Key);
                bytesToFree -= candidate.SizeBytes;

                if (bytesToFree <= 0)
                    break;
            }
        }

        return CacheWriteDecision.Retain(
            TimeSpan.FromSeconds(5),
            keysToEvict.ToArray());
    }

    // ---------------------------------------------------------
    // CACHE POPULARITY
    // ---------------------------------------------------------

    private double GetKeyPopularity(
        string key,
        TimeSpan now)
    {
        if (!_keyPopularity.TryGetValue(key, out var data))
            return 0.0;

        double elapsedSeconds =
            Math.Max(0.0, (now - data.LastSeen).TotalSeconds);

        double decay = Math.Pow(
            0.5,
            elapsedSeconds / PopularityHalfLifeSeconds);

        return data.Score * decay;
    }

    // ---------------------------------------------------------
    // JOB ASSIGNMENT
    // ---------------------------------------------------------

    public JobAssignmentDecision? SelectJobAssignment(
        JobSchedulingContext context,
        Random random)
    {
        if (context.QueuedJobs.Count == 0 ||
            context.AvailableWorkers.Count == 0)
            return null;

        var selectedJob = context.QueuedJobs
            .OrderByDescending(job =>
                (double)job.Request.Revenue /
                Math.Max(job.Request.EstimatedWorkSeconds, 0.001))
            .ThenBy(job => job.QueuedAt)
            .ThenBy(job => job.Id)
            .First();

        var selectedWorker = context.AvailableWorkers
            .OrderByDescending(worker => worker.Speed)
            .ThenBy(worker => worker.Id)
            .First();

        double queueTime =
            (context.Now - selectedJob.QueuedAt).TotalSeconds;

        double processingTime =
            selectedJob.Request.EstimatedWorkSeconds /
            Math.Max(selectedWorker.Speed, 0.001);

        double projectedTime = queueTime + processingTime;

        if (projectedTime >= 25.0)
            return JobAssignmentDecision.Reject(selectedJob.Id);

        return JobAssignmentDecision.Assign(
            selectedJob.Id,
            selectedWorker.Id);
    }

    // ---------------------------------------------------------
    // JOB RECOVERY
    // ---------------------------------------------------------

    public JobRecoveryDecision ReviewJob(
        JobRecoveryContext context,
        Random random)
    {
        var job = context.Job;

        if (job.AssignedAt == null)
            return JobRecoveryDecision.KeepWaiting();

        TimeSpan lastActivity =
            job.LastHeartbeat ?? job.AssignedAt.Value;

        if (context.Now - lastActivity >=
            TimeSpan.FromSeconds(2.5))
        {
            return JobRecoveryDecision.RequeueAfter(
                TimeSpan.Zero);
        }

        return JobRecoveryDecision.KeepWaiting();
    }

    // ---------------------------------------------------------
    // CAPACITY MANAGEMENT
    // ---------------------------------------------------------

    public CapacityPlan ReviewCapacity(
        CapacityContext context,
        Random random)
    {
        const int desiredApiCount = 2;
        const int minimumWorkers = 2;
        const int maximumWorkersLimit = 5;

        int availableWorkers =
            (context.Settings.ComputeUnitLimit -
             desiredApiCount * context.Settings.Api.ComputeUnits) /
            context.Settings.Worker.ComputeUnits;

        int maximumWorkers = Math.Min(
            maximumWorkersLimit,
            Math.Min(
                context.Settings.MaximumWorkers,
                availableWorkers));

        int minWorkers = Math.Min(
            minimumWorkers,
            maximumWorkers);

        int readyWorkers = context.Instances.Count(i =>
            i.Role == InstanceRole.Worker &&
            i.State == InstanceState.Ready);

        int startingWorkers = context.Instances.Count(i =>
            i.Role == InstanceRole.Worker &&
            i.State == InstanceState.Starting);

        int drainingWorkers = context.Instances.Count(i =>
            i.Role == InstanceRole.Worker &&
            i.State == InstanceState.Draining);

        int queuedJobs = context.QueuedJobs.Count;

        int assignedJobs = context.Instances
            .Where(i =>
                i.Role == InstanceRole.Worker &&
                i.State != InstanceState.Stopped)
            .Sum(i => i.AssignedJobs);

        int totalWorkload = queuedJobs + assignedJobs;

        int activeWorkers = readyWorkers + startingWorkers;

        int desiredWorkerCount = _lastDesiredWorkerCount;

        long[] retireIds = Array.Empty<long>();

        // Масштабирование вверх.
        if (totalWorkload > 0)
        {
            _lowLoadSince = null;

            int requiredWorkers = Math.Min(
                maximumWorkers,
                Math.Max(
                    minWorkers,
                    Math.Min(maximumWorkers, totalWorkload)));

            if (requiredWorkers > activeWorkers)
            {
                int targetWorkers = Math.Min(
                    requiredWorkers,
                    desiredWorkerCount + 2);

                desiredWorkerCount = Math.Max(
                    desiredWorkerCount,
                    targetWorkers);
            }
        }
        // Масштабирование вниз.
        else
        {
            if (!_lowLoadSince.HasValue)
                _lowLoadSince = context.Now;

            if (context.Now - _lowLoadSince.Value >=
                TimeSpan.FromSeconds(20))
            {
                var idleWorker = context.Instances
                    .Where(i =>
                        i.Role == InstanceRole.Worker &&
                        i.State == InstanceState.Ready &&
                        i.Mode == EndpointMode.AllowTraffic &&
                        i.AssignedJobs == 0 &&
                        i.OutstandingCalls == 0)
                    .OrderBy(i => i.Id)
                    .FirstOrDefault();

                if (idleWorker != null &&
                    desiredWorkerCount > minWorkers)
                {
                    desiredWorkerCount--;

                    retireIds = new[] { idleWorker.Id };

                    _lowLoadSince = context.Now;
                }
            }
        }

        desiredWorkerCount = Math.Clamp(
            desiredWorkerCount,
            minWorkers,
            maximumWorkers);

        _lastDesiredWorkerCount = desiredWorkerCount;

        return new CapacityPlan(
            desiredApiCount,
            desiredWorkerCount,
            retireIds);
    }

    // ---------------------------------------------------------
    // OBSERVATIONS
    // ---------------------------------------------------------

    public void Observe(SystemObservation observation)
    {
        // Нас интересуют события поступления запросов:
        // они показывают, какие ключи действительно востребованы.
        if (observation.Kind != ObservationKind.RequestArrived)
            return;

        string key = observation.Key;

        if (string.IsNullOrEmpty(key))
            return;

        if (_keyPopularity.TryGetValue(key, out var previous))
        {
            double elapsedSeconds = Math.Max(
                0.0,
                (observation.Now - previous.LastSeen).TotalSeconds);

            double decay = Math.Pow(
                0.5,
                elapsedSeconds / PopularityHalfLifeSeconds);

            double updatedScore =
                previous.Score * decay + 1.0;

            _keyPopularity[key] = (
                updatedScore,
                observation.Now);
        }
        else
        {
            _keyPopularity[key] = (
                1.0,
                observation.Now);
        }
    }

    // ---------------------------------------------------------
    // ADAPTIVE RPC POLICY — LAB 1
    // ---------------------------------------------------------

    private sealed class BirdAdaptiveRequestPolicy : IRequestPolicy
    {
        private const double InitialTimeoutMs = 700.0;
        private const double MinTimeoutMs = 100.0;
        private const double MaxTimeoutMs = 1000.0;

        private const double EwmaAlpha = 0.2;
        private const double SafetyFactor = 2.5;

        private const double TimeoutIncreaseFactor = 1.2;
        private const int SuccessesBeforeDecrease = 50;
        private const double DecreaseFactor = 0.95;

        private const double RetryDelayMs = 100.0;

        private readonly object _sync = new();

        private readonly HashSet<long> _retriedRequests = new();

        private double _timeoutMs = InitialTimeoutMs;
        private double? _latencyEwmaMs;
        private int _consecutiveSuccesses;

        public TimeSpan GetTimeout(
            RequestContext context,
            Random random)
        {
            lock (_sync)
            {
                return TimeSpan.FromMilliseconds(_timeoutMs);
            }
        }

        public RetryDecision OnResult(
            RequestObservation observation,
            Random random)
        {
            lock (_sync)
            {
                if (observation.Outcome == RequestOutcome.Timeout)
                {
                    _consecutiveSuccesses = 0;

                    _timeoutMs = Math.Min(
                        MaxTimeoutMs,
                        _timeoutMs * TimeoutIncreaseFactor);

                    if (_retriedRequests.Contains(
                        observation.RequestId))
                    {
                        _retriedRequests.Remove(
                            observation.RequestId);

                        return RetryDecision.NoRetry();
                    }

                    _retriedRequests.Add(
                        observation.RequestId);

                    return RetryDecision.RetryAfter(
                        TimeSpan.FromMilliseconds(RetryDelayMs));
                }

                if (observation.Outcome == RequestOutcome.Success)
                {
                    _retriedRequests.Remove(
                        observation.RequestId);

                    _consecutiveSuccesses++;

                    double elapsedMs =
                        observation.Elapsed.TotalMilliseconds;

                    if (elapsedMs >= 0)
                    {
                        _latencyEwmaMs = _latencyEwmaMs.HasValue
                            ? EwmaAlpha * elapsedMs +
                              (1.0 - EwmaAlpha) * _latencyEwmaMs.Value
                            : elapsedMs;
                    }

                    if (_consecutiveSuccesses >=
                        SuccessesBeforeDecrease)
                    {
                        if (_latencyEwmaMs.HasValue)
                        {
                            double targetTimeoutMs = Math.Clamp(
                                _latencyEwmaMs.Value * SafetyFactor,
                                MinTimeoutMs,
                                MaxTimeoutMs);

                            if (_timeoutMs > targetTimeoutMs)
                            {
                                _timeoutMs = Math.Max(
                                    targetTimeoutMs,
                                    _timeoutMs * DecreaseFactor);
                            }
                        }

                        _consecutiveSuccesses = 0;
                    }
                }

                return RetryDecision.NoRetry();
            }
        }
    }
}
