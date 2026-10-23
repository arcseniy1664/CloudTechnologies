
using System;
using RRC.Abstractions;

namespace RRC.Student;

public sealed class SampleRequestPolicy : IRequestPolicy
{
    private const double InitialTimeoutMs = 700.0;
    private const double MinTimeoutMs = 100.0;
    private const double MaxTimeoutMs = 1000.0;

    private const double TimeoutIncreaseMs = 100.0;
    private const double EwmaAlpha = 0.2;
    private const double SafetyFactor = 2.5;
    private const int SuccessesBeforeDecrease = 50;
    private const double DecreaseStepMs = 20.0;

    private readonly object _sync = new();

    private double _timeoutMs = InitialTimeoutMs;
    private double? _latencyEwmaMs;
    private int _consecutiveSuccesses;

    public TimeSpan GetTimeout(RequestContext context, Random random)
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
                    _timeoutMs + TimeoutIncreaseMs);
            }
            else if (observation.Outcome == RequestOutcome.Success)
            {
                _consecutiveSuccesses++;

                double elapsedMs = observation.Elapsed.TotalMilliseconds;

                if (elapsedMs >= 0)
                {
                    _latencyEwmaMs = _latencyEwmaMs.HasValue
                        ? EwmaAlpha * elapsedMs
                            + (1.0 - EwmaAlpha) * _latencyEwmaMs.Value
                        : elapsedMs;
                }

                if (_consecutiveSuccesses >= SuccessesBeforeDecrease)
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
                                _timeoutMs - DecreaseStepMs);
                        }
                    }

                    _consecutiveSuccesses = 0;
                }
            }

            return RetryDecision.NoRetry();
        }
    }
}
