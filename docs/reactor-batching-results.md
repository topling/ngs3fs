# FUSE remote-dispatch batching experiment

## Decision

The bounded same-receive, same-worker `MSG_RING` batching experiment was
withdrawn. It reduced internal notifications and completion-queue entries, but
the measured end-to-end CPU and latency changes were too small to justify the
additional dispatch-chain, rollback, and shutdown-lifetime machinery.

The comparison used GitHub Actions run `34263310963` on one four-vCPU runner.
The baseline was `a3009ab3998e7cdad6e7c2321ecf2219dbade5e6`, with one
`MSG_RING` notification per remote request. The experimental revision was
`34016f8777f3644631c6b0f52bc9b81c0fca1eda`, with bounded per-worker FIFO
batching. Both variants used four total reactors (one ingress and three
workers), eight HTTP connections, identical workloads, and alternating sample
order.

## Headline results

Each row is the median of three unsampled runs with 8,192 operations. Daemon
CPU includes the kernel workers attributed to the daemon. Total CPU is daemon
plus workload CPU; it excludes the S3 server and unattributed global kernel
work. Negative percentages mean that the batched revision used less CPU or
wall time.

| Scenario | Daemon CPU/op, baseline | Daemon CPU/op, batched | Delta | Total CPU/op, baseline | Total CPU/op, batched | Delta | Wall delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| normal | 560,302 ns | 561,523 ns | +0.22% | 981,308 ns | 985,100 ns | +0.39% | -0.16% |
| random | 819,091 ns | 806,884 ns | -1.49% | 1,353,856 ns | 1,341,758 ns | -0.89% | +0.37% |
| cache cold | 452,880 ns | 445,556 ns | -1.62% | 934,684 ns | 928,980 ns | -0.61% | -0.01% |
| cache warm | 499,267 ns | 494,384 ns | -0.98% | 995,095 ns | 988,603 ns | -0.65% | -0.50% |

The per-repetition daemon CPU changes were `-0.44%, +1.09%, -1.08%` for
normal; `-1.05%, -2.95%, -0.89%` for random; `-1.62%, -2.41%, -0.54%` for
cache cold; and `-0.98%, -2.18%, -0.98%` for cache warm. This is a small,
partly noisy effect rather than a broad improvement.

## Internal transport effect

The transport counters below are medians from the same three runs. They cover
the whole mount lifetime, including setup and shutdown, rather than only the
timed workload. Remote-request counts differed by at most 0.12% between
variants. For the unbatched baseline, the batch count is inferred from its
one-notification-per-remote-request contract and the worker dispatch counts;
the experimental revision recorded it explicitly.

| Scenario | Requests per notification | Notification reduction | CQ completion reduction | `wait_calls` reduction |
|---|---:|---:|---:|---:|
| normal | 1.409 | 29.0% | 9.5% | 0.04% |
| random | 1.672 | 40.4% | 24.1% | 11.3% |
| cache cold | 2.146 | 53.4% | 38.0% | 15.9% |
| cache warm | 2.099 | 52.3% | 37.4% | 15.2% |

`wait_calls` is reactor-loop accounting and equals the recorded completion
batch count in these runs. It does not prove that a thread slept or imply an
equal reduction in scheduler context switches.

## Profile evidence and limitations

Matched 4,000 Hz profiles were available for normal, cache-cold, and
cache-warm runs. Each folded profile was independently scaled to its own
measured task clock, and leaf costs were treated as a partition; inclusive
frames were not added together.

- Normal changed by -0.54% task clock, -0.92% context switches, and +0.34%
  elapsed time. `io_msg_ring` and its remote wake chain did not show a clear
  reduction in this sample.
- Cache cold changed by -2.15% task clock, -3.60% context switches, and -2.22%
  elapsed time. The clearest batching-related change was about 20.3 ms less in
  the `io_msg_ring` remote `wake_up_state` spin-unlock chain; total
  `_raw_spin_unlock_irqrestore` leaf time fell by about 35.6 ms.
- Cache warm reported -8.91% task clock but +8.80% context switches and +45.0%
  elapsed time. That single profile pair was affected by scheduling or runner
  variance and is not suitable as a headline result. Its three-run unsampled
  result was only -0.98% daemon CPU and -0.50% wall time.
- No matched pre-batching random-advice profile was collected, so the random
  flame graph cannot establish a batching delta.

The experiment demonstrated that batching can halve notifications for
cache-heavy traffic, but that internal reduction translated to at most 1.62%
daemon CPU and 0.89% total CPU in these tests, with essentially unchanged wall
time. The simpler one-request-per-notification transport is therefore the
maintained design. A future batching proposal should require a workload where
the end-to-end benefit is materially larger, not only lower internal counters.
