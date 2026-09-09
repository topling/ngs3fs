# Rejected 64 KiB cached-reply copy threshold

## Decision

Remove the multi-worker cached-reply override introduced in `7199db2`.
Restore flags zero and libfuse's existing source selection: PREAD below two
system pages, SPLICE otherwise when supported. Retain asynchronous source I/O,
the no-MOVE final reply policy, source-mode/size counters, and their report.
Do not replace the override with another guessed threshold.

## Matched runner evidence

[CI run 34304198753](https://github.com/topling/ngs3fs/actions/runs/34304198753)
passed all seven jobs. It compared baseline
`591c018746cff847c30df3768deae007ae6d7cb4` against experimental
`7199db271f1df7e138e401f4adbaf0f423d99eac` on one runner, with four total
reactors, eight HTTP connections, and three alternating repetitions. Each
timed sample used 32 files, 16 workers and 8,192 operations, half pread and half
mmap, with random advice for the cached suites. These are unsampled
measurements, not perf-instrumented timings.

| Cached workload | Daemon CPU/op baseline → experiment | CPU change | Total client CPU change | Workload wall change | RSS baseline → experiment |
|---|---:|---:|---:|---:|---:|
| Cold | 468,750 → 466,308 ns | -0.52% | -0.31% | -0.17% | 10,008 → 12,348 KiB |
| Warm | 473,632 → 476,074 ns | +0.52% | +0.76% | +1.13% | 9,672 → 12,060 KiB |

Values are medians, not best repetitions. Warm daemon CPU increased in all
three paired repetitions. Cold/warm RSS increased by 2,340/2,388 KiB; HWM
equals RSS in these post-workload process snapshots. These values do not
measure kernel pagecache consumption. Total client CPU includes daemon and
workload, excludes the S3 server and unattributed global kernel work. Workload
wall time is not a per-read latency percentile.

The untouched uncached controls were effectively flat: normal daemon/total
CPU changed +0.00%/+0.14%, random -0.29%/-0.56%. A sub-percent mixed cached
CPU result does not justify retaining additional resident payload buffers.

## Actual request sizes

In these matched random-advice suites, roughly 97% of source submissions were
4 KiB, which already selected PREAD before this experiment. The newly affected
8–64 KiB ranges accounted for less than 1% of submissions. Changing that small
population did not address the dominant source path.

Counters cover the entire mount lifetime, including preparation and warmup,
not just the timed 8,192 application operations. They count accepted source
attempts, including fallback attempts, and exclude exact-I/O continuation
submissions. Bins are mutually exclusive. The baseline predates these counters;
its histogram is unavailable, not zero. Do not infer a baseline histogram or
FUSE request sizes from application pread sizes. The separate normal-advice
profiles have a different request-size distribution; this 97% figure must not
be used to explain those profiles.

## Matched flame-graph findings

Separate 4,000 Hz, DWARF-16,384 profiles did not show a consistent scheduling
benefit. Cold context switches changed from 108,196 to 110,920; warm from
106,123 to 108,102. Overall io-wq enqueue samples decreased, but source splice,
filemap and scheduler subtrees did not move consistently in both workloads.
The final reply moved toward writev and away from splice, with added source
PREAD/copy work. None of these overlapping subtrees is an additive CPU saving.

The ordinary PREAD source stack can run on a reactor's submitting thread;
the matched profiles did not attribute their `io_read` stacks to io-wq.
The already-PREAD 4 KiB path still submits source I/O, receives its completion,
then issues the final writev. The threshold trial leaves that dominant
random-advice path unchanged. A future proposal must account for this path,
not treat all cached reads as forced source-splice worker handoffs.

All four matched profiles recorded 8,192 operations and zero reported lost
samples. Their folded-period coverage differs from whole-process task-clock,
so flame percentages or scaled subtrees are not exact cross-run CPU times.
Use the unsampled table above for the acceptance decision. The warm baseline
needed the narrowly scoped one-time perf reattach after a transient io-wq TID
disappeared; its original failure, successful reattach and enable/disable
control acknowledgements are retained. Another reattach elsewhere in the
artifact belongs to the separate SQPOLL profile, not this matched comparison.

## Reproducible evidence

Artifact `random-read-benchmark-and-flamegraphs`, ID `10086712272`, contains:

- `benchmarks/github-io-engine-affinity-cache-{cold,warm}/samples.csv`: all
  raw daemon CPU and wall samples.
- Each sample's `client-cpu.csv`, `ngs3fs-threads-after-workload.txt`, and
  `ngs3fs.log`: total client CPU, RSS/HWM and actual source-size counters.
- `benchmarks/random-read-report/owner-complete-comparison.{md,html}`:
  matched medians, provenance and source-size evidence.
- `profiles/github-io-engine-affinity-cache-{cold,warm}-{baseline,current}`:
  separate readable 4,000 Hz profiles for hotspot analysis.

No raw `perf.data` or `perf.script` belongs in the artifact. The rollback's
next CI compares against `7199db2`, so its labels and recorded SHAs must clearly
distinguish the rejected experiment from the restored default policy.

This rejection does not mean the four-reactor CPU target is complete. On this
run's separate same-workload cached engine comparison, four reactors still
used materially more daemon CPU than one reactor. Further changes need
evidence from that dominant path, not another small-copy threshold trial.
