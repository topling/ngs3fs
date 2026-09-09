# Cached FUSE reply `SPLICE_F_MOVE` experiment

## Decision

Keep the no-MOVE cached-reply path introduced by `4efbe1d`. For a
multi-worker cached read, `AsyncCachedReadTask::reply_pinned()` still asks the
owner reactor to send the cache-file range with the existing libfuse
file-descriptor reply API, but passes no `FUSE_BUF_SPLICE_MOVE` flag. Legacy
and one-reactor replies, uncached replies, and other `fuse_reply_data()` callers
are unchanged.

This is a narrow retained improvement, not completion of the broader reactor
CPU goal. In these same runs, four-reactor cached daemon CPU remained higher
than one-reactor daemon CPU. The decision only says that, within the
four-reactor owner-ring cached-reply implementation, omitting MOVE was better
than requesting it in two matched trials.

## Matched results

Both GitHub Actions trials ran three alternating baseline/current repetitions
on one four-vCPU host per trial. Each repetition performed 8,192 operations
(4,096 `pread` and 4,096 `mmap`) against 32 files with 16 client threads and
eight connections. The baseline revision was `931674a` (cached source SPLICE
with MOVE); the current binaries contained the one-line no-MOVE runtime change.
The current checkout revisions were `ad7107a` and `76708ef`, but both artifacts
record the same current binary SHA-256; their later differences were profiling
harness changes. Cold runs issued 63 or 64 S3 GETs and warm runs issued none.
Daemon CPU comes from `/proc/<pid>/stat`; daemon CPU/op divides it by 8,192.
Total CPU/op adds the measured workload-process CPU. RSS is the post-workload
`/proc/<pid>/status` `VmRSS` median. Negative deltas favor no-MOVE.

| Run and host | Cache | Daemon CPU, MOVE -> no-MOVE | Daemon CPU/op, MOVE -> no-MOVE | Total CPU/op, MOVE -> no-MOVE | Wall, MOVE -> no-MOVE | RSS, MOVE -> no-MOVE |
|---|---|---:|---:|---:|---:|---:|
| [34288614586](https://github.com/topling/ngs3fs/actions/runs/34288614586), Intel Xeon Platinum 8573C | cold | 2.30 -> 2.18 s (-5.22%) | 280,761 -> 266,113 ns (-5.22%) | 592,430 -> 573,461 ns (-3.20%) | 1.886 -> 1.823 s (-3.34%) | 10,016 -> 10,028 KiB (+0.12%) |
| [34288614586](https://github.com/topling/ngs3fs/actions/runs/34288614586), Intel Xeon Platinum 8573C | warm | 2.39 -> 2.30 s (-3.77%) | 291,748 -> 280,761 ns (-3.77%) | 601,563 -> 590,288 ns (-1.87%) | 2.096 -> 2.142 s (+2.19%) | 9,776 -> 9,756 KiB (-0.20%) |
| [34292327617](https://github.com/topling/ngs3fs/actions/runs/34292327617), AMD EPYC 7763 | cold | 3.97 -> 3.84 s (-3.27%) | 484,619 -> 468,750 ns (-3.27%) | 971,071 -> 952,240 ns (-1.94%) | 3.030 -> 2.987 s (-1.43%) | 9,936 -> 9,880 KiB (-0.56%) |
| [34292327617](https://github.com/topling/ngs3fs/actions/runs/34292327617), AMD EPYC 7763 | warm | 4.32 -> 3.94 s (-8.80%) | 527,343 -> 480,957 ns (-8.80%) | 1,025,317 -> 968,105 ns (-5.58%) | 3.263 -> 3.018 s (-7.50%) | 9,588 -> 9,612 KiB (+0.25%) |

The first warm wall median moved in the wrong direction by 2.19% even though
daemon CPU fell; its individual wall samples were especially variable. The
second run repeated the CPU reduction and also reduced the warm wall median.
RSS changes were tens of KiB and changed sign, so these measurements do not
show a userspace-memory penalty or saving.

## Mechanism and scope

The source evidence is deliberately narrower than the performance claim:

- `AsyncCachedReadTask::reply_pinned()` passes flags `0` only through the
  `reactor->is_multi_worker()` cached-read branches. The existing libfuse
  selection still chooses PREAD for small/unsupported replies and SPLICE for
  large negotiated replies; that selection does not depend on MOVE.
- In the patched libfuse `fuse_reply_fd_async()`, `SPLICE_F_MOVE` is added to
  the final pipe-to-FUSE splice only when both `FUSE_BUF_SPLICE_MOVE` and the
  negotiated capability are present. With flags `0`, a large cached reply
  therefore remains owner-ring file-to-pipe SPLICE followed by direct
  pipe-to-FUSE splice, but requests a copy rather than pipe-page transfer into
  FUSE.
- The focused large-reply unit negotiates both splice-write and splice-move,
  observes SPLICE mode, and asserts that the final splice flags are zero. The
  assertion therefore distinguishes no-MOVE from an unsupported-splice or
  PREAD fallback.
- In run `34288614586`'s matched warm folded profiles, the baseline contains
  the final-splice `fuse_try_move_folio` subtree while the no-MOVE profile does
  not. This corroborates that the intended kernel branch changed; it is not
  used as the headline CPU measurement.

Avoiding page transfer may leave both the cache-file page and the destination
FUSE folio resident for a time. The RSS census measures only the userspace
daemon and does not measure this possible duplicate kernel page-cache
footprint. The experiment also does not establish that four reactors now use
less CPU than one reactor; in these two runs, cached four-reactor daemon CPU
was still about 23--42% higher depending on cache state and host.

## Reproducible evidence

The downloaded artifact roots are:

- `build/artifacts/ci-34288614586/benchmark`
- `build/artifacts/ci-34292327617/benchmark`

For each run, the medians above come from
`benchmarks/github-io-engine-affinity-cache-{cold,warm}/summary.csv`; the
alternating raw observations are in the adjacent `samples.csv`; host,
revisions, binary hashes, and variant descriptions are in `system.txt`; and
workload/total CPU is in each repetition's `client-cpu.csv`. The RSS
observations are in
`{baseline,current}-4-r*/ngs3fs-threads-after-workload.txt`.

The relevant benchmark stages completed in both runs. Run `34292327617` was
not an all-green workflow because an independent `perf record` process exited
during a profile stage. That recorder failure removes that particular sampled
profile; it does not invalidate the already-written unsampled three-repetition
CPU, wall, request-count, or RSS files used here. Do not cite the run as an
all-green CI result.
