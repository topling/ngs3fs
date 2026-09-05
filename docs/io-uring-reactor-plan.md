# io_uring reactor execution contract

Status: approved for classic-fusefd A+Patch execution. Caller-owned reactors
use libfuse for protocol semantics while preserving fd-backed splice replies;
FUSE-over-io-uring remains excluded until its payload UAPI avoids the extra
copy.

## Approved revision: explicit continuations, no fibers (2026-09-05)

This revision supersedes the fiber-based implementation described in the
historical implementation sections below. It does not change filesystem,
checksum, caching, protocol-selection, or close-to-open semantics.

### Approved follow-up: bounded transient prefetch storage (2026-09-05)

Execution clarification (budget integration): explicit per-file limits are caps,
not clamped to object length; the automatic default alone uses min(file size,
two windows). Speculative windows shrink under pressure.

**Latest user correction:** when read verification is enabled, proactively
STORE only a checksum unit that has passed verification. Do not STORE an
unverified unit early. Normal READ replies may still complete early as already
approved. An unpublished checksum unit can reuse its memfd for the one retry
after active source replies drain; pending READs wait on retry state. This
supersedes the previous dual-generation staging/reservation design and the
previous instruction that checksum work must not delay proactive STORE.
Invalidation of already returned READ pages remains asynchronous, and retry
does not wait for that invalidation. Publication after retry waits for both
successful verification and invalidation. A partial range without a verifiable
checksum unit is not proactively STOREd; do not misrepresent the existing
uncached full-object check as multipart-manifest verification.

Published prefixes can be punched while the download continues. Admission
pins conservatively protect a whole source window; any active READ delays a
prefix punch. Checksum source ownership also delays punching. STORE completion
advances the reclaimable prefix; a source-generation swap resets that prefix.
Legacy publication runs on a separate bounded worker queue, never on the
download/checksum workers. Tests must distinguish a server-send barrier from
client checksum/publication completion. These are implementation tactics within
the approved memory/early-reply requirements; review: approved with focused
low-budget, retry, prefix-retirement, and shutdown gates still required.

This follow-up supersedes the retained-memfd prefetch cache and the earlier
"no new mount options" restriction only for the two limits below. memfd is
download/transfer staging, not a second completed-data cache. Already requested
bytes still reply as soon as available. Without read verification, progressively
publish complete prefetched pages during download. With verification, publish
only completed, verified checksum units. Retire each published
staging interval only once dependent reads, checksum work, and actual
notification I/O have finished.
"Publish" is a logical handoff, not a guarantee of physical page stealing.
Failed publication must be reported and must not leave an unbounded retained
memfd cache; the remote object remains the source for a later cache miss.

- `--max-prefetch-memory SIZE` limits the entire mount. Default: 10% of physical
  RAM, not 10% of currently free RAM. All allocated staging, including windows
  waiting for a connection and completed windows awaiting publication or last
  consumers, remains charged. Idle pool entries must not retain payload pages.
- `--max-file-prefetch-memory SIZE` adds a per-file limit, shared across that
  inode's open handles. Default: `min(file_size, 2 * maximum_prefetch_window)`.
  Both limits apply simultaneously. Page-granularity allocation and final-page
  rounding must not create an unaccounted allocation loophole.
- Admit memory before allocation. Shrink speculative windows to available
  allowance; if even demand cannot fit, await budget without blocking a reactor.
  Do not discard useful in-flight data solely because access is out of order.
  Budget exhaustion prints a rate-limited stderr warning using `fprintf`.
- Reserve one maximum-demand-sized allowance inside each applicable limit for
  demand-only progress. Speculative/STORE-retained windows cannot consume it.
  This is a floor protected from speculation, not a ceiling on demand. Demand
  may also use all otherwise idle capacity, under the same global/per-file caps.
  A demand-only window replies and retires without STORE. This also lets a READ
  crossing existing window boundaries finish while STORE waits on its folios.
  Explicit limits must accommodate the negotiated maximum READ and page
  rounding; smaller files use their rounded file size. The reservation is not
  extra memory beyond the configured cap.
- Publish outside the reactor's blocking path, preserving stable inode/handle
  identity. Pending READ replies must be able to run while STORE waits on their
  locked pages. Never wait for STORE while withholding such a READ reply.
- Batch currently available whole pages, targeting 256 KiB per STORE without
  waiting for further network input to reach that target. Keep at most one
  STORE in flight per download; its completion advances the next batch from
  the then-current receive progress. EOF may publish the exact final partial
  page, never bytes beyond the object's published size.
- Overlapping in-flight READs pin their source interval atomically with range
  selection. Keep STORE-pending intervals selectable by READ, including READs
  from other handles of the same inode/generation. Never make READ completion
  depend on STORE. Removing an interval from selection precedes hole punching;
  existing READ and checksum pins delay physical reclamation. A post-retirement
  kernel cache miss may fetch again, but cannot read a sparse zero-filled hole.
- Already delivered READ data and STORE data must belong to the same immutable
  generation. A checksum retry invalidates earlier READ data, drains only active
  READ source replies, and reuses its unpublished staging. Retry must not wait
  for invalidation: it can be needed to complete the READ blocking invalidation.
  Verified retry data can satisfy waiting READs; STORE waits for both retry
  verification and invalidation. Successful READs are not retroactively failed.
  Checksum work delays proactive STORE and reclamation, not an available READ.
- Preserve asynchronous checksum verification/retry and generation isolation.
  Do not publish stale or poisoned bytes after an invalidation or replace newer
  data with an older generation. Cancellation retires original I/O ownership
  before freeing storage. Shutdown wakes budget waiters and drains publishers.
- STORE must neither hold session/inode locks needed by READ callbacks nor
  occupy every worker needed by READ/checksum prerequisites. Generation changes
  close old-generation publication admission, asynchronously drain its STOREs,
  then invalidate, rather than checking an epoch only before a blocking STORE.
- Keep memfd plus splice. The mmap/vmsplice comparison and mapped-RECV experiment
  are deferred at the user's request. Implement and test budget accounting as
  an isolated first gate before integrating memory admission and publication;
  do not advertise mount options as effective before that integration passes.

| Finding / decision | Contract change | Re-review |
|---|---|---|
| Idle-only pool cap misses live windows | Account mount-wide before allocation through final release | Required |
| Completed memfds duplicate retained data | Publish into kernel cache and retire transient staging | Required |
| User requires per-file budget | Shared per-inode accounting with min(size, 2 * max window) default | Required |
| STORE waited for the complete download | Accepted: publish available pages incrementally, bound in-flight STOREs | Required |
| STORE overlaps locked in-flight READ pages | Accepted: independent READ replies, atomic source pins and interval retirement, demand progress reserve | Required |
| Waiting for invalidation before starting checksum retry creates a READ/INVAL cycle | Latest user correction: no unverified STORE, drain active source replies, reuse staging, and a two-condition fence before publication | Required |

Verification must cover CLI defaults/validation; aggregate and per-file peak
charges including multiple handles; early demand replies and prefetched-tail
cache hits after memfd retirement; overlap and disjoint concurrent reads; tiny
files/final pages; low-budget contention without reactor starvation; checksum
retry; rename/forget/close/unmount while publication is pending. Then run the
existing full suite and compare CPU/latency plus memory evidence. Do not claim
implementation or completion until these gates pass.

The incremental-publication review additionally requires a paused-tail test
that observes STORE before GET completion; a blocked STORE plus overlapping
READ test that completes the READ first; cross-handle and cross-batch-boundary
READs; a READ racing with source reclamation; checksum retry while early READs
are in flight (with STORE withheld); and old-generation STORE completion
preceding invalidation. These are
correctness gates, not benchmark-only scenarios. Review decision: executable
with these gates; re-review implementation ordering before enabling publication.

#### Current follow-up execution evidence

- Cross-client namespace visibility is **best effort**, not a strong guarantee.
  Tests that directly mutate the server backing directory must not require a
  LIST already in flight to observe a later external change. Keep same-mount
  mutation races in scope: a stale LIST must not undo a completed local write,
  unlink, or rename, or replace the stable source inode during rename.
- Follow-up CI `33962137323` for `d1de34a`: benchmark, arm64, clang and
  ASan/UBSan succeeded; build's 73 CTest cases passed but libfuse uring stress
  failed, and TSan failed. Repairs are in progress, not yet a green CI claim.
  MSG_RING task publication now has an explicit release/acquire handoff;
  directory LIST uses a local-mutation epoch. Rename fences LIST publication
  during its remote/local transition using per-directory state, without a
  global lock or waiting recursively on the mutation lock. Concurrent readers
  may use the pre-mutation cache; its expiry stays zero until a later refresh.
- External fixture setup is isolated per stress worker; mount-originated
  namespace operations still share a directory. Local uring-1 libfuse stress
  after the fixture and mkdir repair passed 8 workers x 20 iterations plus
  20 syscall rounds (94 seconds). Additional rename/LIST identity stress is
  being added; it is not covered by that earlier passing run.
- Subsequent uring-4 VersityGW stress passed, including 8 workers x 20
  rename-overwrite/LIST interleavings with O_PATH inode pins, followed by the
  upstream syscall rounds (59 seconds). Full normal suite initially passed
  72/74; both failures exposed a mock LIST bug that hid unrelated objects
  when the main object moved under the private prefix. That fixture is fixed.
- Rebuilt TSan full suite passed 72/74 with no reported data races. Remaining
  failures were legacy prefetch GET accounting and OSS write/read cache
  retention. The mock now tracks active data GETs individually (not LIST GETs)
  and retires them on connection destruction. A focused rerun no longer had
  the leaked-active-GET timeout, but exposed an exact sequential-window-count
  assertion. Ordered-window accounting now disables independent kernel
  readahead and issues page-sized application reads; it does not change the
  production access-pattern policy.
- OSS retention exposed a real inode-size bug: accepted writes updated the
  handle but not the inode. A concurrent GETATTR could restore the old size,
  preventing the partial EOF page from remaining uptodate. Write admission
  now protects local size from LIST until commit, every accepted write
  publishes its size, and failed open restores the previous state. No mutex or
  extra fields were added to InodeFile. Both the OSS and legacy prefetch TSan
  cases subsequently passed ten consecutive runs each.
- The rename/checksum regression now downloads half a checksum unit, renames,
  then completes the unit and verifies that retry uses the new identity.
  This replaces a timing assumption about which wins the identity lock.
  Both cached engine variants passed three consecutive runs each. Final normal
  suite: 74/74 (73.03 seconds); final TSan suite: 74/74 (135.86 seconds), with
  no reported races. ASan also passed 74/74 (84.72 seconds). The follow-up
  runner CI and performance comparison remain pending.
- Runner `33970172874` passed all hardening jobs, but its legacy libfuse
  stress exposed a second directory-refresh race, reproduced locally:
  mutation callbacks bypassed the per-directory `refreshing` coordinator,
  allowing two LIST generations to prune each other's children. They now use
  the same refresh entry point as LOOKUP/READDIR. Legacy mkdir also accepts
  its own marker already installed by a concurrent LIST after the successful
  conditional PUT, as the uring path already does. Legacy 8 x 20 stress,
  including 160 rename/LIST overwrites and 20 syscall rounds, passed in 60 s.
  Normal/TSan/ASan suites passed 74/74 each (73.39/142.68/86.65 s).
- The same runner's multipart recovery found that the newly added parent
  directory lock dereferenced the recovery task's private, parentless inode.
  Metadata publication now takes that lock only for tree-attached inodes.
  ASan reproduced the null-parent access before the fix. Normal and ASan
  64 MiB crash recovery passed afterward; the benchmark now also requires
  dirty-marker retirement, a mounted content read, and a live daemon rather
  than treating remote Complete alone as recovery success.
- The new runner performance evidence is not an improvement: normal advice
  legacy/uring-1 CPU per operation is 1.050/1.094 ms, wall 1390.920/1686.041 ms;
  random advice 1.138/1.216 ms, wall 1441.670/1683.597 ms. The uring-1 profile
  attributes 38.524% inclusive cost to io_splice and 12.030% to
  shmem_undo_range; HTTP parsing is under 1%. Continue investigating staging
  allocation/reclamation rather than moving header parsing back to workers.
- Terminal publication now releases completed staging directly instead of
  punching its last prefix immediately before the final truncate. Earlier
  prefixes still retire incrementally, and active READ/checksum pins still
  prevent reclamation. The new suite passes this change; CPU benefit remains
  unmeasured until the follow-up runner benchmark.
- Runner `33970172874` still shows no material improvement: normal advice
  legacy/uring-1 CPU per operation 1.064/1.108 ms and wall 1444.990/1758.348 ms;
  random advice 1.167/1.235 ms and wall 1544.647/1817.660 ms. The uring-1
  profile has shmem_file_write_iter 35.861%, shmem_undo_range 13.027%, and
  llhttp 0.668% inclusive. These costs overlap and must not be added.
  Investigation found an admission asymmetry: legacy uses try_acquire_bulk
  before speculative expansion and falls back to demand-sized I/O if it fails;
  uring currently allocates expanded staging then waits for an ordinary lease.
  Align speculative admission without changing checksum/STORE guarantees or
  discarding useful in-flight windows; validate with the same runner workload.
- Correctness follow-up `c179624` passed the entire runner CI `33971426554`,
  including all hardening jobs, upstream filesystem stress, multipart recovery,
  benchmarks, and Pages deployment.
- Speculative admission now reserves an actual bulk HTTP lease before staging
  expansion; no lease means demand-sized staging, not a queued expanded GET.
  The lease follows the request through credentials and reactor submission.
  Retries keep bulk eligibility. Separate bulk/demand notification pipes avoid
  a bulk waiter consuming the reserved-slot wakeup intended for demand.
  Seven paused speculative GETs plus an eighth demand-only GET are tested in
  both engines, including multi-reactor operation. Both passed three repeats.
  Normal and TSan full suites passed 76/76 before the notification split;
  final ASan suite passed 76/76 (81.35 s). Final TSan admission/budget pressure
  passed all four tests three times each (61.44 s). The performance runner
  remains pending. No CPU win is claimed yet.
- Runner `33972853791` passed every job, but admission alone is not sufficient:
  normal advice legacy/uring-1 CPU per operation 1.074/1.074 ms and wall
  1437.936/1686.323 ms; random advice 1.191/1.450 ms and 1512.045/2222.225 ms.
  Further inspection found demand staging incorrectly capped at the protected
  reserve even when the rest of the budget was idle. Demand now borrows idle
  capacity while speculation still cannot consume the protected minimum.
  A new deterministic test fails on the old code and passes after the fix;
  mixed demand/speculation stress checks both total and per-inode caps.
  Normal/TSan/ASan full suites pass 76/76 (82.10/129.54/90.35 s). Local normal
  advice medians before/after are uring-1 CPU/op 1.138/1.045 ms, wall
  1404.563/1390.237 ms; uring-4 1.973/1.470 ms, wall 1144.694/1020.419 ms.
  These local measurements do not establish runner parity: legacy still has
  lower wall time. Continue the runner comparison and hotspot analysis.

- Mount and per-file budget admission now cover every production prefetch
  allocation. Limits are exposed and validated; explicit per-file limits are
  not clamped to object size. Exhaustion shrinks speculation or waits on an
  eventfd through the reactor, with a rate-limited stderr warning.
- Successful STORE completion permits page-aligned prefix hole punching while
  the rest of the download continues. READ pins and checksum ownership prevent
  premature reclamation. Legacy publication uses a dedicated worker.
- Verification-enabled publication waits for verification. Partial ranges
  without a checksum unit and responses without a usable checksum are not
  proactively published. Retry drains active READ source replies and reuses
  the original staging; no second staging reservation is required.
- Focused verification after the checksum-policy correction:
  `ctest --test-dir build -R prefetch --output-on-failure -j1`: 17/17 passed
  (19.19 seconds), including legacy/uring low-budget contention, progressive
  publication, checksum-gated retry, and option validation.
- Review found legacy overwrite OPEN lacked the existing uring publication
  fence. It now advances the generation and drains old STORE before exposing
  truncation. Full suite after that repair: 70/70 passed (77.31 seconds).
- Three added gates passed: legacy/uring shutdown during a paused download
  after prefix publication, and successful first-pass checksum publication.
  Eight publication/retry/shutdown variants repeated ten times: 80/80 passed
  (67.59 seconds). This tests graceful unmount with active download; it does
  not prove every forced kernel-folio/rename/forget interleaving.
- ASan/UBSan rebuilt from the current sources: 25/25 focused tests passed
  (37.07 seconds), covering budget contention, shutdown, retry, publication,
  reactor ownership, cached integration, and HTTP/1 bodyless connection reuse.
- Verdict: partial. Performance/runner evidence and precise forced
  old-STORE/rename/forget ordering evidence remain pending. Continue execution;
  none of the historical performance comparisons is a new improvement claim.

#### Historical follow-up evidence (superseded intermediate implementation)

The following records earlier increments, not current behavior or remaining
implementation work. In particular, dual retry staging and completed-window-only
reclamation have been superseded above; historical passing counts do not verify
the latest checksum policy.

| Gate | Evidence | Status |
|---|---|---|
| Budget accounting core | `PrefetchBudget` RAII reservations, per-inode counters, separately reserved demand allowance within both caps, page-aligned admission, peak counters | Implemented; storage can own a reservation, production allocation admission remains pending |
| Storage charge lifetime | Charged pool allocation rejects undersized reservations; moves preserve charges; final destruction unmaps/closes before returning capacity; pooling truncates before returning capacity, with callbacks outside the pool lock | Core suite repeated 20 times successfully; tests retain separate READ/STORE owners and reenter the pool from a budget wakeup |
| Retry capacity transfer | Page-aligned reservation splitting transfers already admitted capacity without release, wakeup, or competition with other allocators | Unit-tested with two live storage generations under a fully occupied speculative allowance; not yet used by production retry admission |
| Budget defaults | Physical-memory 10% rounded down to pages; per-file min(size, 2 * window), final page rounded up, explicit limits required page aligned | Unit-tested; mount options not yet exposed |
| Concurrency / lifecycle | Shared inode charges, global exhaustion, smaller windows, moved/destroyed leases, cancellation, release-before-subscribe, shutdown wake, 8 threads x 1000 reservations | Core suite repeated 20 times before final default/shutdown additions; final full suite passes |
| Actual STORE completion | Owner-only fd-backed `notify_store`; terminal callback captured for both WRITE and SPLICE, best-effort STORE failure distinct from fatal transport replies | Connected to uring prefetch progress; one in-flight batch per window |
| Notification content | Nonzero source and destination offsets, full header/payload validation, non-inline and reentrant completion | Reactor suite repeated 30 times |
| Incremental publication | Pause the S3 body halfway; `mincore` observes a never-read page resident while GET is still pending and kernel read-ahead is disabled | Single- and two-reactor mounted tests pass |
| Overlapping READ | Eight concurrent reads cross the received/pending boundary through two handles; one GET supplies all requests | Shared reader per inode/generation; no READ waits for STORE completion |
| Repeated interleavings | `ctest --test-dir build -R prefetch_store --repeat until-fail:10 --output-on-failure --timeout 30` | Four mounted variants x 10 runs: all 40 pass, 54.55 seconds |
| Source retirement | Range matching acquires a READ pin under the same mutex as withdrawal. On completed handoff, `/proc/PID/fd` shows memfd length and blocks both zero; a later read gets correct kernel-cached bytes without another GET | Mounted tests pass, including successful checksum retry |
| Descriptor-only reuse | Idle pool truncates/unmaps retired storage and keeps at most 16 empty descriptors; pipe references preserve old data across fd reuse | Unit test and eight-thread pool stress pass |
| Checksum retry ordering | Corrupt a proactively stored page. Retry downloads into separately owned staging while old STORE/invalidation drains. Republish only after both successful verification and invalidation | Single- and two-reactor retry tests pass; fixed mock GET checksum to describe original rather than corrupted data |
| Generation / overwrite fence | Epoch checks close old publication admission. Invalidation waits asynchronously for admitted STORE. Overwrite OPEN drains old STORE before exposing O_TRUNC; ordinary READ replies bypass this fence | Existing generation and write-after-read regressions pass; precise forced old-STORE/rename/forget timing still needs dedicated injection |
| Regression found and repaired | An extra write-open INVAL expired attributes and broke write-then-read cache retention. The write-open fence now only drains STORE; kernel O_TRUNC performs cache removal | Both uncached and cached retention restored, with unchanged test assertions |
| Existing correctness | `cmake --build build -j 4`; `ctest --test-dir build --output-on-failure -j 1` | Final 59/59 pass, 62.70 seconds |
| Sanitizers | `ctest --test-dir build/asan -R 'prefetch_store\|ngs3fs_core_tests\|reactor_io_test\|fuse_multi_reactor_cached_integration_test' --output-on-failure --timeout 60` | Final ASan/UBSan 7/7 pass, 14.53 seconds |
| Remaining integration | Mount parameters and admission (including retry staging), low-budget progress tests, legacy/TLS progressive publication, reclamation of completed subregions while the rest of a window is still downloading, profiling and runner evidence | Pending; the mount does not yet enforce the new memory budgets |

The current reclamation granularity is a completed window: no payload remains
after download/checksum/publication completion and the last READ source pin.
Publication itself is incremental. Reclaiming prefixes of a still-downloading
window remains a separate gate, not an implemented claim. The publication fence
currently excludes other publications mount-wide during invalidation, but does
not lock READ dispatch/replies or add a mutex to InodeFile. Measure this boundary
before deciding whether narrower fencing is worthwhile.

The charge-lifetime increment does not enable mount limits by itself. Reserving
both checksum generations in advance is possible through reservation splitting,
but is not a complete small-limit admission policy: a per-file cap equal to its
rounded size cannot accommodate two full-file copies. Do not silently exceed
that cap or wait for a STORE whose blocked READ needs the retry. Demand-only
and small-file retry progress still require integration and mounted tests.

Verification verdict for the whole bounded-prefetch follow-up: **partial**.

Charge-lifetime increment verification: build succeeds; the core suite passes
20 consecutive runs; the final full suite passes 59/59 (63.87 seconds); the
seven focused ASan/UBSan tests pass 7/7 (16.81 seconds). Final mounted
STORE/overlap/retry repetitions pass 40/40 (60.00 seconds). An earlier full run
failed the random-extent GET-count assertion in the two-reactor retry test.
Inspection found its barrier waited only for server sends, not asynchronous
client checksum/publication completion. The exact-count test now also waits
for uring staging retirement, leaving early READ and overlap assertions intact.
The old test passed 40 isolated repetitions, so that failure's precise schedule
was not reproduced; retain range diagnostics rather than claim conclusive
fault attribution. Mount admission, tiny-budget retry progress, incremental
reclamation, legacy publication, and performance/runner gates remain pending.

Continue execution for the remaining integration and evidence gates. No
mmap/vmsplice comparison, new performance claim, commit, push, or runner run was
performed for this increment. Existing unrelated pending CI/HTTP changes remain
in the working tree; this evidence concerns the current combined local build.

### Goal and boundaries

Replace stackful fibers with small operation-specific state machines. An I/O
completion advances its operation on the owning reactor without a synchronous
worker rendezvous or a stack switch. Preserve legacy as the startup-time
fallback, TLS as the existing compatibility boundary, and bounded background
execution for local filesystem writes and checksum computation. Do not add
coroutines, another scheduler framework, a giant filesystem-wide switch, new
mount options, or provider/kernel changes to implement this revision.

This is active development: old source interfaces and on-disk cache formats
are not compatibility commitments. Refactor or remove old APIs directly; do
not add data migrations, legacy-format readers, or permanent adapter layers.
The separately requested legacy I/O engine remains a supported runtime mode,
not a requirement to preserve its previous internal implementation.

The allowed changes remain the source/build/test/documentation and benchmark
scope above. Transitional synchronous interfaces may remain while callers are
migrated, but an intermediate implementation using them for the cleartext hot
path is not completion of this contract.

### Request and I/O ownership

- `AsyncIoRequest` owns stable operation parameters and a completion callback.
  Submitting it is nonblocking. Rejection returns immediately without invoking
  the callback; acceptance produces exactly one terminal callback on its owner
  reactor, never inline in the submit call.
- Short transfers continue from the recorded offset. Header parsing runs in
  the reactor's receive completion. Local-file writes retain `IOSQE_ASYNC` so
  filesystem work cannot unexpectedly run inside ring submission.
- Deadlines use the existing monotonic nearest-deadline loop. Cancellation is
  only a request to stop: the original I/O CQE must be retired before releasing
  its buffers, fd ownership, or request storage. The cancel CQE alone is not
  proof that the original I/O no longer references memory.
- Fixed-size internal I/O/control objects use reactor-local free lists. A
  one-reactor request does not need atomic refcounts or queues solely because
  the implementation supports multiple reactors; genuinely cross-thread
  ownership still requires synchronization.
- FUSE operation objects copy callback-scoped names and file-info values,
  retain fd-backed input ownership when needed, and own inode/handle/cache
  references until their final dependent operation completes.
- A satisfied read replies once and separates its FUSE lifetime from an
  ongoing prefetch/checksum lifetime. Later failure affects only operations
  that have not already succeeded, following existing checksum policy.
- Connection, pending-page, metadata refresh, and handle-completion waits
  register continuations and return to the event loop. No reactor blocks on a
  condition variable, an eventfd read, or a contended filesystem lock.
- Release closes admission, then retires the handle after its final request;
  it cannot delete a handle merely because an observed count became zero
  while another completion still accesses that handle.

### Ordered implementation tasks

1. Add the callback-driven asynchronous I/O interface and tests of short I/O,
   timeout, rejection, cancellation, and exactly-once completion. Keep the
   synchronous interface available only for legacy/transitional callers.
2. Add HTTP/1 and HTTP/2 continuation state machines sharing existing parsers
   and protocol validation. Cover send, receive headers, body progress, body
   completion, connection reuse, and failed/canceled connection disposal.
3. Migrate FUSE data paths and their retained input/reply lifetimes. Preserve
   early read replies, in-flight prefetch reuse, asynchronous checksum policy,
   sequential writes, first-flush seal, and background local-file writes.
4. Migrate remaining metadata/pool/cache/handle waiting points; remove fiber
   stacks, context assembly, fiber-local state, and their build dependencies.
5. Review/fix/test all modes, then collect repeated A/B and kernel-inclusive
   flamegraphs. Record executable identity, cache-drop outcome, CPU, wall time,
   latency, S3 request counts, syscall/context-switch evidence, and io-wq load.
   Commit/push and run the approved CI matrix only after local correctness
   gates; retain readable evidence artifacts, not raw perf.data.

### Verification and stop conditions

| Requirement | Required evidence | Missing evidence |
|---|---|---|
| No fibers or hidden synchronous hot-path handoff | Source audit plus reactor/worker call stacks | Incomplete |
| Stable asynchronous ownership | Focused short-I/O/timeout/cancel/shutdown tests and sanitizer where supported | Block completion |
| Preserved behavior | Existing full CTest plus legacy/one/multi-reactor cache and uncached integrations | Block completion |
| Early reply and background progress | Prefetch, overlapping reads, close while I/O is pending tests | Block completion |
| No CPU/latency regression | Repeated comparable A/B samples and kernel-inclusive flamegraphs | Continue analysis/fix |
| Reproducible evidence | Actual executable paths/hashes, event/frequency, workload and cache state | Discard unsupported comparison |

Stop and seek a scope decision if this requires an extra payload copy, changes
approved filesystem behavior, or needs a new external dependency/kernel patch.
Do not present temporary fallback workers, unconverted waits, or unmeasured
performance as a completed fiber removal. A failed test is investigated and
fixed without weakening its assertion.

### Revision execution evidence

| Task | Evidence | Result |
|---|---|---|
| Existing-lifetime repair before migration | Build and existing uring, legacy, multi-reactor and multi-reactor cached integrations | 4/4 pass |
| Explicit asynchronous I/O | Reactor-local request pools, original/cancel CQE retirement, short-I/O/deadline tests, shutdown continuation overflow tests | Focused tests pass; final suite pending |
| HTTP and FUSE migration | H1/H2 continuation parsers, asynchronous read/write/metadata operations; fiber stacks/context switching removed | Implemented; final integration gate pending |
| First full correctness run | 49/55 passed; a serial rerun passed four of the six failures, leaving the two cached integrations failing page-cache retention | Not a passing full-suite result |
| Follow-up fixes | Actual background NOTIFY_STORE completion before flush; nonblocking cache retirement; background cache initialization; credential-refresh and eviction-lock fixes; checksum/rename regression cases | Full 55/55 pass on the final 2026-09-05 local rebuild (53.03 seconds) |
| Notification and owner-thread regression | Real custom-I/O notification completion, blocked notification versus unrelated READ, reentrant notifications, initialize/run on different threads, disabled-destination exclusion, callback/CQE fairness | Focused test passed 30 consecutive runs |
| Local-cache metadata stages | Marker creation/cleanup, discard, remove, rename and eligibility queries run in background continuations; retirement busy exits the worker instead of waiting there; normal cache open needs one worker round-trip | Included in the final 55/55 passing build |
| Shutdown review | Early owner failure wakes peers; every owner waits for cross-ring admission to close; rejected dispatch consumes neither destination admission nor source SQE | Focused test passed 30 consecutive runs; full suite passes |
| ASan + UBSan | Reactor I/O, cache, asynchronous HTTP, credential provider | 4/4 pass; fixed missing helper-target build dependency, without changing test timeouts |
| Preliminary performance evidence | Local normal-advice legacy/one/four-reactor previews and kernel-inclusive four-reactor flamegraph | Diagnostic only; not the performance gate |

The first cached-retention failure exposed a notification completion bug:
`fuse_lowlevel_notify_store` returned after queuing a deferred notification,
so flush could overtake the actual STORE. Notifications issued from existing
background threads now complete their local fusefd write there before returning;
this adds no worker handoff and does not make reactor callbacks block. Ordinary
FUSE request transport, HTTP parsing and socket I/O remain reactor-driven.

Local profiling evidence is under
`build/profiles/states-preview-uring4-20260905`. Its 4000 Hz `cpu-clock` stacks
include kernel and io-wq execution and show substantial splice/scheduling costs;
the capture reported lost events, so its percentages are diagnostic estimates.
The preliminary four-reactor wall times varied markedly. Old blocked FUSE tasks
also caused a global `drop_caches` attempt to stall before the repeated benchmark
could run. Do not claim a successful cache drop, a completed A/B comparison, or
a no-regression result from these previews. Repeat the final matrix on the
GitHub runner and retain readable stacks, counters, identities and tables.

The next scheduling experiment enables each pre-created ring on its actual
owner using `R_DISABLED | SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN`.
Multi-reactor mounts therefore use the same single-owner kernel optimization
as one-reactor mounts. Startup does not dispatch to disabled rings. Busy
callback iterations explicitly reap deferred work without sleeping, and
cross-owner dispatch admission is synchronized with shutdown draining.
This is not a claimed performance improvement until repeated measurements
complete. The setup/enable contract follows the
[liburing setup documentation](https://github.com/axboe/liburing/blob/master/man/io_uring_setup.2).

The local three-repetition screening run
`build/benchmarks/states-owner-normal-20260905` explicitly skipped global cache
dropping (each sample still used a new mount). Its median wall time / daemon
CPU per operation was legacy 1377.9 ms / 1.265 ms, one-reactor 1935.9 ms /
1.348 ms, and four-reactor 1660.9 ms / 1.973 ms. Samples varied substantially,
and the first repetition overlapped a build-system dependency check. These are
diagnostic results, not an accepted no-regression result. Performance remains
unfinished. Runner evidence now includes five repetitions of both random and
normal advice plus uncached legacy/one/four-reactor kernel-inclusive profiles.

### Measured follow-up: receive directly into the existing memfd mapping

Status: proposal paused; the experimental writable mapping and sink hook have
been withdrawn. FUSE page stealing is conditional, and populated shared mappings
prevent stealing those pages. This proposal does not establish a single physical
page cache. The pool's capacity currently bounds idle retained storage, not all
active windows across the mount. Establish an aggregate memory bound and safe
release of consumed storage before proceeding with the receive experiment.

The uncached prefetch pool already owns a shared memfd mapping. Its current
socket-to-pipe-to-memfd path incurs a copy in `iter_file_splice_write` and two
asynchronous splice stages. The next bounded change makes that same mapping
writable and lets asynchronous HTTP receive directly into its unfilled span.
It adds no user-space staging buffer and no additional payload copy. Publish
only bytes actually received, never use `MSG_WAITALL`, respect H2 DATA-frame
boundaries, and keep the mapping alive until the original I/O CQE retires.
Existing retries/checksums/prefetch ownership and early FUSE replies remain
unchanged. Persistent-cache sinks continue using background file writes;
they must not expose disk-backed mappings to reactor receives. Measure cold
mapping page-fault costs as well as steady-state pool reuse before claiming
an improvement. Cover short reads, tail boundaries, cancellation and FD-visible
data in both H1/H2, then rerun the full suite and A/B profiles.

## Goal

Add an io_uring I/O engine that supports one or multiple reactors while preserving the current threaded implementation as a startup-time fallback. `reactor_count == 1` is the ordinary one-element `ReactorGroup`, not a separate implementation. The hot I/O path must not acquire locks merely because it uses the group abstraction.

## Non-goals

- TLS does not need to use io_uring. The existing threaded TLS tunnel remains supported and is an explicit compatibility boundary.
- No live migration between the io_uring and legacy engines.
- No dynamic reactor resizing or migration of active sockets between reactors.
- No CPU-pool work stealing or per-checksum scheduling framework in the first implementation.
- No requirement to recover a FUSE operation that already returned success before a later asynchronous checksum failure was discovered.
- No kernel patch unless the single-ring/multi-qid prototype proves that the current UAPI forbids the selected mapping and the user separately approves expanding scope.

## Allowed changes

- ngs3fs source, build files, bootstrap scripts, tests, CI, benchmarks, and documentation;
- a pinned libfuse source dependency and the smallest maintained patch needed for caller-owned rings, CQE dispatch, queue grouping, resource reservation, and stable deferred-request payload ownership;
- local prototype programs and test fixtures required to validate the kernel/libfuse UAPI;
- no changes to unrelated repositories or provider services.

## Selected FUSE transport: libfuse classic protocol on caller-owned rings

ngs3fs owns `ReactorGroup` rings and their event loops. A patched libfuse
accepts caller-owned asynchronous classic-fusefd receives and replies while
retaining request decoding, callback dispatch, reply formatting,
notifications, and protocol compatibility. ngs3fs does not implement request
cancellation callbacks, so it negotiates `no_interrupt` instead of paying for
an interrupt list that cannot cancel S3 work. S3, cache, inode, handle, and
operation semantics remain shared with the legacy engine.

The first gate is a minimal prototype proving ordinary asynchronous replies
and fd-backed splice replies through classic fusefd without an intermediate
payload copy. Failure stops execution rather than silently switching to a raw
FUSE protocol implementation or accepting the FUSE-over-io-uring copy path.

The alternatives below are retained as decision history.

### A. libfuse-owned FUSE-over-io-uring

libfuse owns the FUSE io_uring queues and workers. ngs3fs owns separate rings for S3 sockets, pipes, cache fds, and timers. A FUSE callback creates or routes an operation to its business reactor; completion returns to the FUSE queue that owns the request.

Advantages:

- upstream owns FUSE negotiation, request parsing, reply formatting, interrupts, notifications, protocol evolution, buffer pools, and FUSE-over-io-uring quirks;
- uses the kernel's real `IORING_OP_URING_CMD` FUSE transport rather than emulating asynchronous `/dev/fuse` reads and writes;
- can inherit upstream fixed-buffer and privileged direct page-cache-folio zero-copy improvements;
- smallest ngs3fs-specific FUSE protocol surface and lowest long-term correctness risk.

Disadvantages:

- current libfuse owns one FUSE queue per CPU and does not expose those rings for ngs3fs sockets/cache fds, so it cannot provide the literal "all fds in one ring" architecture;
- a FUSE request normally crosses from a libfuse queue worker to an ngs3fs reactor and the final reply may cross back;
- `io_uring_single_issuer` optimization constrains the reply to the receiving FUSE queue thread;
- not every FUSE request type currently uses the io_uring transport; `/dev/fuse` fallback handling remains necessary inside libfuse;
- requires upgrading to an io_uring-enabled, patched libfuse release and accepts a still-evolving upstream interface.

### B. ngs3fs-owned ring with classic fusefd and libfuse dispatch

ngs3fs obtains the session fd, receives classic `/dev/fuse` messages through its ring, and asks libfuse to dispatch parsed request buffers.

Advantages:

- ngs3fs owns the ring and can place fusefd, network, pipe, cache, and timers in one event loop;
- retains libfuse callback dispatch and much of its request decoding;
- classic `/dev/fuse` wire protocol is more mature than the new FUSE-over-io-uring transport.

Disadvantages:

- libfuse custom-I/O hooks are synchronous `read`, `writev`, and splice-style calls; they are not an asynchronous caller-owned completion API;
- a truly asynchronous reply either blocks the reactor, copies libfuse-owned iovecs before returning, or requires a maintained libfuse patch;
- large write-buffer and fd-backed reply lifetime/zero-copy rules become difficult to preserve;
- consequently this option risks combining custom transport complexity with incomplete ownership.

This alternative is selected with the approved libfuse patch, subject to the
prototype gate proving safe deferred request/reply ownership and fd-backed
splice without blocking the reactor.

### C. ngs3fs-owned ring and FUSE request/reply protocol

libfuse may still be used for mount/session setup and public UAPI definitions, but ngs3fs owns fusefd reads/splices, request decoding for its supported operation subset, operation objects, and reply writes/splices.

Advantages:

- the one-reactor configuration can genuinely drive fusefd, sockets, pipes, cache files, and timers from the same ring;
- no libfuse-worker-to-business-reactor handoff on the hot path;
- exact ownership of FUSE input buffers, fd-backed data, SQEs, replies, cancellation, and batching;
- reactor count and CPU affinity are controlled by ngs3fs rather than libfuse's current per-CPU queue topology.

Disadvantages:

- largest implementation and verification cost;
- ngs3fs becomes responsible for protocol negotiation, opcode validation, malformed lengths, compatibility, interrupts, notifications, forget batching, reply formatting, and kernel-version evolution for every supported request;
- loses the new kernel FUSE-over-io-uring fixed-buffer/page-cache-folio path unless ngs3fs later implements the still-evolving `URING_CMD` protocol too;
- a protocol bug can corrupt requests or hang the mount, so differential testing against libfuse is mandatory.

### Decision rule

- Choose A if upstream correctness, future FUSE features, and implementation risk dominate; accept separate FUSE and business rings.
- Choose C if the literal one-ring small-instance data path and minimum handoff CPU dominate; accept owning the supported FUSE wire subset.
- Do not choose B as the final architecture without a prototype that disproves its synchronous ownership problems.

### Selected classic-fusefd A+Patch shape

- caller creates one or more ordinary io_uring instances with sufficient
  SQ/CQ reserve and owns submit, wait, CQ advancement, affinity, and shutdown;
- caller receives classic fusefd messages into stable reactor-owned buffers
  and passes completed buffers to libfuse dispatch;
- a libfuse external-reply hook transfers ordinary reply iovecs or an
  fd-backed splice source to the owner reactor and returns a distinguished
  deferred result;
- libfuse retains `fuse_req_t` until the caller reports the terminal CQE;
- ordinary reply headers/iovecs are copied only into small stable control
  storage; file data is never copied into that storage;
- fd-backed replies transfer a populated pipe read end to the caller, which
  submits `IORING_OP_SPLICE` into fusefd and owns that fd until completion;
- receive buffers, deferred replies, fixed resources, and SQE capacity use
  explicit bounded ownership and exactly-once retirement;
- existing libfuse-owned threaded behavior remains unchanged for the legacy fallback.

## Ownership model

### Metadata authority

One reactor is the metadata authority. Only it may mutate:

- the `InodeDir::children` directory tree;
- inode insertion, replacement, removal, `nlookup`, and stable-pointer lifetime;
- directory expiration and LRU state;
- lookup, readdir reconciliation, create, unlink, rmdir, rename, and forget state.

In one-reactor mode the metadata authority and I/O reactor are the same thread. In multi-reactor mode, an opened handle has one immutable owner reactor. Read, write, flush, fsync, and release are routed by `fh` to that owner. Cross-directory rename remains entirely on the metadata authority. An I/O reactor may hold a stable reference to an inode or handle but may not delete or move it.

### I/O ownership

Scoped cleartext socket operations run on the callback reactor or on a reactor
captured by background work, and an fd is never concurrently submitted to more
than one ring. A read or cache fill crosses from its FUSE dispatch worker to a
reactor exactly once. A bounded reactor-local fiber then parses HTTP/1 or
HTTP/2 and suspends directly on socket, pipe, and cache-file CQEs; it does not
use the old synchronous per-operation worker/reactor rendezvous. Uncached
prefetch tails remain reactor tasks rather than threads waiting synchronously
for reactor I/O.

Cache-file `pwrite` and pipe-to-file splice set `IOSQE_ASYNC`, because buffered
regular-file writes are otherwise allowed to execute inline during ring
submission. This deliberately sends local filesystem work to io-wq while
keeping socket and FUSE transport processing on the reactor. HTTP connections
are still leased from a shared pool instead of being permanently reactor-owned.
TLS remote sockets and checksum-state waits remain explicit threaded
compatibility boundaries.

Cached `FUSE_WRITE` transfers its FD-backed dispatch to a reactor task once.
The callback marks the original `fuse_bufvec` consumed and returns; dispatch
recycling is deferred until the task has consumed or drained the pipe. The
payload therefore crosses neither a userspace buffer nor a synchronous
worker/reactor rendezvous, while the cache-file write still runs on io-wq.

Successful I/O uses one SQE and one CQE. Each request records an absolute
monotonic deadline; the event loop waits only until the nearest deadline and
submits `IORING_OP_ASYNC_CANCEL` solely for an operation that actually expires.
This replaces linked timeouts, which doubled normal-path completion traffic.

## FUSE request lifetime

Every deferred FUSE request has a reactor-owned `Operation`. `fuse_req_t` remains valid until its single reply, but all other libfuse callback arguments are treated as callback-scoped and copied when needed later. Large write payloads must not be copied merely to cross a thread boundary: they must be consumed or transferred into reactor-owned fd-backed storage before the callback returns, unless the selected FUSE transport explicitly keeps the backing request buffer valid until reply.

An operation cannot be retired until all related CQEs, timeout/cancel completions, CPU completions, and FUSE reply ownership have been resolved.

## Operation state machine

Only the owner reactor changes operation state:

```text
ACTIVE -> COMPLETING -> RETIRED
ACTIVE -> CANCELING  -> RETIRED
ACTIVE -> FAILED     -> RETIRED
```

Normal I/O completion, deadline-triggered async cancellation, socket failure,
and shutdown race through this state machine. Exactly one path may issue the
FUSE reply. Every SQE user-data token remains valid until the last possible CQE
has been consumed; token generations prevent stale completions from acting on
reused storage. FUSE request interrupts are intentionally disabled until
ngs3fs can propagate cancellation into the active S3 operation.

## CPU work and checksum semantics

CPU workers receive immutable, self-contained work and never mutate inode, handle, HTTP, cache, or FUSE request state. Reactor-to-worker submission is bounded. Each completion returns to the operation's owner reactor.

Checksum verification preserves the existing asynchronous behavior:

- the FUSE operation returns as soon as its requested data is available;
- checksum completion is not a dependency of that already issued reply;
- if verification later fails, the relevant object/version or cache region becomes poisoned;
- subsequent FUSE operations observe the poison and fail according to the existing checksum retry/error policy;
- operations that already returned success remain successful;
- checksum work therefore normally outlives the FUSE read `Operation` and uses its own stable verification object/version epoch.

Read-side checksum verification remains asynchronous CPU work and never runs as a prerequisite on the reactor's FUSE-reply path. CPU offload uses the stable data ownership already provided to the existing asynchronous verifier; the io_uring conversion must not introduce rereads or extra data copies solely to offload checksum work. Checksums required to construct an S3 write request remain part of the upload pipeline and do not change this read-verification rule.

## CPU completion channel

Each reactor owns one completion pipe. CPU workers write a fixed-size `Completion*` record to the pipe belonging to the target reactor. The pipe contains control pointers only, never file data.

Required invariants:

- completion objects are heap-, pool-, or arena-backed and remain valid until consumed;
- each record is at most `PIPE_BUF`; writes retry `EINTR` and are atomic;
- the reader handles partial fixed-size records even though normal writes are pointer-sized;
- admitted CPU work is bounded so the maximum number of unconsumed completion records fits the configured pipe capacity;
- worker threads block or ignore `SIGPIPE` and handle `EPIPE` during fatal shutdown;
- the read end remains alive and the reactor continues draining until all workers have stopped;
- one outstanding io_uring read is maintained per completion pipe; whether a blocking pipe read is handled natively or through io-wq is measured and recorded during the prototype;
- if pipe reads materially consume io-wq threads or regress CPU/latency, the implementation may replace the transport with a bounded intrusive MPSC queue plus eventfd without changing the completion ownership contract.

Shutdown order is fixed:

```text
stop accepting CPU work
drain admitted work and completion pipes
join CPU workers
close completion write ends
cancel/collect completion read SQEs
destroy rings
```

## Engine selection and fallback

Engine selection happens before the mount begins serving requests.

```text
explicit legacy -> LegacyThreadedEngine
explicit uring  -> fail mount if uring initialization fails
auto            -> try UringEngine, warn and use LegacyThreadedEngine on startup failure
```

After a mount has started, a fatal ring or transport failure fails the mount. It never attempts an in-place transition to the legacy engine.

`ReactorGroup(1)` and `ReactorGroup(N)` share all code. Auto reactor count uses effective CPU availability from affinity and cgroup quota rather than `hardware_concurrency()` alone. SQPOLL is not enabled by default, especially on one- and two-vCPU instances.

## TLS boundary

TLS connections may continue to use the existing `TlsTunnel` thread. Such mounts are described and measured as an io_uring data plane with threaded TLS, not as a fully single-threaded daemon. TLS reactor integration and kTLS are future work.

## Implementation tasks

1. Prototype caller-owned classic-fusefd receive, ordinary reply, fd-backed
   splice reply, interrupt/unmount, and exactly-once retirement paths.
2. Introduce the engine boundary without duplicating S3 or filesystem semantics.
3. Implement `ReactorGroup`, one-reactor mode, fd ownership, and bounded admission.
4. Add the metadata authority and handle-owner routing.
5. Add deferred `Operation` lifetime, generation-tagged CQE ownership, cancellation, timeout, and exactly-once replies.
6. Add bounded CPU submission and per-reactor completion pipes with the specified shutdown protocol.
7. Preserve asynchronous checksum poison semantics independently of FUSE request completion.
8. Add multi-reactor sharding without changing the one-reactor hot path.
9. Add startup probes, explicit mode selection, auto fallback, warnings, and statistics.
10. Retain the legacy engine and run the full correctness, stress, provider, benchmark, and flamegraph matrices against every supported engine.

## Verification matrix

### Correctness

- Existing unit, libfuse, xfstests subset, provider, local-cache, rename, write, and concurrent random-read tests pass under legacy, one-reactor, and multi-reactor modes.
- FUSE interrupt, timeout, cancellation, unmount, HTTP reconnect, checksum failure, and worker shutdown fault injection produces exactly one reply and no leaks or use-after-free.
- Rename replacement and forget preserve stable inode-pointer rules under multi-reactor routing.
- A checksum mismatch discovered after a successful read poisons only subsequent operations; the earlier read remains successful.
- Completion-pipe capacity, `EINTR`, `EPIPE`, partial record, stopped-reactor, and full-shutdown cases are tested.

### Performance

- Compare 256 KiB and 1 MiB sequential reads, multi-file random reads, writes, and cache hit/miss paths for legacy, one reactor, and multi-reactor modes.
- Record latency, throughput, daemon user/system CPU, kernel CPU, syscalls, context switches, io-wq worker activity, memory, and completion-pipe occupancy.
- Produce interactive flamegraphs and retain human-readable benchmark tables as CI artifacts/pages.
- One-reactor latency must not regress materially against legacy on one- and two-vCPU runners; multi-reactor throughput must scale on larger runners.
- Verify that the engine conversion does not increase copied payload bytes or reduce externally spliced bytes.

## Stop conditions

- Stop if one caller-owned ring cannot safely register and serve multiple FUSE qids without a kernel change; report evidence and request a scope decision.
- Stop if the chosen FUSE path requires unconditional copying of large write or read payloads.
- Stop if deferred libfuse request buffers cannot be given a provably safe lifetime.
- Stop if a one-reactor implementation requires locks on reactor-owned hot-path state.
- Stop and fail the mount, rather than live-fallback, after any fatal post-start ring failure.

## Prototype evidence (2026-09-04)

Environment:

- Linux `6.18.33.2-microsoft-standard-WSL2`, 12 available CPUs;
- `/sys/module/fuse/parameters/enable_uring=Y`;
- libfuse release `fuse-3.18.2`, commit `033844748010a3b8265bf1c90b9ae8ffe4cd9ca7`;
- liburing 2.5; caller ring created with `IORING_SETUP_SQE128`.

The local prototype at
`.deps/src/libfuse-fuse-3.18.2` adds a minimal external-ring API.  The caller
creates the ring, processes the classic `FUSE_INIT`, and thereafter owns
`io_uring_submit`, wait, and CQ advancement.  Patched libfuse registers and
dispatches all FUSE entries on that ring and prepares `COMMIT_AND_FETCH`
without submitting.  The daemon had one thread.  Reads pinned to CPUs 0
through 11 produced successful `OPEN`, `READ`, and `RELEASE` requests on qids
0 through 11 respectively, all on the same caller-owned ring.  This proves
that the UAPI permits a caller-owned single-ring/multi-qid mapping.

The next data-path inspection triggered a stop condition.  Both libfuse
3.18.2 and current libfuse master implement `fuse_reply_data_uring()` by
calling `fuse_buf_copy()` into the registered userspace `op_payload`.  The
kernel FUSE-over-io-uring implementation then copies the result from that ring
buffer into the FUSE request pages.  An fd-backed socket/pipe reply therefore
cannot retain ngs3fs's classic pipe-to-fusefd splice path.  This is a property
of the current FUSE-over-io-uring payload-buffer UAPI, not merely libfuse's
per-qid ring/thread policy.

Execution stopped before integration and presented these scope choices:

1. accept the extra payload copy and continue with FUSE-over-io-uring;
2. keep classic fusefd for data and patch libfuse with a caller-owned,
   asynchronous classic transport that preserves fd-backed splice replies;
3. add a kernel/UAPI extension for fd-backed or page-backed zero-copy
   FUSE-over-io-uring replies.

Option 2 was approved on 2026-09-04. It retains libfuse protocol semantics,
provides a caller-owned reactor, preserves the project's low-copy data path,
and does not make ngs3fs depend on an unreleased kernel ABI. This deliberately
replaces the selected FUSE-over-io-uring transport with classic-fusefd
A+Patch.

### Classic-fusefd A+Patch invariants

- the caller owns ring creation, submission, waiting, CQ advancement,
  affinity, admission, and shutdown;
- libfuse continues to decode requests, dispatch low-level callbacks, format
  replies, enforce request lifetime, and implement protocol compatibility;
- fixed receive buffers remain owned until `fuse_session_process_buf()` has
  returned or a deferred operation has explicitly retained their ownership;
- small replies may use caller-owned asynchronous `WRITEV` storage;
- fd-backed read replies use `IORING_OP_SPLICE` or the existing synchronous
  splice fallback and never pass through a userspace payload buffer;
- each deferred reply is queued exactly once and its `fuse_req_t`, headers,
  iovecs, source fd, and pipe state remain valid until the terminal CQE;
- unsupported ring operations, TLS, and startup probe failures select the
  legacy engine only before the mount starts; there is no live fallback;
- the prototype must first prove async receive, ordinary reply, fd-backed
  splice reply, interruption/unmount, and exactly-once retirement before
  ngs3fs integration.

### Execution evidence

| Task | Evidence | Result |
|---|---|---|
| Caller-owned classic fusefd receive and ordinary async replies | libfuse 3.18.2 patched prototype; one ring/event-loop thread; `LOOKUP`, `OPEN`, `FLUSH`, and `RELEASE` completed through queued io_uring writes | pass |
| fd-backed asynchronous reply | 128 KiB read replies logged as `(splice)` and completed as one `IORING_OP_SPLICE` of 131088 bytes (16-byte header plus payload), with no userspace payload buffer | pass |
| reply serialization and header lifetime | fusefd output queue serialized; async mode copies only the stack-backed control header into the pipe instead of retaining invalid `vmsplice` pages | pass |
| concurrent correctness stress | 16 readers, 200 complete 1 MiB reads; length 200/200 and all-`x` content 200/200 | pass |
| patch reproducibility | `git apply --check scripts/libfuse-3.18.2-async-reply.patch` against clean commit `033844748010a3b8265bf1c90b9ae8ffe4cd9ca7` | pass |
| production fd-backed receive | an 8+ MiB integration write reported `copied_write_bytes=0`; request payload remained pipe-backed | pass |
| notification ordering | A background `FUSE_NOTIFY_STORE` publishes an output sequence. The primary reactor drains that sequence before a local reply; another reactor waits only when the primary sequence has not reached its terminal CQE, so notification precedes flush without deadlocking the upload worker | pass |
| external completion ownership | background notifications cross a bounded nonblocking pointer pipe; submitted, consumed, and terminal sequence counters preserve exactly-once retirement and cross-reactor fences | pass |
| multi-reactor FUSE transport | two independent rings and cloned fusefds pass the complete mmap/read/write/rename/checksum integration test | pass |
| normal receive cleanup | patched libfuse asks the reactor to drain its receive pipe only after an error or a short consumer; normal integration and benchmark runs report `receive_drains=0`, eliminating one `FIONREAD` per request | pass |
| interruption and clean unmount with queued replies | session exit wakes every reactor through its eventfd; complete fault-injection coverage remains pending | partial |

### Current implementation boundary

The production implementation now covers tasks 1 and 2, the FUSE-transport
part of task 3, external completion routing, multi-reactor clone-fd sharding,
startup engine selection, and cleartext HTTP socket submission. FUSE callbacks
run in a bounded dispatch pool. Hot read and cache-fill callbacks submit one
bounded task to the originating reactor; HTTP/1 and HTTP/2 parsing then runs on
that reactor's reusable fiber and suspends directly on socket CQEs. Uncached
prefetch tails are reactor tasks as well. Each successful operation uses one
SQE/CQE; the event-loop wait uses the nearest monotonic deadline and submits an
asynchronous cancellation only for a real timeout. Buffered cache-file writes
are forced onto io-wq.

This remains an intermediate architecture. HTTP connections are leased from
a shared pool and are not yet permanently assigned to a reactor. Local-cache
maintenance, TLS remote sockets, checksum-state waits, and unscoped background
work remain threaded. Metadata callbacks and checksum-enabled cache reads may
still use the synchronous compatibility submission path. Local random-read
tests therefore still show higher CPU and latency than the legacy engine. The
legacy engine remains the deployment default until ownership is stable and the
event-driven path is competitive.
