# io_uring reactor execution contract

Status: approved for classic-fusefd A+Patch execution. Caller-owned reactors
use libfuse for protocol semantics while preserving fd-backed splice replies;
FUSE-over-io-uring remains excluded until its payload UAPI avoids the extra
copy.

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
