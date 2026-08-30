# ngs3fs

`ngs3fs` is an experimental S3 FUSE client written in C++20 directly on
libnghttp2 and llhttp. Its design target is low client-side latency for
buffered Linux I/O, especially mmap-heavy applications.

The filesystem mounts an S3 bucket, or an optional key prefix within it, as a
multi-file namespace. It remains experimental rather than production-ready.

## What works now

- Stock libnghttp2 v1.70.0 is used without a local fork or patch. After an
  unpadded DATA payload is consumed externally, a protected shadow span
  advances libnghttp2's receive state without containing payload bytes.
- Cleartext endpoints are detected automatically. The client sends an HTTP/2
  preface and SETTINGS first; a valid server SETTINGS frame keeps HTTP/2,
  while an HTTP/1.x response or close causes a clean reconnect using llhttp
  v9.4.3. A silent probe is bounded to one second. GOAWAY or a failed HTTP/2
  connection triggers protocol detection again before the next request.
  Protocol selection is not exposed as a mount option. Established plain TCP
  sockets use 30-second send and receive timeouts, including socket-to-pipe
  `splice(2)` waits.
- HTTPS uses the system trust store, verifies the endpoint hostname, sends SNI,
  and negotiates HTTP/2 or HTTP/1.1 with ALPN. Port 443 enables TLS
  automatically; `-S/--tls` enables it on any port. Active TLS requests have
  a 30-second no-I/O-progress timeout; timeout discards the connection and the
  next request reconnects. Idle keep-alive connections are not timed out.
- HTTP/1.1 response headers are scanned once by llhttp. For a fixed-length
  response, llhttp pauses at the header boundary, body bytes already returned
  by that `recv(2)` are copied once into the transport pipe, and the remainder
  is requested from `splice(2)` at its full remaining length; kernel short
  returns are retried without an artificial batching threshold. Chunked and
  EOF-delimited responses are strictly parsed by llhttp and use a bounded
  copied fallback.
- Range GET payload path:

  ```text
  socket -> splice -> worker-local reused pipe -> fuse_reply_data -> page cache -> mmap
  ```

  Payload bytes do not enter a userspace data buffer. A stock Linux TCP/FUSE
  path still has one kernel copy into the page cache.
- FUSE remains in cached mode; `direct_io` is never enabled.
- The preferred application transfer size reported through `statfs.f_bsize`
  defaults to 256 KiB and is independently configurable with
  `--io-size`. The allocation-unit field `statfs.f_frsize` remains 4 KiB.
- Read-ahead defaults to 256 KiB and is controlled by
  `-R/--read-ahead`; raw bytes and KiB/MiB/GiB suffixes are accepted. Before
  FUSE INIT, ngs3fs raises the mount's BDI `read_ahead_kb` when permitted and
  lets the kernel page cache schedule and deduplicate read-ahead. Linux makes
  that sysfs write root-only. If raising it is denied, ngs3fs prints a warning
  and falls back to the BDI's current kernel value. There is no userspace
  read-ahead.
- FUSE writeback cache is disabled. Writable mmap, `O_RDWR`, `O_DIRECT`, and
  writers without `O_TRUNC` are rejected. Each writable handle is
  non-seekable and accepts only strictly sequential `FUSE_WRITE` offsets from
  zero; a violation returns `ESPIPE`.
- Write payload is retained in kernel pipe chains, not a userspace file-data
  buffer. FD-backed FUSE buffers enter those chains with `splice(2)`. A
  memory-backed FUSE buffer is copied once and produces a rate-limited warning
  and byte counter. A mount-time splice preflight warns when a seccomp policy
  or runtime blocks the syscall. An FD-backed runtime failure caused by an
  unsupported source, filesystem, or security policy uses a bounded copied
  fallback and emits a rate-limited warning.
- At startup ngs3fs probes attainable pipe capacity and sets FUSE `max_write`
  so libfuse's whole receive buffer fits in its splice pipe. This avoids
  libfuse silently falling back to `read(2)` when an unprivileged process
  cannot enlarge that pipe to its default maximum. The preferred request size
  remains 256 KiB; it is reduced with a warning when the host's pipe limit is
  smaller. Libfuse intentionally copies sub-page requests.
- Parts default to 8 MiB. Full parts enter a fair mount-global upload queue and
  upload concurrently over a prewarmed connection pool. The retained master
  pipe chain remains intact until success; each attempt uses a `tee(2)` clone,
  allowing three additional retries for transport failures and 408/429/5xx
  responses without another payload copy. The default limits are four active
  uploads, eight connections, and 256 MiB of retained payload.
- `fsync(2)` is intentionally non-durable and does not publish or force a
  partial part. The first `flush` permanently seals the handle. A file smaller
  than one part uses `PutObject`; otherwise flush uploads the tail, waits for
  all parts, and runs `CompleteMultipartUpload`. Repeated flush/fsync calls are
  no-ops and every later non-empty write fails with `EIO`. `release` never
  uploads: it only aborts an unfinished multipart upload and frees resources.
- Read-only handles for the same object may coexist. A writable open is
  exclusive with every other handle for that object: existing readers or a
  writer make it return `EBUSY`, and an existing writer makes read-only and
  writable opens return `EBUSY`. The open table is keyed by object path, so
  handles for different objects do not serialize one another. Pending pipe
  parts, multipart state, and upload completion are owned by each handle;
  connection and retained-memory limits remain mount-global.
- An expired directory is refreshed by a complete, paginated, delimiter `/`
  `ListObjectsV2` of its direct children. Results are inserted or updated in
  place, and absent dentries are detached only after the final page succeeds.
  The cache timeout is one second by default and is configurable with
  `-T/--dir-cache-timeout` in milliseconds. `readdir` refreshes at most once
  per open handle, then streams `Directory` hash slots directly into the FUSE
  buffer. Its offset cookies encode the next hash slot, so no per-open
  directory snapshot or duplicate entry array is built. It emits only names,
  inode numbers, and types. A mount-global second-chance CLOCK reclaims
  unreferenced children from expired directories above the soft
  `-I/--max-cached-inodes` limit (default 1,000,000); referenced, open,
  pending, or fresh entries may temporarily keep the cache above that limit.
  Files and directories use the derived `InodeFile` and `InodeDir` types;
  `InodeDir` embeds its `Directory`. The type and three state flags occupy the
  low four bits of the 16-byte-aligned tagged parent pointer. Except for
  `FUSE_ROOT_ID`, the stable `InodeBase*` value is the inode number; lookup/open
  reference counts defer reclamation until the kernel can no longer use that
  pointer.
- Read-only open performs HEAD for close-to-open refresh of size, identity,
  and data. `keep_cache` is disabled. A returned S3 Version ID pins subsequent
  reads; otherwise `If-Match: ETag` is used and a changed object maps to
  `ESTALE`.
- UID, GID, file mode, and directory mode are mount-wide values supplied by
  `--uid`, `--gid`, `--file-mode`, and `--dir-mode`; none come from S3 metadata.
- Object size never determines type: zero-byte `Contents` are files.
  `ListObjectsV2` `CommonPrefixes` are directories, matching the S3/goofys
  implicit-directory model. `mkdir` writes an empty trailing-slash marker;
  the marker itself is hidden from directory contents.
- SigV4 GET/PUT/HEAD/control requests, including temporary credentials. The
  signer is checked against AWS's published S3 Range GET test vector. Static
  credentials come from the AWS environment variables first, then the selected
  profile in `~/.aws/credentials` (or `AWS_SHARED_CREDENTIALS_FILE`). Directory
  buckets obtain and refresh `CreateSession` credentials with a single
  coordinated refresher. Range GET initially signs only the headers required
  by SigV4 plus the close-to-open `If-Match`, allowing each worker to reuse the
  authorization header within the same second. If an S3-compatible endpoint
  rejects the unsigned standard `Range` header, ngs3fs retries once with Range
  signed, remembers that requirement for the mount, and prints a warning.
- File `rename(2)` first attempts `RenameObject`. Explicit unsupported
  responses are remembered, then CopyObject + DeleteObject is used as the
  non-atomic fallback.
  Any open handle makes rename return `EBUSY`. Control requests pin the source
  with a fresh HEAD and ETag. The copy source also pins a returned Version ID.
  Files above 5 GiB use multipart copy with 1 GiB parts. The subsequent
  conditional delete creates the normal delete marker when versioning is
  enabled, so an older version is not exposed as the current source key. Copy
  sources are formed as an encoded `bucket/key` without re-encoding an already
  encoded object path.
- Directory rename paginates every object below the source prefix without a
  1,000-key cap and applies the same RenameObject-or-Copy/Delete operation to
  each key. S3 has no atomic prefix rename: a failure returns `EIO`, logs the
  completed/total counts, and may leave a partially moved directory. A final
  source-prefix check returns `EAGAIN` if objects appeared during the move.
- Symbolic and hard links are unsupported; hard links return `ENOTSUP`.
- A real WSL FUSE integration test verifies read mmap, rejection of writable
  mmap, small `PutObject`, an 8 MiB full part plus multipart tail, duplicate-
  descriptor flush behavior, close-to-open publication, same-object
  read/write exclusion, concurrent Range GETs, version pinning, and
  RenameObject over an in-process HTTP/2 S3 peer. A second end-to-end test uses
  a non-empty prefix on VersityGW's local POSIX backend and its HTTP/1.1 path;
  it starts with more than 1,000 root keys to force paginated
  `ListObjectsV2`, concurrently creates and multipart-uploads two distinct
  objects, reopens and mmap-verifies them, then exercises mkdir, nested
  objects, cross-directory file rename, and nonempty directory rename. A
  separate 1,001-object directory verifies that recursive rename consumes its
  second listing page and moves the final key before unlink and rmdir checks.

## Important current limits

- TLS and ALPN are supported through OpenSSL. The encrypted side necessarily
  passes through OpenSSL userspace buffers; the cleartext Unix-socket side of
  that tunnel retains the same pipe/splice transport used by the HTTP clients.
- Symbolic and hard links are not supported. File and recursive directory
  rename, create, unlink, mkdir, and empty-directory rmdir are supported.
- Writes never download or locally stage the old object. They support only
  replacement from offset zero. The configured fixed part size and S3's
  10,000-part limit bound the object size (about 78 GiB with the 8 MiB
  default), additionally capped by S3's 5 TiB object limit. A descriptor
  retained by `dup` or `fork` cannot write after the first successful flush.
- RenameObject support is detected by an optimistic request. Its atomic form is
  currently useful only where the provider implements the API; the fallback is
  intentionally non-atomic. `--bucket` is required if fallback is needed.
- The mount owns a prewarmed persistent connection pool shared by reads,
  control operations, and uploads. A leased connection runs one synchronous
  request at a time; concurrency comes from multiple physical connections,
  and one HTTP/2 connection does not yet multiplex concurrent streams. The
  write scheduler and retained-memory budget are mount-global. Bulk transfers
  use at most `max-connections - 1`, reserving one connection for reads and
  control requests.

## TODO

- Linux FUSE can currently merge the folios required by the caller with
  speculative readahead folios into one `FUSE_READ`. A reply cannot complete
  incrementally, so the caller may wait for that whole merged request. When
  kernel FUSE supports splitting at the synchronous/asynchronous readahead
  boundary, use it so the caller can return while later readahead requests
  continue in the background. Do not add userspace readahead as a workaround.

## Build in WSL

The bootstrap is unprivileged. It downloads stock nghttp2 v1.70.0 and llhttp
v9.4.3 (their latest official GitHub releases when pinned), verifies their
pinned SHA-256 values, and extracts the libfuse3 development packages into
`.deps/sysroot`.

The current build also uses the minimal SSO and hash-map implementation from a
local topling-zip source checkout selected by `TOPLING_ZIP_SOURCE_DIR`; jemalloc
is explicitly disabled for that imported code.

```sh
./scripts/bootstrap.sh
cmake --build --preset dev
ctest --preset dev --output-on-failure
./build/dev/ngs3fs --self-test
```

## Filesystem correctness and stress tests

CI builds the Linux kernel filesystem test suite (`xfstests`) at pinned commit
`56c410ad0f69da5b13c5807bc47b4876dcfa02b2` and runs it as root against a real
ngs3fs FUSE mount backed by VersityGW. The small compatibility patch
`scripts/xfstests-fuse-subtype.patch` makes the upstream harness recognize
`fuse.ngs3fs` as its generic `fuse` filesystem type.

The correctness allowlist is `generic/001` and `generic/245`. Together they
cover sequential data creation and copying, byte-for-byte read verification,
unlink, directory operations, rename, and rejection of a rename onto a
non-empty directory. The stress phase uses the suite's own `ltp/fsstress` with
all defaults disabled, then enables only `creat`, `getattr`, `getdents`,
`mkdir`, `rename`, `rnoreplace`, `rmdir`, `stat`, and `unlink`. CI runs eight
worker processes with 500 operations each.

Because `fsstress` does not import pre-existing files and its supported create
operation produces empty files, a separate concurrent random-read stress phase
first writes 32 deterministic 4 MiB files strictly sequentially. Sixteen
threads then perform 128 reads each, choosing a fresh file, offset, and
1-byte-to-256-KiB non-aligned length for every operation. Each operation uses
a new read-only open, exercises close-to-open refresh and page-cache
invalidation, and alternates between `pread(2)` and read-only `mmap(2)` access.
Every returned byte is validated. This catches short reads, wrong ranges,
cross-file mixups, corruption, page-fault failures, and concurrent
connection/handle failures rather than merely checking syscall return codes.

Tests that require a scratch block device, random or overwriting writes,
`O_RDWR`, truncate, writable mmap, hard or symbolic links, xattrs, locks,
special files, direct I/O, or persistence across fsync/remount are excluded
because those operations are outside the ngs3fs contract. They are not run as
expected failures.

## Mount a bucket or prefix

The endpoint may speak HTTP/2 prior knowledge or HTTP/1.1; detection is
automatic and HTTP/2 is preferred. Credentials are optional for
anonymous/test endpoints.

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...        # optional
export AWS_REGION=us-east-1

mkdir -p mount
./build/dev/ngs3fs \
  -e 127.0.0.1 -p 9000 \
  -a bucket.example.test:9000 \
  -b bucket -k optional/raw/prefix \
  --io-size 256KiB -R 256KiB -T 1000 -I 1000000 \
  -P 8MiB -c 4 -C 8 -B 256MiB \
  -u 1000 -g 1000 -m 0644 -D 0755 \
  -f mount
```

`--prefix` is a raw S3 key prefix; leading slashes are removed and a trailing
slash is added internally. Omit it to mount the bucket root. Path-style versus
virtual-hosted requests are inferred from `--authority` and `--bucket`.
`--io-size` controls only the preferred transfer-size hint returned by
`statfs(2)` and must be nonzero; it does not change FUSE request sizing or
kernel read-ahead.
`--read-ahead` must be page-aligned; `0` disables it. Values above the
per-request pipe capacity are split by kernel FUSE read limits when kernel BDI
tuning is available.

Use `-S/--tls` for HTTPS on a non-443 port. Protocol selection remains
automatic: ALPN or the cleartext probe prefers HTTP/2 and falls back to
HTTP/1.1 without a user protocol switch.

## Performance accounting

Run the daemon with `--metrics` to emit one JSONL record per remote FUSE read:

```json
{"event":"read","bytes":262144,"fetched_bytes":262144,"total_ns":900000,"transport_span_ns":760000,"residual_ns":140000,"cpu_ns":210000,"external_bytes":260000,"fallback_bytes":2144}
```

The apples-to-apples network-independent metric is:

```text
cpu_ns = worker-thread CPU time consumed from FUSE callback entry
         through fuse_reply_data completion
```

`CLOCK_THREAD_CPUTIME_ID` excludes time blocked on the provider or network but
includes client parser, splice syscalls, signing, and FUSE reply work.
`residual_ns` is the wall-clock `total_ns - transport_span_ns` diagnostic. It
is useful within ngs3fs, but client work overlaps transport and a competitor
cannot provide the same boundary without equivalent instrumentation, so the
CPU metric is the fair cross-client comparison.

`mmap_fault_bench` produces comparable cold-page mmap timings for any mounted
filesystem. `bench/summarize_metrics.py` reports p50/p99/p99.9 and, when given
a comparable baseline metrics file, enforces the project target of no more
than 20% of the fastest comparable S3 FUSE client's client-side CPU.

```sh
./build/dev/mmap_fault_bench mount/object.bin 262144 100 > mmap.jsonl
./bench/summarize_metrics.py ngs3fs-metrics.jsonl \
  --baseline competitor-metrics.jsonl --field cpu_ns
```

Comparisons must use the same endpoint, object, host, cold-page method,
concurrency, request size distribution, and the same worker-CPU accounting in
the competitor. The benchmark treats failed `posix_fadvise`/`madvise` cache
eviction as an error rather than silently reporting a warm-page sample.
`scripts/compare_goofys.sh` runs both clients against the same VersityGW
instance and sums every task's `/proc/PID/task/TID/schedstat` runtime, avoiding
the leader-only and 10 ms quantization errors of `/proc/PID/stat`. Run it with
enough privilege to raise the mount BDI read-ahead when evaluating the default
256 KiB setting; the unprivileged fallback is reported explicitly.

The same script can reuse the concurrent multi-file random-read stress as a
comparison workload. It prepares identical deterministic objects directly in
the backend, warms the backend before each client, validates every byte, and
records wall time, all-thread daemon CPU, and S3 GET count in
`random-read-summary.csv`. Run isolated samples with a fresh VersityGW and
backend per client to avoid connection and server-lifetime order effects:

```sh
sudo env WORKLOAD=random-read CLIENT=ngs3fs PORT=17081 \
  ./scripts/compare_goofys.sh build/e2e/random-read-ngs3fs
sudo env WORKLOAD=random-read CLIENT=goofys PORT=17082 \
  ./scripts/compare_goofys.sh build/e2e/random-read-goofys
```

`CLIENT=both` is available for a quick paired run; `ORDER=reverse` swaps its
order. `RANDOM_READ_FILES`, `RANDOM_READ_THREADS`,
`RANDOM_READ_OPERATIONS`, `RANDOM_READ_FILE_SIZE`, `RANDOM_READ_MAXIMUM`, and
`RANDOM_READ_SEED` override the defaults. The workload deliberately applies
`POSIX_FADV_RANDOM` and `MADV_RANDOM`, so it measures random access with kernel
read-ahead disabled; a client-side user-space read-ahead policy remains part of
the client being compared. Set `RANDOM_READ_ADVICE=normal` to retain the same
random file/offset/length sequence while allowing the kernel's configured
read-ahead policy.

`scripts/benchmark_random_read.sh` runs both advice modes with isolated,
alternating ngs3fs/goofys samples and writes an aggregate `samples.csv` plus
the raw per-sample logs. It drops kernel caches between samples when run with
enough privilege and removes only the generated backend objects after each
sample.

The profiler accepts the same workload and advice controls. It produces a
standard SVG, a searchable/zoomable interactive HTML flame graph, folded
stacks, and flat/self/inclusive/DSO reports:

```sh
sudo env WORKLOAD=random-read RANDOM_READ_ADVICE=random PERF_EVENT=cpu-clock \
  ./scripts/profile_ngs3fs.sh build/profiles/random-advice
```

## nghttp2 receive invariant

For unpadded DATA, the payload is spliced to the destination pipe before
libnghttp2 is advanced with a same-length `PROT_NONE` shadow span. The pinned
v1.70.0 DATA state machine uses that span only for pointer/length accounting;
the registered DATA callback recognizes shadow advancement and never reads it.
The default span is 256 KiB and larger frames are advanced in chunks, so it is
a preferred transfer size rather than a frame-size limit.

At mount startup, the daemon also passes the transport pipe capacity
to libfuse as `max_read`. Larger application reads and mmap faults remain
supported; the kernel splits them into multiple FUSE reads. This prevents a
larger FUSE request from becoming an accidental file-size or correctness cap.

This relies on a tested v1.70.0 implementation invariant rather than a
documented receive-side zero-copy API. CMake rejects other nghttp2 versions and
the state-machine test deliberately uses a protected mapping, so an upstream
change that starts reading payload bytes fails during validation instead of
silently accepting fabricated data. Re-audit this path before changing the
pinned nghttp2 release.

## HTTP/1.1 receive path

llhttp v9.4.3 parses response status lines, headers, chunking, trailers, and
connection persistence. For fixed-length bodies the parser pauses when headers
complete. Any body tail already present in the 2 KiB receive buffer is copied
once into the transport pipe; the unreceived remainder is spliced directly
from the socket. The known Content-Length accounts for those externally
consumed bytes, so there is no second parse, `MSG_PEEK`, `tee(2)`, or protected
shadow span. Chunked and EOF-delimited bodies remain correct but use the copied
fallback because their framing is interleaved with payload bytes.

## License

ngs3fs is licensed under the GNU Affero General Public License v3.0. See
[LICENSE](LICENSE).
