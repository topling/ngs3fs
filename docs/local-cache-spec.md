# Persistent local cache specification

Status: implemented as an experimental feature; production validation is ongoing.

This document is normative for the ngs3fs persistent local cache. Where this
document conflicts with an older README description, this document governs the
cache-enabled path. The uncached path keeps its existing behavior unless this
document explicitly says otherwise.

## Goals and governing principles

The cache has four goals:

1. Serve cache hits through an FD-backed FUSE reply without a userspace file
   data buffer.
2. Make writes recoverable after an ngs3fs process crash.
3. Expand cache-miss reads beyond the immediate FUSE request so that S3 is not
   forced to serve a large number of small Range GETs.
4. Preserve the uncached fast path and keep cached and uncached code isolated.

When full POSIX behavior cannot be implemented cheaply over S3, ngs3fs uses
low-cost best effort and fails explicitly rather than silently changing data.
The implementation must not add expensive common-path machinery solely for a
rare pathological access pattern.

This cache is not an offline mode. It is designed to survive an ngs3fs process
crash while the operating system remains alive. It does not promise recovery
from power loss, operating-system crash, cache-device loss, manual cache-tree
modification, or silent storage corruption.

## Page-cache policy

The FUSE inode pagecache is the primary hot-data cache. Sparse local data files
use ordinary buffered I/O so cache hits, fills, checksums, recovery and uploads
share a simple portable path. Every data-file open description is marked once
with `POSIX_FADV_NOREUSE`; on Linux 6.3 and newer this makes accesses through
that description ineligible for page-cache reference promotion. Local pages
may coexist transiently with FUSE pages, but reclaim should prefer the local
copy instead of polluting the active working set. The call is exactly
`posix_fadvise(fd, 0, 0, POSIX_FADV_NOREUSE)`: it is issued once immediately
after each data-file open, and length zero covers the whole file.

`POSIX_FADV_NOREUSE` is advisory rather than an immediate eviction request.
ngs3fs does not use `POSIX_FADV_DONTNEED`, direct I/O, page retrieval, or FUSE
passthrough to enforce a strict single-copy invariant. The advice is best effort:
ngs3fs neither detects kernel support nor makes mount correctness depend on it.

## Mount options

- `-L, --cache-dir PATH` enables the persistent local cache. Omitting it keeps
  the uncached implementation.
- `--cache-size SIZE` sets the maximum physical cache allocation. Zero, the
  default, means that no fixed maximum is imposed.
- `--cache-reserve SIZE|PERCENT` preserves free space on the cache filesystem.
  The default is `5%`.
- `UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE` sets the upper bound for the
  adaptive speculative window for both uncached reads and cache fills. The
  default is 128 MiB. An override is an integer byte count of at least 1 MiB
  and must be page aligned; invalid values warn and retain the default. A
  larger FUSE demand is never truncated to this speculative limit.

The existing multipart upload size is also the cache write-reservation unit.
It defaults to 8 MiB and is not duplicated as a cache-specific option.

When both `--cache-size` and `--cache-reserve` apply, the stricter limit wins.
Invalid values fail the mount; they are never silently rounded to a materially
different policy.

## Implementation boundary

Cached and uncached I/O are separate vertical slices behind these interfaces:

- `FileReader`
- `UncachedFileReader`
- `CachedFileReader`
- `FileWriter`
- `UncachedFileWriter`
- `CachedFileWriter`
- `make_file_reader`
- `make_file_writer`

There is no combined reader/writer interface because `O_RDWR` is unsupported.
Concrete implementations live in `.cpp` files under `src`; they do not use
Pimpl. Common S3 signing, HTTP, inode, and scheduler facilities may be shared,
but cache policy must not leak through conditional branches throughout the
uncached data path.

## Cache-root ownership and identity

ngs3fs takes an exclusive advisory lock on the cache root for the lifetime of
the mount. A second mount using the same root fails.

A root superblock binds the cache to the canonical mount namespace:

- URL scheme and endpoint authority, including port;
- bucket;
- mounted key prefix;
- addressing mode and provider mode when they affect key interpretation;
- cache format version, byte order, page size, and path-encoding version.

Credentials are not part of this identity. Reusing a cache root with a
different namespace or an incompatible format fails without deleting data.
Clean entries may be rebuilt during a compatible format migration. Dirty
entries are never silently discarded or reinterpreted.

At mount time ngs3fs verifies that the cache filesystem:

- is case-sensitive;
- preserves filename bytes for non-ASCII names;
- supports `fallocate(FALLOC_FL_KEEP_SIZE)`;
- supports `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`;
- permits safe `openat` traversal without following symlinks.

Failure of any required probe rejects the cache-enabled mount. Directories are
private to the mounting user and cache files are not opened through symlinks.

## Key-to-path mapping

The data and metadata trees normally mirror the S3 key hierarchy exactly.
Spaces and non-ASCII names are preserved. Empty components, `.`, `..`, control
bytes, overlong components, and components beginning with the reserved prefix
`.~ngs3fs~.` are escaped only inside the cache tree.

- A short exceptional component becomes
  `.~ngs3fs~.b<base64url-without-padding>`.
- A component still exceeding `NAME_MAX` becomes
  `.~ngs3fs~.h<SHA-256-hex>`.
- If the same S3 path is both an object and a prefix, the directory wins and
  the object value is stored as `.~ngs3fs~.value` below it.

Metadata always stores the full original key. A hashed-name collision is
detected by comparing that key and fails explicitly; it never aliases two
objects. Traversal is component-by-component with `openat` and
`AT_SYMLINK_NOFOLLOW`, so aggregate host `PATH_MAX` is not a cache-key limit.

## On-disk object layout

Each S3 key has at most one active cached generation:

- one sparse data file;
- one mmap-backed metadata file;
- when necessary, a small dirty or pending-operation marker in a separately
  scannable tree.

The metadata format contains at least:

- magic, format version, header length, and page size;
- full S3 key;
- ETag, Version ID, object size, and S3 modification time;
- logical `written_end` for a replacement writer;
- write phase and monotonically increasing generation epoch;
- multipart Upload ID, upload part size, and checksum algorithm;
- the two-bit page-state bitmap.

Variable-length recovery records are bounds-checked before use. Metadata space
is allocated before it is mapped, so an in-range metadata store cannot fail
later with `SIGBUS` because the file was sparse.

## Page state and publication

Each runtime page consumes two persistent bits:

| Bits | State | Meaning |
| --- | --- | --- |
| `00` | MISSING | No usable local data |
| `01` | READ_PENDING | One request owns the S3 download; overlapping readers wait |
| `10` | CLEAN | Usable local data; the entry-level dirty flag identifies a local replacement generation |
| `11` | BAD | Data failed checksum validation and its retry |

`READ_PENDING` is published in the persistent bitmap before the GET begins.
It is an inter-thread ownership state, not durable work: after a process
restart, every surviving READ_PENDING page is changed to MISSING and its
possibly partial extent is punched before it can be served. `VERIFYING`,
`RETRYING`, reference bits, and pin counts remain transient in-memory state.
Locally written pages are CLEAN in the bitmap while the entry-level dirty flag
keeps the whole replacement generation non-evictable and authoritative until
the final S3 commit succeeds. The server's paginated `ListParts` result is the
authoritative upload-progress record; duplicating every part ETag in local
metadata adds another crash-consistency problem without improving recovery.

Publication ordering is:

1. reserve physical space;
2. write data;
3. publish CLEAN with release semantics;
4. for a write, publish the new `written_end`;
5. reply to FUSE.

Readers acquire page state before pinning and issuing an FD-backed reply.
Eviction waits for pins, changes CLEAN to MISSING, punches the extent, and then
releases capacity accounting. A page is CLEAN only after a whole page has been
received, except for the final partial page at object EOF.

Every fetch claim carries a generation epoch. Completion from an old GET,
checksum task, retry, or writer is discarded after a generation change and
must never publish into the new generation.

## Open and generation semantics

Every new clean read-only open performs `HeadObject`, even for a complete cache
hit. HEAD failure fails the open. This is the close-to-open validation boundary
and deliberately prevents the clean cache from becoming an offline cache.

Version ID is the preferred generation identity. ETag is used when Version ID
is absent; size and modification time are supporting attributes, not a
replacement for an available ETag.

- An unchanged identity reuses the cached generation.
- A changed identity cancels or drains old fills, invalidates the FUSE inode
  page cache, and installs a new generation.
- Existing readers of the old generation become `ESTALE`; they never switch
  silently and never combine bytes from two generations.
- A Range GET uses Version ID when available, otherwise `If-Match: ETag`.
  A failed precondition makes that reader stale.

An entry in DIRTY or recovery state is locally authoritative and is the sole
exception to mandatory HEAD-on-open. It may be opened read-only from the
sealed local range while recovery runs. Writable open, unlink, and rename of
that key return `EBUSY`.

## Cached reads

The remote payload path is:

```text
socket -> pipe -> sparse cache file -> FD-backed FUSE reply -> FUSE page cache
```

HTTP framing bytes and body bytes already consumed with a header receive may
use a bounded copy. Fixed-length body data that remains on the socket is
spliced into the cache file. HTTP/2 flow-control credit is returned as file
progress is published.

### Fetch selection

The first read and a detected jump request at least 1 MiB. Sequential reads
double the per-handle window up to 128 MiB by default, capped by
`UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE`. The actual claim:

- begins at the first missing page needed by the FUSE request;
- is page aligned except at EOF;
- expands only forward;
- may cross already CLEAN islands to avoid another S3 request;
- stops before another READ_PENDING or BAD range;
- never crosses the object generation or EOF.

Overlapping fetches coalesce through READ_PENDING. Readers that need a pending
page wait for its state to become CLEAN, MISSING, or BAD; they never issue a
duplicate GET for that page. Disjoint misses may fetch concurrently. Once
every page needed by the FUSE request is CLEAN, ngs3fs sends exactly one reply
immediately. The same GET may continue filling its speculative tail afterward.

Demand misses have priority over speculative tails. A cache miss expands only
when a bulk connection is immediately available. Uploads and expanded fills
use only the first `max-connections - 1` connections, leaving one connection
available to an exact demand read or control call. When no bulk connection is
immediately available, the miss fetches only the page-aligned range needed by
the FUSE request. Whole pages already published by a failed tail remain valid.

### Read checksum behavior

Read verification remains optional and best effort. The help text for
`--verify-read-checksum` must state that it is background verification: the
first read or mmap fault may complete before verification.

ngs3fs verifies only a complete independently verifiable unit for which the
provider returned a usable checksum: a full object or a known multipart part.
It does not pretend that an object checksum verifies an arbitrary byte range.
With local caching and `--verify-read-checksum`, multipart metadata is loaded
lazily and once per cache generation with GetObjectAttributes. ObjectParts
pages use `max-parts=1000` and `NextPartNumberMarker`; unsupported, forbidden,
or incomplete responses mark the manifest unavailable and do not fail reads.
Part offsets are the cumulative sizes reported by S3 and never assume the
configured upload part size.
After the unit is fully present, the request worker reads the local file and
computes the checksum without delaying a FUSE request that was already
answered from the completed prefix.

On mismatch:

1. mark the unit BAD/RETRYING;
2. issue exactly one new GET for the same independently verifiable unit;
3. make subsequent reads and mmap faults that reach FUSE wait for that shared
   retry;
4. publish CLEAN on successful revalidation;
5. otherwise leave BAD and return `EIO` to later requests that reach FUSE.

The task performing the failed verification owns the retry. Waiting FUSE
workers never need to run the retry themselves, avoiding worker-pool deadlock.
Already returned bytes cannot be revoked retroactively. After the active FUSE
reply has completed, checksum failure queues range invalidation to a separate
worker; this avoids calling `notify_inval_inode` against the request's locked
folio. Later reads and mmap faults then reach the persistent BAD state and fail
with `EIO`. A missing or unusable checksum merely skips verification.

## Cached writes

### Open contract

FUSE writeback cache is disabled. `O_RDWR`, `O_DIRECT`, writable mmap, seeking,
and non-sequential replacement are unsupported.

- A new file or an existing zero-length file may be opened writable without
  `O_TRUNC`.
- An existing non-empty file requires `O_TRUNC`; otherwise writable open
  returns `EOPNOTSUPP`, whether or not `O_APPEND` is present.
- `O_TRUNC | O_APPEND` is valid. Truncation happens first and writes append to
  the current EOF of the new empty generation.
- `O_APPEND` is accepted as a best-effort compatibility flag for new, empty,
  or truncated files. It does not enable append to an existing non-empty S3
  object.

A writable handle is exclusive with readers and writers of the same key in
the same mount. Different files write concurrently. Every received non-empty
write must start exactly at `written_end`; a different offset fails with
`ESPIPE`.

### Local acceptance and upload

Before replying to a write, ngs3fs creates the dirty marker, reserves one full
upload unit, writes the supplied range to the sparse data file, publishes the
dirty generation's data as CLEAN pages plus `written_end`, and only then
replies. The entry-level dirty flag prevents those pages from being mistaken
for a committed remote generation. The default reservation is therefore 8 MiB
even for a very small first write. This deliberate common-path simplicity may
reject many concurrent tiny writers near capacity; that access pattern is not
optimized.

Successful local data write plus mmap metadata publication is the write
success boundary. Remote visibility is not implied. `fdatasync` and `msync`
are not performed for every write.

Each full upload-sized region is submitted to the existing fair multipart
scheduler. Parts for different files and parts of one file may upload
concurrently within `--max-uploads`; completion XML orders them by part number.
At least 5 MiB is used for every non-final multipart part. The final part may
be shorter.

The maximum writable file size is the lower of the provider object limit and
`part_size * 10,000`. A write crossing that limit fails with `EFBIG` before
any bytes from that request are accepted. ngs3fs does not rebuild an MPU with
larger parts.

### fsync, flush, and release

`fsync` synchronizes the local data file, mmap metadata, and recovery marker,
and reports an upload error already known by the handle. It does not upload an
incomplete tail, Complete the multipart upload, make the object remotely
visible, or seal the handle. The project does not extend this into a promise of
tested recovery after power or OS failure.

The first writable `flush` permanently seals the handle:

- an empty replacement uses `PutObject` to create a zero-length object;
- a file fitting one part uses `PutObject`;
- otherwise flush submits the tail, waits for every part, and executes
  `CompleteMultipartUpload`;
- on success it records the returned identity and Last-Modified; if the
  response omits Last-Modified, it performs one HEAD;
- the entry-level dirty flag is cleared; its CLEAN pages remain available to
  subsequent readers;
- repeated flush with no new data succeeds;
- every later non-empty write fails and emits one stderr diagnostic containing
  the path.

This permanent seal is intentional. FUSE may issue an intermediate flush for
one descriptor produced by `dup` or `fork`; another descriptor then cannot
continue writing. Supporting post-flush reconstruction would impose excessive
copy/append/recovery complexity for a rare pattern. Concurrent writing through
both aliases is also unsupported.

`release` never uploads or completes data. It frees resources and aborts an
unfinished uncached multipart upload. For a cached dirty handle that somehow
reaches release without successful flush, it preserves the dirty marker for
recovery rather than deleting locally accepted data.

Flush failure is sticky for the handle and is returned by later flush/fsync
calls. Dirty data and recovery metadata remain intact.

## Process-crash recovery

Mount startup synchronously scans only the dirty and pending-operation marker
trees. This bounded discovery protects affected keys before serving requests;
it does not scan every clean cache file and does not wait for network recovery.
Recovery work then runs in the background under the normal connection and
upload limits.

For each replacement writer recovery:

1. validate the key, page size, `written_end`, checksum configuration, part
   size, and record bounds;
2. seal the local object at `written_end`;
3. call `ListParts` when an Upload ID exists and treat the server response as
   authoritative;
4. upload missing complete parts and the final tail from the local data file;
5. Complete the upload;
6. identify an outcome-unknown completion by Version ID/checksum when
   available, otherwise by the persisted ngs3fs write ID metadata fallback;
7. mark pages CLEAN and remove the dirty marker only after successful remote
   identification.

Recovery files are readable from their complete local range. A recovery error
isolates the key and returns `EIO`/`EBUSY` for operations on that key; it does
not delete dirty data. A malformed record that still identifies one key
isolates that key. An incompatible root format or corruption that prevents safe
marker enumeration rejects the mount.

A clean shutdown may cancel recovery and speculative reads. It leaves recovery
records in a state that the next mount can resume. It never starts or resumes a
partially completed directory rename.

## Capacity and eviction

Capacity accounting includes allocated data extents, metadata, dirty markers,
and outstanding physical reservations. Concurrent reservations are atomic.
Before reserving, ngs3fs checks both the configured maximum and current
`statvfs` free-space reserve.

When space is needed, clean data is reclaimed in fixed regions of
`max(1 MiB, page_size)`. The policy is an approximate, mount-global
second-chance CLOCK:

- a cache access sets a transient reference bit;
- the first CLOCK pass clears the bit;
- a later unreferenced pass may evict the region;
- dirty generations, READ_PENDING, BAD, pinned, or pending-operation data is
  never evicted;
- eviction changes page state before punching the corresponding hole;
- a region containing CLEAN and MISSING pages remains evictable;
- closed clean entries from earlier process lifetimes are discovered only
  under capacity pressure, never by a full clean-tree startup scan;
- metadata and the empty sparse data file are reclaimed after every page is
  MISSING and no runtime owner remains.

If sufficient clean space cannot be reclaimed, the initiating fetch bypasses
the cache when safe. A write cannot bypass its authoritative cache and fails
with `ENOSPC`. Dirty data is never sacrificed to satisfy a read miss.

An unlinked object retained for an old reader is not evictable until the last
such reader closes.

## Unlink behavior

For an object with no open handle, unlink deletes the S3 object and removes the
namespace entry. Clean cache data becomes immediately evictable.

With open readers:

- If native `RenameObject` is available, rename the remote object to a random
  hidden key, detach the visible dentry, make old readers follow the hidden
  key, and persist a pending-delete marker. Delete the hidden object after the
  last reader closes or during recovery.
- Without native rename, delete the visible remote object immediately. If the
  complete object is already CLEAN locally, detach and pin that cache entry so
  old readers continue to work. Otherwise mark the old handles `ESTALE`; do not
  download missing data merely to preserve unlink semantics.

An active or recovering writer becomes stale and stops uploading when unlink
is accepted. Continued writing to an unlinked replacement is not supported.

## Rename behavior

File rename prefers native `RenameObject` and falls back to conditional
CopyObject plus DeleteObject. Source readers follow the destination key after
successful rename. A writer or recovery operation on the source returns
`EBUSY`.

When overwriting a destination with old readers, native rename first hides the
old destination. Without native rename, a completely CLEAN local destination
may remain detached and pinned for those readers; otherwise they become
`ESTALE`.

The pending-delete marker for an overwritten destination starts in rollback
phase before the old destination is hidden. It records the visible key and the
replacement identity. Only after the source rename commits does it transition
to delete phase. After a process crash, rollback phase restores the hidden
object when the visible destination is absent, deletes it when the visible
destination is positively identified as the committed replacement, and
otherwise isolates the marker rather than guessing. This is recovery or
rollback of one hidden object, not continuation of the interrupted rename.

S3 has no atomic prefix rename. A non-empty directory rename without a genuine
provider-side atomic prefix operation returns `EXDEV`. This intentionally lets
normal tools such as `mv` perform their visible cross-filesystem copy/delete
workflow instead of hiding an unbounded, partially failing tree operation
inside one `rename(2)` call. An empty directory marker may be renamed as one
object. ngs3fs does not persist or resume a directory-rename journal.

Local cache paths move only after the corresponding remote file rename has
succeeded. Failed remote rename never makes the local cache authoritative for
a destination that the server did not commit.

## Directory-cache interaction

Local data caching does not make a metadata directory complete.

- `readdir` performs the complete paginated delimiter-based ListObjectsV2,
  inserts or updates every direct child, and only after the last successful
  page detaches entries that disappeared and have no namespace protection.
- `lookup` uses a valid complete directory cache when available. On an expired
  cache or miss, it performs the same complete paginated direct-child listing
  as `readdir`, inserts or updates every returned child, and detaches children
  absent from the completed result. This accepts the uncommon cost of listing
  a very large directory so expiration and negative lookup state remain
  unambiguous.
- Negative lookup results receive a bounded kernel timeout. A later complete
  readdir remains the truth-reconciliation operation.

Stable inode numbers continue to use `InodeBase*`. Removing a dentry detaches
it immediately; object reclamation still waits for `open_count == 0` and
`nlookup == 0`. A directory is not reclaimed while it owns children.

Rename preserves the source `InodeBase` object and therefore its inode number.
After inserting it at the destination, ngs3fs updates both its `dentry_slot`
and, for a cross-directory rename, its tagged parent pointer. An overwritten
destination inode may outlive its old dentry until its final forget. Reclaiming
such a detached inode may erase a parent slot only after rechecking under the
parent's children lock that the recorded slot still contains that exact inode
pointer. A late forget must never erase the source inode that replaced it.

## Errors and observability

The implementation uses `fprintf(stderr, ...)`, not a logging framework or
C++ iostreams. Repeated hot-path diagnostics are rate-limited. At minimum it
reports:

- cache-root identity/probe failure;
- cache fallback after allocation failure;
- dirty-capacity exhaustion;
- checksum mismatch, retry, and permanent failure;
- recovery start, success, isolation, and outcome-unknown state;
- write attempted after the first successful flush;
- generation changes and `ESTALE` transitions;
- cancellation of speculative work under connection pressure.

Errors use stable meanings:

- `EOPNOTSUPP`: unsupported open or operation contract;
- `ESPIPE`: non-sequential writable offset;
- `EBUSY`: active writer or recovery excludes an operation;
- `ESTALE`: an open reader can no longer safely identify its generation;
- `ENOSPC`: authoritative dirty allocation cannot be reserved;
- `EFBIG`: writable object would exceed the fixed part-count/provider limit;
- `EIO`: verified corruption, persistent recovery failure, or unknown commit
  outcome that cannot be resolved.

## Required validation

The implementation is not complete until the following pass:

1. Unit tests for component escaping, file/prefix collision, bitmap atomics,
   generation fencing, metadata bounds, and cache-root identity.
2. Capacity tests covering concurrent reservations, CLOCK second chance,
   pinning, dirty non-eviction, hole punching, and reserve enforcement.
3. HTTP/1.1 and HTTP/2 tests that stream bodies larger than pipe capacity into
   a sparse file and publish progress without flow-control stalls.
4. Cached read tests for hit, miss, mixed ranges, coalescing, disjoint fetches,
   adaptive expansion, early FUSE reply, cancellation, and generation change.
5. Checksum tests for background success, one shared retry, retry failure,
   read `EIO`, and mmap invalidation/`SIGBUS` where the kernel permits it.
6. Cached write tests for all open-flag combinations, zero-length replacement,
   sequential enforcement, 8 MiB reservation, concurrent files, multipart
   completion, first-flush sealing, repeated flush, and write-after-flush.
7. Kill-and-remount tests at every metadata publication and multipart phase,
   including outcome-unknown Complete.
8. Unlink and rename tests with and without native RenameObject, including a
   fully cached open reader and `EXDEV` for non-empty directory rename.
9. Existing libfuse, xfstests, stress, real-provider, ASAN, TSAN, and clang
   gates with unsupported operations removed from their allowlists.
10. Runner benchmarks against goofys and Mountpoint for S3, followed by an
    interactive ngs3fs flamegraph. Client-side time excluding network transfer
    remains targeted at no more than 20% of the fastest relevant competitor.

The integration server deliberately commits one multipart object and closes
the connection before returning the Complete response; ngs3fs must resolve the
ambiguous outcome by HEAD. The runner recovery benchmark separately waits for
an observable UploadPart, sends SIGKILL before `flush`, remounts the same cache,
and records verified restart-to-publication latency and request counts.
