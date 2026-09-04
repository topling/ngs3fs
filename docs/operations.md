# Production operations

ngs3fs is suitable for a controlled workload whose applications accept the
documented sequential replacement-write and close-to-open contract. Treat that
contract as part of the application deployment, not as a POSIX-compatible
default.

## I/O engine selection

`--io-engine legacy` is the deployment default while the uring engine remains
experimental and consumes more CPU in the current local random-read
benchmark. `--io-engine auto` is experimental. At startup it probes
the patched classic-fusefd io_uring transport and uses it when initialization
succeeds. If the probe or reactor initialization fails, it logs a warning and
starts the legacy libfuse threaded engine. This is the only fallback point:
after requests are being served, a fatal transport failure fails the mount and
does not migrate it to another engine.

`--io-engine legacy` forces the existing threaded libfuse path. `--io-engine
uring` forces the caller-owned FUSE transport reactor group and fails the
mount if that transport cannot be initialized. `--reactors N` creates N rings
and N cloned FUSE device fds. `N=1` uses the same group implementation and
does not acquire a group lock on the request/reply hot path.

The uring engine covers classic `/dev/fuse` receive/reply and cleartext HTTP
connect, send, receive and splice operations reached from dispatched FUSE
callbacks, upload workers and uncached-prefetch continuations. Socket
operations carry linked request timeouts. TLS remains implemented by the
threaded `TlsTunnel`; local-cache file I/O and unscoped maintenance work also
remain threaded. Consequently this is not yet a complete all-FD reactor, and
an HTTP connection is not yet permanently assigned to one reactor. The uring
build uses a
pinned libfuse 3.18.2 source
dependency plus the small ngs3fs asynchronous-reply patch. Bootstrap fetches
and builds that dependency; verify the pinned version and patch in CI before
rollout.

## Required bucket policy and lifecycle

- Grant only the mounted bucket/prefix operations used by the deployment:
  ListBucket, GetObject, HeadObject, PutObject, DeleteObject, multipart upload,
  multipart-copy and, where available, RenameObject.
- Configure `AbortIncompleteMultipartUpload`. A process or host crash cannot
  abort an UploadId that was never returned to the client.
- Use bucket versioning when rollback and forensic recovery are required. It is
  supported but deliberately not required by ngs3fs.
- Select server-side encryption in the bucket default policy. This avoids
  placing provider-specific KMS material in every request.

## Capacity planning

S3 allows at most 10,000 multipart parts. The fixed part size therefore defines
the writable object limit before the 5 TiB S3 ceiling:

| Part size | Approximate limit |
| ---: | ---: |
| 8 MiB | 78.1 GiB |
| 16 MiB | 156 GiB |
| 128 MiB | 1.22 TiB |
| 512 MiB | 4.88 TiB |

`--max-pinned-memory` must be at least one part and should accommodate active
writers without exhausting kernel pipe memory. `--max-connections` must exceed
`--max-uploads`; one connection remains reserved for reads and control calls.

## Credentials

The runtime chain supports environment credentials, shared credentials files,
`credential_process`, Web Identity/STS, ECS/EKS credentials and IMDSv2.
Refresh runs in the background before expiration, so normal requests never
perform credential I/O. A still-valid snapshot is retained during a transient
refresh failure. Long-running services should use Web Identity,
container/instance roles or `credential_process`, rather than static keys.

Profiles requiring SSO or a source-profile AssumeRole are rejected unless they
provide `credential_process`; this prevents an accidental anonymous mount.

## Monitoring and response

Capture stderr in journald or the service supervisor. Alert on:

- `commit outcome unknown`, `orphan upload`, checksum mismatch and `ESTALE`;
- repeated transport retries, 429/5xx responses or credential refresh failure;
- pinned-memory or inode-cache budget warnings;
- copied FUSE write fallback and failure to raise kernel read-ahead;
- partial non-atomic directory rename.

After an unknown multipart outcome, inspect current object VersionId/checksum
before retrying an application transaction. After a partial directory rename,
reconcile source and destination prefixes; do not blindly repeat the rename.

## Validation before rollout

1. Run `scripts/test_provider_e2e.sh` against the exact endpoint and IAM policy.
2. Run the application canary with production part, connection and memory
   budgets.
3. Kill ngs3fs and sever the network during create, part upload, completion,
   read and rename operations; verify application error handling and lifecycle
   cleanup.
4. Soak for at least seven days while tracking RSS, file descriptors, pipe
   memory, cached inodes, S3 request errors and close failures.
5. Repeat the provider test and canary after any nghttp2, llhttp, OpenSSL,
   libfuse, kernel or S3-provider upgrade.

## Real-provider CI

The `Provider E2E` workflow has AWS and OSS matrix entries. Configure repository
variables named `NGS3FS_AWS_*` and `NGS3FS_OSS_*` for endpoint, authority,
bucket, prefix, region and checksum. Configure the corresponding
`*_ACCESS_KEY_ID`, `*_SECRET_ACCESS_KEY` and optional `*_SESSION_TOKEN` as
repository secrets. Every provider runs uncached and cached matrix entries.
An unconfigured entry fails explicitly rather than presenting a false
provider pass.
