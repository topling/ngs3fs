# Local-cache page-advice execution contract

Status: approved; execution in progress.

## Goal and non-goals

Keep the existing buffered local-cache data path and mark every cache data-file
open description with `POSIX_FADV_NOREUSE`, making the FUSE inode pagecache the
preferred hot working set on Linux 6.3 and newer. Preserve all cache consistency,
crash-recovery and performance behavior.

Do not implement direct I/O, `FUSE_NOTIFY_RETRIEVE`, passthrough, explicit page
eviction, kernel changes, a strict one-resident-copy guarantee, or any change to
write/flush semantics.

## Finding decisions

| Finding | Decision | Contract change | Re-review |
|---|---|---|---|
| Direct-I/O tail alignment makes a simple advisory cache feature disproportionately complex | accepted | Replace direct I/O with buffered data files plus NOREUSE | yes |
| FUSE target folios are not directly exposed and RETRIEVE adds lifecycle/deadlock risks | accepted | Remove RETRIEVE from production scope and delete its prototype test | yes |
| NOREUSE may be unsupported or ineffective | accepted | Treat it as an unchecked best-effort hint with no correctness dependency | yes |
| NOREUSE immediately removes duplicate local pages | rejected | Specify advisory reclaim priority, not eviction or a strict invariant | yes |

## Allowed scope

Cache data-fd creation/reopen paths, a small page-advice helper, cache tests,
local-cache documentation, README compatibility notes, and existing benchmark/
CI evidence. No unrelated formatting or dependency changes.

## Ordered tasks

1. Apply NOREUSE exactly once to every newly opened cache data-file description:
   normal open, writer creation, and recovery. Rename retains its existing open
   description and therefore requires no second call. Ignore the advisory
   call's result; cache correctness must be unchanged.
2. Add tests for all data-open call sites and unchanged cache state transitions.
3. Run the complete local suite and sanitizers; review and fix the scoped diff.
4. Benchmark cached cold/warm reads and collect `mincore`/memory-pressure evidence.
   Treat residency as advisory and compare CPU/latency with the prior baseline.
5. Commit and push scoped changes, run CI, download and analyze useful artifacts.

## Verification matrix

| Requirement | Evidence |
|---|---|
| Every cache data description carries NOREUSE | Call-site audit plus injected helper test |
| Unsupported/no-op advice remains correct | Existing cache semantics tests |
| Cache semantics unchanged | Complete unit and mounted integration suite |
| No performance regression | Cached benchmark CPU/latency comparison |
| Advisory policy has effect where supported | `mincore` or pressure-based residency evidence |
| Reproducible handoff | CI run URL, conclusion and analyzed artifacts |

## Stop conditions

Stop and revise if applying NOREUSE changes read-ahead behavior, a required data
open path cannot be marked, correctness depends on reclaim occurring, or the same
failure class repeats three times without new evidence. NOREUSE failure must
never make existing cache data unreadable.

## Change log

The unimplemented direct-I/O contract and prototype were removed. The revised
scope is intentionally smaller, retains buffered I/O, documents NOREUSE's Linux
6.3 boundary, and verifies policy without asserting deterministic eviction.
