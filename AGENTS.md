# MiniDB Agents Guide (Target Architecture)

## Why Agents Exist
This project aims to build a lightweight relational database MVP with a strict control-plane/data-plane split:
- Control plane: Spring Boot API boundary for network and JSON.
- Data plane: pure Java engine for storage, MVCC, and execution.

Agents must enforce this boundary during all refactors.

## Non-Negotiable Architecture Rules
1. Use a multi-module layout:
- `minidb-server`: Spring Boot only.
- `minidb-engine`: pure Java, zero third-party dependencies.

2. Keep engine package responsibilities explicit:
- `org.minidb.access`: memory-mapped file and page access.
- `org.minidb.tx`: MVCC transaction context and global commit log (CLOG).
- `org.minidb.execution`: Volcano iterator operators.
- `org.minidb.catalog`: singleton metadata/catalog manager.

3. No object-heavy tuple flow in hot path:
- Operators pass `TuplePointer` only.
- Do not instantiate per-row DTOs inside scan/filter loops.
- Materialize Java DTOs only at server boundary (late materialization).

4. Concurrency target for MVP:
- Lock-free append-write path for `INSERT`.
- Snapshot-based visibility for `SELECT` (`Read Committed`).
- No heavyweight lock manager in MVP path.

## Required Core Contracts
Agents must converge code toward these interfaces/contracts:

### Physical & Access Layer
- `MMapFileChannel`: wraps `FileChannel.map()` and exposes mapped-byte operations.
- `TuplePointer`: compact numeric pointer (e.g., pageId + offset in a `long`).

### Transaction Layer
- `GlobalCommitLog`: compact transaction state machine (`IN_PROGRESS`, `COMMITTED`, `ABORTED`) using primitive bit/array storage.
- `TransactionContext`: immutable snapshot per statement with:
  - `T_current`
  - active transaction bitmap/set
  - high/low watermark
  - `boolean isVisible(long xmin)`

### Execution Layer (Volcano Model)
- `DbIterator`: `open()`, `next()`, `close()`.
- `SeqScanNode`: scans pointers, extracts `xmin`, applies `TransactionContext.isVisible(xmin)`.
- `FilterNode`: applies predicate directly on off-heap bytes/pointer view.

## MVP Dataflow to Preserve
### INSERT
1. Allocate transaction id (`xmin`).
2. Lock-free atomic append to mapped storage region.
3. Write tuple bytes with `xmin` header.
4. Commit by atomic state update in `GlobalCommitLog`.

### SELECT
1. Build read-only `TransactionContext` snapshot at statement start.
2. Execute Volcano tree (`SeqScanNode -> FilterNode`) using pointer flow.
3. Materialize records only when crossing engine/server boundary.

## Agent-Level Definition of Done
For each refactor batch, agents should ensure:
- Module boundaries are not violated.
- Engine remains Spring-free and dependency-light.
- Visibility logic is testable independently from operators.
- Operators are iterator-based and pointer-driven.
- Verification includes compile/test checks plus targeted MVCC visibility tests.

## Session Constraints From User
- Do not run any tests. The user will run tests manually.
- Keep all commands and file operations within `~/IdeaProjects/minidb` only.
