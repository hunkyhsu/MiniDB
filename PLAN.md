# MiniDB Refactor Plan

## Why This Project Exists
MiniDB exists to provide a lightweight relational database MVP that demonstrates:
- clear control-plane/data-plane separation,
- high-throughput append-write behavior,
- MVCC `Read Committed` visibility without a heavyweight lock manager,
- Volcano-style execution with delayed materialization.

The target scope is intentionally narrow: single-table `INSERT` and `SELECT` with basic `WHERE` filtering.

## Core Design Philosophy
- Separate concerns physically and logically:
  - Spring Boot handles HTTP and JSON at the boundary.
  - Engine handles storage, MVCC, and query execution.
- Keep hot paths object-light:
  - pointer/byte operations in engine loops,
  - DTO materialization only at API return boundary.
- Favor lock-free or low-contention primitives for MVP concurrency.
- Define behavior through small, explicit contracts (`DbIterator`, `TransactionContext`, `GlobalCommitLog`, `TuplePointer`).

## Current State (As-Is)
- Build layout: single Maven module (`minidb`) with Spring Boot parent.
- Package layout: `com.hunkyhsu.minidb.*`; target package split (`server`, `transaction`, `execution/*`) exists partly but mostly empty.
- Implemented code focus:
  - storage primitives (`DiskManager`, `BufferPoolManager`, `Page`, `LRUReplacer`),
  - heap page/table classes (`TablePage`, `TableHeap`, `Tuple*`),
  - metadata primitives (`Type`, `Schema`, `Column`).
- Missing or incomplete target capabilities:
  - no REST endpoints for `/api/select` and `/api/insert`,
  - no explicit engine/server module boundary,
  - no `GlobalCommitLog` / `TransactionContext` snapshot visibility contract,
  - no Volcano iterator contracts (`DbIterator`, `SeqScanNode`, `FilterNode`),
  - no `TuplePointer`-first execution path.
- Build health:
  - `./mvnw -q test` currently fails at compile stage due to syntax placeholders in heap classes (e.g., `TableHeap`, `TableIterator`).

## Spec vs Implementation Gap Summary
- Multi-module architecture: **Missing**.
- Engine zero third-party dependency boundary: **Missing** (single Spring Boot module).
- `org.minidb.access/tx/execution/catalog` package architecture: **Missing**.
- MMap-based channel abstraction (`MMapFileChannel`): **Missing**.
- Pointer-based tuple flow (`TuplePointer`) in operators: **Missing**.
- Lock-free CLOG transaction state tracking: **Missing**.
- Transaction snapshot visibility function `isVisible(xmin)`: **Missing**.
- Volcano executor contracts and nodes: **Missing**.
- Late materialization at server boundary: **Missing**.
- Insert/select MVP end-to-end flow: **Not integrated**.

## Technology Stack
### Current
- Java 17
- Maven (single module)
- Spring Boot 3.5.9 (in root module)
- Lombok (optional)
- JUnit 5

### Target
- Java 17+
- Maven multi-module:
  - `minidb-server` (Spring Boot Web)
  - `minidb-engine` (pure Java, no Spring)
- JUnit 5 for both modules
- Engine tests focused on storage/MVCC/execution correctness and concurrency behavior

## Directory Structure To Create or Refactor
```text
/
  pom.xml                          # aggregator
  AGENTS.md
  BLUEPRINT.md
  MiniDBReport.md
  minidb-server/
    pom.xml
    src/main/java/org/minidb/server/
      MinidbServerApplication.java
      api/QueryController.java
      dto/InsertRequest.java
      dto/SelectRequest.java
      dto/RowDto.java
      config/EngineWiring.java
    src/test/java/org/minidb/server/
  minidb-engine/
    pom.xml
    src/main/java/org/minidb/
      access/
        MMapFileChannel.java
        TuplePointer.java
        PageLayout.java
      tx/
        GlobalCommitLog.java
        TransactionContext.java
        TransactionManager.java
      execution/
        DbIterator.java
        SeqScanNode.java
        FilterNode.java
        predicates/
      catalog/
        CatalogManager.java
        TableMetadata.java
      storage/
        AppendOnlyTableStore.java
    src/test/java/org/minidb/...
```

## Implementation Phases (Layered, With Refactoring Tasks and Verification)
### Phase 0: Baseline Stabilization
Tasks:
- Capture current compile failures and freeze baseline.
- Remove placeholder/broken code paths that block compilation.
- Decide temporary compatibility strategy for old heap APIs (deprecate or isolate).

Verification:
- `./mvnw -q test` on current branch reaches test execution (not compile failure).
- A short baseline report lists remaining failing tests and why.

### Phase 1: Build and Module Refactor
Tasks:
- Convert root `pom.xml` into aggregator.
- Create `minidb-server` and `minidb-engine` modules.
- Move Spring Boot entrypoint into `minidb-server`.
- Move storage/engine code into `minidb-engine`; remove Spring dependencies from engine.
- Introduce package namespace transition plan to `org.minidb.*`.

Verification:
- `mvn -q -pl minidb-engine test` passes independently.
- `mvn -q -pl minidb-server test` starts Spring context with engine wiring.
- Module dependency graph: server -> engine only.

### Phase 2: Physical Access Layer Refactor (`access`)
Tasks:
- Introduce `MMapFileChannel` abstraction around mapped files.
- Define `TuplePointer` and use it as scan unit.
- Implement append-only storage writer with atomic offset allocation.
- Keep tuple header layout with `xmin` at fixed parseable position.
- Refactor old page/tuple APIs to avoid per-row object churn in scan path.

Verification:
- Unit tests for mapped write/read and pointer decode (pageId/offset).
- Concurrency test: parallel inserts produce unique, ordered append regions.
- Allocation and scan benchmarks run without creating tuple objects per row in hot loop.

### Phase 3: Transaction Layer (`tx`)
Tasks:
- Implement `GlobalCommitLog` using primitive compact storage.
- Implement statement-level `TransactionContext` snapshot with `isVisible(xmin)`.
- Add transaction id allocation policy and watermarks.
- Refactor any lock-manager assumptions out of read path.

Verification:
- Deterministic tests for each visibility branch:
  - own txn visible,
  - future txn invisible,
  - historical committed visible,
  - active-window in-flight invisible,
  - active-window committed visible.
- Concurrency tests for commit-state atomic transitions.

### Phase 4: Execution Layer (`execution`)
Tasks:
- Define `DbIterator` contract (`open/next/close`).
- Implement `SeqScanNode` to read pointers and apply `isVisible`.
- Implement `FilterNode` predicate evaluation on bytes/pointer views.
- Ensure iterator lifecycle and failure cleanup are deterministic.

Verification:
- Unit tests for iterator lifecycle and row counts.
- Integration tests for `SeqScan + Filter` against mixed visible/invisible tuples.
- No DTO creation inside iterator pipeline.

### Phase 5: Control Plane Integration (`server`)
Tasks:
- Implement REST APIs:
  - `POST /api/insert`
  - `GET /api/select`
- Build request/response DTOs and engine adapter.
- Enforce late materialization only when returning API payload.

Verification:
- Controller tests for API contracts and status codes.
- End-to-end tests: insert then select with predicate and visibility correctness.
- Boundary test proving server layer is the only materialization point.

### Phase 6: Catalog and Metadata Hardening (`catalog`)
Tasks:
- Introduce singleton `CatalogManager` for table metadata.
- Consolidate schema/column/type metadata to stable engine contracts.
- Remove duplicated metadata models if both old and new coexist.

Verification:
- Catalog tests for create/load/get metadata.
- Engine integration tests consume catalog metadata without reflection hacks.

### Phase 7: Cleanup and Final Refactor Pass
Tasks:
- Remove deprecated legacy classes and dead test code.
- Unify naming/package conventions under `org.minidb`.
- Add architecture guardrails in CI (module dependency + package rules).
- Update docs and runbook for development workflow.

Verification:
- Full `mvn -q test` passes at root.
- Static checks validate no Spring dependency in engine module.
- Architecture checklist in `AGENTS.md` is satisfied.

## Success Criteria
- Project compiles and tests pass in a two-module structure.
- Engine module is pure Java and independent from Spring Boot.
- End-to-end MVP flow works:
  - concurrent append `INSERT`,
  - snapshot-based `SELECT` visibility,
  - Volcano scan/filter pipeline,
  - late materialization at API boundary.
- Key contracts exist and are tested: `MMapFileChannel`, `TuplePointer`, `GlobalCommitLog`, `TransactionContext`, `DbIterator`, `SeqScanNode`, `FilterNode`.
- Refactor eliminates current compile-breaking placeholder code and old/new API drift.

## Risks and Mitigations
- Risk: large API churn breaks too much at once.
  - Mitigation: phase-gated migration with temporary adapters and compile-first milestones.
- Risk: performance goals regress while refactoring for architecture.
  - Mitigation: add microbenchmarks and object-allocation assertions in hot paths.
- Risk: MVCC correctness bugs under concurrency.
  - Mitigation: deterministic visibility matrix tests plus high-contention randomized tests.

## Out of Scope (Current MVP Refactor)
- Multi-table joins.
- Secondary indexes and complex optimizer work.
- Serializable isolation and full `UPDATE/DELETE` version-chain semantics.
