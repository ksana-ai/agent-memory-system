# ADR 0004: Fenced embedding projection worker

- Status: accepted
- Date: 2026-08-19

## Context

ADR 0003 made reviewed-card projection durable by committing a content-free job with the approved card. A durable row alone is insufficient: the processor must survive crashes, avoid holding PostgreSQL locks during embedding HTTP, prevent a late result from restoring superseded or erased data, and bind every vector to an explicitly approved public-probe fingerprint rather than a mutable model alias.

Caller wall clocks cannot safely decide leases across processes. A startup-only model probe is also insufficient because a long-running local endpoint can reload a different model behind the same name. Finally, automatic target registration on worker startup would turn an endpoint drift into an implicit deployment decision.

## Decision

`cmd/projection-worker` has three explicit environment-configured modes:

- `probe` calls the live endpoint and reports the derived behavior fingerprint and embedding-space ID without opening PostgreSQL;
- `register-shadow` requires that exact expected space and explicitly creates its immutable definition plus a shadow target;
- `run` requires an existing matching target and never auto-registers a missing or drifted space.

The v1 runtime processes one job at a time:

1. PostgreSQL locks the target against a state update, reads `clock_timestamp()`, and leases one pending/retry or expired-lease job with `FOR UPDATE SKIP LOCKED`. Claim increments both attempt count and lease version.
2. The claim transaction commits before external I/O. No PostgreSQL transaction or scope lock is held while the worker calls LM Studio.
3. One bounded provider request contains `[ProbeTextV1, reviewed-memory-card-json-v1]`. The returned probe vector must have the registered exact float32 behavior hash before the document vector can be used.
4. Finalization locks scope → target → job → card, then uses one database-clock value to revalidate the fence, target state, active card/version, expiration, document SHA-256, and immutable embedding space.
5. Vector upsert and succeeded acknowledgement commit atomically. A material serving projection advances `context_revision`; a shadow projection does not.

PostgreSQL time is authoritative for lease eligibility, expiry, retry availability, card expiry, and finalization. Go monotonic elapsed time is used only as a conservative outbound-I/O budget: if claim plus processing consumes the configured lease duration, the worker starts no further request, stops waiting locally, discards any late result, and leaves the lease for database-clock recovery. Context cancellation does not prove provider-side cancellation.

Retryable transport, timeout, rate-limit, and server failures become a fenced durable retry with bounded exponential backoff. A process crash also consumes an attempt when its expired lease is reclaimed; after the configured maximum, the repository dead-letters it before selecting the next job. Permanent protocol/model/vector failures become dead letters. Persisted failures use a closed non-secret error-code vocabulary; endpoint URLs, credentials, input documents, response bodies, and raw provider errors are not stored or returned.

## Consequences

- A killed process leaves a lease that another process can reclaim after database-clock expiry with a higher fence version.
- The old process cannot finalize after reclaim, supersession, erasure, target retirement, or lease expiry.
- A blocked target pauses new claims and requeues a successful in-flight result. Retirement serializes with approval and atomically cancels every pending, retry, or leased job; permanently invalid runnable work is cancelled by claim/finalize lifecycle checks.
- Public-probe behavior is checked on every document request, not just at startup. A mismatch dead-letters the job. This detects probe-visible drift but is neither a weights hash nor proof that all other inputs behave identically.
- Shadow projection is durable but does not change the current FTS server path. Backfill, reconciliation, atomic serving-space rotation, and dense/hybrid query integration remain separate gates.
- Database erasure cascades jobs and vectors, but it cannot recall content already sent to a provider or guarantee physical zeroization of a document already copied into process memory. The local request wait is bounded and late results cannot persist; provider and process-memory retention/deletion need separate contracts.

## Executable evidence

- Repository integration tests cover disjoint claims, expired-lease recovery, retry/dead/cancel fencing, database-clock scheduling, atomic rollback, serving-versus-shadow revision behavior, target-state serialization, supersession, expiration, and erasure.
- A process test blocks embedding HTTP, proves a same-scope write can commit without waiting on the provider, kills the worker, waits on PostgreSQL time, starts a new process, observes attempt/fence version 2, finalizes one vector, erases the scope, and proves a third run cannot resurrect it.
- A live LM Studio process test registers a shadow space, projects one reviewed card, inspects the succeeded job/vector, and verifies erasure propagation.
- A live client test compares a single public-probe fingerprint with the probe at index zero of six two-item batches across short, multilingual, and long document shapes.

These are local component/process facts, not deployment, load, remote-provider privacy, or production-retrieval evidence.

## Rejected alternatives

- **Hold a scope transaction across embedding HTTP:** converts provider latency/outage into lifecycle lock contention.
- **Use worker wall-clock timestamps for leases:** permits skewed processes to reclaim too early or retain crashed leases too long.
- **Finalize vector and job acknowledgement separately:** recreates a crash gap and ambiguous revision changes.
- **Trust only lease owner without a version:** allows a reclaimed old attempt to collide with later ownership.
- **Probe only at startup:** misses probe-visible endpoint drift that occurs while the worker remains running.
- **Auto-register the live model on normal startup:** silently turns model drift into an enabled deployment target.
- **Claim a serial batch larger than one:** spends later jobs' lease budgets while earlier provider calls run.
