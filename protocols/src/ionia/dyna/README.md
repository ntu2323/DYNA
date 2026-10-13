# DYNA

The final DYNA implementation: multi-key, multi-partition transactions over a
shared-replica topology (`N = 2f+1` servers; every server stores all `N` partitions;
server `i` leads partition `i`, follows the rest). Write **and** read paths are complete
(deadline ordering → speculative release → fast/slow multi-leader commit → background
ts-agreement → replication → dependency-gated disk execution → leader reads).

- **Ground truth:** `dyna_algo_design.txt` (algorithm) and `spec.md` (§1–2).
- **Staging / design notes:** `implementation_plan.md`.
- **Architecture summary:** see `Dyna/CLAUDE.md` (§ Architecture → `dyna`).

The **`MockClock`-driven unit tests in `tests/` are the authoritative correctness gate**
(deterministic); the UDP smoke confirms the wire path end-to-end.

---

## Testing in Docker

All builds/tests run inside the `ionia-udp` Docker image (the host lacks the toolchain).
Run every command below from the `Ionia/` directory.

### 0. Build the image (one-time; rebuild after any `dyna/` source change)

The image `COPY`s the repo and bakes it in, so the `docker run --rm` commands below use
whatever source was present at build time. Rebuild it whenever you edit the code.

```bash
docker build --platform linux/amd64 -f docker/Dockerfile.udp -t ionia-udp .
```

> First build is slow (compiles Folly + SplinterDB); subsequent rebuilds cache those.

Each check below runs in a throwaway container (`--rm`), compiles the relevant test, and
runs it. A green run ends with `[  PASSED  ]`.

### Check WRITE correctness

Writes execute in dependency order and apply the correct values to the KV store
(`apply`, stored with their commit timestamp), with w-w conflicts serialized by `OrderKey`.
The store is the harness DB (SplinterDB by default; `dyna-storage-test` covers the
`DbBackend` + value encoding); commit never waits on it (invariant 2).

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   make dyna/tests/dyna-exec-test && \
   ./dyna/tests/dyna-exec-test'
```

Supporting commit/durability logic (leader quorum, replication to COMMITTED):

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   for t in quorum orderer-commit replicate; do \
     make dyna/tests/dyna-$t-test >/dev/null && \
     ./dyna/tests/dyna-$t-test; done'
```

### Check READ correctness

A leader read returns the committed value once its conflicting write has executed; a read
is held until then; a late read at a pure follower is dropped silently (the client retries
at a larger t); an on-time follower read endorses without logging.

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   make dyna/tests/dyna-read-test && \
   ./dyna/tests/dyna-read-test'
```

### Check SLOW-PATH correctness

Two involved leaders assign different timestamps (one on-time, one late-bumped) ⇒ the
write does **not** fast-commit; the client resolves to `SlowCommit(max)`, both servers
adopt the agreed max (`COMMITTING`), and the `CommitReply` quorum re-forms at that max.
Also: a follower that silently buffered the late write adopts the max on `SlowCommit`.

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   make dyna/tests/dyna-slowpath-test && \
   ./dyna/tests/dyna-slowpath-test'
```

### Run the whole unit-test suite (all 13 binaries, 93 tests)

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   for t in types indexed-pq orderer-release quorum orderer-commit \
            tsagree replicate exec read slowpath robust storage recovery; do \
     make dyna/tests/dyna-$t-test >/dev/null 2>&1 && \
     printf "%-16s " "$t" && \
     ./dyna/tests/dyna-$t-test 2>&1 | grep -E "PASSED|FAILED" | tail -1; done'
```

### End-to-end (3-replica UDP smoke; the trace interleaves writes and reads)

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'SMOKE_PROTOCOL=dyna bash docker/run_udp_smoke.sh'
```

Expect `UDP smoke test finished successfully.` (rc=0). `DYNA_DEBUG=1` logs the
request/reply/quorum flow.

### Recovery smoke (kill a replica mid-run)

```bash
SMOKE_PROTOCOL=dyna SMOKE_KILL_REPLICA=2 ./docker/docker-smoke.sh   # or 0 = the VR leader
```

`kill -9`s the replica 1.5 s (`SMOKE_KILL_AFTER_MS`) into an 8 s closed-loop run (bound
20 ms, `DYNA_FAIL_TIMEOUT_MS=500`) and fails unless every survivor logs
`view 1 installed`; it then prints how many requests the client completed across the
failure. With `sample/test` the trace keys are homed at servers 0 and 2, so kill 0 or 2
to exercise a range takeover (killing 1 still exercises the view change, with an empty
delta-p). Add `SMOKE_DYNA_DB=splinter` for real SplinterDB.

Rejoin: add `SMOKE_RESTART_AFTER_MS=2000` — the killed replica is restarted (log
`replica-<i>.restart.log`); it finds `dyna-vr-<i>.state`, catches up as a non-member and is
re-added; the run requires `view 2 installed` on all three replicas.

---

## Notes

- **Always use a fresh container for the smoke** (`docker run --rm` does this). A reused
  container accumulates on-disk SplinterDB state; the replica app-init then aborts at
  startup (`rc_allocator_mount` assertion) *before any DYNA code runs* — an environment
  artifact, not a protocol bug.
- **Deadlines.** DYNA followers silently buffer *late* writes (by design); when the client
  then lacks `f+1` matching replies it takes the slow path (`SlowCommit` → `CommitReply`)
  after `DYNA_SLOW_TIMEOUT_US`. The smoke bakes a generous deadline
  (`DYNA_INITIAL_BOUND_US=200000`, non-busy-poll) so the shared-CPU container mostly takes the
  fast path; `DYNA_INITIAL_BOUND_US=50 DYNA_MAX_OWD_US=100` forces late followers and
  exercises the slow path. The smoke counts a client timeout as success, so check
  `client.log` for `Completed N requests`.
- **Iterating on code:** to avoid a full image rebuild each edit, run a detached
  container and `docker cp` the current source in before `make`:
  ```bash
  docker run -d --name dm --platform linux/amd64 ionia-udp sleep infinity
  docker cp ./protocols/src/ionia/dyna/. dm:/work/ionia/protocols/src/ionia/dyna/
  docker cp ./docker/run_udp_smoke.sh dm:/work/ionia/docker/run_udp_smoke.sh   # for the smoke
  docker exec dm bash -lc 'cd protocols/src/ionia && make dyna/tests/dyna-read-test && ./dyna/tests/dyna-read-test'
  docker rm -f dm
  ```
  (Recreate `dm` fresh before running the smoke, per the note above.)

## What each correctness check maps to

| Check      | Test binary                    | Key assertions |
|------------|--------------------------------|----------------|
| Write      | `dyna-exec-test`         | committed writes apply correct values; w-w in `OrderKey` order; non-conflicting reorder; disk async |
| Read       | `dyna-read-test`         | leader read returns the committed value; read blocked until conflicting write executes; late follower read dropped silently; on-time follower endorse |
| Robustness | `dyna-robust-test`       | no veto drop; commit never downgrades / re-applies after GC; lost TsAgree / ReplicateCommit recover; event-driven executor serves reads |
| Slow path  | `dyna-slowpath-test`     | leader disagreement → `SlowCommit(max)` → servers adopt → quorum re-forms at the max; follower adopts from lateBuffer |
| Recovery   | `dyna-recovery-test`     | consistent-hash takeover; M merge precedence; fast-committed (leader dead) / slow-committed writes survive at their t; executed key-values recovered (t-guarded); leftovers kept; no downgrade / duplicate; VR view change converges (follower or VR-leader death, message loss, n=5 sequential failures, VR leader dying after a partial StartView → recover from the oldest last-normal view); fencing |
| End-to-end | `run_udp_smoke.sh`             | 3 real UDP replicas commit a mixed write/read trace |
| Rejoin     | `dyna-recovery-test` (`Rejoin.*`) | restarted server rejoins (follower / VR leader); restart before detection; falsely excluded server rejoins; whole-cluster restart starts fresh (incl. staggered); joiner never votes before caught up; snapshot paging + t-guard; home range handed back with state; executed-write resend answered |
| Failover   | `run_udp_smoke.sh` + `SMOKE_KILL_REPLICA` | survivors install view 1; the client keeps completing requests |
| Rejoin E2E | `run_udp_smoke.sh` + `SMOKE_RESTART_AFTER_MS` | the restarted replica rejoins; all install view 2; the client keeps completing requests |
