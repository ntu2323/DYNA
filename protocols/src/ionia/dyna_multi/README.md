# DYNA-Multi

The final DYNA implementation: multi-key, multi-partition transactions over a
shared-replica topology (`N = 2f+1` servers; every server stores all `N` partitions;
server `i` leads partition `i`, follows the rest). Write **and** read paths are complete
(deadline ordering → speculative release → fast/slow multi-leader commit → background
ts-agreement → replication → dependency-gated disk execution → leader reads).

- **Ground truth:** `Dyna-Algorithm - Google Docs.pdf` (algorithm) and `spec.md` (§1–2).
- **Staging / design notes:** `implementation_plan.md`.
- **Architecture summary:** see `Dyna/CLAUDE.md` (§ Architecture → `dyna_multi`).

The **`MockClock`-driven unit tests in `tests/` are the authoritative correctness gate**
(deterministic); the UDP smoke confirms the wire path end-to-end.

---

## Testing in Docker

All builds/tests run inside the `ionia-udp` Docker image (the host lacks the toolchain).
Run every command below from the `Ionia/` directory.

### 0. Build the image (one-time; rebuild after any `dyna_multi/` source change)

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
(`mem_apply`), with w-w conflicts serialized by `OrderKey` and disk persistence async
(invariant 2).

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   make dyna_multi/tests/dyna-multi-exec-test && \
   ./dyna_multi/tests/dyna-multi-exec-test'
```

Supporting commit/durability logic (leader quorum, replication to COMMITTED):

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   for t in quorum orderer-commit replicate; do \
     make dyna_multi/tests/dyna-multi-$t-test >/dev/null && \
     ./dyna_multi/tests/dyna-multi-$t-test; done'
```

### Check READ correctness

A leader read returns the committed value once its conflicting write has executed; a read
is held until then; a late read at a pure follower is rejected; an on-time follower read
endorses without logging.

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   make dyna_multi/tests/dyna-multi-read-test && \
   ./dyna_multi/tests/dyna-multi-read-test'
```

### Check SLOW-PATH correctness

Two involved leaders assign different timestamps (one on-time, one late-bumped) ⇒ the
write does **not** fast-commit; the client resolves to `SlowCommit(max)`, both servers
adopt the agreed max (`COMMITTING`), and the `CommitReply` quorum re-forms at that max.
Also: a follower that silently buffered the late write adopts the max on `SlowCommit`.

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   make dyna_multi/tests/dyna-multi-slowpath-test && \
   ./dyna_multi/tests/dyna-multi-slowpath-test'
```

### Run the whole unit-test suite (all 10 binaries)

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'cd protocols/src/ionia && \
   for t in types indexed-pq orderer-release quorum orderer-commit \
            tsagree replicate exec read slowpath; do \
     make dyna_multi/tests/dyna-multi-$t-test >/dev/null 2>&1 && \
     printf "%-16s " "$t" && \
     ./dyna_multi/tests/dyna-multi-$t-test 2>&1 | grep -E "PASSED|FAILED" | tail -1; done'
```

### End-to-end (3-replica UDP smoke; the trace interleaves writes and reads)

```bash
docker run --rm --platform linux/amd64 ionia-udp bash -lc \
  'SMOKE_PROTOCOL=dyna_multi bash docker/run_udp_smoke.sh'
```

Expect `UDP smoke test finished successfully.` (rc=0). `DYNA_MULTI_DEBUG=1` logs the
request/reply/quorum flow.

---

## Notes

- **Always use a fresh container for the smoke** (`docker run --rm` does this). A reused
  container accumulates on-disk SplinterDB state; the replica app-init then aborts at
  startup (`rc_allocator_mount` assertion) *before any DYNA code runs* — an environment
  artifact, not a protocol bug.
- **Deadlines.** DYNA followers silently drop *late* writes (by design), so a write's
  `f+1` commit needs on-time delivery at ≥ `f+1` replicas. The smoke bakes a generous
  deadline (`DYNA_INITIAL_BOUND_US=200000`, non-busy-poll) for the shared-CPU container;
  under WAN with synchronized clocks and measured OWD the default tight bound is used.
- **Iterating on code:** to avoid a full image rebuild each edit, run a detached
  container and `docker cp` the current source in before `make`:
  ```bash
  docker run -d --name dm --platform linux/amd64 ionia-udp sleep infinity
  docker cp ./protocols/src/ionia/dyna_multi/. dm:/work/ionia/protocols/src/ionia/dyna_multi/
  docker cp ./docker/run_udp_smoke.sh dm:/work/ionia/docker/run_udp_smoke.sh   # for the smoke
  docker exec dm bash -lc 'cd protocols/src/ionia && make dyna_multi/tests/dyna-multi-read-test && ./dyna_multi/tests/dyna-multi-read-test'
  docker rm -f dm
  ```
  (Recreate `dm` fresh before running the smoke, per the note above.)

## What each correctness check maps to

| Check      | Test binary                    | Key assertions |
|------------|--------------------------------|----------------|
| Write      | `dyna-multi-exec-test`         | committed writes apply correct values; w-w in `OrderKey` order; non-conflicting reorder; disk async |
| Read       | `dyna-multi-read-test`         | leader read returns the committed value; read blocked until conflicting write executes; late-follower reject; on-time follower endorse |
| Slow path  | `dyna-multi-slowpath-test`     | leader disagreement → `SlowCommit(max)` → servers adopt → quorum re-forms at the max; follower adopts from lateBuffer |
| End-to-end | `run_udp_smoke.sh`             | 3 real UDP replicas commit a mixed write/read trace |
