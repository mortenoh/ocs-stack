# icechunk

**Versioned storage.** Versioned, transactional storage for Zarr v3 -- the layer under every open-climate-service dataset. Writes are transactions that commit or do not; history is a chain of immutable snapshots; readers never see a half-written store.

```bash
cd icechunk
make install
make run-all
```

14 examples, in phases:

### Phase 1 — Repositories, sessions, commits

- `0101_repo_basics` — create/open a repository; what is on disk; sessions
- `0102_commits` — write, commit, and see the store change only at commit time
- `0103_history` — walk ancestry; snapshot ids, messages, timestamps

### Phase 2 — Time travel

- `0201_reading_the_past` — read any snapshot; the store as of an older commit
- `0202_tags_and_branches` — name a snapshot with a tag; branch for an experiment
- `0203_diffing` — what changed between two snapshots

### Phase 3 — Transactions and safety

- `0301_atomicity` — a failed write leaves no trace; readers never see a partial store
- `0302_isolation` — a reader holding a snapshot is unaffected by concurrent writes
- `0303_conflicts` — two writers on one branch; conflict detection and rebasing

### Phase 4 — The OCS ingest pattern

- `0401_append_periods` — append one period at a time, committing each; the streaming shape
- `0402_resume` — read committed time steps to work out where an interrupted ingest stopped
- `0403_rewriting_history` — correcting a bad period, and why the old snapshot survives

### Phase 5 — Operations

- `0501_storage_growth` — how snapshots share chunks; what a commit actually costs
- `0502_expiry_and_gc` — expiring old snapshots and garbage-collecting unreachable chunks
