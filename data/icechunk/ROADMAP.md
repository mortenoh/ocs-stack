# icechunk roadmap

Learning [icechunk](https://icechunk.io): versioned, transactional storage for
Zarr v3. Every dataset in
[open-climate-service](https://github.com/dhis2/open-climate-service) lives in
one — `{data_dir}/downloads/{dataset_id}.icechunk` — so this is the storage
layer under everything the earlier projects computed.

What icechunk adds to plain zarr: writes are transactions that commit or do
not, history is a chain of immutable snapshots you can read at any point,
branches and tags name those points, and readers never see a half-written
store. That last property is what makes it safe to append to a dataset that is
simultaneously being served over HTTP.

Helpers live in `src/playground_data_icechunk/`: repository open, commit-and-
write, read-at-a-point, history listing, and OCS-shaped synthetic data.

## Phase 1 — Repositories, sessions, commits

- [x] `0101_repo_basics` — create/open a repository; what is on disk; sessions
- [x] `0102_commits` — write, commit, and see the store change only at commit time
- [x] `0103_history` — walk ancestry; snapshot ids, messages, timestamps

## Phase 2 — Time travel

- [x] `0201_reading_the_past` — read any snapshot; the store as of an older commit
- [x] `0202_tags_and_branches` — name a snapshot with a tag; branch for an experiment
- [x] `0203_diffing` — what changed between two snapshots

## Phase 3 — Transactions and safety

The properties that make icechunk different from a directory of chunk files.

- [x] `0301_atomicity` — a failed write leaves no trace; readers never see a partial store
- [x] `0302_isolation` — a reader holding a snapshot is unaffected by concurrent writes
- [x] `0303_conflicts` — two writers on one branch; conflict detection and rebasing

## Phase 4 — The OCS ingest pattern

- [x] `0401_append_periods` — append one period at a time, committing each; the streaming shape
- [x] `0402_resume` — read committed time steps to work out where an interrupted ingest stopped
- [x] `0403_rewriting_history` — correcting a bad period, and why the old snapshot survives

## Phase 5 — Operations

- [x] `0501_storage_growth` — how snapshots share chunks; what a commit actually costs
- [x] `0502_expiry_and_gc` — expiring old snapshots and garbage-collecting unreachable chunks
