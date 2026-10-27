# Audio parent: concurrent parity task packets

Control paths `docs/parallel/status/audio.md`, `docs/parallel/handoffs/`, `docs/parallel/proposals/` and `docs/parallel/qualification/` are relative to the canonical workspace `/Users/pmarko.alt/Desktop/track-tree`, outside this child repository. The audio parent is the sole status writer; each child owns its own root handoff. Keep mutable control files outside child source hashes.

Prepared 2026-10-23. This is a launch plan, not implementation or passing runtime evidence. The current audio baseline is `e5b4c91fd726c89ab43cec446739dacb63c70f08`; recheck HEAD, worktree and the root release tuple before starting. Batch20 added no audio source changes.

Read [root coordination](../PARITY_COORDINATION.md), [workspace instructions](../AGENTS.md), [architecture](../docs/e2e/ARCHITECTURE.md), [historical coordination](../docs/e2e/COORDINATION.md), [runtime qualification](../docs/e2e/INFERENCE_RUNTIME_UPGRADE.md) and [audio runtime handoff](../docs/e2e/evidence/INFERENCE_UPGRADE_HANDOFF.md). The root coordinator is the **contracts parent**, not a fifth implementation parent. This audio parent owns only `track-tree-audio` and its own [root status file](../docs/parallel/status/audio.md). Children write their individual handoffs; they do not update shared status or root coordination.

The initial three child packets jointly deliver **RND-1**, the bounded A02 rendition foundation. They are three independent ownership areas within one cross-repository feature, not three completed competitor capabilities. API owns durable jobs, authorization, funding and preallocated objects; contracts owns the wire/schema/goldens; web owns playback. Audio executes approved work and reports exact measurements. UPL-1 multipart uploads and BRAND-1 presentation need no audio source edits in this cohort.

## Repository ownership and integration bottlenecks

| Owner | May write | Must request through parent |
| --- | --- | --- |
| AUDIO-21-01 | New `src/rendition_runner.py`, `test_rendition_runner.py`, `docs/parallel/qualification/RND-1-profiles.md` | Existing inference/supervision helpers, shared environment, profile registration |
| AUDIO-21-02 | New `src/rendition_jobs.py`, strict Python wire binding in `src/rendition_models.py`, `test_rendition_jobs.py` | HTTP routes, Celery registration, state-key namespace and shared transfer/callback helpers |
| AUDIO-21-03 | New `test_rendition_security.py`, `test_rendition_runtime.py`, `docs/parallel/qualification/RND-1-runtime.md` | Production runner/transport changes discovered by tests; canonical fixture corrections |
| Audio parent only | `src/main.py`, `src/models.py`, `src/queues.py`, `src/state.py`, `src/security.py`, `src/env.py`, `src/demucs_runner.py`, `src/inference.py`, `src/s3.py`, `src/webhook.py`, `test_basic.py`, `README.md`, `pyproject.toml`, `uv.lock`, `Dockerfile`, own parent status | Other repositories, root coordination/runtime leases and canonical protocol |

Paths for new modules are proposed ownership reservations. The parent confirms they remain unused before assigning them. A child returns a proposed patch or explicit helper request for parent-owned files; it never edits them concurrently. The parent performs small shared integrations serially and notifies affected children of the resulting digest. No wholesale split-worker refactor is required to start renditions.

Current source inspection matters: transfers, subprocess supervision, leases, recovery and independent callback retries are concentrated in `src/demucs_runner.py`. `src/s3.py` and `src/webhook.py` currently contain descriptive stubs, not reusable implementations. There is no `src/tasks.py`. Do not build a second transfer/security implementation merely because a convenient filename exists. Reuse reviewed behavior through a parent-owned narrow extraction where needed, with existing split regressions.

## Readiness and immediate work

| Packet | Start immediately | Production implementation gate |
| --- | --- | --- |
| AUDIO-21-01 | Inspect existing managed FFmpeg/FFprobe and supervision; define finite original fixture corpus, CPU/disk/time budgets and measurement procedure | Run measurement only with a granted private runtime lease; freeze selected profile/timing/channel/gain policy with contracts/API/web before implementing advertised profiles |
| AUDIO-21-02 | Review actual signed enqueue, Redis leases, refresh and callback state machine; propose rendition-specific lifecycle and shared-helper requests | Exact `C-21-01` canonical protocol/fixtures and parent-approved resource/state interfaces; profile allowlist policy recorded |
| AUDIO-21-03 | Prepare adversarial fixture cases, fault matrix and independent resource measurement plan | Canonical fixture release to write conformance expectations; assembled runner/transport and runtime lease to execute actual checks |

When a gate is pending, return a concrete design/test artifact or dependency question in the child handoff and continue other permitted inspection. Do not invent DTOs, fixed quality promises or alternate model behavior to keep a child busy. The parent may keep fewer than three coding workers active when dependencies or advertised runtime limits require it.

## AUDIO-21-01 — Bounded rendition engine and profile evidence

**Capability:** A02 private playback renditions; RND-1. **Parent dependency:** contracts `C-21-01`, API `API-21-01`, web `WEB-21-01` consume the same measured profile decision.

Deliver an isolated CPU FFmpeg runner for the selected finite stereo profiles without modifying original source bytes. Do not choose codec, bitrate, sample rate, loudness normalization, channel conversion or profile IDs by copying an advertised vendor setting. First prepare a bounded benchmark proposal with source provenance, fixture sizes/durations, maximum concurrent processes, wall time, temporary bytes and explicit stop conditions. The coordinator records the resulting supported profile matrix and tolerances before a profile becomes a production allowlist entry. Lossless and compressed candidates can be measured; their successful measurement is separate from selecting them for release.

The runner accepts only a server-selected, versioned profile from that allowlist and a private local input/output allocation supplied by the trusted job layer. It must not accept uploaded model paths, arbitrary FFmpeg switches, callback destinations or tenant output keys. Subprocess invocation uses argument arrays and managed executable paths, bounded stdout/stderr, no shell interpolation, finite process/thread/disk/output limits and guaranteed child reaping on cancel, timeout and parent loss. Originals stay available if optional rendition processing fails. Unsupported channel layouts remain explicitly unsupported for rendition rather than being silently downmixed.

Acceptance:

- **PAR-RND-ENGINE-01:** Actual managed FFmpeg/FFprobe encode and independent decode of the chosen formats; source checksum remains exact. Record actual executable/version/OS and profile revision.
- **PAR-RND-ENGINE-02:** Measure codec priming/padding, decoded duration/sample count, reference impulse/seek alignment, channel identity, finite samples, loudness and true/sample peak as appropriate. Declare tolerances and observed exceptions; no unexplained offset or clipping. Synthetic fixtures establish timing/plumbing, not blind musical quality.
- **PAR-RND-ENGINE-03:** Exercise declared short/long and near-limit valid fixtures plus corrupt/mislabeled/truncated inputs. Actual CPU, peak process RSS where measured, temporary/output bytes and elapsed cost are reported with method and limits. An allocation cap is not an RSS guarantee.
- **PAR-RND-ENGINE-04:** Real cancellation, deadline and parent-loss cases leave no live child or unbounded temporary file. Failure does not alter originals or masquerade as a completed rendition.

Handoff: `docs/parallel/handoffs/AUDIO-21-01.md`, linking `docs/parallel/qualification/RND-1-profiles.md`, exact owned files/digests, commands, finite corpus declaration, measured results and unselected/unsupported profiles. Private logs/audio stay outside Git. Do not fetch copyrighted music or new engines/checkpoints to satisfy the corpus.

## AUDIO-21-02 — Signed rendition execution and independent callback delivery

**Capability:** A02; RND-1. **Prerequisites:** frozen `C-21-01` request/response/refresh/cancel/callback shapes and exact-byte fixtures; API-owned source/output allocation and lifecycle; selected profile allowlist. New route/task names are not approved by this planning document.

Implement a dedicated rendition task type. Do not add a fake Demucs model or overload the existing split request/callback to represent codec conversion. A strict Python binding may validate the canonical schema/fixtures, but it cannot define a competing wire protocol. Existing split HTTP behavior, checkpoint allowlist and stem types remain compatible.

Reuse exact raw-body timestamped HMAC verification and configured transfer/callback destination validation. Bind immutable source/version/profile revision/output allocations in the idempotency identity; renewable URL credentials cannot silently change that identity. Distinguish rendition keys/task names from inference jobs even if their UUID strings collide. Keep bounded pending/executing/callback delivery/recovery state in a namespaced worker namespace, while the API/Postgres remains the authority for application status and final publication.

The API allocates quota and output keys; the worker needs no PostgreSQL, Clerk, Stripe or long-lived storage credentials. Refresh only through the frozen signed endpoint for the known job. Reject unknown profiles, changed allocations, unsafe redirect destinations, excess outputs and unapproved callbacks. Transfers have finite connection/read/write/total/output bounds, not an unbounded streaming loop. Persist the immutable terminal callback separately from execution so delivery retries and restarts cannot rerun a successful encode. Recheck current cancellation/expiry through the agreed protocol before transfer/publication; the API must independently reject stale terminal results.

Acceptance:

- **PAR-RND-JOBS-01:** Canonical exact-byte positive/negative fixtures validate across languages; stale/tampered/invalid HMAC and schema/destination/allocation negatives fail before compute.
- **PAR-RND-JOBS-02:** Same signed job retries converge on one logical execution; changed source/profile/allocation conflicts. Queue publish failure after durable worker state is recoverable without duplicate outputs.
- **PAR-RND-JOBS-03:** Namespaced real Redis/Celery execution demonstrates lease exclusivity, restart/recovery and URL refresh with expired credentials. No shared Redis flush or broad prefix deletion.
- **PAR-RND-JOBS-04:** Callback receiver failure/restart/retry converges from persisted exact payload without recomputing; late canceled/archived/superseded results cannot become published media. Record actual versus controlled receiver/state boundaries.
- **PAR-RND-JOBS-05:** Existing split exact-byte signatures, lightweight health/import, supported `htdemucs` enqueue, independent split callback retry and absence of Redis result subscriptions remain intact.

Handoff: `docs/parallel/handoffs/AUDIO-21-02.md`, including canonical contract release/digest, API helper/protocol agreement, state/lock/queue names, exact owned files/digests, commands, measured lifecycle evidence, cleanup and unresolved cases. Parent alone mounts HTTP routes and registers tasks/scheduler/configuration after reviewing the child module.

## AUDIO-21-03 — Independent runtime, security and cleanup qualification

**Capability:** A02 acceptance and G04 regression; RND-1. This worker owns adversarial tests and measurement evidence, not a second encoder or production authorization policy.

Build an independent matrix covering both chosen profiles and unsupported source/profile cases. Derive wire expectations from the frozen contracts fixtures; request canonical fixture fixes from the contracts parent rather than rewriting goldens here. Share the planned runner/job function interfaces with the other audio children before tests depend on them. Parent integration is the single resolution point for shared helper changes.

Acceptance:

- **PAR-RND-SEC-01:** Reject caller-supplied executable/model path, flag injection, cross-job key, hostile/redirected media or callback destinations, oversized input/output, duplicate allocations and expired/tampered signatures. Error/log output contains no synthetic capability/URL/header markers.
- **PAR-RND-SEC-02:** Real child-process timeout/cancel/parent-loss, transfer interruption and disk/output budget faults terminate/reap owned processes and clean only exact owned temporary paths. No caller path can escape the work directory.
- **PAR-RND-SEC-03:** Independent decode/channel/timing/peak checks verify AUDIO-21-01 outputs; corrupt output and wrong checksum/size/codec/duration do not produce a successful callback. Report physical limits actually measured.
- **PAR-RND-SEC-04:** Faults between download/encode/upload/callback/recovery yield recoverable, idempotent outcomes. Callback outage does not trigger re-encoding; late results remain terminally rejected by actual API integration.
- **PAR-RND-SEC-05:** New behavior does not weaken `test_basic.py` split protocol/checkpoint/callback tests. If Demucs execution is affected, run the separate modern locked inference qualification in a granted environment; ordinary CPU conversion tests cannot close that gate.

Use an isolated controlled receiver and a unique Redis/queue/object/temp namespace under the coordinator's lease. Controlled transport/state faults are labeled as such. Actual CPU FFmpeg execution, actual Redis delivery, API/private-storage smoke, actual Clerk/browser evidence and intended production hardware are distinct proof levels; one does not substitute for another.

Handoff: `docs/parallel/handoffs/AUDIO-21-03.md` plus `docs/parallel/qualification/RND-1-runtime.md`, with the complete case matrix, exact commands/runtime manifests, private report pointers, retained failures, measured resource/timing results, namespace cleanup and remaining platform limits. Do not label historical package audit results as a new advisory scan or GPU/OS qualification.

## Parent-owned checks, isolation and handoff

Before assignment, inspect the live repository and status board, verify baseline/worktree changes belong to this parent, check the memory architectural constraints and request exact path/runtime leases. Read a child's completed handoff before integrating any shared patch. Existing `.venv-gpu`, inference workers, model caches and preserved qualification environments are not disposable.

Run Python3.11 with the committed uv lock in a separately owned private **core** environment. Never sync the small core dependency set into an active inference environment. Rendition conversion does not justify arbitrary Torch/model/dependency upgrades. Dependency or lock changes, if genuinely necessary, are parent-owned and require a recorded dependency/security/runtime review; no child mutates them opportunistically.

Future checks, **not run by this planning task**, from the workspace root with an absolute private core environment selected:

```sh
UV_PROJECT_ENVIRONMENT=<absolute-private-core-environment> corepack yarn audio:test 2>&1 | compress "Did audio core tests pass without skips? List failures."
```

Focused Python checks run from `track-tree-audio` using that same private environment and committed lock:

```sh
UV_PROJECT_ENVIRONMENT=<absolute-private-core-environment> uv run --frozen pytest -q test_rendition_runner.py 2>&1 | compress "Did rendition runner checks pass? List failures."
UV_PROJECT_ENVIRONMENT=<absolute-private-core-environment> uv run --frozen pytest -q test_rendition_jobs.py 2>&1 | compress "Did signed rendition lifecycle checks pass? List failures."
UV_PROJECT_ENVIRONMENT=<absolute-private-core-environment> uv run --frozen pytest -q test_rendition_security.py test_rendition_runtime.py 2>&1 | compress "Did rendition security/runtime checks pass? List failures."
```

These proposed test filenames do not exist yet. Do not silently skip missing suites or treat their command lines as passing evidence. Use shell `pipefail` when checking command status through `compress`. Any core-environment creation/sync uses the owned private path and frozen lock, never a shared live environment.

The parent returns one audio handoff with exact source HEAD/worktree digest, canonical contract release/hash, selected profile matrix, completed child evidence, shared-file changes, runtime versions and owned cleanup. Record each feature as design-ready, implemented, focused-tested or integration-ready according to the root state machine. Final root integration uses a fresh compatible four-repository tuple with real private storage and the intended worker; staged/production/native/GPU claims require their own evidence. No commits, pushes, deployments or provider messages are authorized merely by this planning document.

In initial shared-checkout mode, repository ownership and exclusive shared-file/runtime leases apply. Never switch shared branches or mutate another parent's source. For continuous writes during release verification, use the root's optional isolated full sibling-workspace layout so `../track-tree-contracts/fixtures` resolves to the frozen contract release. A standalone audio worktree pointed at mutable integration fixtures is not isolated proof. Different Redis prefixes do not isolate CPU/disk/memory; serialize expensive benchmarks/inference on the resource lease.

## Later audio queue

| Order | Packet family | Next useful work | Gate |
| --- | --- | --- | --- |
| 1 | A02 follow-up / A03-A05 support | Broader profiles, lineage/cost/timing refinements and queue prebuffer support requested by API/web | RND-1 integrated; measured quality and current media access policy |
| 2 | A06/A07 | Spatial source/device feasibility and only then qualified rendering | Format/device/rights matrix; no spatial claim from multichannel input acceptance |
| 3 | X02 | Licensed BPM/key/genre benchmark and provenance/confidence artifacts | Labeled corpus, engine/license and override/search contract |
| 4 | X01/X07 | Mastering or transcription/caption engine evaluation | Explicit engine/data-use/cost decision and perceptual/accuracy target |
| 5 | X13/X14 | Forensic watermark/detection feasibility | Licensed engine, privacy/retention decision, independent quality/false-positive corpus |
| continuous | G03/X22 | Intended hardware/native/OS/staging and capacity/unit-economics qualification | Exact pinned runtime plus configured bounded environment; no reuse of historical success as current proof |

These later rows retain the original backlog scope and are not newly launched work. API owns authorization/funding/persistence for every new job family; audio never becomes a second application database.

## Copy/paste audio parent launch prompt

> Work as the audio repository parent for Track Tree's concurrent parity cohort. Read `/Users/pmarko.alt/Desktop/track-tree/AGENTS.md`, `/Users/pmarko.alt/Desktop/track-tree/PARITY_COORDINATION.md`, this repository's `PARITY_PARENT_TASKS.md` and `docs/parallel/status/audio.md`, the architecture/coordination ledger and inference runtime handoffs. Revalidate the four-repository baseline and check architectural memory constraints. You own only `track-tree-audio` plus your parent status; contracts parent owns root coordination and release integration. Assign AUDIO-21-01, AUDIO-21-02 and AUDIO-21-03 to up to three child agents within your advertised runtime capacity, using the exact exclusive file boundaries, readiness gates and handoff paths in the packets. Begin permitted benchmark/test design immediately; production wire consumers wait for C-21-01 and the recorded profile/runtime decisions. You alone integrate shared HTTP/Celery/config/state/security/legacy-runner files serially. Preserve the committed Python3.11 uv lock and active inference/GPU environments; allocate separate private core/runtime namespaces under a granted lease. Reuse canonical signed fixtures and reviewed transfer/supervision behavior; never invent DTOs, tenant allocations, model paths, URLs or callback policy. Run focused checks with honest proof boundaries and return exact digests/commands/results/cleanup. Follow root freeze and final integration rules. Do not create new user chats, send external messages, modify sibling repositories, commit/push/deploy or enable production inference unless the human launch instruction separately authorizes it. Continue independent ready work while a dependency is pending, and write concrete dependency requests in your own status/handoffs instead of editing the shared root board.

Full remaining scope is routed in the [whole-backlog repo map](../docs/parallel/REPO_BACKLOG_ROUTING.md); the three packets here are the first cohort, not the whole parity target.
