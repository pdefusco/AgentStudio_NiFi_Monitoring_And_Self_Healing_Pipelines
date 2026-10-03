# Self-healing DataFlow prototype

A throwaway prototype that proves one lifecycle end to end:

> **monitor → detect → decide → act → audit**

on a Cloudera DataFlow deployment, from a plain Cloudera AI job on a cron
schedule. **Deliberately not Agent Studio** — no agent, no LLM, no tool
registry. The three templates under `../templates/` observe a deployment;
this is the first thing in the repo that *acts* on one.

Because waiting for a real flow to misbehave is not a test plan, it ships
its own flow that misbehaves on purpose: a queue that fills to ~600 in one
burst, drains at 2/s over the next ~5 minutes, then sits empty until the next
burst 10 minutes after the last. A job running every minute reliably catches
both halves of the cycle.

---

## The honest part: the action does not fix anything

This matters more than any other line in this README, and the whole design
follows from it.

**Neither remediation drains a queue.** The flowfile repository sits on a
persistent volume, and during either a `restart-deployment` or a
`stop-flow` + `start-flow`, *both* the generator and the drain processor are
stopped. The backlog is frozen at breach level, and the breach is **still
true** when the action completes. Only the drain processor — only time —
clears it.

**Measured, and it is worse than "still true".** The armed run restarted the
deployment at 23:28:58 with the queue at 428. When the deployment returned to
`GOOD_HEALTH` three minutes later the queue read **883** — higher than it had
ever been on a normal cycle, because the backlog survived the restart *and*
the generator fired a fresh 600-file burst on startup. The restart did not
fail to help; it actively made the monitored metric worse:

```
23:28:15   413    last sample before the action
23:28:58          restart initiated
(23:29:30)        no bucket published at all during the restart
23:30:45   898    first sample after
23:32:00   823    draining again at exactly 1.0/s
```

This is the single most useful result in the prototype. A remediation chosen
because it is easy to call, rather than because it addresses the cause, can
move the signal in the wrong direction — and a loop that assumed "I acted,
therefore it is better" would have escalated into a restart storm on exactly
this evidence. The state machine is what prevents that, and this run is the
proof it works: five consecutive breach observations, **one** action.

**The damage was permanent, until the flow was given headroom.** The first
version of the synthetic flow generated 600 files per 600 s (1.0/s) and drained
at exactly 1.0/s, so the two rates cancelled: the queue returned to ~20 each
cycle only because nothing ever perturbed it. With **no headroom** the ~500
files the restart added never cleared — the cycle after the action ran
868 → 344 and straight back up, a trough three times the threshold — so one
armed run ended the oscillation for good.

Fixed by running `Slow Drain` at **2 concurrent tasks**, which the builder's
`--check` now enforces (it refuses a configuration where the drain needs more
than 80% of a cycle to clear a burst). Measured on the rebuilt deployment, the
series steps down 150 per 75-second bucket — 2.0/s exactly:

```
23:51:15   528
23:52:30   378
23:53:45   228
23:55:00    78    back under threshold, ~300s after the burst
```

Drain capacity is now 1200 per cycle against 600 generated, so a backlog an
action leaves behind is absorbed within about one cycle and the oscillation
survives a remediation. The queue sits above threshold for ~250 of every 600 s
rather than ~500, which is still three or four samples — more than the debounce
needs.

The accidental lesson is worth keeping even though the flow no longer has the
flaw: a self-healing loop whose action degrades its own signal is exactly the
failure mode worth being able to see, and the only reason it was visible at all
is that the state machine refused to act on it a second time.

So this prototype demonstrates **detect and respond**, not repair. Three
consequences are built in rather than papered over:

- The state machine **never re-arms on "condition cleared"**. It re-arms on
  a wall-clock cooldown plus the deployment returning to a steady state.
  `VERIFYING → COOLING` is unconditional; requiring the breach to have
  cleared would escalate on every single cycle.
- A **circuit breaker** caps actions per rolling window (default 3 per 24 h)
  and ends in a terminal `DISARMED` state that needs a human. A one-minute
  schedule plus a self-perpetuating breach is otherwise a restart storm.
- `UNKNOWN` is a **third verdict**, not a synonym for healthy. A missing
  chart, a stale sample, or an unreadable deployment produces it; it never
  triggers an action and never counts as a clean bill of health.

Real remediation — restarting a single stuck processor, clearing a queue,
rolling back a flow version — is later work. The plumbing is what is being
proven here.

---

## What gets created in your account

Everything carries the `pdf-selfheal-` prefix, because the CDF flow catalog
and the DataFlow service are shared with colleagues. Teardown refuses to
touch anything without it.

| Thing | Name | Cost |
| --- | --- | --- |
| Flow definition in the CDF catalog | `pdf-selfheal-oscillator` | free, but visible to everyone with catalog access |
| CDF deployment, `EXTRA_SMALL`, 1 node | `pdf-selfheal-oscillator` | **bills from provisioning until `terminate-deployment`**; ~15 min to come up |
| A component KPI on one connection | on `pdf-selfheal-queue` | free |
| Cloudera AI job | `pdf-selfheal-monitor` | ~1440 container starts/day at `* * * * *` |

**Tear it down when you are done.** `provision/teardown.py` is the only
thing that stops the deployment billing.

---

## Layout

```
config.py                       every tunable, each overridable by env var
df_api.py                       CDP CLI wrapper + the guardrail + metric merge
state.py                        locked, atomic state file
flow/build_flow_definition.py   emits the synthetic flow (deterministic UUIDs)
flow/oscillating_queue.json     the emitted artifact, committed so git can diff it
provision/01_import_flow.py     catalog import
provision/02_create_deployment.py   deploy, then wait for steady state
provision/03_configure_kpi.py   discover ids -> write KPI -> verify it landed
provision/teardown.py           terminate, then delete
job/monitor_and_remediate.py    the job entrypoint: one pass per invocation
job/setup_runtime.py            one-time `pip install --user cdpcli` in the project
job/create_job.py               registers the job + schedule via cmlapi
state/                          gitignored: state.json, lock, audit-YYYYMMDD.jsonl
```

Nothing here imports from `../templates/`, and nothing there imports from
here.

---

## Running it

Requires `CDP_ACCESS_KEY_ID` and `CDP_PRIVATE_KEY` in the environment (or a
`~/.cdp/credentials` file). They are passed to the CLI subprocess through
the environment — never on a command line, never into the audit log.

Every script refuses with a message naming its predecessor if run out of
order. Every *provisioning* script also takes `--dry-run`; the other two
spell the same idea differently, and the difference is deliberate:
`job/monitor_and_remediate.py` has no `--dry-run` because unarmed **is**
the dry run — it observes, decides and logs the exact command it would
issue unless you pass `--arm` — and `flow/build_flow_definition.py` has
`--check`, because emitting a file nobody deploys costs nothing and the
useful question there is whether the flow it would emit is valid.

```bash
cd self_healing

# 0. prove both API planes are reachable before anything costs money
python job/monitor_and_remediate.py --selftest

# 1. build and inspect the flow, offline and free
python flow/build_flow_definition.py --check

# 2. import into the catalog — THE FIRST MUTATING CALL, into a shared catalog
python provision/01_import_flow.py --dry-run
python provision/01_import_flow.py

# 3. deploy — THIS STARTS BILLING, and takes ~15 minutes
python provision/02_create_deployment.py

# 4. create the metric series the job reads. Not optional: no queue-depth
#    chart exists by default, so without this the monitor can never trip.
python provision/03_configure_kpi.py
python provision/03_configure_kpi.py --verify-only

# 5. watch the oscillation for ~20 minutes before trusting the loop
python job/monitor_and_remediate.py        # observe only; repeat every minute

# 6. one armed run at a breach
python job/monitor_and_remediate.py --arm

# 7. stop the billing
python provision/teardown.py
```

### Why `--selftest` exists

Detection and action do not travel the same path. Every `cdp dfworkload`
command first asks the control plane for a workload auth token and then
calls a workload endpoint on a host the control-plane read path never
touches — and the role needed to restart a deployment is not the role
needed to read its metrics. Without the self-test, the job can detect
perfectly for hours and then 403 at the first real breach.

---

## In Cloudera AI

Workspace `pdf-092826`, in the `pdf0926-cdp-env` environment — the **same**
environment as the DataFlow service it monitors. That co-location is not
incidental, and an earlier version of this section had it wrong twice: it
claimed a different environment, and claimed "the job calls public
endpoints".

**The job does not call only public endpoints.** Reads do, but every
mutation — each remediation in `df_api.act` — is a `cdp dfworkload` call to
the service's own DFX gateway, which is an `internal-` ELB private to the
environment's VPC. So a runner outside that VPC can observe the whole
oscillation and never be able to act on it: reads succeed, writes
connect-time out. Measured 2026-10-03 from a laptop whose VPN carried no
route to the VPC CIDR — the self-test minted a workload token successfully
and then timed out connecting, which is the signature worth recognising.

Hence `--selftest` checks **both** planes, and hence a scheduled job
belongs in a workspace in the same environment as the deployment. There is
a full in-workbench runbook at
[`docs/workbench-setup/README.md`](../docs/workbench-setup/README.md).

1. Clone this repo as the Cloudera AI project.
2. Set `CDP_ACCESS_KEY_ID` and `CDP_PRIVATE_KEY` as project environment
   variables. **Keep the project private:** project environment variables
   are readable by every collaborator.
3. Run `job/setup_runtime.py` once, as a job or in a session terminal. A
   standard runtime has no CDP CLI, so without this the monitor dies on
   `cdp: not found`. It installs into `/home/cdsw/.local`, which persists;
   the monitor prepends that `bin` to `PATH` itself.
4. Register the schedule — either

   ```bash
   python job/create_job.py --dry-run
   python job/create_job.py
   ```

   or, needing no SDK and no second credential, in the UI: **Jobs → New
   Job**, script `self_healing/job/monitor_and_remediate.py`, schedule
   *custom cron* `* * * * *`.

`create_job.py` needs a **workspace API v2 key** (`CML_API_KEY`), which is a
different credential from the CDP key pair, and `cmlapi`, which is not on
PyPI — inside a session it is preinstalled, outside it comes from
`<workspace>/api/v2/python.tar.gz`. It also creates the job **unarmed**;
arming is a separate explicit edit to the job's arguments.

### Arming

Acting requires `--arm`. A bare run observes, decides, logs the exact
command it would issue, and changes nothing. Watch an unarmed job across a
full ten-minute oscillation first — the thing worth checking is that the
verdict flips both ways and that the debounce needs two consecutive
breaches.

`* * * * *` was chosen deliberately, and it is also why the lock and the
debounce are mandatory rather than nice to have. Once the loop is proven,
`*/5 * * * *` observes just as much — the oscillation is 10 minutes and
metrics publish every 75 seconds — for a fifth of the container starts.

---

## The guardrail

Every mutating call goes through `df_api.act`, which applies
`assert_action_allowed` **before** it checks `dry_run`, so a dry run
exercises the same check an armed run would. Four conditions, all of which
must hold:

1. the target is not one of the five deployments that predate this
   prototype (hardcoded denylist)
2. a CRN was recorded at provisioning time at all — an empty value means
   there is nothing this prototype is allowed to touch
3. the target is exactly that recorded CRN
4. the deployment's own name, read live, carries the `pdf-selfheal-`
   prefix, and it lives in the configured DataFlow service

They are redundant on purpose. Any one of them could be defeated by a stale
state file, a mistyped override, or a copied CRN, and the cost of being
wrong is restarting a colleague's production flow.

**Condition 3 cannot fail on the monitor's path, and that is worth knowing
rather than hiding.** `monitor_and_remediate.py` derives both the target and
the expected CRN from the same `state["deployment_crn"]`, so the comparison
is a tautology there; the condition earns its place on the path where a
human supplies a CRN by hand, `teardown.py --deployment-crn`. Treat the
monitor as protected by three live conditions, not four. The denylist and
the live prefix read are the two that would actually stop a wrong target.

**Condition 4 has now fired for real, and not on the half anyone expected.**
Measured 2026-10-03, on a live breach at a queue depth of 402, the monitor
reached its second consecutive breach, decided to act, and was refused:

```
action:           restart_deployment (armed=False)
  REFUSED: deployment is in an unexpected DataFlow service.
  found:    …:service:707b5faf-765b-44b1-977b-8b25a21aca07
  expected: …:service:41519649-07da-49d8-bbdc-0bc6777a7c82
```

The target was correct; `config.SERVICE_CRN` was stale, left pointing at a
DataFlow service that had since been deleted. So the condition did its job
against a **misconfigured guard** rather than a wrong target — which is a
failure mode worth naming, because the loop had detected perfectly for an
hour and would have gone on doing so. It is also the argument for the
service CRN being printed by `--selftest`: the refusal is legible in one
line only if you already know what the guard expects.

Note what the refusal did *not* do: no `ACTING` was written, because that
transition is gated on `--arm`. A refused unarmed run leaves the machine in
`BREACH_PENDING` and nothing needs repairing by hand.

`dfworkload update-deployment --kpis` is used **nowhere** in this
prototype: it is a deployment-level whole-array replace with no undo, and
against the wrong CRN it silently erases someone else's KPIs.

---

## Response keys, because guessing them wastes an afternoon

The CDP CLI wraps each response under a key that rarely matches the
operation name, and a wrong guess reads as "the API returned nothing"
rather than as a bug in the caller. Every one of these was verified live:

| Operation | Key |
| --- | --- |
| `df describe-flow` | `flowDetail` |
| `df list-flow-definitions` | `flows` — and `FlowSummary` carries **no** `description`; use `describe-flow` |
| `df list-flow-definition-versions` | `flowVersions` |
| `df list-flows-in-deployment` | `deployedFlows` |
| `df list-deployment-kpis`, `list-flow-kpis-in-deployment`, `list-deployment-system-metrics` | `metricCharts` |
| `df list-deployment-events`, `list-deployment-active-alerts` | `eventSummaries` |
| `dfworkload list-nifi-versions` | `nifiVersions` |

## Three findings worth keeping

**The two KPI read surfaces return disjoint sets.** Measured live against
`se-sandbox-aws`:

```
list-deployment-kpis          -> NIFI_FLOW / (none) / Data In
                                 NIFI_FLOW / (none) / Data Out
list-flow-kpis-in-deployment  -> NIFI_PROCESSOR / GenerateFlowFile / Bytes Sent
```

A component-scoped chart appears **only** in the flow-scoped call. Our KPI
is component-scoped, so a monitor reading only `list-deployment-kpis` would
report "no breach" forever. `df_api.metric_charts` merges both and records
each surface's outcome, so a failing read shows up in the audit log instead
of looking like an absent chart.

**Metric resolution is the window divided into 25 buckets — not a publish
cadence.** Measured across two windows in the same minute:

```
LAST_THIRTY_MINUTES   25 points, 75s apart,  span 1800s
LAST_ONE_HOUR         25 points, 150s apart, span 3600s
```

So the familiar "75 seconds" is a consequence of asking for 30 minutes and
nothing more, and the points are **aggregated buckets on an anchor that
slides with request time**: two calls a minute apart return different bucket
boundaries, and therefore different values for the same instant. Differencing
samples *within* one response is sound; differencing across two responses is
not, and doing it produced a drain rate 20% off during this prototype's own
verification.

Two consequences in the code. The job runs every 60 s against 75 s buckets, so
consecutive runs routinely see the identical latest sample — hence freshness
checked explicitly against `MAX_SAMPLE_AGE_SECONDS`, `metrics.datas[-1]` read
rather than `currentValue` (no timestamp, and still a pre-action value right
after a restart), and every point at or before `last_action_started_at`
discarded. And because both the window and the freshness limit are
environment-overridable, `config.py` refuses a combination where the bucket
size exceeds the freshness limit — `LAST_ONE_DAY` yields hour-wide buckets, so
every sample would read as stale and the monitor would report `UNKNOWN`
forever while appearing to run fine.

**A `0.0` sample does not mean the queue drained.** The requested window is
returned in full, zero-padded back to its start — including time before the
connection existed. Measured here: the flow started at 22:55:01 and the first
`LAST_THIRTY_MINUTES` read returned 20 leading `0.0` points covering the 1425
seconds *before* that, then the real burst. Read naively that looks like a
generator that failed to fire for 24 minutes.

This is harmless for the job only because freshness is enforced
independently: those padding points are old, so they never become the latest
sample. It would be actively misleading for anything that averaged the series
or asked "has this queue ever been non-empty" — which is the reason to write
it down rather than rely on the guard that happens to cover it.

---

## The state machine

Persisted atomically to `state/state.json`; `/home/cdsw` survives across job
runs. Acting is gated on `DeploymentState ∈ {GOOD_HEALTH,
CONCERNING_HEALTH, BAD_HEALTH}` — every other value means an action is in
flight or the target is gone.

```
IDLE ──► BREACH_PENDING   two consecutive fresh breaches (debounce)
     ──► ACTING           written BEFORE the request is issued
     ──► VERIFYING        poll to a steady state; 20-min deadline → ESCALATED
     ──► COOLING          wall clock, ~30 min. Unconditional.
     ──► IDLE
     ──► DISARMED         terminal: TERMINATED/TERMINATING, or breaker tripped
```

`ACTING` is written *before* the request so that a run overlapping an action
in flight reads `ACTING` instead of `IDLE`. A run that finds a stale
`ACTING` moves to `VERIFYING` rather than issuing a second action. The lock
(`fcntl.flock` on `state/lock`, non-blocking) is what stops two overlapping
runs from both acting; a run that cannot take it logs "skipped" and exits 0.

Useful commands:

```bash
python job/monitor_and_remediate.py --status   # read state, no API calls
python job/monitor_and_remediate.py --reset    # clear DISARMED/ESCALATED
```

`--reset` is for after a human has understood the cause. It is not part of
the loop.

## The audit log

One JSON line per run, appended to `state/audit-YYYYMMDD.jsonl`: the merged
chart inventory and each surface's outcome, the value and threshold that
tripped, the verdict and why, the state transition, the chosen action, the
exact argv, and the deployment state. Credentials never reach it — they
travel in the subprocess environment, and the `argv` recorded is the
argument list this code built.

```bash
tail -1 state/audit-$(date -u +%Y%m%d).jsonl | python -m json.tool
```

Read it alongside `cdp df list-deployment-events` and check the two agree.
That reconciliation is the actual proof the lifecycle worked.
