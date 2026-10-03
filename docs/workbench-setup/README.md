# Example 3, set up entirely inside the workbench

The same build as [`docs/manual-setup/README.md`](../manual-setup/README.md) —
NiFi flow, DataFlow deployment, component KPI, Agent Studio workflow — done
from a Cloudera AI session in the **same CDP environment** as the DataFlow
service, instead of from a laptop.

This is not a stylistic preference. It is the difference between a build that
completes and one that stops two thirds of the way through.

---

## Why in-workbench, and when it matters

The CDP CLI has two DataFlow services and they do not share a network path:

| | Endpoint | Reachable from |
| --- | --- | --- |
| `cdp df …` | `api.us-west-1.cdp.cloudera.com` (public) | anywhere |
| `cdp dfworkload …` | the service's own DFX gateway | **inside the environment's VPC only** |

The gateway (`dfLocalUrl`, e.g. `https://dfx.ca0ubuib.a465-9q4k.cloudera.site/dfx`)
is a DNS alias for an **`internal-` AWS ELB** living in the environment's own
VPC. Private, by design.

That split lands directly on this example, because the two planes carry
different halves of the work:

- **Reads** — metrics, KPIs, charts, events, deployment status — are `cdp df`.
  Public. Fine from anywhere.
- **Mutations** — the component KPI in step 5, and every remediation in the
  [self-healing loop](../../self_healing/README.md) — are `cdp dfworkload`.

So from outside the VPC, **every read succeeds and every write connect-times
out.** You get all the way to the KPI step, which is the step the example
depends on, and stop. Worse, you stop *after* the deployment has started
billing, and the KPI cannot be backfilled later without the same connection.

A Cloudera AI session in the same environment is inside that VPC, so it has
both planes. It is also where Agent Studio already runs — so steps 6 and 7
need no context switch, and the whole build happens in one browser tab.

> **If your laptop has a route to the environment's VPC CIDR** (via VPN or
> Direct Connect), the laptop path in
> [`docs/manual-setup/README.md`](../manual-setup/README.md) works and is a
> nicer place to edit files. Step 0 below tells you which situation you are in,
> and it is the same check either way. Don't guess — a connect timeout and a
> permissions failure look nothing alike once you read the output, and
> guessing cost this repository an afternoon.

---

## Target

Verified **2026-10-03**:

| | |
| --- | --- |
| CDP environment | `pdf0926-cdp-env` — AWS `us-east-2`, VPC CIDR `10.10.0.0/16` |
| DataFlow service | `GOOD_HEALTH`, 3 nodes, workload version `3.2.0-b137` |
| Cloudera AI workspace | `pdf-092826`, `installation:finished` |
| Workspace URL | `https://ml-cbdbb7f3-746.pdf0926.a465-9q4k.cloudera.site` |
| Environment CRN | `crn:cdp:environments:us-west-1:558bc1d2-8867-4357-8524-311d51259233:environment:d1b6341e-1a28-4f9a-8e23-9ea762567b11` |
| Service CRN | `crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233:service:707b5faf-765b-44b1-977b-8b25a21aca07` |

**The workspace and the DataFlow service are in the same environment.** That
co-location is the entire premise of this document. Confirm it for your own
target before relying on it:

```bash
cdp ml list-workspaces --query 'workspaces[].[instanceName,environmentName]'
cdp df list-services   --query 'services[].[name,crn]'
```

If your workspace is in a *different* environment from your DataFlow service,
you are back to needing routing between two VPCs and this document buys you
nothing over the laptop one.

> The CRNs say `us-west-1` while the service runs in `us-east-2`. Not a typo —
> the region in a CRN is the **control plane's**.

---

## Step 0 — Open a session and prove both planes

**UI:** workspace → **Projects** → New Project → **Git** →
`https://github.com/pdefusco/AgentStudio_NiFi_Monitoring_And_Self_Healing_Pipelines`
→ create → **New Session** (a small one is plenty; nothing here is compute-heavy).

Then set the credentials **once, as project environment variables** — Project
**Settings** → **Advanced** → Environment Variables:

| Variable | Value |
| --- | --- |
| `CDP_ACCESS_KEY_ID` | your access key ID |
| `CDP_PRIVATE_KEY` | your private key |

Generate them, if you need to, at Management Console → **User Management** →
your user → **Access Keys** → **Generate Access Key**. The private key is shown
exactly once. The identity needs a DataFlow role on the environment:
`DFFlowUser` to read, **`DFFlowAdmin` to write** — and this build writes, so
read-only is not enough here even though example 3 on its own only reads.

> **Keep the project private.** Project environment variables are readable by
> every collaborator. No workload password is involved anywhere in this
> example; if you have set one, it is unused.

In the session terminal:

```bash
cd self_healing
python job/setup_runtime.py
```

A standard Cloudera AI runtime ships no CDP CLI, so without this everything
below dies on `cdp: not found`. It installs `cdpcli==0.9.164` into
`/home/cdsw/.local` — the project filesystem, which persists across sessions
and job runs — and then calls the monitor's own self-test. Run it once per
project, and again after a runtime upgrade.

**The output you care about is the last few lines:**

```
self-test:
  control plane: ok (N deployments visible)
  workload plane: ok (N NiFi versions)
  workload endpoint: dfx.<id>.<id>.cloudera.site (token expires …)
```

**Both planes `ok` is the green light for everything below.** If the workload
plane fails instead, read *how* it failed before touching anything:

| Workload-plane failure | Meaning |
| --- | --- |
| Connect timeout, but a **token expiry** is printed | Credentials, DataFlow role and service state are all fine. The token was minted; only the network path is missing. Your session cannot reach the VPC's ELB — check that the workspace really is in the same environment. |
| `404 NOT_FOUND … Unable to locate enabled service` | The environment has no enabled DataFlow service. Enable it, or you are pointed at the wrong environment. |
| A permissions error | The identity lacks `DFFlowAdmin` on the environment. |

That token-expiry line is the single most useful discriminator in this whole
document: it separates "I cannot authenticate" from "I authenticated fine and
the packets went nowhere", which otherwise present identically as a hang.

`--selftest` exists for exactly this reason, and it is why it is step 0 rather
than a troubleshooting note. **A working read proves nothing about a write.**

---

## Step 1 — The fast path, if you just want it built

Steps 2–5 of this document, in one gated command:

```bash
cd self_healing
./rebuild-pdf0926.sh --dry-run    # free; prints every call it would make
./rebuild-pdf0926.sh              # the deploy step starts billing
```

It re-runs the step 0 self-test as a hard gate and **refuses to spend anything
unless both planes answer** — precisely so it cannot leave you with a billing
deployment and no KPI. On refusal it resolves the gateway's address itself and
prints the traceroute that distinguishes a missing route from a firewall rule.

It targets `pdf0926-cdp-env` via the two `SELFHEAL_*` CRNs at the top; edit
those two lines to retarget, and nothing else. Steps 6 and 7 (Agent Studio) are
UI operations with no API, so they are never scriptable.

**Read the rest of this document if you want to know what those calls do**, or
if you are building against a different environment and want the field values
rather than a script. Steps 2–5 below are what the script runs, in order.

---

## Step 2 — The NiFi flow

The flow is an oscillator: a queue that fills to 600 in one burst, drains at
2/s over about five minutes, then sits empty until the next burst ten minutes
after the last. Misbehaving on a schedule, so you never wait for a real flow to
break.

```
   Burst Generator ──► pdf-selfheal-queue ──► Slow Drain
   600 files / 600s      (the connection         2 files/s
                          a KPI watches)
```

**The arithmetic is deliberate:** drain capacity is 1200 files per cycle
against 600 generated, so the queue clears with headroom and the oscillation
survives a perturbation. At exactly 1.0/s it does not — see
[`self_healing/README.md`](../../self_healing/README.md) for the measurement
where a restart left the queue permanently above threshold.

The queue sits above the threshold of 100 for roughly 250 of every 600 seconds
— three or four metric buckets. Enough to see, and enough for the self-healing
prototype's two-sample debounce.

**In-session CLI:**

```bash
python provision/01_import_flow.py --dry-run
python provision/01_import_flow.py
```

Idempotent: it adds a new version if `pdf-selfheal-oscillator` already exists
rather than failing, which is how the measured flow reached `v.2`.

**Or by hand:** the full click-by-click build in Flow Designer — every
processor property, the scheduling, and the connection settings — is in
[`docs/manual-setup/README.md` step 2](../manual-setup/README.md#step-2--create-the-nifi-flow).
Two values there are worth repeating because getting them wrong produces a
working-but-useless result:

- **`Slow Drain` concurrent tasks = `2`**, against a `1 sec` schedule. This is
  what makes the drain 2 files/s. Set it to 1 and you rebuild the broken version.
- **Name the connection `pdf-selfheal-queue`.** A metric chart carries no
  component id, only `componentName` — so the connection's name *is* the handle
  you will use to find the chart in step 5 and to filter it in step 7. An
  unnamed connection auto-generates one and leaves you filtering for something
  that does not exist.

> The flow catalog is **account-level, not service-level.** A flow you imported
> survives its DataFlow service being deleted, so check before re-importing:
> `cdp df list-flow-definition-versions --flow-crn …`. An identical v.3 in a
> catalog shared with colleagues is pure noise.

---

## Step 3 — Deploy

```bash
python provision/02_create_deployment.py --dry-run
python provision/02_create_deployment.py
```

`EXTRA_SMALL`, one node, no auto-scaling, flow auto-started. **Expect ~15
minutes, and this is where billing starts.**

Two things the script handles that are easy to get wrong by hand:

- `--cluster-size` is an **object**, not a string: `{"name":"EXTRA_SMALL"}`.
  Passing the bare size name is silently wrong.
- `--auto-start-flow` is required. Without it the flow deploys **stopped**,
  nothing is generated, and the queue never fills — a deployment that looks
  healthy and produces no metrics at all.

`--cfm-nifi-version` comes from `config.NIFI_VERSION` (`2.6.0.4.12.0.2-1` by
default, the version the example was measured on). Step 0's self-test lists
what your service actually offers and **warns if the configured one is
missing** — another reason to read its output rather than skim it. The flow
definition pins every processor bundle to that version, and a bundle the
runtime does not carry is rejected.

The UI equivalent is Catalog → **Deploy**; its wizard steps and field values
are in [`docs/manual-setup/README.md` step 3](../manual-setup/README.md#step-3--deploy-the-flow).
**Skip the wizard's KPI step** — step 5 below does it against the deployed
flow, because the `componentId` a component KPI needs is assigned by NiFi at
deploy time and does not exist until then.

---

## Step 4 — Capture the deployment CRN

```bash
cdp df list-deployments \
  --query 'deployments[?name==`pdf-selfheal-oscillator`].crn'
```

Shaped `crn:cdp:df:us-west-1:<account>:deployment:<service-id>/<deployment-id>`.
**The trailing `/<deployment-id>` is part of it** — truncating at the slash
produces a `404 NOT_FOUND` that reads like a permissions problem.

`02_create_deployment.py` already wrote it to `state/state.json`, so the
scripted path needs nothing here. You need it by hand for step 7.

---

## Step 5 — The component KPI

**This is the step that requires being in the VPC, and the step that makes
example 3 interesting.** Skipping it produces a report that looks successful
and says nothing about your flow.

```bash
python provision/03_configure_kpi.py
python provision/03_configure_kpi.py --verify-only
```

The reason it is not optional: **a component-scoped metric chart exists only
where someone configured a KPI against that specific processor, process group
or connection.** With no component KPI, a deployment reports only
deployment-wide charts — CPU, memory, flow-wide data in and out. The agent will
dutifully report those, and will never mention `pdf-selfheal-queue`, because no
such chart exists.

The values, if you are doing it in **Deployment Manager → Manage KPIs → Add
New KPI**:

| Field | Value |
| --- | --- |
| KPI Scope | **Connection** |
| Connection Name | **`pdf-selfheal-queue`** |
| Metric | **Flow Files Queued** (the API calls it `connectionAmountQueued`) |
| Alert — trigger when | **greater than** `100` |
| Alert — unit | `count` |
| Alert — only when frequency exceeds | `1` `MINUTES` |

Four traps, each of which cost this repository real time, and all four are why
the script exists:

1. **`--kpis` is a whole-array replace, not an append.** Read the existing KPIs
   and send them back, or you silently delete them — including other people's.
2. **Pass `scopeComponents[].id` through unchanged.** It is *already* the
   semicolon-joined ancestry (`<processGroupId>;<connectionId>`). Prepending
   the enclosing `contextGroups[].id` repeats the first segment and yields a
   `componentId` matching no component, which fails **silently**.
3. **The scope is keyed by `type`, not `id`**, and its metric catalog is
   `metricTypes`, not `metrics`.
4. **`update-deployment --kpis` is a different command** — deployment-level
   whole-array replace, no undo. Against the wrong CRN it erases someone
   else's KPIs. This repository uses it **nowhere**.

### Confirm the chart exists

Give it two or three minutes:

```bash
cdp df list-flow-kpis-in-deployment \
  --deployment-crn "<deployment-crn>" \
  --deployed-flow-crn "<deployed-flow-crn>" \
  --metrics-time-period LAST_THIRTY_MINUTES
```

**Use the flow-scoped call, not `list-deployment-kpis`.** The two surfaces
return **disjoint** sets, measured live: a component-scoped chart appears
*only* in the flow-scoped one. Reading only `list-deployment-kpis` reports "no
such metric" forever.

A fresh chart is zero-padded backwards across the whole window, so leading
`0.0` points cover time before the connection existed. That is not a generator
that failed to fire.

---

## Step 6 — Agent Studio, in the same workbench

Agent Studio runs as an application in this workspace, so there is no context
switch and no second set of credentials to arrange.

**The fast path:** import
[`templates/workflow_template_e49veqyy.zip`](../../templates/workflow_template_e49veqyy.zip)
— already in the project you cloned in step 0, so it is a local file pick
rather than a download. Agent Studio recreates the workflow, agent, task and
tool, and builds the tool's virtualenv from `requirements.txt`.

Then set the tool's two **user parameters** to the same credentials from step 0:

| Parameter | Value |
| --- | --- |
| `CDP_ACCESS_KEY_ID` | your access key ID |
| `CDP_PRIVATE_KEY` | your private key |

**These are tool parameters, not agent inputs.** The LLM is never shown them
and cannot put them in its output. They are deliberately *not* inherited from
the project environment variables you set in step 0.

`cdpcli` is a large dependency and the venv build takes a few minutes. There is
no `requests` — every call the tool makes goes through the CDP CLI.

**Building it by hand instead** — the workflow, tool, agent and task with every
field value — is
[`docs/manual-setup/README.md` step 6](../manual-setup/README.md#step-6--build-the-agent-studio-workflow).
The one thing worth restating: the long prose fields (the agent's backstory and
goal, the task's description and expected output) are several hundred words
each and are **load-bearing, not decoration** — they are what stop the agent
inventing metrics. Copy them from
[`workflow_template.json`](../../templates/src/nifi_flow_metrics_monitoring/workflow_template.json)
rather than retyping or paraphrasing.

---

## Step 7 — Run it

| Input | Value |
| --- | --- |
| `deployment_crn` | the CRN from step 4 |
| `metrics_time_period` | `LAST_ONE_HOUR` |

Accepted periods: `LAST_THIRTY_MINUTES`, `LAST_ONE_HOUR`, `LAST_TWELVE_HOURS`,
`LAST_ONE_DAY`.

**Timing matters on the first run.** The flow bursts every ten minutes and the
queue is above threshold for only about four of them. Run it twice, a few
minutes apart, and you should see the breach appear and clear — which is the
whole point of the oscillator.

Three things worth checking in the report, because they tell you the wiring is
right rather than merely working:

- **`pdf-selfheal-queue` is named** in the metrics section. If every metric is
  deployment-wide, step 5 did not take effect.
- **`threshold_breached_derived`** is reported as the *tool's* assessment, not
  as something the API said. It is computed in Python from your threshold and
  the current value — deterministic, so better not left to the model.
- **Zero and not-reported are distinguished.** Different findings, and only one
  of them is about your flow.

### Metric resolution, so the numbers make sense

The window is divided into **25 buckets** — it is not a publish cadence.
`LAST_THIRTY_MINUTES` gives 25 points 75 seconds apart; `LAST_ONE_HOUR` gives
25 points 150 seconds apart. Buckets are anchored to **request time**, so two
calls a minute apart return different boundaries and therefore different values
for the same instant. Differencing points *inside* one response is sound;
differencing *across* two responses is not, and doing it produced a drain rate
20% off during this repo's own verification.

---

## Step 8 — Optional: the self-healing loop on a schedule

Everything above is read-only monitoring. The
[self-healing prototype](../../self_healing/README.md) adds
`detect → decide → act → audit` on top of the same deployment and the same KPI.

**This step is in-workbench by necessity, not convenience.** Remediations are
`cdp dfworkload` calls, so a scheduled job must run somewhere with VPC access —
and a Cloudera AI job in this environment is exactly that. It is also the
prototype's intended runtime: `config.py` reads every setting from the
environment so no code edit is needed to retarget.

Watch it unarmed first, across a full ten-minute oscillation:

```bash
python job/monitor_and_remediate.py --status     # no API calls at all
python job/monitor_and_remediate.py              # observe; repeat
```

A bare run observes, decides, logs the exact command it *would* issue, and
changes nothing. What to watch for: the verdict flipping **both** ways, and the
debounce requiring **two consecutive** breaches before `IDLE → BREACH_PENDING`.

Then register the schedule. **UI:** **Jobs → New Job**, script
`self_healing/job/monitor_and_remediate.py`, schedule *custom cron*
`* * * * *`. That path needs no SDK and no second credential, and is the one to
prefer.

The SDK equivalent is `python job/create_job.py --dry-run` then without it. It
needs a **workspace API v2 key** (`CML_API_KEY`), which is a *different*
credential from the CDP key pair, plus `cmlapi` — preinstalled inside a session,
and not on PyPI outside one. It creates the job **unarmed**; arming is a
separate explicit edit to the job's arguments.

> **Acting requires `--arm`.** Before you add it, read *The honest part: the
> action does not fix anything* in
> [`self_healing/README.md`](../../self_healing/README.md). A measured restart
> drove the queue from 428 to 883 — the remediation made the monitored metric
> **worse**. That is the prototype's most useful finding, and it is why the
> `VERIFYING → COOLING` transition is deliberately unconditional.

`* * * * *` is why the file lock and the debounce are mandatory rather than
nice to have. Once the loop is proven, `*/5 * * * *` observes just as much —
the oscillation is 10 minutes and metrics publish every 75 seconds — for a
fifth of the container starts.

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `cdp: not found` | Step 0's `setup_runtime.py` not run in this project, or run under a different project. It installs to `/home/cdsw/.local`. |
| Every `cdp dfworkload …` hangs, `cdp df …` works, and a **token expiry** was printed | Authentication is fine; there is no network path to the environment's ELB. Check the workspace and the DataFlow service are in the same environment. |
| `404 NOT_FOUND … Unable to locate enabled service` on a `dfworkload` call | That environment has no enabled DataFlow service — wrong environment, or the service was disabled. `cdp df list-services`. |
| Report never mentions `pdf-selfheal-queue`; only CPU, memory, flow-wide data | Step 5 missing or did not take. Confirm with `list-flow-kpis-in-deployment` (**not** `list-deployment-kpis`). |
| `component_name_filter` returns `charts_returned: 0` | Same cause. Read `chart_counts.filter_hint` — it distinguishes "no component-scoped charts at all" from "none matched that name". |
| `404 NOT_FOUND` on the deployment | CRN truncated at the `/`, or the deployment — or its whole DataFlow service — is gone. `cdp df list-services` says which. |
| Permissions error with no mention of roles | The identity lacks `DFFlowUser`/`DFFlowAdmin` on the environment. |
| Queue sits above threshold permanently | `Slow Drain` has 1 concurrent task, not 2. No headroom. |
| Queue never rises | Back pressure too low, or `Burst Generator` deployed stopped (`--auto-start-flow` missing). |
| Agent reports metrics with no tool call | Goal text altered. The instruction to call `pollDeploymentMetrics` before answering is what prevents invented numbers. |
| `ModuleNotFoundError` running `tool.py` by hand | Agent Studio builds the tool's venv at import; running it directly does not. See *Testing a tool on its own* in the [main README](../../README.md). |
| Monitor logs `UNKNOWN` verdicts forever | Metrics window widened past the freshness limit, or the deployment is gone. `--status` shows the persisted state without making any API call. |

---

## Tear it down

The deployment bills from provisioning until termination. Nothing else here
costs money: the flow definition, the KPI, the Agent Studio objects and the
session are all free — though **a running session holds compute**, so stop it
when you are done.

```bash
python provision/teardown.py
```

It refuses to touch anything without the `pdf-selfheal-` prefix. If you
registered the step 8 job, delete it too, or it keeps starting a container
every minute against a deployment that no longer exists.

> A disabled or deleted **DataFlow service takes its deployments with it.**
> That is how the originally measured deployment ended: the service it lived in
> disappeared, and `describe-deployment` began returning `404 NOT_FOUND` on a
> CRN that had worked minutes earlier. Billing stops, but so does your demo —
> worth knowing before planning a long-running one in a shared sandbox. The
> **flow catalog is unaffected**, being account-level.

---

## Where to go next

- [`docs/manual-setup/README.md`](../manual-setup/README.md) — the same build
  from a laptop, with the full click-by-click Flow Designer and Agent Studio
  field values this document links to rather than repeats.
- [`README.md`](../../README.md) — all three examples, and why the NiFi REST
  API is unreachable in CDF Public Cloud.
- [`self_healing/README.md`](../../self_healing/README.md) — the loop that
  *acts* on the breach, the guardrail, the state machine, and the measurement
  showing a restart made the monitored metric worse.
