# Example 3, set up by hand in CDP

A click-by-click runbook for building **NiFi Flow Metrics Monitoring Agents**
from nothing: a NiFi flow, a DataFlow deployment, the component KPI the
example depends on, and the Agent Studio workflow — without importing the
prepackaged `.zip`, and without running any script in this repository.

The top-level [`README.md`](../../README.md) explains *what* example 3 is and
why it reads the DataFlow API instead of the NiFi REST API. This document
assumes you have read that and only want to know which buttons to press.

**Audience:** you know CDP — you can find the Management Console, you know what
a CRN is, you have used the Catalog. What you want spelled out is the exact
field values, because most of them matter and a few of them silently produce a
working-but-useless result.

---

## A word on UI labels versus the CLI

Every CDP UI label in this document was current in **October 2026**, and
Cloudera renames things between releases. So each step gives the UI path
**and** the CLI command that does the same thing. If a label has moved, the CLI
column is the part that still works, and it is also the part this repository has
verified live.

Two honest caveats about what was verified when this was written:

- **The control plane was verified; the workload plane was not.** `cdp df …`
  calls against the target service answered normally. Every `cdp dfworkload …`
  call timed out connecting to `dfx.ca0ubuib.a465-9q4k.cloudera.site`, the
  service's own gateway host. That is precisely the split the repo's
  `--selftest` exists to catch — reads and mutations travel different planes,
  different hosts and different roles — so the `dfworkload` commands below are
  reproduced from this repository's previously verified usage rather than
  re-run against this service. If they fail for you with a connect timeout, it
  is the gateway or your network, not the command.
- **The NiFi version list is not reproduced here** for the same reason
  (`list-nifi-versions` is a `dfworkload` call). Take whatever the deploy
  wizard offers. For reference, the deployment this example was originally
  measured on ran `2.6.0.4.12.0.2-1`.

---

## Target environment

Verified **2026-10-03**:

| | |
| --- | --- |
| DataFlow service | `pdf0926-cdp-env` |
| State | `GOOD_HEALTH`, 3 nodes, `deploymentCount: 0` |
| Cloud / region | AWS `us-east-2` |
| Workload version | `3.2.0-b137` |
| Environment CRN | `crn:cdp:environments:us-west-1:558bc1d2-8867-4357-8524-311d51259233:environment:d1b6341e-1a28-4f9a-8e23-9ea762567b11` |
| Service CRN | `crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233:service:707b5faf-765b-44b1-977b-8b25a21aca07` |

> The CRNs say `us-west-1` while the service runs in `us-east-2`. That is not a
> typo — the region in a CRN is the **control plane's**, not the workload's.

To use a different environment, substitute both CRNs. Nothing else below is
environment-specific.

---

## What you are building

```
   ┌─────────────────────── CDF deployment ───────────────────────┐
   │                                                              │
   │   Burst Generator ──► pdf-selfheal-queue ──► Slow Drain       │
   │   600 files / 600s      (the connection          2 files/s    │
   │                          a KPI watches)                       │
   └──────────────────────────────┬───────────────────────────────┘
                                  │ connectionAmountQueued KPI
                                  │ threshold 100
                                  ▼
                        DataFlow metrics API
                                  │
                                  ▼
            pollDeploymentMetrics tool ──► Agent ──► Flow Metrics Report
```

A flow that misbehaves on a schedule, so you never have to wait for a real one
to break: a queue that fills to 600 in one burst, drains at 2/s over about five
minutes, then sits empty until the next burst ten minutes after the last.

**The arithmetic is deliberate.** Drain capacity is 1200 files per cycle against
600 generated, so the queue clears with room to spare and the oscillation keeps
running. Give it *no* headroom (drain at exactly 1.0/s) and any perturbation
puts the queue permanently above threshold — which is what happened the first
time, and is written up in
[`self_healing/README.md`](../../self_healing/README.md).

The queue sits above the threshold of 100 for roughly 250 of every 600 seconds,
which is three or four metric buckets — enough to see, and enough for the
self-healing prototype's two-sample debounce if you go on to that.

---

## Step 1 — Get a CDP API key pair

Skip if you already have one that can read DataFlow.

**UI:** Management Console → **User Management** → your user (or a machine user)
→ **Access Keys** tab → **Generate Access Key** → choose the key type → copy
both halves.

You get an **access key ID** and a **private key**. The private key is shown
exactly once.

**The identity needs a DataFlow role on the environment:** `DFFlowUser` for
read-only, which is all example 3 requires, or `DFFlowAdmin` for full
privileges. Without one, every call in this document returns a permissions
error that does not mention roles.

> **No workload password is involved** anywhere in this example. If you have set
> one, it is unused.

---

## Step 2 — Create the NiFi flow

Two ways. **Option A is faster and reproduces the measured flow exactly**;
option B is for understanding what the flow is.

### Option A — import the flow definition this repo already contains

[`self_healing/flow/oscillating_queue.json`](../../self_healing/flow/oscillating_queue.json)
is a complete NiFi flow definition, committed so it can be diffed.

**UI:** DataFlow → **Catalog** → **Import Flow Definition**

| Field | Value |
| --- | --- |
| Flow Name | `pdf-selfheal-oscillator` |
| Flow Description | anything, or leave blank |
| NiFi Flow Configuration | upload `self_healing/flow/oscillating_queue.json` |

**CLI:**

```bash
cdp df import-flow-definition \
  --name pdf-selfheal-oscillator \
  --file self_healing/flow/oscillating_queue.json \
  --comments "oscillating queue for metrics monitoring"
```

Importing a second version of an existing flow uses
`import-flow-definition-version --flow-crn … ` instead, which is how the
measured deployment reached `v.2`.

### Option B — build it by hand in Flow Designer

**UI:** DataFlow → **Flow Design** → select your environment → **Create Draft**
→ name it `pdf-selfheal-oscillator`.

Drag two processors onto the canvas and set them as follows. Every value here is
read from the committed flow definition, so option B lands in the same place as
option A.

**Processor 1 — `GenerateFlowFile`, renamed `Burst Generator`**

*Properties:*

| Property | Value |
| --- | --- |
| Batch Size | `600` |
| File Size | `0B` |
| Data Format | `Text` |
| Unique FlowFiles | `false` |
| Character Set | `UTF-8` |

*Scheduling:* Timer driven · Run Schedule **`600 sec`** · Concurrent Tasks **`1`**

`File Size 0B` is intentional. The example measures queue *depth* in files, so
the files carry no content and the flow costs almost nothing to run.

**Processor 2 — `UpdateAttribute`, renamed `Slow Drain`**

*Properties:*

| Property | Value |
| --- | --- |
| Store State | `Do not store state` |
| Cache size for canonical value lookup | `100` |

*Scheduling:* Timer driven · Run Schedule **`1 sec`** · Concurrent Tasks **`2`**

*Relationships:* tick **auto-terminate** on `success`.

`UpdateAttribute` is a deliberate no-op — it is the cheapest processor that
consumes a FlowFile and discards it. **Concurrent Tasks `2` against a `1 sec`
schedule is what makes the drain 2 files per second**, and therefore what gives
the flow its headroom. Set it to `1` and you rebuild the broken version.

**The connection — this one matters most**

Drag from `Burst Generator` to `Slow Drain`:

| Field | Value |
| --- | --- |
| Connection Name | **`pdf-selfheal-queue`** |
| Relationships | `success` |
| Back Pressure Object Threshold | `10000` |
| Back Pressure Data Size Threshold | `1 GB` |

**Name the connection.** The metric chart you are about to configure is
identified by `componentName`, and that name *is* the connection's name. An
unnamed connection produces a chart you cannot find and a
`component_name_filter` that matches nothing.

Back pressure at 10000 is far above the ~600 the queue ever holds, deliberately:
back pressure would stop the generator and flatten the oscillation you are
trying to observe.

Then **Publish To Catalog** — flow name `pdf-selfheal-oscillator`.

---

## Step 3 — Deploy the flow

**UI:** Catalog → `pdf-selfheal-oscillator` → **Deploy** → pick your target
environment (`pdf0926-cdp-env`). The wizard's steps:

| Wizard step | What to enter |
| --- | --- |
| Overview | Deployment Name `pdf-selfheal-oscillator` |
| NiFi Configuration | take the offered NiFi version; leave Inbound Connections off; no custom extensions needed |
| Parameters | none — this flow declares no parameter context |
| Sizing & Scaling | NiFi Node Size **`EXTRA_SMALL`**, nodes **`1`**, auto-scaling **off** |
| Key Performance Indicators | **skip this step** — see below |
| Review | deploy |

**CLI:**

```bash
cdp df create-deployment \
  --service-crn   "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233:service:707b5faf-765b-44b1-977b-8b25a21aca07" \
  --flow-version-crn "<from: cdp df list-flow-definition-versions>" \
  --deployment-name pdf-selfheal-oscillator \
  --cluster-size-name EXTRA_SMALL \
  --auto-start-flow
```

**Expect ~15 minutes**, and **this is where billing starts.** An `EXTRA_SMALL`
single node is the smallest thing that runs a two-processor flow.

### Why skip the wizard's KPI step

You *can* add the KPI here, and it will work. Step 4 does it against the
deployed flow instead, for one reason: the `componentId` a component-scoped KPI
needs is assigned by NiFi **at deploy time**. Before the deployment exists there
is nothing to point at, so configuring it afterwards is the path that also works
on a deployment someone else created — and it is the path the CLI can automate.

---

## Step 4 — Configure the component KPI

**Do not skip this step.** It is the one that makes example 3 interesting, and
skipping it produces a report that looks successful and says nothing about your
flow.

Here is the reason, which the main README measures in detail: **a
component-scoped metric chart exists only where someone configured a KPI against
that specific processor, process group or connection.** With no component KPI, a
deployment reports only deployment-wide charts — CPU, memory, flow-wide data in
and out. The agent will dutifully report those. It will never mention
`pdf-selfheal-queue`, because no such chart exists, and a
`component_name_filter` of `pdf-selfheal-queue` correctly returns zero charts.

**UI:** DataFlow → **Deployment Manager** → `pdf-selfheal-oscillator` →
**Manage KPIs** → **Add New KPI**

| Field | Value |
| --- | --- |
| KPI Scope | **Connection** |
| Connection Name | **`pdf-selfheal-queue`** |
| Metric | **Flow Files Queued** |
| Alert — trigger when | **greater than** `100` |
| Alert — unit | `count` |
| Alert — only when frequency exceeds | `1` `MINUTES` |

**Flow Files Queued** is the UI label; the API calls the same thing
`connectionAmountQueued`. Both names are correct, and knowing the pair saves you
hunting for one when you are reading the other. These exact values were verified
on the live deployment, which read back as:

```json
{"componentType": "NIFI_CONNECTION",
 "componentName": "pdf-selfheal-queue",
 "name":          "Flow Files Queued"}
```

The threshold of `100` is what makes the derived verdict do real work: the queue
crosses it on every cycle, so `threshold_breached_derived` flips to `true` and
back rather than sitting at one value forever.

**CLI** — three calls, in order, and read the warnings:

```bash
ENV="crn:cdp:environments:us-west-1:558bc1d2-8867-4357-8524-311d51259233:environment:d1b6341e-1a28-4f9a-8e23-9ea762567b11"

# 1. the deployed-flow CRN
cdp df list-flows-in-deployment --deployment-crn "<deployment-crn>"      # -> deployedFlows[]

# 2. the component id the KPI must point at
cdp dfworkload get-flow-configuration-metadata-in-deployment \
  --environment-crn "$ENV" --deployment-crn "<deployment-crn>" \
  --deployed-flow-crn "<deployed-flow-crn>"
#    -> kpiMetaData.kpiScopes[].contextGroups[].scopeComponents[]{id,name}

# 3. the current KPIs, which you must send back alongside the new one
cdp dfworkload get-flow-configuration-in-deployment \
  --environment-crn "$ENV" --deployment-crn "<deployment-crn>"

# 4. write
cdp dfworkload update-flow-in-deployment \
  --environment-crn "$ENV" --deployment-crn "<deployment-crn>" \
  --kpis '[ … existing KPIs … , {
    "metricId": "connectionAmountQueued",
    "metricComponentType": "NIFI_CONNECTION",
    "componentId": "<scopeComponents[].id, verbatim>",
    "alert": {
      "thresholdMoreThan": {"unitId": "count", "value": 100},
      "frequencyTolerance": {"value": 1, "unit": {"id": "MINUTES"}}
    }}]'
```

Four traps, each of which cost this repository real time:

1. **`--kpis` is a whole-array replace, not an append.** Read the existing KPIs
   and send them back, or you silently delete them.
2. **Pass `scopeComponents[].id` through unchanged.** It is *already* the
   semicolon-joined ancestry (`<processGroupId>;<connectionId>`). Prepending the
   enclosing `contextGroups[].id` repeats the first segment and yields a
   `componentId` matching no component — which fails silently.
3. **The scope is keyed by `type`, not `id`** (`kpiScopes[].id` does not exist),
   and its metric catalog is **`metricTypes`**, not `metrics`.
4. **`update-deployment --kpis` is a different command** — deployment-level
   whole-array replace, no undo. Against the wrong CRN it erases someone else's
   KPIs. This repository uses it nowhere.

### Confirm the chart actually exists

Give it two or three minutes, then:

```bash
cdp df list-flow-kpis-in-deployment \
  --deployment-crn "<deployment-crn>" \
  --deployed-flow-crn "<deployed-flow-crn>" \
  --metrics-time-period LAST_THIRTY_MINUTES      # -> metricCharts[]
```

**Use the flow-scoped call, not `list-deployment-kpis`.** The two surfaces
return **disjoint** sets, measured live: a component-scoped chart appears *only*
in the flow-scoped one. Reading only `list-deployment-kpis` reports "no such
metric" forever.

A fresh chart is zero-padded backwards across the whole requested window, so
leading `0.0` points cover time before the connection existed. That is not a
generator that failed to fire.

---

## Step 5 — Capture the deployment CRN

The agent's only required input.

**UI:** Deployment Manager → your deployment → the CRN is in the detail pane
(**Deployment Settings** / **KPIs** header area), copyable.

**CLI:**

```bash
cdp df list-deployments --query 'deployments[?name==`pdf-selfheal-oscillator`].crn'
```

Shaped like:

```
crn:cdp:df:us-west-1:<account-id>:deployment:<service-id>/<deployment-id>
```

The trailing `/<deployment-id>` after a slash is part of it. Truncating at the
slash produces a `404 NOT_FOUND` that reads like a permissions problem.

---

## Step 6 — Build the Agent Studio workflow

The fast path is **import
[`templates/workflow_template_e49veqyy.zip`](../../templates/workflow_template_e49veqyy.zip)**
in Agent Studio and skip to step 7. Agent Studio recreates the workflow, agent,
task and tool, and builds the tool's virtualenv from `requirements.txt`.

The rest of this step is the manual equivalent, for when you want to understand
the pieces or build a variant.

Every value below comes from
[`templates/src/nifi_flow_metrics_monitoring/workflow_template.json`](../../templates/src/nifi_flow_metrics_monitoring/workflow_template.json).
The long prose fields — the agent's backstory and goal, the task's description
and expected output — are several hundred words each and are **load-bearing**,
not decoration: they are what stop the agent inventing metrics. **Copy them from
that file rather than retyping or paraphrasing.**

### 6a. Create the workflow

**Workflows** → **Create Workflow**

| Field | Value |
| --- | --- |
| Name | `NiFi Flow Metrics Monitoring Agents` |
| Description | the `workflow_template.description` string from the JSON |
| Process | **Sequential** |
| Conversational | **off** |
| Planning | **off** |

### 6b. Create the tool

**Tools** → **Create Tool** (a Python/venv tool, not a prebuilt one)

| Field | Value |
| --- | --- |
| Name | **`pollDeploymentMetrics`** |
| Python code | paste [`tool.py`](../../templates/src/nifi_flow_metrics_monitoring/studio-data/tool_templates/polldeploymentmetrics_PddIfQLk/tool.py) whole |
| Requirements | `pydantic` and `cdpcli`, one per line |
| Venv tool | **on** |

The name must match exactly — the agent's goal text instructs it to call
`pollDeploymentMetrics` by name.

`cdpcli` is a large dependency; the venv build takes a few minutes. There is no
`requests`: every call in the tool goes through the CDP CLI.

Then set the tool's two **user parameters**, the credentials from step 1:

| Parameter | Value |
| --- | --- |
| `CDP_ACCESS_KEY_ID` | your access key ID |
| `CDP_PRIVATE_KEY` | your private key |

**These are tool parameters, not agent inputs.** The LLM is never shown them and
cannot put them in its output. Keep the project private — project environment
variables are readable by every collaborator.

### 6c. Create the agent

**Agents** → **Create Agent**

| Field | Value |
| --- | --- |
| Name | `NiFi Flow Metrics Monitoring Agent` |
| Role | `Cloudera DataFlow Flow Metrics Analyst` |
| Backstory | the `backstory` string from the JSON, verbatim |
| Goal | the `goal` string from the JSON, verbatim |
| Temperature | **`0.1`** |
| Max iterations | `0` (unlimited) |
| Allow delegation | **off** |
| Verbose | off |
| Cache | off |
| Tools | attach `pollDeploymentMetrics` |

Temperature `0.1` is a deliberate choice for a reporting agent: you want the
same numbers described the same way, not variety.

### 6d. Create the task

**Tasks** → **Create Task**, assigned to the agent above.

Copy `description` and `expected_output` from the JSON. The description contains
two placeholders in braces, which is how Agent Studio turns them into inputs at
run time:

```
{deployment_crn}
{metrics_time_period}
```

Keep both, spelled exactly like that.

---

## Step 7 — Run it

Open the workflow and fill in the two inputs:

| Input | Value |
| --- | --- |
| `deployment_crn` | the CRN from step 5 |
| `metrics_time_period` | `LAST_ONE_HOUR` |

Accepted periods are `LAST_THIRTY_MINUTES`, `LAST_ONE_HOUR`, `LAST_TWELVE_HOURS`
and `LAST_ONE_DAY`. Anything else comes back as a readable error listing the
allowed values, rather than a crash.

**Timing matters on the first run.** The flow bursts every ten minutes and the
queue is above threshold for only about four of them. Run it twice, a few
minutes apart, and you should see the breach appear and clear — which is the
whole point of the oscillator.

### What a good report looks like

The agent returns a **Flow Metrics Report**: request status, the deployment's
name and control-plane status, the window covered, the metrics grouped by
component, the KPIs with their thresholds and derived verdicts, active alerts,
recent events, and a short health assessment citing specific component names and
values.

Three things worth checking, because they tell you the wiring is right rather
than merely working:

- **`pdf-selfheal-queue` is named** in the metrics section. If every metric is
  deployment-wide, step 4 did not take effect.
- **`threshold_breached_derived`** is reported as the *tool's* assessment, not as
  something the API said. It is computed in Python from the threshold you set and
  the current value — deterministic, so better not left to the model.
- **Zero and not-reported are distinguished.** Those are different findings and
  only one of them is about your flow.

### Metric resolution, so the numbers make sense

The window is divided into **25 buckets** — it is not a publish cadence.
`LAST_THIRTY_MINUTES` gives 25 points 75 seconds apart; `LAST_ONE_HOUR` gives 25
points 150 seconds apart. The buckets are anchored to request time, so two calls
a minute apart return different boundaries and therefore different values for the
same instant. Differencing points *inside* one response is sound; differencing
across two responses is not, and doing it produced a drain rate 20% off during
this repo's own verification.

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| Report has no mention of `pdf-selfheal-queue`; only CPU, memory, flow-wide data | Step 4 missing or did not take. Confirm with `list-flow-kpis-in-deployment` (not `list-deployment-kpis`). |
| `component_name_filter` returns `charts_returned: 0` | Same cause. Read `chart_counts.filter_hint` — it distinguishes "no component-scoped charts at all" from "none matched that name". |
| `404 NOT_FOUND` on the deployment | CRN truncated at the `/`, or the deployment, or its whole DataFlow service, is gone. `cdp df list-services` tells you which. |
| Permissions error with no mention of roles | The identity lacks `DFFlowUser`/`DFFlowAdmin` on the environment. |
| Every `cdp dfworkload …` connect-timeouts while `cdp df …` works | Workload-plane egress to `dfx.*.cloudera.site`. Reads and mutations use different hosts; one working proves nothing about the other. |
| Queue sits above threshold permanently | `Slow Drain` has 1 concurrent task, not 2. No headroom. |
| Queue never rises | Back pressure too low, or `Burst Generator` stopped. |
| Agent reports metrics with no tool call | Goal text altered. The instruction to call `pollDeploymentMetrics` before answering is what prevents invented numbers. |
| `ModuleNotFoundError` running `tool.py` locally | Agent Studio builds the venv at import; running locally does not. See *Testing a tool on its own* in the main README. |

---

## Tear it down

The deployment bills from provisioning until it is terminated. Nothing else here
costs money: the flow definition, the KPI and the Agent Studio objects are free.

**UI:** Deployment Manager → your deployment → **Terminate**. Then Catalog →
`pdf-selfheal-oscillator` → delete, if you are finished with it.

**CLI:**

```bash
cdp df terminate-deployment --deployment-crn "<deployment-crn>"
cdp df delete-flow --flow-crn "<flow-crn>"      # after termination completes
```

> A disabled or deleted **DataFlow service** takes its deployments with it. That
> is how the originally measured deployment ended: the service it lived in
> disappeared, and `describe-deployment` began returning `404 NOT_FOUND` on a
> CRN that had worked minutes earlier. Billing stops, but so does your demo —
> worth knowing before you plan a long-running one in a shared sandbox.

---

## Where to go next

- [`../../README.md`](../../README.md) — all three examples, and why the NiFi
  REST API is unreachable in CDF Public Cloud.
- [`../../self_healing/README.md`](../../self_healing/README.md) — the same
  deployment and the same KPI, with a loop that *acts* on the breach, plus the
  measurement showing a restart drove the queue from 428 to 883 and made the
  monitored metric worse.
