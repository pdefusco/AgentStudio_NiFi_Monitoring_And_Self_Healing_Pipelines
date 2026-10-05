# AgentStudio NiFi Monitoring and Self-Healing Pipelines

A growing collection of [Cloudera AI](https://www.cloudera.com/products/machine-learning.html)
**Agent Studio** workflow templates that use LLM agents to observe — and eventually
remediate — Apache NiFi data flows running as deployments in **Cloudera DataFlow
(CDF) Public Cloud**.

Each example is an exported Agent Studio workflow template (a `.zip` under
[`templates/`](templates/)) that you import into your own Agent Studio project, point
at your own CDP environment, and run. All of them need the same single credential: a CDP
API access key pair.

Examples 1–3 are read-only. **Example 4 can change a flow** — it edits the deployed flow
definition and swaps the running deployment onto the result — so it is gated differently
from the rest and worth reading about before you import it.

If you only want the one that works and gives the most detail without touching anything,
it is [example 3](#3-nifi-flow-metrics-monitoring-agents).

One thing here is not an Agent Studio template at all:
[`self_healing/`](self_healing/) is a prototype that acts on a deployment without any
agent or LLM — a plain Cloudera AI cron job, built to prove the
`monitor → detect → decide → act → audit` lifecycle on its own before an agent was asked
to drive it. Its most useful result is a negative one: neither action available to it
actually repaired anything, and the restart it chose left the queue *higher* than it
found it. That measurement is why example 4 exists and what shaped its design, so the
[Roadmap](#roadmap) is worth reading alongside it.

---

## Why

NiFi flow health in CDF Public Cloud is normally checked by a human looking at a
dashboard. That works until you have dozens of deployments, or until the interesting
question is not "what is the status field" but "is this deployment behaving the way it
did last week, and if not what should I do about it."

Agents are a natural fit for that second kind of question. They can call the Cloudera
APIs to fetch real state, reason over the response in natural language, summarize it
for a human, and — once you trust them — take a remediation action. Agent Studio makes
the pieces (agents, tasks, tools) reusable and shareable as templates, so a monitoring
pattern built once can be imported into any Cloudera AI workspace.

This repo is where those patterns get built up, one template at a time.

---

## Concepts

If you know one half of this stack but not the other:

| Term | What it is |
| --- | --- |
| **Cloudera AI (CAI)** | Cloudera's ML/AI platform. Hosts the Agent Studio application. |
| **Agent Studio** | A low-code application (deployed in CAI as an AMP) for building, running, and deploying agentic workflows. |
| **Workflow template** | An exportable `.zip` bundling a workflow and all of its agents, tasks, and tools. The unit of sharing — and what this repo stores. |
| **Agent** | An LLM persona: role, goal, backstory, temperature, and a set of tools it may call. |
| **Task** | A unit of work assigned to an agent: a description (with input placeholders) and an expected output. |
| **Tool** | A Python function the agent can call. Receives validated parameters and returns data the agent reasons over. |
| **Venv tool** | A tool whose `requirements.txt` Agent Studio installs into a dedicated virtualenv, so the tool can depend on packages the platform doesn't ship. |
| **CDF Public Cloud** | Cloudera DataFlow — runs NiFi flows as managed *deployments* on cloud infrastructure. |
| **Deployment CRN** | The Cloudera Resource Name identifying one CDF deployment, e.g. `crn:cdp:df:us-west-1:...`. The primary input to every example here. |
| **CDP CLI (`cdpcli`)** | Cloudera's control-plane CLI, pip-installable and importable as a Python module. The tools here drive the Cloudera API through it. |

### Four places to ask "is this flow healthy"

| | **Deployment status** | **Deployment metrics** | **NiFi canvas** | **Flow definition** |
| --- | --- | --- | --- | --- |
| API | Cloudera DataFlow (CDP) | Cloudera DataFlow (CDP) | NiFi REST API (`/nifi-api`) | Cloudera DataFlow (CDP) |
| Answers | Is the *deployment* up? | What are the *numbers* inside it? | What is the *state* of each component? | How is each component *configured*? |
| Sees | Status, sizing, NiFi version, configured KPIs | Measured metric charts with current vs. average values, KPI thresholds, alerts, event history | Processor run states, validity, queue depths, bulletins | Every processor's scheduling, concurrency and properties; every connection's endpoints and back-pressure thresholds |
| Writable? | no | no | yes, but a canvas edit reverts on the next redeploy | **yes, and durably** — edit the JSON, import a version, swap the deployment onto it |
| Auth | CDP API access key pair | CDP API access key pair | **No credential works** — see [below](#why-the-nifi-rest-api-is-not-reachable) | CDP API access key pair |
| Example | [1. NiFi Monitoring Agents](#1-nifi-monitoring-agents) | [3. NiFi Flow Metrics Monitoring Agents](#3-nifi-flow-metrics-monitoring-agents) | [2. NiFi Canvas Monitoring Agents](#2-nifi-canvas-monitoring-agents) — blocked | [4. NiFi Flow Repair Agents](#4-nifi-flow-repair-agents) |

The first distinction matters because **a deployment can be perfectly healthy at the
control-plane level while the flow inside it is broken**. The control plane says `RUNNING`
while throughput sits at zero and a queue only grows.

The fourth column is the one worth knowing about if you came here looking for the third.
We spent real time establishing that the canvas is unreachable, and concluded from that
we could not get processor-level granularity — the whole of example 2 is that dead end.
That conclusion was wrong, and the mistake is worth naming because it is easy to repeat:
**the canvas is not the only per-component view.** `cdp df get-flow-version` returns the
flow *definition*, which names every processor and every one of its settings. It answers a
different question — configured intent rather than live runtime state — but for anything
you would want to *change*, intent is the better question anyway, and it is the only one
with a durable write path. A canvas edit in CDF reverts on the next redeploy; a version
swap does not.

So the two columns are complements rather than substitutes. Metrics tell you a queue is
backing up; the definition tells you the draining processor is set to two concurrent
tasks. Neither alone gets you to a repair.

Three of the four columns are reachable with one CDP API key pair. The canvas is the one
you would reach for first and the one you cannot have: a CDF Public Cloud deployment's
NiFi REST API rejects every credential CDP can mint, for reasons worked out in detail
[further down](#why-the-nifi-rest-api-is-not-reachable).

The metrics column is the substitute for *observing*, and it is worth being precise about
how good a substitute it is. It gives **measured values over a time window**, which is
what makes "is this normal?" answerable at all — a number is only alarming next to its own
average. What it does **not** give is per-component detail by default. See
[how much component detail you actually get](#how-much-component-detail-you-actually-get)
before planning around it.

The flow-definition column is the substitute for *changing*, and it has no such caveat:
it is always per-component, because a flow definition cannot describe a processor without
naming it.

---

## How it works

```
  deployment_crn (workflow input)
        │
        ▼
  ┌───────────────────────────┐
  │ Task: inspect deployment  │
  └───────────────────────────┘
        │
        ▼
  ┌───────────────────────────┐        ┌──────────────────────────────┐
  │ Agent                     │ calls  │ Tool: pollNifiFlow           │
  │ "NiFi Monitoring Agent"   │ ─────▶ │ (venv tool: pydantic, cdpcli) │
  └───────────────────────────┘        └──────────────────────────────┘
        ▲                                        │
        │                                        │ cdpcli, in a subprocess
        │          raw CDF JSON response         │ df describe-deployment
        │                                        ▼
        │                              ┌──────────────────────────────┐
        └───────────────────────────── │ Cloudera DataFlow Public Cloud│
                                       └──────────────────────────────┘
        │
        ▼
  Deployment Status Report (natural language, grounded in the tool output)
```

A few design choices are deliberate and worth copying into future examples:

- **The agent is told, repeatedly, not to fabricate.** The role, goal, *and* task
  description all state that deployment information must come from the tool
  observation and never from prior knowledge. Infrastructure status is exactly the
  kind of plausible-sounding data an LLM will happily invent.
- **Temperature is 0.1.** This is a reporting task, not a creative one.
- **The tool returns the raw Cloudera API JSON, unwrapped.** No
  `{"success": true, "response": ...}` envelope — the agent sees every field CDF
  returned, so the same tool supports questions you didn't anticipate when you wrote it.
  There are two documented exceptions, and they are exceptions for opposite reasons:
  [chart compaction](#chart-compaction) in example 3, where the response would otherwise
  be mostly time-series points, and example 4's `getDeployedFlow`, which **withholds the
  flow definition on purpose**. A model that can see flow JSON can be coaxed into
  authoring flow JSON, so it is shown a named inventory instead, with an explicit
  `notes.flow_definition_withheld` saying the omission was deliberate rather than a
  failure.
- **Failures return structured data, not exceptions.** Every error path returns a dict
  containing `error` and the `deployment_crn`, so the agent can report "the API call
  failed, here's why" instead of guessing around a blank response.
- **A tool that writes defaults to not writing.** Example 4's `applyFlowRepair` takes
  `arm: bool = False` and, unarmed, returns the exact commands it would have run. The
  flag is a *tool* parameter the agent cannot set on its own initiative without the
  incident telling it to, and the deployment-name prefix that scopes what may be touched
  is a module constant rather than a parameter — a parameter could be set to `""`, and
  then every deployment in the tenant would pass the check.

---

## Prerequisites

- A **Cloudera AI** workspace with the **Agent Studio** AMP deployed, and an LLM model
  registered in it.
- A **CDF Public Cloud** environment with at least one running NiFi deployment, and
  that deployment's **CRN**.

Every example needs the same single credential: a **CDP API access key pair**
(`access key id` + `private key`) for a user or machine user that can read DataFlow
deployments. Nothing here needs a second one.

The identity also needs a DataFlow role granting access to the deployment:
`DFFlowUser` for read-only, `DFFlowAdmin` for full privileges. Examples 1–3 need only
the former. **Example 4 writes — a catalog version import and a deployment flow-version
swap — so it needs `DFFlowAdmin`.** Read access proves nothing about whether the swap
will be permitted, which is a distinction worth knowing in advance because the failure
arrives at the mutation, after the agent has already produced a proposal.

> **No workload password is involved.** If you have set one, it is not used here, and it
> will not authenticate against `/nifi-api` — NiFi behind the DFX gateway has no
> username/password login provider at all.

---

## Setup

1. **Import the template.** In Agent Studio, import a workflow template and upload the
   `.zip` from [`templates/`](templates/) — `workflow_template_e49veqyy.zip` for
   example 3, which is the one to start with. Agent Studio recreates the workflow,
   agent, task, and tool, and builds each tool's virtualenv from its `requirements.txt`.
   `cdpcli` is large, so expect a few minutes; example 4 declares three tools and so
   builds three virtualenvs.

2. **Supply credentials to the tool.** Every tool here declares the same two
   user-configurable parameters. Set them in the tool's configuration:

   | Parameter | Value |
   | --- | --- |
   | `CDP_ACCESS_KEY_ID` | Your CDP access key ID |
   | `CDP_PRIVATE_KEY` | Your CDP private key |

   These are *tool* parameters, not agent inputs — the LLM never sees or supplies them.
   Set them on **each** tool: example 4 has three, and a tool with no credentials fails
   at its first call rather than at import.

3. **Assign an LLM to the agent.** The [Prerequisites](#prerequisites) call for a model
   registered in the workspace, but registering it does not attach it to an imported
   agent — that is a separate selection on the agent itself, and an unassigned agent
   fails at run time rather than at import.

4. **Run the workflow.** Paste the CRN of the deployment you want inspected into
   `deployment_crn`. Example 3 takes a second input, `metrics_time_period` — start with
   `LAST_ONE_HOUR`. You should get back a report grounded in the live API response.

   **Example 4 takes six inputs**, because it is written to be launched by the monitoring
   job rather than filled in by hand: `incident_id`, `deployment_crn`, `queue_depth`,
   `drain_rate`, `breach_count` and `observed_at`. To run it manually, copy them from an
   incident record in the job's audit log — that is the intended human-gated path, not a
   workaround. Expect a report ending in `PROPOSED`; nothing is changed unless you both
   set `arm: true` and say in the incident text that a human armed it.

### Testing a tool on its own

Every `tool.py` has a `__main__` entrypoint, so you can validate credentials and
connectivity without going through an agent at all. This is the fastest way to tell a
credentials problem apart from a workflow-wiring problem.

**Install the dependencies first.** Agent Studio builds each venv tool's virtualenv for
you at import time, but running `tool.py` locally does not — so do it yourself once, or
you will get `ModuleNotFoundError`:

```bash
# from the repository root
python3 -m venv .venv
./.venv/bin/python -m pip install \
  -r templates/src/<template>/studio-data/tool_templates/<tool>/requirements.txt
```

Use `./.venv/bin/python -m pip`, not `./.venv/bin/pip`. The `pip` wrapper hard-codes an
interpreter path in its shebang, so if the venv is ever recreated against a different
Python it will silently install into the wrong place — the install reports success and the
import still fails. Going through `python -m pip` cannot drift.

`cdpcli` is a large dependency and takes a few minutes to install; that is normal.

Then export your credentials and run the tool through `run_tool_local.py`:

```bash
export CDP_ACCESS_KEY_ID=<your-key-id>
export CDP_PRIVATE_KEY=<your-private-key>   # the key itself, or a path to a file holding it

./.venv/bin/python templates/src/run_tool_local.py nifi_canvas_monitoring \
  --tool-params '{"deployment_crn":"crn:cdp:df:<region>:<account>:deployment:<id>"}'
```

The helper reads the template's own `workflow_template.json` to find the tool, then fills
each `UserParameters` field from an environment variable of the same name — standing in for
the injection Agent Studio does from the tool's saved configuration. Optional fields with no
variable set keep their defaults, and it says which ones those were. Add `--tool` when a
template declares more than one tool.

Passing credentials this way, rather than inside `--user-params`, keeps a private key out of
your shell history and out of the process list, where any other user on the machine could
read it from `ps`.

Each `tool.py` also has its own `__main__` entrypoint taking `--user-params` and
`--tool-params` as JSON strings — the same contract Agent Studio uses. Reach for that when
you want to pass a value no environment variable holds, or when you have unzipped a template
and are running its `tool.py` outside this repository:

```bash
./.venv/bin/python tool.py \
  --user-params '{"CDP_ACCESS_KEY_ID":"<your-key-id>","CDP_PRIVATE_KEY":"<your-private-key>"}' \
  --tool-params '{"deployment_crn":"crn:cdp:df:<region>:<account>:deployment:<id>"}'
```

Either way you get the tool's return value as indented JSON. If this works and the agent
doesn't, the problem is in the workflow wiring, not in the Cloudera API call.

`.venv/` is git-ignored.

---

## Repository layout

```
.
├── README.md
├── templates/                              # the Agent Studio examples: read-only
│   ├── workflow_template_13nuodia.zip      # importable: NiFi Monitoring Agents
│   ├── workflow_template_gvie9za2.zip      # importable: NiFi Canvas Monitoring Agents
│   ├── workflow_template_e49veqyy.zip      # importable: NiFi Flow Metrics Monitoring Agents
│   ├── workflow_template_k3rhzv7m.zip      # importable: NiFi Flow Repair Agents (mutates)
│   └── src/                                # unpacked sources, for review and rebuilds
│       ├── build_template.py               # packs a source dir into an importable .zip
│       ├── run_tool_local.py               # runs one template's tool outside Agent Studio
│       ├── nifi_monitoring/
│       │   ├── workflow_template.json
│       │   └── studio-data/tool_templates/pollnififlow_5LneKOim/
│       │       ├── tool.py
│       │       └── requirements.txt
│       ├── nifi_canvas_monitoring/
│       │   ├── workflow_template.json
│       │   └── studio-data/tool_templates/pollnificanvas_ZG6cbJh4/
│       │       ├── tool.py
│       │       └── requirements.txt
│       ├── nifi_flow_metrics_monitoring/
│       │   ├── workflow_template.json
│       │   └── studio-data/tool_templates/polldeploymentmetrics_PddIfQLk/
│       │       ├── tool.py
│       │       └── requirements.txt
│       ├── vendor_repairs.py               # copies repairs.py into the repair tools
│       └── nifi_flow_repair/
│           ├── workflow_template.json
│           ├── guardrail_walk.py           # runs every refusal against a stubbed CDP CLI
│           └── studio-data/tool_templates/
│               ├── getdeployedflow_3mFBJGxl/{tool.py,requirements.txt}
│               ├── proposeflowrepair_On1fGSOC/{tool.py,requirements.txt}
│               └── applyflowrepair_1imeQOp7/{tool.py,requirements.txt}
└── self_healing/                           # NOT Agent Studio: the one thing that acts
    ├── README.md                           # the prototype's own writeup — read this first
    ├── config.py                           # every tunable, each overridable by env var
    ├── df_api.py                           # CDP CLI wrapper + the guardrail + metric merge
    ├── repairs.py                          # the repair whitelist + validate_edit(); vendored
    ├── trigger.py                          # launches the repair workflow; backend pluggable
    ├── state.py                            # locked, atomic state file
    ├── flow/
    │   ├── build_flow_definition.py        # emits the synthetic misbehaving flow
    │   └── oscillating_queue.json          # the emitted artifact, committed so git can diff it
    ├── provision/
    │   ├── 01_import_flow.py               # catalog import
    │   ├── 02_create_deployment.py         # deploy, then wait for steady state
    │   ├── 03_configure_kpi.py             # discover ids -> write KPI -> verify it landed
    │   └── teardown.py                     # the only thing that stops the billing
    └── job/
        ├── monitor_and_remediate.py        # the job entrypoint: one pass per invocation
        ├── setup_runtime.py                # one-time `pip install --user cdpcli`
        └── create_job.py                   # registers the job + schedule via cmlapi
```

`templates/` and `self_healing/` **import** nothing from each other, in either direction, on
purpose: Agent Studio loads a tool as a single file, so a tool that imported from this repo
would work locally and fail the moment it was imported into Agent Studio.

One piece of code is nevertheless *shared*, and the distinction matters if you are editing it.
[`self_healing/repairs.py`](self_healing/repairs.py) — the repair whitelist and
`validate_edit()` — is **copied** into all three tools of example 4 by
[`templates/src/vendor_repairs.py`](templates/src/vendor_repairs.py), between sentinel comments
in both files. The copy is generated rather than hand-maintained because the two would
otherwise drift in the worst available way: both sides would keep passing their own tests
while disagreeing about what a repair means, and one of them decides what the agent is shown
while the other decides what gets written into a permanent catalog version. Edit the source,
then run `python templates/src/vendor_repairs.py` and rebuild the zip.

Each template `.zip` has the Agent Studio export structure:

```
workflow_template.json                          # manifest: workflow, agents, tasks, tools
studio-data/
└── tool_templates/
    └── <toolname>_<id>/
        ├── tool.py                             # tool implementation
        └── requirements.txt                    # tool's venv dependencies
```

`workflow_template.json` is readable and reviewable — it's where the agent role, goal,
backstory, task description, and expected output live. Worth reading before importing
anyone's template, including these.

**Why both a zip and an unpacked copy?** A zip is what Agent Studio imports, but it is
opaque in git — you cannot diff or code-review a tool whose source only exists inside an
archive. So every template keeps its source unpacked under `templates/src/` and is packed on
demand:

```bash
python templates/src/build_template.py nifi_flow_metrics_monitoring \
  --output templates/workflow_template_e49veqyy.zip
```

The output filename matters: Agent Studio exports use opaque 8-character ids, and rebuilding
to the same filename keeps existing links and bookmarks working. Without `--output` the
script names the archive after the source directory instead.

`build_template.py` validates the manifest before writing — that it parses, that
workflow/agent/task/tool cross-references resolve, and that every tool's declared source
folder and files exist — so a broken bundle fails at build time rather than at import
time.

---

## Examples

| Template | Workflow | What it demonstrates |
| --- | --- | --- |
| [`workflow_template_13nuodia.zip`](templates/workflow_template_13nuodia.zip) | **NiFi Monitoring Agents** | Read-only inspection of a CDF deployment's live status via the Cloudera DataFlow API |
| [`workflow_template_gvie9za2.zip`](templates/workflow_template_gvie9za2.zip) | **NiFi Canvas Monitoring Agents** | Read-only inspection of the NiFi canvas itself — component states, queues, throughput, bulletins — via the NiFi REST API. **Blocked on CDF Public Cloud** ([why](#why-the-nifi-rest-api-is-not-reachable)) |
| [`workflow_template_e49veqyy.zip`](templates/workflow_template_e49veqyy.zip) | **NiFi Flow Metrics Monitoring Agents** | Read-only per-component flow metrics — processor and connection charts, KPI thresholds, active alerts, event history — via the DataFlow API. **Start here** |
| [`workflow_template_k3rhzv7m.zip`](templates/workflow_template_k3rhzv7m.zip) | **NiFi Flow Repair Agents** | The first example that can *change* a flow. Reads the deployed flow definition at processor level, proposes one repair from a fixed whitelist, and applies it by catalog version swap — mutation gated on `arm: true` and six refusals. See [4](#4-nifi-flow-repair-agents) |

### 1. NiFi Monitoring Agents

A single-agent, single-task sequential workflow. Read-only — it cannot change anything
in your CDF environment.

**Agent — "NiFi Monitoring Agent"**
Role: *Cloudera DataFlow NiFi Deployment Monitor*. Temperature `0.1`, no delegation.
Instructed that it **must** call `pollNifiFlow` before answering any deployment
question, and that the tool observation is the only valid source of deployment facts.

**Task**
Input: `deployment_crn`.
Expected output — a *Deployment Status Report* containing:

- API request status (succeeded / failed)
- Deployment CRN
- Current deployment status
- Relevant deployment information returned by Cloudera DataFlow
- Error message, if the request or tool execution failed

**Tool — `pollNifiFlow`**
A venv tool (`pydantic`, `cdpcli`) that runs:

```bash
python -c 'import sys; from cdpcli.clidriver import main; sys.exit(main())' \
  df describe-deployment --deployment-crn <crn> --output json
```

in a subprocess, with the CDP credentials injected into that subprocess's environment
and a 60-second timeout. On the entry point, see the note below.

| | |
| --- | --- |
| **User parameters** (from config) | `CDP_ACCESS_KEY_ID`, `CDP_PRIVATE_KEY` |
| **Tool parameters** (from the agent) | `deployment_crn` |
| **On success** | The raw `describe-deployment` JSON response, verbatim |

Error returns — each one also echoes `deployment_crn`:

| Condition | Additional keys |
| --- | --- |
| CDP CLI exited non-zero | `return_code`, `stderr` |
| Exited cleanly but returned nothing | `stderr` |
| Output wasn't valid JSON | `raw_output` |
| Request exceeded the 60s timeout | — |
| Any other exception | `error_type` |

> **Fixed: `-m cdpcli.clidriver` does not invoke the CLI.** This example originally ran
> `python -m cdpcli.clidriver <command>`. With the `cdpcli` version tested here
> (`0.9.164`), `cdpcli/clidriver.py` has no `if __name__ == "__main__"` guard, so that
> form imports the module, runs nothing, prints nothing, and **exits 0** — the tool always
> landed in its "exited cleanly but returned nothing" branch and never reached Cloudera
> DataFlow at all. The failure is quiet in the worst way: a zero exit code and an empty
> stdout read as "the command worked and there was no data".
>
> The working entry point is the one the installed `cdp` script uses, and is what all three
> examples now run:
>
> ```bash
> python -c 'import sys; from cdpcli.clidriver import main; sys.exit(main())' \
>   df describe-deployment --deployment-crn <crn> --output json
> ```
>
> Verified: `-m` gives exit `0` with empty stdout *and* empty stderr; the `-c` form gives
> exit `255` with the real `AUTHENTICATION_FAILURE` message on stderr. The empty-output
> branch now also returns `stderr`, because the CLI can exit 0 while writing a diagnostic
> there and that text is the whole diagnosis.

### 2. NiFi Canvas Monitoring Agents

> **This template does not work against CDF Public Cloud.** A deployment's `/nifi-api`
> cannot be reached with any CDP credential — see
> [Why the NiFi REST API is not reachable](#why-the-nifi-rest-api-is-not-reachable) for the
> finding and for everything that was ruled out. It is kept, and documented, for two
> reasons: the investigation is the most reusable thing in this file, and the tool itself is
> correct for any NiFi whose API *is* reachable — a self-managed cluster, or one behind a
> gateway you control. **For flow-level numbers on CDF Public Cloud, use
> [example 3](#3-nifi-flow-metrics-monitoring-agents) instead.**

The same question as example 1 — "what is the state of this flow" — asked one layer
down, against the NiFi REST API of the deployment instead of the Cloudera DataFlow
control plane. Also read-only.

Built against **NiFi 2.x** (developed for Cloudera runtime `2.6.0.4.12.0.1-9`), using
only status endpoints that are stable across NiFi 1.x and 2.x.

**Agent — "NiFi Canvas Monitoring Agent"**
Role: *Apache NiFi Flow Health Monitor*. Temperature `0.1`, no delegation. Instructed to
call `pollNifiCanvas` before answering, to distinguish running / stopped / invalid /
disabled components, and to treat invalid components, growing queues, and `ERROR`
bulletins as health concerns worth calling out.

**Task**
Inputs: `deployment_crn` — the deployment whose canvas to inspect — and
`process_group_id`, where `root` means the whole canvas and a specific process group UUID
narrows the scope.
Expected output — a *NiFi Flow Health Report* with the API request status, the
deployment's control-plane status, NiFi version, component counts, queue state,
throughput, active bulletins, and a short health assessment citing the observed values
that support it. The agent is also told to call out when the control plane and the canvas
disagree, and never to echo a credential or token.

**Tool — `pollNifiCanvas`**
A venv tool (`pydantic`, `requests`, `cdpcli`). It takes a deployment CRN and resolves
everything else itself, which means this example **chains both monitoring layers in one
call**:

```
1. df describe-deployment --deployment-crn <crn>
      -> deployment.nifiUrl                    (the NiFi API base URL)
      -> deployment.service.environmentCrn     (what the token is scoped to)
      -> name, status, cfmNifiVersion, alert counts   (reported as context)

2. iam generate-workload-auth-token --workload-name DF --environment-crn <env-crn>
      -> token   (a short-lived signed JWT)

3. GET <nifiUrl>/nifi-api/...   with  Authorization: Bearer <token>
```

The base URL comes from `nifiUrl`, not from the `dfxLocalUrl` the same response also
carries. `dfxLocalUrl` is the shared dfx-local base for the environment and omits the
per-deployment namespace the gateway routes on, so every request built from it is answered
with a blanket `403` regardless of credential.

That `403` is not a malformed URL, though — it is the right URL for a different API.
`dfxLocalUrl` is the base the **`cdp dfworkload` service** talks to (`/dfx/api/rpc-v1/...`),
which is why it resolves, authenticates, and still refuses a `/nifi-api` path. Reaching it
through `cdp dfworkload` rather than by hand is what makes it useful.

A token is minted per call and **never returned to the agent or logged** — only
`token_obtained` and `expires_at` appear in the output. Then up to four read-only `GET`
requests, 30s timeout each:

| Endpoint | Returned as | What it gives |
| --- | --- | --- |
| `/nifi-api/flow/about` | `about` | NiFi version, so the report states which NiFi answered |
| `/nifi-api/flow/status` | `controller_status` | running / stopped / invalid / disabled tallies, active threads, total queued |
| `/nifi-api/flow/process-groups/{id}/status?recursive=` | `process_group_status` | per-component states, queue depths, throughput |
| `/nifi-api/flow/bulletin-board` | `bulletin_board` | active warnings and errors |

| | |
| --- | --- |
| **User parameters** (from config) | `CDP_ACCESS_KEY_ID`, `CDP_PRIVATE_KEY`; optionally `NIFI_BASE_URL` and `DF_ENVIRONMENT_CRN` |
| **Tool parameters** (from the agent) | `deployment_crn`, `process_group_id` (default `root`), `recursive` (default `true`), `include_bulletins` (default `true`) |
| **On success** | Each section's raw NiFi API response, verbatim, plus a `deployment` context block |

The same CDP API key pair as example 1 — no second credential to manage, and no workload
password.

`NIFI_BASE_URL` and `DF_ENVIRONMENT_CRN` are optional overrides that skip the
`describe-deployment` lookup, for when the control plane reports a URL that isn't
reachable from where the tool runs. `NIFI_BASE_URL` accepts the deployment URL with or
without a trailing slash and with or without a trailing `/nifi-api`, so all of these work:

```
https://dfx.<env-id>.<region>.cloudera.site/<deployment-namespace>
https://dfx.<env-id>.<region>.cloudera.site/<deployment-namespace>/
https://dfx.<env-id>.<region>.cloudera.site/<deployment-namespace>/nifi-api
```

**Partial failure is a first-class outcome.** Each of the four sections is fetched
independently, so one failing endpoint degrades that section rather than the whole call.
Every response carries a `request_status` block:

```json
"request_status": {
  "all_requests_succeeded": false,
  "failed_sections": ["bulletin_board"]
}
```

The agent is instructed to state explicitly when sections failed, so a partial report is
never presented as a complete one.

Steps 1 and 2 are different: they are prerequisites, not sections. If the deployment
lookup or the token mint fails there is nothing to report, so the tool returns early with
a single `deployment_lookup` or `authentication` failure rather than four identical
downstream errors.

| Condition | Section | Keys |
| --- | --- | --- |
| CDP CLI returned non-zero | `deployment_lookup` / `authentication` | `error`, `command`, `return_code`, `stderr`, `hint` about key validity and DataFlow roles |
| CDP CLI timed out (60s) or wasn't runnable | `deployment_lookup` / `authentication` | `error`, `command` |
| Deployment had no `nifiUrl` / `environmentCrn` | `deployment_lookup` | `error`, `missing_fields`, `hint` about the overrides |
| Control plane returned no token | `authentication` | `error`, `returned_fields` |
| `401` / `403` | per-section | `error`, `url`, `status_code`, `www_authenticate`, `hint` |
| `404` | per-section | `error`, `url`, `status_code`, `hint` about base URL and process group ID |
| `3xx` redirect | per-section | `error`, `url`, `status_code`, `redirected_to` |
| Other non-2xx | per-section | `error`, `url`, `status_code`, `raw_output` (truncated) |
| Body wasn't JSON | per-section | `error`, `url`, `raw_output`, `hint` that an HTML body means a gateway login page |
| TLS verification failed | per-section | `error`, `url`, `detail` |
| Connect / read timeout | per-section | `error`, `url` |
| Any other request error | per-section | `error`, `url`, `error_type`, `detail` |

On `401`/`403` the gateway's `WWW-Authenticate` header is passed straight through, because
it is far more informative than the status code — it distinguishes an expired or malformed
token (`error="invalid_token"`) from a valid token without sufficient access.

Standalone test:

```bash
python tool.py \
  --user-params '{"CDP_ACCESS_KEY_ID":"<key-id>","CDP_PRIVATE_KEY":"<private-key>"}' \
  --tool-params '{"deployment_crn":"crn:cdp:df:...","process_group_id":"root"}'
```

#### How authentication was worked out

This tool originally used HTTP Basic auth with CDP workload credentials, which seemed the
most plausible mechanism and is undocumented either way. It returned `401` on every
endpoint. Probing the gateway settled why, and the answer is useful enough to write down.

**HTTP Basic auth cannot work here.** Three independent signals, all reproducible against
a live deployment with `curl`:

```console
$ curl -s <base>/nifi-api/access/config
{"config":{"supportsLogin":false}}

$ curl -s -X POST <base>/nifi-api/access/token -d 'username=x&password=y'
Username/Password login not supported by this NiFi.          # HTTP 409

$ curl -si <base>/nifi-api/flow/about | head -1               # no credentials
HTTP/2 401
```

NiFi behind the gateway has **no username/password login provider at all**
(`supportsLogin: false`). The `401` carries **no `WWW-Authenticate: Basic` header**, which
per [RFC 7235](https://datatracker.ietf.org/doc/html/rfc7235#section-4.1) a server
accepting Basic auth is required to send. And the response is **byte-identical** with
correct credentials, with deliberately wrong ones, and with none at all — the credentials
are never evaluated. So a `401` here is not a credential problem, a permissions problem,
or a wrong-deployment problem.

**The gateway is an OAuth2 resource server expecting a signed JWT.** Sending a `Bearer`
header gets it parsed rather than ignored:

```console
$ curl -s -H 'Authorization: Bearer notatoken' <base>/nifi-api/flow/about
Unauthorized error="invalid_token", error_description="An error occurred while
attempting to decode the Jwt: Malformed token", ...

$ curl -s -H 'Authorization: Bearer <well-formed JWT, bad signature>' <base>/nifi-api/flow/about
Unauthorized error="invalid_token", error_description="An error occurred while
attempting to decode the Jwt: Signed JWT rejected: Another algorithm expected,
or no matching key(s) found", ...
```

It decoded the JWT and rejected the *signature*. That is a server telling you exactly
which credential it wants.

**CDP mints that credential.** `cdp iam generate-workload-auth-token` takes
`--workload-name DF` and, for DF, a required `--environment-crn`, returning `token`,
`endpointUrl` and `expireAt`. That is what the tool now sends. The field names used to
chain the calls were read from the CDP CLI's own published service models
(`cdpcli/data/df/df.yaml`, `cdpcli/data/iam/iam.yaml`) rather than guessed.

#### Why the NiFi REST API is not reachable

A DF workload token was eventually minted and sent. **NiFi rejects it.** Settled
empirically against two deployments in one environment — NiFi `1.28.1` and `2.6.0`:

```console
$ curl -s -H "Authorization: Bearer $(cdp iam generate-workload-auth-token \
    --workload-name DF --environment-crn <env-crn> --query token --output text)" \
    <base>/nifi-api/flow/about
Unauthorized error="invalid_token", error_description="An error occurred while
attempting to decode the Jwt: Signed JWT rejected: Another algorithm expected,
or no matching key(s) found", ...
```

The same `invalid_token` / wrong-algorithm refusal as a token with a forged signature —
and the token itself is well formed and correctly scoped: audience is the dfx host,
workload name is `DF`, signed by `consoleauth.cdp.cloudera.com/<account>`.

**The reason is the signing algorithm, and it is not configurable.** NiFi signs the bearer
tokens its REST API accepts with its own Ed25519 keys — **EdDSA**. A CDP-minted token is
**RS256**. Even where NiFi delegates interactive login to an external OIDC provider, the
credential its API accepts is NiFi-minted, not the provider's token. So there is no CDP
credential that can satisfy it: the control plane cannot sign with NiFi's keys, and NiFi
will not accept anything else.

A structured `401` rather than a network error is itself informative: external requests
**do** reach NiFi's filter chain. The mTLS proxy identity `proxy.dfx-nifi.<ns>` that
appears in NiFi's logs is NiFi describing its own proxy, not a network boundary keeping you
out. The request arrives; the credential is what fails.

**Already ruled out — do not spend time on token variants:**

| Tried | Result |
| --- | --- |
| Both NiFi generations (`1.28.1`, `2.6.0`) | Identical refusal |
| Bare `Authorization: Bearer <CDP workload token>` | `401 invalid_token`, wrong algorithm |
| `hadoop-jwt`, `knoxtoken`, `__Host-Authorization-Bearer` cookies | Ignored — byte-identical to sending nothing |
| `X-Forwarded-Access-Token` header | Ignored — byte-identical to sending nothing |
| HTTP Basic with workload credentials | `supportsLogin: false`; `access/token` returns `409` |
| `/dfx` paths on the same host | Blanket `403` for every path, including invented ones |

Cloudera documents only browser SSO ("View in NiFi", gated by `DFFlowUser` /
`DFFlowAdmin`). CDE publishes a per-cluster token-vending endpoint for exactly this
problem; **DataFlow has no published equivalent.**

**What to use instead:** [example 3](#3-nifi-flow-metrics-monitoring-agents). The DataFlow
API's own `list-deployment-system-metrics` returns metric charts scoped by `componentType`
and `componentName` — Processor, Process Group, Connection — which is the per-component
view this template was built to get, reachable with the same CDP API key pair.

What the documentation says, for context:

- Cloudera documents **interactive SSO** for reaching a deployment's NiFi UI —
  [viewing a deployment in NiFi](https://docs.cloudera.com/dataflow/cloud/managing-deployments/topics/cdf-viewing-dataflow-in-nifi.html).
- NiFi is specifically listed as an **SSO-based** interface, which is consistent with
  `supportsLogin: false` —
  [workload password](https://docs.cloudera.com/management-console/cloud/user-management/topics/mc-setting-the-ipa-password.html),
  [non-SSO interfaces](https://docs.cloudera.com/management-console/cloud/user-management/topics/mc-accessing-non-sso-interfaces-using-ipa-credentials.html).
- The only documented HTTP Basic path through the DFX gateway is the **Prometheus metrics
  endpoint**, using a dedicated generated `nifi-metrics` credential —
  [accessing NiFi metrics](https://docs.cloudera.com/dataflow/cloud/manage-environment/topics/cdf-access-nifi-metrics.html).
  Note that this lives on its own port and path; a bare `/federate` against the deployment
  base URL returns NiFi's "Did you mean /nifi" HTML page with a `200` status, which is easy
  to mistake for success.

**The tool is built to fail legibly**, which is why the finding above could be reached at
all. It does not follow redirects, because a gateway that bounces the request to SSO would
otherwise return an HTML login page with a `200` status — which looks like a parsing bug
rather than an auth refusal. And it surfaces `status_code` and `www_authenticate` per
section, so the refusal arrives as a readable reason rather than as a blank failure. Run it
against a NiFi whose API you can reach and it works as written.

If your environment behaves differently — a CDF release that vends a NiFi-acceptable token,
or a gateway configuration that changes this — that result belongs in this section.

### 3. NiFi Flow Metrics Monitoring Agents

The closest reachable substitute for the view example 2 was built to get: the DataFlow API's
own measured metrics, with the same CDP API key pair as example 1. Also read-only.
**This is the example to start with.**

What it gives that example 1 does not: example 1 reports a deployment's `status` field. This
reports what the deployment measured — throughput, CPU, memory, queue movement — as a
current value *and* an average over a chosen window, so "zero right now" can be
distinguished from "zero all day", plus the KPI thresholds someone configured, the alerts
firing against them, and the event history behind them.

What it does *not* give, compared to the NiFi canvas: component run states and validity.
Metrics are measured values, so a component that reports no metric does not appear at all.
The control plane knows what the flow *did*, not what each component *is*.

#### How much component detail you actually get

This is the limit worth knowing before you build on it, because it is easy to assume
otherwise from the schema. `MetricChart` carries `componentType` and `componentName`, and the
service model says `componentName` "will exist for Processor, Process Group, and Connection
metrics" — so per-component charts are clearly *possible*.

They are not *automatic*. Verified against a live single-flow deployment
(`cfmNifiVersion 2.6.0.4.12.0.2-1`): `list-deployment-system-metrics` returned three charts,
all `componentType: SYSTEM` with `componentName: null` — Core Allocation, CPU Utilization,
Memory Utilization. `list-deployment-kpis` returned three, two `SYSTEM` and one `NIFI_FLOW`
(Data Out, flow-wide). `list-flow-kpis-in-deployment`, resolved through the deployed-flow CRN
from `list-flows-in-deployment`, returned **zero**. Not one chart was component-scoped.

The reason: **a component-scoped chart exists only where someone configured a KPI against
that specific processor, process group or connection.** System metrics are system-scoped by
design — the name is literal. So the per-component view is available, but it is something you
*enable per component* in CDF, not something the API volunteers.

What this means in practice:

- On a deployment with no component KPIs configured, expect deployment-wide numbers. That is
  still the real signal for "throughput is zero" or "memory is climbing" — it just will not
  name the processor responsible.
- `component_name_filter` will match nothing on such a deployment. Because the tool excludes
  deployment-wide charts when a filter is set, that correctly returns `charts_returned: 0` —
  which looks like a bug. So `chart_counts` gains a `filter_hint` saying whether the
  deployment reported *no component-scoped charts at all* or reported some whose names simply
  did not match, and in the latter case lists the names that were present.
- To get per-processor charts, configure per-processor KPIs on the deployment in CDF. The tool
  needs no change; they appear in the `kpis` section with `componentName` populated, and the
  derived threshold verdict starts doing real work.
- That configuration can be done in the UI — Deployment Manager → Manage KPIs — **or from the
  CDP CLI**, on a deployment that already exists. An earlier version of this section claimed
  there was no CLI path. That was wrong, and the mistake is worth recording because it is easy
  to repeat: only the `df` service was enumerated. The CLI ships **two** DataFlow services, and
  every deployment mutation lives in the second one:

  | Need | Command |
  | --- | --- |
  | read the component ids a KPI needs | `cdp dfworkload get-flow-configuration-metadata-in-deployment` → `kpiMetaData.kpiScopes[].contextGroups[].scopeComponents[]{id,name}` |
  | read the current KPIs and config version | `cdp dfworkload get-flow-configuration-in-deployment` → `configurationVersion`, `kpis`, `kpisDirty` |
  | write a KPI | `cdp dfworkload update-flow-in-deployment --kpis` |

  Every `dfworkload` operation requires `--environment-crn`. Three things to know before using
  the write:

  - `--kpis` is a **whole-array replace**, not an append. Read the existing KPIs and send them
    back alongside the new one, or you will silently delete them.
  - `componentId` is **semicolon-joined ancestry** — `"<processGroupId>;<connectionId>"` — and
    NiFi assigns those ids at deploy time, so they must be discovered after deployment rather
    than predicted. `componentId` is *not* confined to `MetricSummary` as previously stated; it
    is a field of `ConfiguredKpi`, and the metadata call above is the read path to it.
  - **Do not build that string yourself.** `scopeComponents[].id` is *already* joined —
    verified live 2026-10-01: a connection reads back as two segments
    (`<processGroupId>;<connectionId>`) and the root process group as one. Prepending the
    enclosing `contextGroups[].id` repeats the first segment and produces a `componentId`
    matching no component. Pass `scopeComponents[].id` through unchanged.
  - The scope is keyed by **`type`**, not `id` (`kpiScopes[].id` is absent — reading it
    yields `None` for every scope), and its metric catalog is **`metricTypes`**, not
    `metrics`.
  - `update-deployment --kpis` also exists and is **deployment-level** whole-array replace.
    Against the wrong CRN it erases someone else's KPIs with no undo.
- For a multi-flow deployment, per-flow KPIs live behind
  `list-flows-in-deployment` → `list-flow-kpis-in-deployment`. This tool does not call those: it
  would add a lookup chain that breaks the independent-sections property. But do not conclude
  the call is redundant — an earlier version of this section said it "returned nothing the
  deployment-level call did not already cover", which was true only of the one deployment
  tested. The two surfaces in fact return **disjoint** sets. Measured against a live deployment
  in `se-sandbox-aws`:

  ```
  list-deployment-kpis          → NIFI_FLOW / (none) / Data In
                                  NIFI_FLOW / (none) / Data Out
  list-flow-kpis-in-deployment  → NIFI_PROCESSOR / GenerateFlowFile / Bytes Sent
  ```

  The component-scoped chart appears **only** in the flow-scoped call. So anything reading a
  component KPI it wrote through `update-flow-in-deployment` must read
  `list-flow-kpis-in-deployment`, or merge both; reading only `list-deployment-kpis` reports "no
  such metric" forever. `self_healing/df_api.py:metric_charts` merges the two.

**Agent — "NiFi Flow Metrics Monitoring Agent"**
Role: *Cloudera DataFlow Flow Metrics Analyst*. Temperature `0.1`, no delegation.
Instructed to call `pollDeploymentMetrics` before answering; to name the components findings
belong to rather than reporting deployment-wide aggregates; to compare each series' current
value against its own average over the window and say which direction it moved; to present
`threshold_breached_derived` as the tool's derived assessment rather than as something the
API reported; to state the time period the numbers cover; and to distinguish a value that
was **zero** from one that was **not reported**, which are different findings and only one of
them is about the flow.

**Task**
Inputs: `deployment_crn`, and `metrics_time_period` — start with `LAST_ONE_HOUR`.
Expected output — a *Flow Metrics Report* with the request status, the deployment's name and
control-plane status, the time period, component metrics grouped by component, KPIs with
their thresholds and derived verdicts, active alerts, recent events, and a short health
assessment citing the component names and observed values that support it.

**Tool — `pollDeploymentMetrics`**
A venv tool (`pydantic`, `cdpcli`) — no `requests`, because every call goes through the CDP
CLI. Five read-only commands, each taking the deployment CRN directly:

| Section | Command |
| --- | --- |
| `deployment` | `df describe-deployment` — identity and control-plane status, for context |
| `system_metrics` | `df list-deployment-system-metrics --metrics-time-period <period>` |
| `kpis` | `df list-deployment-kpis --metrics-time-period <period>` |
| `active_alerts` | `df list-deployment-active-alerts --sort firstOccurrence:desc` |
| `events` | `df list-deployment-events --max-items <event_count>` |

Nothing here is a prerequisite for anything else — unlike example 2 there is no
lookup-then-authenticate chain — so each section degrades on its own and every response
carries the same `request_status` block.

> `--max-items` on `list-deployment-events` is **not optional**. The CDP CLI auto-paginates
> that operation, so omitting it pulls the deployment's entire event history into the agent's
> context.

| | |
| --- | --- |
| **User parameters** (from config) | `CDP_ACCESS_KEY_ID`, `CDP_PRIVATE_KEY` |
| **Tool parameters** (from the agent) | `deployment_crn`; `metrics_time_period` (default `LAST_ONE_HOUR`); `component_name_filter` (default none); `include_kpis`, `include_events` (default `true`); `event_count` (default `25`, capped at `200`); `include_data_points` (default `false`) |
| **On success** | Each section's DataFlow API response, plus a `query` block recording the window actually used and a `request_status` block |

`component_name_filter` is a case-insensitive substring match on each chart's
`componentName`, for narrowing to one processor. When it is set, charts with no component
name — the deployment-wide aggregates — are excluded too, since the filter was asked for in
order to look at one component. Every metrics response carries `chart_counts` with
`charts_total` and `charts_returned`, so a filter that matched nothing is distinguishable
from a deployment that reported no charts at all.

#### Chart compaction

This is the one place in the repo that does not return a response verbatim, and the
deviation is deliberate. Everywhere else these tools hand the API response to the agent
untouched. A `MetricChart` is the exception: each one carries a `metrics.datas` array of
`{timestamp, value}` points, and those points are the bulk of the response while being the
part an LLM can do least with. Measured on the deployment tested, a `LAST_ONE_HOUR` series
holds 25–50 points, so `LAST_ONE_DAY` runs to several hundred per series — doubled where a
chart has a `mirroredMetrics` series, multiplied again by chart count, and multiplied once
more on a deployment that *does* have per-component KPIs configured. Compaction keeps that
from crowding out the numbers that matter.

So `summarize_chart()` keeps **every field except the point arrays** — `name`, `unitType`,
`componentType`, `componentName`, the full `alert` block with its `thresholdMoreThan`,
`thresholdLessThan` and `frequencyTolerance`, and each series' `currentValue`,
`averageValue` and label fields — and replaces `datas` with:

```json
"datas_summary": {
  "point_count": 60,
  "first_timestamp": 1759276800000,
  "last_timestamp": 1759280400000,
  "min_value": 0.0,
  "max_value": 184.0,
  "last_value": 0.0
}
```

Set `include_data_points: true` and `datas` comes back verbatim instead, so nothing is
permanently unavailable — it is a default, not a ceiling.

Because the thresholds and the current value both survive compaction, the tool also emits a
derived verdict per series:

```json
"threshold_breached_derived": {
  "breached": true,
  "reasons": ["current value 412 is above the configured thresholdMoreThan 300"]
}
```

That comparison is deterministic and is the entire point of a configured KPI, so it is
better done in Python than left to the model — and it is named `_derived` so the agent
reports it as the tool's assessment rather than as an API finding. It appears only on the
primary `metrics` series, never on `mirroredMetrics`: the threshold was not configured for
that series, and a verdict there would claim a judgement nobody made.

Error returns — each also echoes `deployment_crn`:

| Condition | Section | Keys |
| --- | --- | --- |
| `metrics_time_period` not one of the four accepted values | `parameters` | `error`, `supplied_value`, `allowed_values` — returned before any request is made |
| CDP CLI exited non-zero | per-section | `error`, `command`, `return_code`, `stderr`, `hint` matched to the stderr text (expired key, missing DataFlow role, unparseable private key) |
| CDP CLI wasn't runnable, or timed out (60s) | per-section | `error`, `command` |
| Output wasn't JSON | per-section | `error`, `command`, `raw_output` (truncated), `stderr` |
| CLI exited cleanly with no output | per-section | `error`, `command`, `stderr` |

A bad `metrics_time_period` is returned as a readable error listing `allowed_values` rather
than enforced with a pydantic `Literal`, because a `Literal` raises a `ValidationError` whose
text the agent never sees — it would just fail. This way the agent is told what it sent, what
is accepted, and can retry.

### 4. NiFi Flow Repair Agents

The first example here that can change anything. Examples 1–3 read; this one edits a flow
definition and swaps the running deployment onto the result.

It exists because the self-healing prototype in [`self_healing/`](self_healing/) proved its
own two remediations were not repairs: a measured `restart-deployment` went in with the
queue at 428 files and the queue read **883** three minutes later, because restarting stops
the draining processor along with the generator and the backlog simply freezes at breach
level. Fixing that needs processor-level granularity, which we assumed meant the NiFi REST
API — and that is [not reachable](#why-the-nifi-rest-api-is-not-reachable). The assumption
was wrong: `cdp df get-flow-version` returns the flow definition JSON, editing one
processor field in it is as granular as a canvas change and more durable, and
`cdp df change-flow-version-in-deployment` swaps the running flow onto a new version in
place. So the repair is a one-field edit through a fully supported API, with the same
credential examples 1 and 3 already use.

```
  ┌─ Cloudera AI job, cron * * * * * ───────────────────┐
  │  monitor → detect → debounce → guardrail           │   runs forever
  │  measures the queue depth and the drain rate       │   owns every clock
  └────────────────────────┬───────────────────────────┘
                           │ six scalars, no flow JSON:
                           │ incident_id, deployment_crn, queue_depth,
                           │ drain_rate, breach_count, observed_at
                           ▼
  ┌─ Agent Studio workflow, launched once per incident ─┐
  │                                                     │
  │   getDeployedFlow ──────▶ version + named inventory │  read
  │          │                (definition withheld)     │
  │          ▼                                          │
  │   proposeFlowRepair ────▶ field-level before/after   │  read
  │          │                + apply_echo              │
  │          ▼                                          │
  │   applyFlowRepair ──────▶ 6 refusals, then          │  WRITE
  │                           import version → swap     │  arm: false
  └────────────────────────┬───────────────────────────┘
                           │ an incident report; prose, not an outcome
                           ▼
  ┌─ the same job, later ───────────────────────────────┐
  │  re-reads the KPI and decides whether it worked     │
  │  the agent's claim is never the evidence            │
  └─────────────────────────────────────────────────────┘
```

**Three tools, because the agent must be able to propose without being able to act.**

| Tool | Mutates | Does |
| --- | --- | --- |
| `getDeployedFlow` | no | the deployed flow version plus a named per-processor and per-connection inventory |
| `proposeFlowRepair` | no | applies a named repair in memory and returns a field-level before/after diff |
| `applyFlowRepair` | **yes**, gated | re-derives the same edit, imports a catalog version, swaps the deployment, re-reads |

**The agent never sees flow JSON, and there is no path by which it could submit any.** It
names a repair from a fixed whitelist — `speed_up_slow_drain` (bounded `1 ≤ N ≤ 8`) or
`restore_baseline` — and supplies one integer. The tools own the transformation. This is not
a courtesy restriction; it is the reason a model is allowed near a production flow at all,
and it is why `getDeployedFlow` returns an inventory with an explicit
`notes.flow_definition_withheld` rather than the definition itself.

The upper bound of 8 is not arbitrary and must not be raised. A drain is only measurable if
it takes longer than one metric bucket; at 600 files and a 75-second bucket that ceiling is
exactly 8 files per second, which is `concurrentlySchedulableTaskCount: 8`. Beyond it the
repair would heal the queue in a way this monitor cannot see.

#### What stops a wrong CRN

`applyFlowRepair` refuses six times before it mutates anything. On a shared tenant a CRN is a
hundred characters of hex in which one wrong digit names someone else's production system, so
these are the load-bearing part of the example:

| Refusal | Why it is separate from the others |
| --- | --- |
| the repair is on the whitelist | an invented name is reported with `allowed_values`, never approximated |
| the deployment's live name starts with `pdf-selfheal` | a module constant, deliberately **not** a tool parameter: a parameter could be set to `""` and every deployment in the tenant would pass |
| the catalog flow appears in a prefix-scoped listing, **and** its name is re-checked | `--search-term` is a server-side substring match, so `not-pdf-selfheal-oscillator` matches it; this gates a write |
| the deployed `flowVersion` equals the one the proposal was computed from | a stale proposal replayed against a flow that has since changed |
| the caller's echoed `expected_values` match the edit re-computed here | this is what prevents a proposal being widened between proposing and applying |
| `validActions` offers neither `RETRY_` nor `CANCEL_CHANGE_FLOW_VERSION` | the API volunteering that a previous swap is half-applied — never retry that automatically |

Then `apply()` runs `validate_edit()` internally, which asserts the edit changed *only* what
the repair declared: identifiers, group ancestry, bundle versions and connection names all
unchanged, and no JSON path outside the repair's allow-list differs. Allow-list, not
deny-list, for the same reason the log redaction here is: a deny-list passes through whatever
it failed to anticipate. Renaming a connection is in that list because a `MetricChart` carries
`componentName` and no component id, so a rename silently orphans the KPI — which is to say,
blinds the monitor.

`arm` defaults to `false` and the dry run returns the exact two `argv` lines it would have
executed. Two flags on the swap are chosen rather than defaulted:

**`--strategy ONLY_RESTART_AFFECTED_COMPONENTS`.** `STOP_AND_EMPTY_QUEUES` is the only call in
the whole DataFlow surface that genuinely drains a backlog, which makes it the tempting
choice — it is exactly what `restart-deployment` could not do. It would also make the proof
worthless, because it empties the queue as a *side effect*: the monitored metric improves
whether or not the repair did anything. This repo has already been burned once by an action
whose effect on the signal was not what it looked like, and the fix is not to pick an action
that flatters the signal. Restarting only the changed processor leaves the existing backlog
in place, so a faster drain can be watched actually draining it.

**No `--kpis`.** The flag's own documentation says that if KPIs are specified they *replace*
all existing ones, so omitting it preserves them. A swap that dropped the connection KPI would
leave the monitor reporting health while unable to see the queue at all — which is why
`chart_present_before` is compared against the live chart afterwards, and why that mismatch
escalates instead of cooling. ([`self_healing/README.md`](self_healing/README.md) records the
same whole-array hazard on `update-deployment --kpis`, which is used nowhere in this repo.)

#### It is launched, not scheduled

An Agent Studio workflow runs **once** and makes one pass. It cannot sit in a loop polling an
API, so detection cannot live inside it. The split is that a Cloudera AI job monitors
continuously and launches this workflow once per incident, handing it six scalars —
`incident_id`, `deployment_crn`, `queue_depth`, `drain_rate`, `breach_count`, `observed_at` —
as task placeholders. Everything else the agent reads through its own tools. Those
placeholders are *prompt text*, which is the other reason the flow definition is not passed
in: it would mean pasting 6KB of JSON into the model's context.

The agent is told to report the drain rate it predicts **and to say plainly that it has not
measured it**. The job measures the real rate afterwards from the KPI, and only that
measurement decides whether the repair worked. The reason is in the agent's own backstory: a
faster drain means the queue spends less time above its threshold, so a successful repair
makes the breach count a *worse* signal, not a better one. The acceptance criterion is the
slope, never the absence of a breach — and that is why `restore_baseline` is in the whitelist
rather than being a nicety.

#### Editing it

The repair whitelist and `validate_edit()` live in
[`self_healing/repairs.py`](self_healing/repairs.py) and are **copied** into all three tools,
because Agent Studio imports a tool as a single file and so a tool cannot import anything
from this repo. The copy is generated, not hand-maintained:

```bash
python templates/src/vendor_repairs.py --check     # verify; exit 1 on drift
python templates/src/vendor_repairs.py             # regenerate
python templates/src/nifi_flow_repair/guardrail_walk.py
```

Run all three before rebuilding the zip. The last one is not redundant: the first generated
tools shipped with a missing `import copy`, which no static check caught — they imported
cleanly, exposed the right contract, and answered three guardrail cases correctly before
failing inside `apply()`. The vendoring script now refuses to generate a tool whose import
block lacks what the copied region uses, and `guardrail_walk.py` executes every refusal
against a stubbed CDP CLI, so the whole gate runs with no credentials and no VPC — which
matters, because there is **no `deleteFlowVersion` operation** and every live test of the
apply path leaves a permanent catalog version behind.

---

## Credentials and safety

- **CDP keys are tool parameters, never agent inputs.** The LLM is never shown them and
  cannot put them in its output. Keep it that way in new tools: secrets belong in
  `UserParameters`, never in `ToolParameters`.
- **Never commit keys.** No credentials belong in this repo, in a template `.zip`, or in
  a `tool.py`. Configure them in Agent Studio at import time.
- **Prefer a dedicated machine user** scoped to the DataFlow permissions the tool
  actually needs, rather than a human user's personal keys. Examples 1–3 need only read
  permissions; give them only read permissions.
- **Two things here have write access, and they are different kinds of risk.**
  [`self_healing/`](self_healing/) is not an agent and not importable: no LLM chooses
  anything in it. [Example 4](#4-nifi-flow-repair-agents) *is* an agent with a write path,
  which is a different risk profile and is gated accordingly.

  Both share the same guardrail shape, and it is worth reading before you give any agent
  a write path of your own, because the cost of getting it wrong is breaking a colleague's
  production flow. Acting requires an explicit arming step. Every mutating call checks a
  denylist of pre-existing deployments, requires the CRN recorded at provisioning, and
  re-reads the target's name *live* to confirm the prefix — the live re-read matters
  because a recorded CRN proves only what was true when it was recorded.
- **Scope a write by something the model cannot widen.** Example 4's deployment-name
  prefix and its repair whitelist are module constants, not tool parameters. The agent
  selects from the whitelist; it cannot extend it, and it cannot relax the prefix to `""`.
- **Don't show a model the document you don't want it to write.** Example 4's agent is
  never given the flow definition, only a named inventory of it. The withholding is the
  mechanism, not the prompt: there is no parameter on any of its three tools that accepts
  flow JSON, so "never author a flow definition" is enforced by the absence of a path
  rather than by an instruction the model might reinterpret.
- **Know what a mutation leaves behind.** There is no `deleteFlowVersion` operation in the
  DataFlow API, so every applied repair leaves a permanent catalog version and
  `delete-flow` is all-or-nothing. That is why example 4's guardrails are exercised
  against a stubbed CLI rather than live, and why its dry run is the default rather than
  a flag you remember to pass.

---

## Roadmap

> **Mostly planned, not yet implemented.** Examples 1–3 are read-only. Example 4 is the
> first that can write, and the last bullet below — self-healing — is where it comes from:
> there is a working non-agentic prototype in [`self_healing/`](self_healing/) whose
> measured failure to *repair* anything is the reason example 4 exists.
>
> Be precise about what example 4 has and has not shown. Its three tools are built, its
> six refusals are each exercised against a stubbed CDP CLI, and its dry run emits the
> exact commands it would issue. **The armed apply has not yet been run against the live
> deployment**, so the claim "a whitelisted edit swapped onto a running flow makes the
> queue drain measurably faster" is designed for and not yet earned. The number that will
> settle it is the drain rate: −1.99 files/s at concurrency 2, measured across nine
> buckets with no scatter, against an expected ≈ −4.0 at concurrency 4.

- **Comparing the layers, not just reading them.** Example 1 reports what the control
  plane thinks of a deployment and example 3 reports what its components actually measured,
  so the data is there; the next step is an agent whose job is specifically to find
  *disagreement* — control plane `RUNNING` while the throughput of every processor sits at
  zero — and to treat that gap as the finding rather than reporting the layers side by side.
- **A Prometheus-metrics tool** — using the documented metrics endpoint and the
  `nifi-metrics` credential. Example 3 is now the answer for flow-level numbers, so this is
  no longer a fallback but a higher-resolution alternative: the DataFlow API returns charts
  over fixed windows, while the metrics endpoint exposes raw NiFi counters at whatever
  interval you scrape them. It needs a Prometheus-text parser and a second credential, which
  is the trade for that resolution.
- **Provenance and deeper flow inspection** — provenance queries and per-connection
  back-pressure analysis, beyond the status summaries used today.
- **Fleet sweeps** — inspect every deployment in an environment and summarize the
  outliers, instead of one CRN at a time.
- **Alerting and summarization** — turn a sweep into a digest or an alert on a
  meaningful change, rather than a report produced on request.
- **Self-healing** — agents that take corrective action: restart a failed deployment,
  resize one that's saturated, or roll back a bad flow version.

  Remediation is a write path, and an agent with write access to production data flows
  is a different risk profile from one with read access. The intent here is for
  remediation actions to be proposed by the agent and gated on human approval before
  they execute, not fired autonomously.

  **There is now a prototype of the lifecycle underneath this**, in
  [`self_healing/`](self_healing/) — and it is deliberately *not* an Agent Studio
  workflow. No agent, no LLM, no tool registry: a plain Cloudera AI job on a cron
  schedule, running `monitor → detect → decide → act → audit` against a deployment of a
  flow built to misbehave on purpose. Proving the lifecycle and proving an agent can
  drive it are different problems, and mixing them would have hidden which half was
  failing. The human gate above is the `--arm` flag: unarmed, it decides and logs the
  exact command it would issue, and changes nothing.

  **What it proves is detect-and-respond, not repair**, and the distinction came out of
  a measurement rather than caution. Neither available remediation drains a queue — the
  flowfile repository is on a persistent volume and every processor stops for the
  duration — so the breach is still true when the action completes. Armed, the restart
  went in with the queue at 428 and the queue read **883** three minutes later: the
  action moved the monitored signal the wrong way. A loop that assumed "I acted,
  therefore it is better" would have escalated into a restart storm on exactly that
  evidence; five consecutive breach observations produced **one** action instead.

  That is the thing worth carrying into any agentic version of this. An agent asked to
  pick a remediation will reach for the one that is easiest to call, and
  `restart-deployment` is both the easiest to call and, here, actively harmful to the
  metric it was chosen to fix. The guardrails that matter are not about restricting
  which API an agent may touch — they are about refusing to re-act on a signal the
  previous action is expected to have made worse.

  **Example 4 is the answer to that measurement**, and it took the opposite route from the
  one we expected. Repairing a flow needs processor-level control, which we assumed meant
  the NiFi REST API — and that is unreachable here. It does not: the DataFlow API returns
  the flow definition as JSON, and swapping the deployment onto an edited version of it is
  both processor-granular and durable in a way a canvas edit is not. So the repair is a
  one-field edit through the same control-plane API example 1 already uses, with the agent
  choosing a repair *name* and one bounded integer and never seeing the JSON at all.

  What remains genuinely open is the loop's closure, not the repair. An Agent Studio
  workflow runs once and cannot poll, so the monitoring job has to *launch* it — and
  whether Agent Studio offers any programmatic launch is still unverified. Until that is
  settled, `TRIGGER_BACKEND` defaults to `record_only`: the job detects, writes a complete
  incident payload to the audit log, and launches nothing. That is the honest fallback
  rather than a stub — a human opens the workflow with the recorded payload — and it is
  what makes the whole loop demoable with no Agent Studio dependency at all.

---

## Adding a new example

Either direction works:

**From Agent Studio** — build the workflow in the UI, export the template, drop the
`.zip` into [`templates/`](templates/). Optionally unzip it under `templates/src/<name>/`
so the tool code is reviewable.

**From source** — copy an existing directory under `templates/src/`, edit
`workflow_template.json` and the tool code, generate fresh UUIDs for the workflow, agents,
tasks, and tools, then build and import:

```bash
python templates/src/build_template.py <name> --output templates/workflow_template_<id>.zip
```

Either way, add a row to the [Examples](#examples) table and a short section describing
the agent, the task's inputs and expected output, and each tool's parameters and return
shape.

Conventions the existing templates follow, worth keeping:

- Tool names are lowerCamelCase verb+noun — `pollNifiFlow`, `pollDeploymentMetrics`.
- Workflow and agent names are Title Case — "NiFi Monitoring Agent".
- Drive the CDP CLI through
  `python -c 'import sys; from cdpcli.clidriver import main; sys.exit(main())'`, never
  `-m cdpcli.clidriver` — see the [note on example 1](#1-nifi-monitoring-agents).
- Pass credentials to a subprocess through its environment, never on the command line,
  where any other user on the machine can read them from `ps`.
- Tools implement the Agent Studio contract: a `UserParameters` model for
  configuration and secrets, a `ToolParameters` model for what the agent supplies,
  a `run_tool(config, args)` function, and a `__main__` block taking `--user-params`
  and `--tool-params` as JSON so the tool can be tested standalone.
- Return real API responses unwrapped; return errors as structured dicts rather than
  raising.
- Validate an enumerated parameter inside `run_tool` and return `allowed_values` on a
  miss, rather than typing it as a pydantic `Literal`. A `Literal` raises a
  `ValidationError` whose text the agent never sees, so the call just fails; this way the
  agent is told what it sent and what is accepted, and can retry.

If the new example **writes** anything, four more apply. Example 4 is the worked case:

- **Separate the reading tool from the writing tool**, so the agent can propose without
  being able to act, and make the writing one default to a dry run that returns the exact
  commands it would have issued.
- **Scope what may be touched with a module constant, not a tool parameter.** Anything the
  agent can set, it can set to something permissive.
- **Make the agent name an operation, never author its payload.** A whitelist of repair
  names plus one bounded integer is a surface you can reason about; a JSON document the
  model wrote is not.
- **Require the writer to echo back what the proposer produced**, and re-derive the change
  independently before comparing. This is what stops a proposal being widened between
  being shown to a human and being applied.

And if a tool has to share logic with code elsewhere in this repo, generate the copy
rather than writing it twice — see
[`templates/src/vendor_repairs.py`](templates/src/vendor_repairs.py) and the note on
[what `templates/` and `self_healing/` share](#repository-layout).
