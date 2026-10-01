# AgentStudio NiFi Monitoring and Self-Healing Pipelines

A growing collection of [Cloudera AI](https://www.cloudera.com/products/machine-learning.html)
**Agent Studio** workflow templates that use LLM agents to observe — and eventually
remediate — Apache NiFi data flows running as deployments in **Cloudera DataFlow
(CDF) Public Cloud**.

Each example is an exported Agent Studio workflow template (a `.zip` under
[`templates/`](templates/)) that you import into your own Agent Studio project, point
at your own CDP environment, and run. The first example is read-only: an agent that
takes a deployment CRN and reports that deployment's real status, retrieved live from
the Cloudera DataFlow API.

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

### A note on scope: control plane, not `/nifi-api`

The current example monitors at the **CDF control-plane** level — deployment status,
size, version, configured KPIs — via the Cloudera DataFlow API. It does *not* call
NiFi's own `/nifi-api` endpoints, so it does not report processor-level state,
bulletins, or provenance. Flow-internal inspection is on the [roadmap](#roadmap).

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
        │                                        │ python -m cdpcli.clidriver
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
- **Failures return structured data, not exceptions.** Every error path returns a dict
  containing `error` and the `deployment_crn`, so the agent can report "the API call
  failed, here's why" instead of guessing around a blank response.

---

## Prerequisites

- A **Cloudera AI** workspace with the **Agent Studio** AMP deployed, and an LLM model
  registered in it.
- A **CDF Public Cloud** environment with at least one running NiFi deployment, and
  that deployment's **CRN**.
- A **CDP API access key pair** (`access key id` + `private key`) for a user or machine
  user with permission to read DataFlow deployments.

---

## Setup

1. **Import the template.** In Agent Studio, import a workflow template and upload the
   `.zip` from [`templates/`](templates/) (for the first example,
   `workflow_template_13nuodia.zip`). Agent Studio recreates the workflow, agent, task,
   and tool, and builds the tool's virtualenv from its `requirements.txt`.

2. **Supply credentials to the tool.** The `pollNifiFlow` tool declares two
   user-configurable parameters. Set them in the tool's configuration:

   | Parameter | Value |
   | --- | --- |
   | `CDP_ACCESS_KEY_ID` | Your CDP access key ID |
   | `CDP_PRIVATE_KEY` | Your CDP private key |

   These are *tool* parameters, not agent inputs — the LLM never sees or supplies them.

3. **Run the workflow.** The workflow takes one input, `deployment_crn`. Paste the CRN
   of the deployment you want inspected and run it. You should get back a Deployment
   Status Report grounded in the live API response.

### Testing the tool on its own

`tool.py` has a `__main__` entrypoint, so you can validate credentials and the CRN
without going through an agent at all:

```bash
python tool.py \
  --user-params '{"CDP_ACCESS_KEY_ID":"<your-key-id>","CDP_PRIVATE_KEY":"<your-private-key>"}' \
  --tool-params '{"deployment_crn":"crn:cdp:df:<region>:<account>:deployment:<id>"}'
```

It prints the tool's return value as indented JSON. If this works and the agent
doesn't, the problem is in the workflow wiring, not in the Cloudera API call.

---

## Repository layout

```
.
├── README.md
└── templates/
    └── workflow_template_13nuodia.zip     # exported Agent Studio workflow template
```

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

---

## Examples

| Template | Workflow | What it demonstrates |
| --- | --- | --- |
| [`workflow_template_13nuodia.zip`](templates/workflow_template_13nuodia.zip) | **NiFi Monitoring Agents** | Read-only inspection of a CDF deployment's live status via the Cloudera DataFlow API |

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
python -m cdpcli.clidriver df describe-deployment --deployment-crn <crn> --output json
```

in a subprocess, with the CDP credentials injected into that subprocess's environment
and a 60-second timeout.

| | |
| --- | --- |
| **User parameters** (from config) | `CDP_ACCESS_KEY_ID`, `CDP_PRIVATE_KEY` |
| **Tool parameters** (from the agent) | `deployment_crn` |
| **On success** | The raw `describe-deployment` JSON response, verbatim |

Error returns — each one also echoes `deployment_crn`:

| Condition | Additional keys |
| --- | --- |
| CDP CLI exited non-zero | `return_code`, `stderr` |
| Exited cleanly but returned nothing | — |
| Output wasn't valid JSON | `raw_output` |
| Request exceeded the 60s timeout | — |
| Any other exception | `error_type` |

---

## Credentials and safety

- **CDP keys are tool parameters, never agent inputs.** The LLM is never shown them and
  cannot put them in its output. Keep it that way in new tools: secrets belong in
  `UserParameters`, never in `ToolParameters`.
- **Never commit keys.** No credentials belong in this repo, in a template `.zip`, or in
  a `tool.py`. Configure them in Agent Studio at import time.
- **Prefer a dedicated machine user** scoped to the DataFlow read permissions the tool
  actually needs, rather than a human user's personal keys.
- **The current example is read-only** by design. When remediation tools arrive, treat
  write access as a separate decision — see the roadmap note below.

---

## Roadmap

> **Planned, not yet implemented.** The repo name describes where this is going; today
> only the read-only monitoring example above exists.

- **Flow-level NiFi monitoring** — processor state, bulletins, queue depths, and
  provenance via NiFi's own API, beyond control-plane deployment status.
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

---

## Adding a new example

1. Build the workflow in Agent Studio — agents, tasks, and tools.
2. Export it as a workflow template and drop the `.zip` into [`templates/`](templates/).
3. Add a row to the [Examples](#examples) table and a short section describing the
   agent, the task's inputs and expected output, and each tool's parameters and return
   shape.

Conventions the existing template follows, worth keeping:

- Tool names are lowerCamelCase verb+noun — `pollNifiFlow`.
- Workflow and agent names are Title Case — "NiFi Monitoring Agent".
- Tools implement the Agent Studio contract: a `UserParameters` model for
  configuration and secrets, a `ToolParameters` model for what the agent supplies,
  a `run_tool(config, args)` function, and a `__main__` block taking `--user-params`
  and `--tool-params` as JSON so the tool can be tested standalone.
- Return real API responses unwrapped; return errors as structured dicts rather than
  raising.
