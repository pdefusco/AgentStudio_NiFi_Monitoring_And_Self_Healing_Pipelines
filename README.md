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

### Two layers of monitoring

There are two distinct places to ask "is this flow healthy", and this repo has an
example of each:

| | **Control plane** | **NiFi canvas** |
| --- | --- | --- |
| API | Cloudera DataFlow (CDP) | NiFi REST API (`/nifi-api`) |
| Answers | Is the *deployment* up? What size, version, KPIs? | What is happening *inside the flow*? |
| Sees | Deployment status, sizing, NiFi version, configured KPIs | Processor run states, queue depths, throughput, bulletins |
| Auth | CDP API access key pair | CDP API access key pair, exchanged for a short-lived DataFlow workload token |
| Example | [1. NiFi Monitoring Agents](#1-nifi-monitoring-agents) | [2. NiFi Canvas Monitoring Agents](#2-nifi-canvas-monitoring-agents) |

The distinction matters: **a deployment can be perfectly healthy at the control-plane
level while the flow inside it is broken** — processors invalid or stopped, queues
backing up, components throwing errors. The control plane says `RUNNING`; only the
NiFi API tells you the flow stopped doing useful work.

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

Both examples need the same single credential: a **CDP API access key pair**
(`access key id` + `private key`) for a user or machine user that can read DataFlow
deployments. The NiFi API example exchanges it for a short-lived workload token rather
than needing a second credential — see
[how authentication was worked out](#how-authentication-was-worked-out).

The identity also needs a DataFlow role granting access to the deployment:
`DFFlowUser` for read-only, `DFFlowAdmin` for full privileges.

> **No workload password is involved.** If you have set one, it is not used here, and it
> will not authenticate against `/nifi-api` — NiFi behind the DFX gateway has no
> username/password login provider at all.

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
and are running its `tool.py` outside this repository (example 1's source lives only inside
its `.zip`):

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
└── templates/
    ├── workflow_template_13nuodia.zip      # importable: NiFi Monitoring Agents
    ├── workflow_template_gvie9za2.zip      # importable: NiFi Canvas Monitoring Agents
    └── src/                                # unpacked sources, for review and rebuilds
        ├── build_template.py               # packs a source dir into an importable .zip
        ├── run_tool_local.py               # runs one template's tool outside Agent Studio
        └── nifi_canvas_monitoring/
            ├── workflow_template.json
            └── studio-data/tool_templates/pollnificanvas_ZG6cbJh4/
                ├── tool.py
                └── requirements.txt
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

**Why both a zip and an unpacked copy?** A zip is what Agent Studio imports, but it is
opaque in git — you cannot diff or code-review a tool whose source only exists inside an
archive. So newer templates keep their sources unpacked under `templates/src/` and are
packed on demand:

```bash
python templates/src/build_template.py nifi_canvas_monitoring \
  --output templates/workflow_template_gvie9za2.zip
```

`build_template.py` validates the manifest before writing — that it parses, that
workflow/agent/task/tool cross-references resolve, and that every tool's declared source
folder and files exist — so a broken bundle fails at build time rather than at import
time.

---

## Examples

| Template | Workflow | What it demonstrates |
| --- | --- | --- |
| [`workflow_template_13nuodia.zip`](templates/workflow_template_13nuodia.zip) | **NiFi Monitoring Agents** | Read-only inspection of a CDF deployment's live status via the Cloudera DataFlow API |
| [`workflow_template_gvie9za2.zip`](templates/workflow_template_gvie9za2.zip) | **NiFi Canvas Monitoring Agents** | Read-only inspection of the NiFi canvas itself — component states, queues, throughput, bulletins — via the NiFi REST API |

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

> **Known issue: `-m cdpcli.clidriver` does not invoke the CLI.** With the `cdpcli`
> version tested here (`0.9.164`), `cdpcli/clidriver.py` has no
> `if __name__ == "__main__"` guard, so `python -m cdpcli.clidriver <command>` imports the
> module, runs nothing, prints nothing, and **exits 0**. This tool would therefore always
> land in its "exited cleanly but returned nothing" branch rather than ever reaching
> Cloudera DataFlow.
>
> The working entry point is the one the installed `cdp` script uses:
>
> ```bash
> python -c 'import sys; from cdpcli.clidriver import main; sys.exit(main())' \
>   df describe-deployment --deployment-crn <crn> --output json
> ```
>
> Verified: `-m` gives exit `0` with empty stdout and stderr; the `-c` form gives exit
> `255` with the real `AUTHENTICATION_FAILURE` message on stderr. Example 2 uses the `-c`
> form. Example 1's zip still has the `-m` form and needs the same fix.

### 2. NiFi Canvas Monitoring Agents

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
      -> deployment.dfxLocalUrl                (the NiFi API base URL)
      -> deployment.service.environmentCrn     (what the token is scoped to)
      -> name, status, cfmNifiVersion, alert counts   (reported as context)

2. iam generate-workload-auth-token --workload-name DF --environment-crn <env-crn>
      -> token   (a short-lived signed JWT)

3. GET <dfxLocalUrl>/nifi-api/...   with  Authorization: Bearer <token>
```

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
| Deployment had no `dfxLocalUrl` / `environmentCrn` | `deployment_lookup` | `error`, `missing_fields`, `hint` about the overrides |
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

> **Still to confirm end-to-end.** What is *proven* is that Basic auth cannot work and
> that the gateway wants a signed JWT. What is **not yet proven** is that a DF workload
> token is accepted by `/nifi-api` — minting one needs a valid CDP API key, and the key
> available during development had been rotated (`NOT_FOUND: Access key ... not found`).
> If your run fails, the `www_authenticate` value in the output says whether the token was
> rejected as `invalid_token` (wrong or expired credential) or for insufficient access
> (right credential, missing DataFlow role).

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

**The tool is built to fail legibly.** It does not follow redirects, because a gateway that
bounces the request to SSO would otherwise return an HTML login page with a `200` status —
which looks like a parsing bug rather than an auth refusal. Remaining fallbacks if the
token route does not work out:

| Alternative | Trade-off |
| --- | --- |
| **Prometheus metrics endpoint** with the generated `nifi-metrics` credential | Officially documented and supported. Returns Prometheus text, not NiFi API JSON, so it needs a different parser — but it gives real flow-level numbers. |
| **Stay on the control plane** ([example 1](#1-nifi-monitoring-agents)) | Fully supported, but cannot see inside the flow. |

If you confirm what works in your environment, that result belongs in this section.

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
> only the read-only monitoring examples above exist.

- **Comparing the two layers, not just reading both.** Example 2 now resolves a
  deployment and reads its canvas in one call, so the data is there; the next step is an
  agent whose job is specifically to find *disagreement* — control plane `RUNNING` while
  the canvas has invalid processors or a stalled queue — and to treat that gap as the
  finding rather than reporting the layers side by side.
- **A Prometheus-metrics tool** — using the documented metrics endpoint and the
  `nifi-metrics` credential, which may be the supported route to flow-level numbers if
  direct `/nifi-api` access proves unavailable.
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

Conventions the existing template follows, worth keeping:

- Tool names are lowerCamelCase verb+noun — `pollNifiFlow`.
- Workflow and agent names are Title Case — "NiFi Monitoring Agent".
- Tools implement the Agent Studio contract: a `UserParameters` model for
  configuration and secrets, a `ToolParameters` model for what the agent supplies,
  a `run_tool(config, args)` function, and a `__main__` block taking `--user-params`
  and `--tool-params` as JSON so the tool can be tested standalone.
- Return real API responses unwrapped; return errors as structured dicts rather than
  raising.
