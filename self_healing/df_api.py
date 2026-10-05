#!/usr/bin/env python3
"""
Cloudera DataFlow API access for the self-healing prototype.

Every CDP call in this prototype goes through this module, which exists
for two reasons:

  1. One place where credentials are handled. `CDP_ACCESS_KEY_ID` and
     `CDP_PRIVATE_KEY` are read from the environment and passed to the
     subprocess through `env`, never on a command line (a command line
     is readable by anyone who can run `ps`) and never logged.

  2. One place where the guardrail lives. `assert_action_allowed` is
     called by `act()` itself rather than by its callers, so a mutating
     call cannot reach Cloudera DataFlow without passing the check. The
     CDP account and the DataFlow service are shared with colleagues,
     and a stale CRN in a state file must not be able to restart
     somebody else's deployment.

Usage:

    from df_api import df, dfw, act, require, wait_for_steady_state

    deployment = require(df("describe-deployment",
                           "--deployment-crn", crn))

There are two DataFlow services in the CDP CLI and they are not
interchangeable:

    df          the control plane. Reads, plus flow-catalog writes.
    dfworkload  the workload plane. Every deployment mutation lives
                here, and every operation requires --environment-crn.

`dfw()` injects the environment CRN so no caller can forget it.
"""

import json
import os
import random
import shlex
import subprocess
import sys
import time
import urllib.parse

import config


# ---------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------

# Deployment states in which a deployment is settled enough to act on.
# Every other DeploymentState value means an action is already in
# flight, the deployment is being provisioned, or it is gone.
STEADY_STATES = frozenset(
    {
        "GOOD_HEALTH",
        "CONCERNING_HEALTH",
        "BAD_HEALTH",
    }
)

# Truncation limit for any CLI output quoted back in an error.
MAX_RAW_OUTPUT_CHARS = 4000

POLL_INTERVAL_SECONDS = 20


# ---------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------

CDP_CREDENTIALS_FILE = os.path.expanduser("~/.cdp/credentials")


def _credential_environment() -> dict:
    """
    Build the subprocess environment.

    Two credential sources are supported, because the two places this
    code runs supply credentials differently:

      - `CDP_ACCESS_KEY_ID` / `CDP_PRIVATE_KEY` in the environment, which
        is how the Cloudera AI job gets them
      - `~/.cdp/credentials`, which is how a laptop with a configured
        CDP CLI already has them

    The environment wins when both are present. Values are copied into
    the child environment and nowhere else: not into a command line
    (which `ps` would expose), not into a return value, not into the
    audit log.
    """

    environment = dict(os.environ)

    access_key_id = environment.get("CDP_ACCESS_KEY_ID")
    private_key = environment.get("CDP_PRIVATE_KEY")

    if access_key_id and private_key:
        return environment

    if os.path.exists(CDP_CREDENTIALS_FILE):
        # Leave the environment alone and let cdpcli read its own
        # credentials file. Passing a half-set pair would override the
        # file with something incomplete.
        environment.pop("CDP_ACCESS_KEY_ID", None)
        environment.pop("CDP_PRIVATE_KEY", None)

        return environment

    raise SystemExit(
        "error: no CDP credentials found.\n"
        "  Set CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY, or configure "
        f"{CDP_CREDENTIALS_FILE} with `cdp configure`.\n"
        "  In Cloudera AI, set both as project environment variables. Note "
        "they are readable by every collaborator on the project, so keep "
        "the project private."
    )


# ---------------------------------------------------------------------
# The CDP CLI
# ---------------------------------------------------------------------

def command_line(args: list, prefix: str = "cdp") -> str:
    """
    Render an argument list as a command line someone can actually paste.

    Nothing here is ever executed through a shell — `run_cdp` passes a
    list to `subprocess.run` — so this is purely for display. But the
    audit log and the dry-run output exist to be read and re-run by a
    human, and an unquoted argument invites a paste that behaves
    differently from what ran. `--cluster-size {"name": "EXTRA_SMALL"}`
    is the case that prompted this: valid as one argv element, three
    broken words at a prompt.
    """

    return " ".join([prefix] + [shlex.quote(str(a)) for a in args])


def run_cdp(args: list) -> dict:
    """
    Invoke the CDP CLI and return its parsed JSON output.

    On any failure this returns a dict carrying an `error` key rather
    than raising, so a caller can degrade one section of a report
    instead of collapsing. Use `require()` to turn a failure into a
    clean exit.
    """

    # Invoked through `-c` rather than `-m cdpcli.clidriver`, because
    # cdpcli.clidriver has no `if __name__ == "__main__"` guard: running
    # it as a module is a silent no-op that exits 0 having printed
    # nothing, which looks exactly like a command that returned no data.
    # `main()` is the entry point the installed `cdp` script itself uses.
    command = [
        sys.executable,
        "-c",
        "import sys; from cdpcli.clidriver import main; sys.exit(main())",
    ] + [str(a) for a in args] + ["--output", "json"]

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=config.CDP_TIMEOUT_SECONDS,
            env=_credential_environment(),
        )

    except subprocess.TimeoutExpired:
        return {
            "error": (
                f"The CDP CLI did not complete within "
                f"{config.CDP_TIMEOUT_SECONDS} seconds."
            ),
            "command": command_line(args),
        }

    except OSError as exc:
        return {
            "error": "The CDP CLI could not be executed.",
            "command": command_line(args),
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }

    if completed.returncode != 0:
        return {
            "error": "The CDP CLI returned an error.",
            "command": command_line(args),
            "return_code": completed.returncode,
            "stderr": (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS],
            "hint": _hint_for(
                completed.stderr or "", completed.returncode
            ),
        }

    stdout = (completed.stdout or "").strip()

    if not stdout:
        # stderr is included because the CLI can exit 0 while writing a
        # diagnostic there, and that text is the whole diagnosis.
        return {
            "error": "The CDP CLI produced no output.",
            "command": command_line(args),
            "stderr": (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS],
        }

    # The CLI prefixes its JSON with a plain-text warning on operations
    # it auto-paginates ("Max items received. Refine your filter..."),
    # which makes json.loads fail with "Extra data" on output that is
    # otherwise perfectly good. Slicing from the first brace is what
    # makes those operations usable at all.
    brace = stdout.find("{")

    if brace > 0:
        stdout = stdout[brace:]

    try:
        return json.loads(stdout)

    except ValueError:
        return {
            "error": "The CDP CLI returned output that was not JSON.",
            "command": command_line(args),
            "raw_output": stdout[:MAX_RAW_OUTPUT_CHARS],
            "stderr": (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS],
        }


def _hint_for(stderr: str, return_code: int = None) -> str:
    """
    Translate a known CDP CLI failure into an actionable sentence.

    `return_code` is taken as well as the text because the single most
    misleading case — a usage error diagnosed as a credential problem —
    is identified by the exit status more reliably than by any message.
    """

    # A missing cdpcli is an environment problem, not a credential
    # problem, and saying so saves a pointless key investigation.
    if "No module named 'cdpcli'" in stderr:
        return (
            "cdpcli is not installed for this interpreter. Locally: "
            "pip install cdpcli. In Cloudera AI: run job/setup_runtime.py "
            "once, which installs it into /home/cdsw/.local where it "
            "persists across job runs."
        )

    # cdpcli accepts CDP_PRIVATE_KEY as either a path to a key file or
    # the key itself, and reports a malformed key as a missing file,
    # which sends people looking for the wrong problem.
    if "Private key file" in stderr and "does not exist" in stderr:
        return (
            "CDP_PRIVATE_KEY is read as a file path first and as a literal "
            "private key only if no such file exists, so this message also "
            "appears when the key itself is malformed or truncated. Supply "
            "the complete private key, newlines included."
        )

    # Workload *configuration* operations are gated per environment,
    # separately from metrics reads. Verified: the same command 403s on
    # one environment in this account and succeeds on another.
    if "Access Denied" in stderr or "403" in stderr:
        return (
            "This identity lacks the DataFlow permission this operation "
            "needs on this environment. Workload configuration operations "
            "(get-flow-configuration*, update-flow-in-deployment) and "
            "deployment mutations are gated separately from metrics reads, "
            "so read access proves nothing about this call. DFFlowUser is "
            "enough to read metrics; configuring KPIs and restarting a "
            "deployment need more."
        )

    # A usage error is not a credential error, and the fallback below
    # says it is. That misdiagnosis cost real time once already: a
    # mistyped argument exited 2 and the hint sent the reader off to
    # check their API key, which was fine all along.
    #
    # Return code 2 is argparse's own usage exit, which the CLI never
    # reaches the network to produce, so it is a reliable discriminator;
    # the text patterns catch the cases where cdpcli wraps the error and
    # exits differently.
    usage_markers = (
        "Unknown options",
        "unrecognized arguments",
        "Invalid choice",
        "invalid choice",
        "arguments are required",
        "argument operation",
        "Unknown command",
    )

    if return_code == 2 or any(marker in stderr for marker in usage_markers):
        return (
            "This is a usage error from the CLI's own argument parsing, not "
            "an authentication or permission failure — the command never "
            "reached the API, so the credentials are not implicated. Check "
            "the operation name and its flags against "
            "`cdp df <operation> --help` (export PAGER=cat MANPAGER=cat "
            "first, and pipe through `tr -d '\\b' | col -b`)."
        )

    return (
        "Confirm CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY belong to an active "
        "CDP API key, and that the identity has a DataFlow role on this "
        "environment."
    )


def df(*args) -> dict:
    """Call a DataFlow control-plane operation."""

    return run_cdp(["df"] + list(args))


def dfw(*args) -> dict:
    """
    Call a DataFlow workload operation.

    The environment CRN is injected here because every `dfworkload`
    operation requires it and forgetting it is the most common way these
    calls fail.
    """

    return run_cdp(
        ["dfworkload"]
        + list(args)
        + ["--environment-crn", config.ENVIRONMENT_CRN]
    )


def require(result: dict, context: str = "") -> dict:
    """
    Return `result`, or exit with a legible message if it carries an
    error.

    Used by the provisioning scripts, where a failure means stop. The
    job uses the raw dicts instead, so one failing read degrades one
    section of its report.
    """

    if isinstance(result, dict) and "error" in result:
        lines = ["error: " + (context or "a CDP call failed")]

        for key in ("error", "command", "stderr", "hint", "detail"):
            value = result.get(key)

            if value:
                lines.append(f"  {key}: {value}")

        raise SystemExit("\n".join(lines))

    return result


# ---------------------------------------------------------------------
# Deployment state
# ---------------------------------------------------------------------

def describe_deployment(deployment_crn: str) -> dict:
    """Read a deployment, returning the inner `deployment` object."""

    result = df("describe-deployment", "--deployment-crn", deployment_crn)

    if "error" in result:
        return result

    return result.get("deployment", result)


def deployment_facts(deployment_crn: str) -> dict:
    """
    Read everything one `describe-deployment` already tells us.

    The response carries `flowVersion`, `flowVersionCrn`, `flowCrn` and
    `updated` alongside the state, and until this function existed
    `deployment_state()` read that response and threw all of it away.

    Those four fields are the entire "before" half of verifying a flow
    version swap, so having them costs no extra call:

      - `flow_version` is an int32, so `after > before` is a monotonic,
        non-negotiable fact that nothing an agent says can influence.
      - `updated` is milliseconds and catches a mutation that did *not*
        bump the version, which `update-flow-in-deployment` can do.

    Never raises. On a failed read every field is None and `error`
    carries the reason, because a monitor that crashes on a transient
    API failure is worse than one that reports UNKNOWN for a minute.
    """

    deployment = describe_deployment(deployment_crn)

    if "error" in deployment:
        return {
            "state": "UNKNOWN",
            "name": None,
            "flow_version": None,
            "flow_version_crn": None,
            "flow_crn": None,
            "updated": None,
            "error": deployment.get("error"),
        }

    return {
        "state": (deployment.get("status") or {}).get("state") or "UNKNOWN",
        "name": deployment.get("name"),
        "flow_version": deployment.get("flowVersion"),
        "flow_version_crn": deployment.get("flowVersionCrn"),
        "flow_crn": deployment.get("flowCrn"),
        "updated": deployment.get("updated"),
        "error": None,
    }


def deployment_state(deployment_crn: str) -> str:
    """
    Return the deployment's current state string.

    Returns "UNKNOWN" if the deployment cannot be read, which the
    callers treat the same way as any other non-steady state: do not
    act.

    A thin wrapper over `deployment_facts()` so there is one place that
    knows the response shape. Note what this state is *not* good for:
    `DeploymentState` carries `CHANGING_FLOW_VERSION` among its
    `x-deprecated-enum-values`, so a flow version swap does **not**
    show up here. Use `flow_facts()` to watch one of those.
    """

    return deployment_facts(deployment_crn)["state"]


def flow_facts(deployment_crn: str, flow_crn: str = "") -> dict:
    """
    Read the *deployed flow* object, which is where a version swap shows.

    Takes **two** CRNs because the operation requires both:
    `--deployed-flow-crn` is mandatory, not optional, and omitting it is
    an argparse usage error rather than a server-side one. When
    `flow_crn` is empty this resolves it via `deployed_flow_crn()`,
    which costs one extra call — so pass the value the job already keeps
    in its state file where possible.

    Three things live here and nowhere else:

      - `flow_version`, the catalog version actually deployed.
      - the deployed flow's own state and detailed state, which unlike
        the deployment's does move during a version change.
      - `valid_actions`, which is the API volunteering what it thinks
        can be done next. When it offers RETRY_CHANGE_FLOW_VERSION or
        CANCEL_CHANGE_FLOW_VERSION it is telling us the swap is
        **half-applied** — the control plane mutated and the workload
        hop did not. That is a machine-readable signal for a state no
        amount of reading the deployment would reveal.

    Never raises, for the same reason as `deployment_facts()`.
    """

    def empty(reason: str) -> dict:
        return {
            "crn": flow_crn or None,
            "flow_version": None,
            "flow_version_crn": None,
            "state": "UNKNOWN",
            "detailed_state": None,
            "message": None,
            "valid_actions": [],
            "error": reason,
        }

    if not flow_crn:
        flow_crn = deployed_flow_crn(deployment_crn)

        if not flow_crn:
            return empty(
                "The deployment has no deployed flow yet, or it could not "
                "be listed."
            )

    result = df(
        "describe-flow-in-deployment",
        "--deployment-crn",
        deployment_crn,
        "--deployed-flow-crn",
        flow_crn,
    )

    if "error" in result:
        return {
            "crn": flow_crn,
            "flow_version": None,
            "flow_version_crn": None,
            "state": "UNKNOWN",
            "detailed_state": None,
            "message": None,
            "valid_actions": [],
            "error": result.get("error"),
        }

    # The response key is `deployedFlow`, not `flow`.
    flow = result.get("deployedFlow") or {}
    status = flow.get("status") or {}

    return {
        "crn": flow.get("crn"),
        "flow_version": flow.get("flowVersion"),
        "flow_version_crn": flow.get("flowVersionCrn"),
        "state": status.get("state") or "UNKNOWN",
        "detailed_state": status.get("detailedState"),
        "message": status.get("message"),
        "valid_actions": list(flow.get("validActions") or []),
        "error": None,
    }


# The two actions the API offers when a flow version change got stuck
# between the control plane and the workload plane. Their presence in
# `validActions` is how the job recognises a half-applied swap.
HALF_APPLIED_ACTIONS = frozenset(
    {"RETRY_CHANGE_FLOW_VERSION", "CANCEL_CHANGE_FLOW_VERSION"}
)


def is_half_applied(facts: dict) -> bool:
    """True when the API is advertising a stuck flow version change."""

    return bool(
        HALF_APPLIED_ACTIONS.intersection(facts.get("valid_actions") or [])
    )


def wait_for_steady_state(
    deployment_crn: str,
    deadline_seconds: int,
    verbose: bool = True,
) -> str:
    """
    Poll until the deployment reaches a steady state, or the deadline
    expires.

    This exists because `df create-deployment` does not wait: the CLI
    extension issues the request and returns the deployment CRN
    immediately, so anything that touches the deployment next races it.
    The job's VERIFYING state uses the same function.

    Returns the final state, which the caller must check: a deadline
    expiry returns the last state seen rather than raising, so the
    caller can escalate rather than crash.
    """

    started = time.monotonic()
    last_state = None

    while True:
        state = deployment_state(deployment_crn)

        if state != last_state:
            if verbose:
                elapsed = int(time.monotonic() - started)
                print(f"  [{elapsed:>5}s] {state}", flush=True)

            last_state = state

        if state in STEADY_STATES:
            return state

        if time.monotonic() - started > deadline_seconds:
            return state

        time.sleep(POLL_INTERVAL_SECONDS)


def deployed_flow_crn(deployment_crn: str) -> str:
    """
    Return the deployment's deployed-flow CRN, or "" if it has none yet.

    This CRN is required by every flow-scoped operation —
    `list-flow-kpis-in-deployment`, `update-flow-in-deployment`,
    `stop-flow-in-deployment` — and it does not exist until the
    deployment has actually deployed its flow, which is later than the
    deployment itself existing.
    """

    result = df(
        "list-flows-in-deployment",
        "--deployment-crn",
        deployment_crn,
    )

    if "error" in result:
        return ""

    flows = result.get("deployedFlows") or []

    if not flows:
        return ""

    return flows[0].get("crn") or ""


# ---------------------------------------------------------------------
# Metric charts
# ---------------------------------------------------------------------

def metric_charts(
    deployment_crn: str,
    flow_crn: str = "",
    time_period: str = "",
) -> dict:
    """
    Read and merge both KPI chart surfaces.

    There are two, they take different arguments, and which one a given
    KPI appears in is not documented:

        list-deployment-kpis          deployment-scoped
        list-flow-kpis-in-deployment  needs the deployed-flow CRN

    A KPI written through `update-flow-in-deployment` is flow-scoped, so
    reading only the deployment surface could report "no breach" forever.
    Reading only the flow surface has the same failure in reverse. Both
    are read and merged, deduplicated on the display-string tuple —
    `MetricChart` carries no ids, so that tuple is the only identity a
    chart has.

    Returns {"charts": [...], "sources": {...}}. Each chart gains a
    `_source` key naming where it came from, and `sources` records each
    surface's outcome, so a failing read is visible in the audit log
    instead of looking like an absent chart.
    """

    period = time_period or config.METRICS_TIME_PERIOD
    charts = []
    seen = set()
    sources = {}

    reads = [
        (
            "list-deployment-kpis",
            [
                "list-deployment-kpis",
                "--deployment-crn",
                deployment_crn,
                "--metrics-time-period",
                period,
            ],
        )
    ]

    if flow_crn:
        reads.append(
            (
                "list-flow-kpis-in-deployment",
                [
                    "list-flow-kpis-in-deployment",
                    "--deployment-crn",
                    deployment_crn,
                    "--deployed-flow-crn",
                    flow_crn,
                    "--metrics-time-period",
                    period,
                ],
            )
        )

    else:
        sources["list-flow-kpis-in-deployment"] = "skipped: no deployed-flow CRN"

    for name, arguments in reads:
        result = df(*arguments)

        if "error" in result:
            sources[name] = f"error: {result.get('error')}"

            continue

        found = result.get("metricCharts") or []
        sources[name] = f"ok: {len(found)} chart(s)"

        for chart in found:
            key = (
                chart.get("componentType"),
                chart.get("componentName"),
                chart.get("name"),
            )

            if key in seen:
                continue

            seen.add(key)
            chart = dict(chart)
            chart["_source"] = name
            charts.append(chart)

    return {"charts": charts, "sources": sources}


def is_our_connection_chart(chart: dict) -> bool:
    """
    True if this chart measures our synthetic queue.

    `componentType` is matched case-insensitively on the substring
    CONNECTION because the two vocabularies differ: the KPI scope enum is
    `NIFI_CONNECTION` while the chart field is documented in terms of
    "Connection", and this is the one place they meet.
    """

    component_type = (chart.get("componentType") or "").upper()

    return (
        chart.get("componentName") == config.CONNECTION_NAME
        and "CONNECTION" in component_type
    )


# ---------------------------------------------------------------------
# The guardrail
# ---------------------------------------------------------------------

class NotAllowed(Exception):
    """A mutating call was refused by the guardrail."""


def assert_action_allowed(deployment_crn: str, expected_crn: str) -> dict:
    """
    Refuse to mutate anything that is not our synthetic deployment.

    All four conditions must hold. They are deliberately redundant: any
    single one of them could be defeated by a stale state file, a
    mistyped override, or a copied CRN, and the cost of being wrong is
    restarting a colleague's production flow.

      1. the target is not one of the deployments that predate this
         prototype
      2. the target is exactly the CRN recorded at provisioning time
      3. the deployment's own name, read live, carries our prefix
      4. it lives in the DataFlow service we configured

    Returns the deployment object on success so the caller does not need
    a second read.
    """

    if deployment_crn in config.DENYLIST_DEPLOYMENT_CRNS:
        raise NotAllowed(
            f"{deployment_crn} is on the denylist of deployments that "
            "existed before this prototype. Refusing."
        )

    if not expected_crn:
        raise NotAllowed(
            "no deployment CRN is recorded in the state file, so there is "
            "nothing this prototype is allowed to act on. Run "
            "provision/02_create_deployment.py first."
        )

    if deployment_crn != expected_crn:
        raise NotAllowed(
            "target does not match the deployment recorded at provisioning "
            f"time.\n  target:   {deployment_crn}\n  recorded: {expected_crn}"
        )

    deployment = describe_deployment(deployment_crn)

    if "error" in deployment:
        raise NotAllowed(
            "could not read the deployment to verify it is ours, so the "
            f"action is refused: {deployment.get('error')}"
        )

    name = deployment.get("name") or ""

    if not name.startswith(config.PREFIX):
        raise NotAllowed(
            f"deployment is named {name!r}, which does not carry the "
            f"{config.PREFIX!r} prefix. Refusing."
        )

    service_crn = (deployment.get("service") or {}).get("crn") or ""

    if service_crn != config.SERVICE_CRN:
        raise NotAllowed(
            "deployment is in an unexpected DataFlow service.\n"
            f"  found:    {service_crn}\n"
            f"  expected: {config.SERVICE_CRN}"
        )

    return deployment


def assert_flow_version_allowed(
    flow_version_crn: str,
    expected_flow_crn: str,
) -> dict:
    """
    Refuse to point our deployment at a flow version that is not ours.

    **This closes the one gap `assert_action_allowed` cannot see.** That
    function validates the *target* — which deployment is being mutated
    — and all four of its conditions are about `--deployment-crn`. But
    `change-flow-version-in-deployment` takes a **second** CRN,
    `--flow-version-crn`, naming what the deployment will be pointed at.
    A correct deployment plus a colleague's flow-version CRN therefore
    passes every existing check and repoints our deployment at their
    flow.

    On a shared tenant whose catalog holds 20 flow definitions by
    different authors, that is a plausible copy-paste, not a contrived
    attack. And it is the worst shape of failure available here: every
    guardrail reports success, the control plane reports success, and
    the running flow is now somebody else's.

    Three layers, cheapest first:

      1. the version CRN is exactly our flow CRN plus a `/v.N` suffix
      2. our flow CRN really does name a flow carrying `config.PREFIX`,
         re-checked locally after a server-side search, because that
         search is a substring match and this value authorises a
         mutation — the same discipline `teardown.delete_flow` applies
      3. the version actually exists in that flow's own version list

    Layer 2 is why layer 1 is not sufficient on its own: layer 1
    compares against `expected_flow_crn`, which comes from the state
    file, so a state file carrying a colleague's flow CRN would satisfy
    it. Layer 2 is what notices.

    Returns the version record on success, so the caller gets the
    version integer without a second read.
    """

    if not flow_version_crn:
        raise NotAllowed(
            "no flow-version CRN was supplied, so there is nothing to "
            "validate and the version swap is refused."
        )

    if not expected_flow_crn:
        raise NotAllowed(
            "no flow CRN is recorded in the state file, so there is no "
            "flow this prototype is allowed to deploy from. Run "
            "`monitor_and_remediate.py --reconcile` first."
        )

    # --- 1: the version belongs to the flow we recorded ---------------
    #
    # A prefix test rather than a split, and the trailing separator is
    # load-bearing: without it, a flow named `pdf-selfheal-oscillator-2`
    # would be accepted as a version of `pdf-selfheal-oscillator`.
    if not flow_version_crn.startswith(expected_flow_crn + "/"):
        raise NotAllowed(
            "the flow-version CRN does not belong to the flow recorded at "
            "provisioning time, so it names someone else's flow.\n"
            f"  version: {flow_version_crn}\n"
            f"  flow:    {expected_flow_crn}"
        )

    # --- 2: that flow is really ours ----------------------------------
    listing = df("list-flow-definitions", "--search-term", config.PREFIX)

    if "error" in listing:
        raise NotAllowed(
            "could not list flow definitions to verify the flow is ours, "
            f"so the version swap is refused: {listing.get('error')}"
        )

    ours = [
        flow
        for flow in (listing.get("flows") or [])
        if isinstance(flow, dict) and flow.get("crn") == expected_flow_crn
    ]

    if not ours:
        raise NotAllowed(
            f"{expected_flow_crn} is not among the flows matching "
            f"{config.PREFIX!r}, so it is not ours to deploy from."
        )

    name = ours[0].get("name") or ""

    if not name.startswith(config.PREFIX):
        raise NotAllowed(
            f"the flow is named {name!r}, which does not carry the "
            f"{config.PREFIX!r} prefix. The listing above was a server-side "
            "substring search, so this local re-check is what actually "
            "decides. Refusing."
        )

    # --- 3: the version exists in that flow ---------------------------
    versions = df(
        "list-flow-definition-versions",
        "--flow-crn",
        expected_flow_crn,
    )

    if "error" in versions:
        raise NotAllowed(
            "could not list the flow's versions to verify the target "
            f"exists, so the version swap is refused: "
            f"{versions.get('error')}"
        )

    matches = [
        version
        for version in (versions.get("flowVersions") or [])
        if isinstance(version, dict)
        and version.get("crn") == flow_version_crn
    ]

    if not matches:
        known = sorted(
            str(version.get("version"))
            for version in (versions.get("flowVersions") or [])
            if isinstance(version, dict)
        )

        raise NotAllowed(
            f"{flow_version_crn} is not a version of {name!r}. That flow "
            f"has versions {known}. Refusing to deploy a version the "
            "catalog does not report."
        )

    return matches[0]


# ---------------------------------------------------------------------
# Mutating calls
# ---------------------------------------------------------------------

def act(
    deployment_crn: str,
    expected_crn: str,
    args: list,
    dry_run: bool = True,
    plane: str = "dfworkload",
) -> dict:
    """
    Perform a mutating DataFlow call, guardrail first.

    The guardrail is applied here rather than at the call sites so that
    no mutation can bypass it, and `dry_run` is checked after it so that
    a dry run exercises the same check an armed run would.

    `plane` selects which CLI surface the call goes to, and it exists
    because not every mutation lives on the workload plane:

      "dfworkload"  the default. Needs `--environment-crn`, which `dfw()`
                    injects, and is served by a private `internal-*` ELB
                    reachable only from inside the VPC.
      "df"          the control plane, reachable from anywhere.
                    `change-flow-version-in-deployment` lives here — it
                    is a CLI *extension* that makes the workload hop
                    internally — and it takes no `--environment-crn`,
                    so passing one is an error rather than a harmless
                    extra.

    This used to hard-code `dfworkload` in three places, which is how
    the two planes diverged: the argv echoed into the audit log was
    assembled separately from the argv actually run, so a dry run could
    report a command shape the armed path would not have used. Both now
    come from `_argv()`.

    Returns a dict describing what happened, suitable for the audit log.
    It never contains a credential: `argv` is the argument list this
    module built, and credentials travel in the environment.
    """

    if plane not in ("df", "dfworkload"):
        raise ValueError(
            f"plane must be 'df' or 'dfworkload', got {plane!r}"
        )

    def _argv():
        """The exact command, so dry run and armed cannot disagree."""

        if plane == "dfworkload":
            return (
                ["dfworkload"]
                + list(args)
                + ["--environment-crn", config.ENVIRONMENT_CRN]
            )

        return ["df"] + list(args)

    try:
        assert_action_allowed(deployment_crn, expected_crn)

    except NotAllowed as exc:
        return {
            "performed": False,
            "refused": True,
            "reason": str(exc),
            # The argv is reported on a refusal too, without the plane
            # prefix it was never going to reach: what the caller needs
            # from a refusal is which command was blocked.
            "argv": list(args),
        }

    if dry_run:
        return {
            "performed": False,
            "dry_run": True,
            "plane": plane,
            "argv": _argv(),
        }

    result = dfw(*args) if plane == "dfworkload" else df(*args)

    return {
        "performed": "error" not in result,
        "plane": plane,
        "argv": _argv(),
        "result": result,
    }


def restart_deployment(
    deployment_crn: str,
    expected_crn: str,
    dry_run: bool = True,
) -> dict:
    """
    Restart the deployment.

    This does not drain the queue. The flowfile repository is on a
    persistent volume and both processors are stopped for the duration,
    so the backlog is frozen at breach level and the breach is still
    true when the restart completes. It is a response, not a fix.
    """

    return act(
        deployment_crn,
        expected_crn,
        [
            "restart-deployment",
            "--deployment-crn",
            deployment_crn,
        ],
        dry_run=dry_run,
    )


def stop_then_start_flow(
    deployment_crn: str,
    expected_crn: str,
    flow_crn: str,
    dry_run: bool = True,
) -> dict:
    """
    Stop the deployed flow and start it again.

    Same caveat as `restart_deployment`: stopping the flow stops the
    drain processor too, so the queue does not shrink while this runs.
    """

    stop = act(
        deployment_crn,
        expected_crn,
        [
            "stop-flow-in-deployment",
            "--deployment-crn",
            deployment_crn,
            "--deployed-flow-crn",
            flow_crn,
            "--wait-for-flow-to-stop-in-minutes",
            "5",
        ],
        dry_run=dry_run,
    )

    if stop.get("refused") or not (stop.get("performed") or dry_run):
        return {"steps": [stop], "performed": False}

    start = act(
        deployment_crn,
        expected_crn,
        [
            "start-flow-in-deployment",
            "--deployment-crn",
            deployment_crn,
            "--deployed-flow-crn",
            flow_crn,
        ],
        dry_run=dry_run,
    )

    return {
        "steps": [stop, start],
        "performed": bool(stop.get("performed") and start.get("performed")),
        "dry_run": dry_run,
    }


def change_flow_version(
    deployment_crn: str,
    expected_crn: str,
    flow_version_crn: str,
    expected_flow_crn: str,
    strategy: str = "ONLY_RESTART_AFFECTED_COMPONENTS",
    wait_minutes: int = None,
    dry_run: bool = True,
) -> dict:
    """
    Swap the running flow to a different catalog version, in place.

    This is the only mutation in this module that is a genuine *repair*
    rather than a response: it changes what the flow does, instead of
    restarting it and leaving the same backlog with the same behaviour.

    **It is a control-plane call**, which is why `act()` needed a
    `plane`. `cdp df change-flow-version-in-deployment` is a CLI
    extension that makes the workload hop internally; the `dfworkload`
    command of the same name is only that inner hop and takes no version
    argument at all.

    Two guardrails, not one, because this call takes two CRNs and the
    existing guardrail only ever saw the first.

    On `strategy`:

      ONLY_RESTART_AFFECTED_COMPONENTS  the default here. Restarts just
        the changed processor and leaves the backlog in place, which is
        the only way the repair can be *proven*: the new drain rate is
        measured against a queue that was not emptied for us.
      STOP_AND_PROCESS_DATA  the API's own default, and the fallback if
        the above turns out not to treat a concurrency change as
        "affecting" a component.
      STOP_AND_EMPTY_QUEUES  deliberately not used. It is the one call
        in the whole df surface that truly drains a backlog, which makes
        it tempting — and worthless here, because it improves the
        monitored metric whether or not the repair did anything.

    `--kpis` is omitted on purpose. The flag's own documentation says
    that if KPIs are specified they *replace* all existing ones, so
    omitting it is what preserves them — and a correct `componentId`
    could not be supplied anyway, since it is not knowable before the
    swap.
    """

    # `--wait-for-flow-to-stop-in-minutes` is valid *only* with the
    # default STOP_AND_PROCESS_DATA strategy, and passing it alongside
    # another raises DfExtensionError before any call is made. Caught
    # here rather than by the CLI so the message says why.
    if wait_minutes is not None and strategy != "STOP_AND_PROCESS_DATA":
        return {
            "performed": False,
            "refused": True,
            "reason": (
                "--wait-for-flow-to-stop-in-minutes is only valid with the "
                f"default STOP_AND_PROCESS_DATA strategy, not {strategy}."
            ),
            "argv": [],
        }

    try:
        version = assert_flow_version_allowed(
            flow_version_crn, expected_flow_crn
        )

    except NotAllowed as exc:
        return {
            "performed": False,
            "refused": True,
            "reason": str(exc),
            "argv": [],
        }

    args = [
        "change-flow-version-in-deployment",
        "--deployment-crn",
        deployment_crn,
        "--flow-version-crn",
        flow_version_crn,
        "--strategy",
        strategy,
    ]

    if wait_minutes is not None:
        args += ["--wait-for-flow-to-stop-in-minutes", str(wait_minutes)]

    result = act(
        deployment_crn,
        expected_crn,
        args,
        dry_run=dry_run,
        plane="df",
    )

    # The target version, so the caller can assert afterwards that
    # `flowVersion` actually became this integer rather than merely
    # changing.
    result["target_version"] = version.get("version")

    return result


ACTIONS = ("restart_deployment", "stop_then_start_flow")


def choose_action(action_count: int) -> str:
    """
    Pick which remediation to attempt.

    Deterministic alternation by default: the two actions differ in
    duration and in the state they leave behind, so randomness adds
    variance to the very thing being demonstrated and makes the audit
    log hard to reproduce. Set ACTION_SELECTION=random for a random
    pick.
    """

    if config.ACTION_SELECTION == "random":
        return random.choice(ACTIONS)

    return ACTIONS[action_count % len(ACTIONS)]


# ---------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------

def workload_endpoint() -> str:
    """
    Return the hostname `dfworkload` commands actually talk to.

    This is the discovery hop every `dfworkload` command makes before its
    real request: the control plane is asked for a workload auth token for
    this environment and answers with the endpoint to call. That endpoint
    is a *different host* from the control plane
    (`dfx.<env>.<region>.cloudera.site`), so when the job runs from a
    Cloudera AI workspace in another environment, "can it reach the
    control plane" and "can it reach this" are two separate questions.
    Naming the host turns an egress failure into something diagnosable
    instead of a bare 403 at the first real breach.

    The response also carries a bearer token. Only the hostname and the
    expiry are ever returned, and the token is never printed or logged —
    the same rule the Agent Studio templates follow.
    """

    result = run_cdp(
        [
            "iam",
            "generate-workload-auth-token",
            "--workload-name",
            "DF",
            "--environment-crn",
            config.ENVIRONMENT_CRN,
            "--output",
            "json",
        ]
    )

    if "error" in result:
        return f"UNRESOLVED ({result.get('error')})"

    url = result.get("endpointUrl") or ""
    host = urllib.parse.urlparse(url).hostname or url or "unknown"

    return f"{host} (token expires {result.get('expireAt')})"


def selftest(verbose: bool = True) -> bool:
    """
    Prove both API planes are reachable before anything is armed.

    Detection and action do not use the same path. Every `dfworkload`
    command first asks the control plane for a workload auth token and
    then calls a workload endpoint on a host the control-plane-only read
    path never touches, and the role needed to restart a deployment is
    not the role needed to read its metrics. Without this check, the job
    can detect perfectly for hours and then fail at the first real
    breach.
    """

    ok = True

    # Name the target before dialling it. A CRN that points at a deleted
    # service fails in a way that reads as a credentials or permissions
    # problem -- measured 2026-10-03, it cost an afternoon -- and the one
    # fact that would have settled it in a line is which service was
    # being asked. It is also what the guardrail compares against, so
    # printing it here explains a later "unexpected DataFlow service"
    # refusal without a code read.
    if verbose:
        print(f"  service:  {config.SERVICE_CRN}")

    control = df("list-deployments")

    if "error" in control:
        ok = False

        if verbose:
            print("  control plane: FAILED")
            print(f"    {control.get('error')}")
            print(f"    {control.get('hint', '')}")

    elif verbose:
        count = len(control.get("deployments") or [])
        print(f"  control plane: ok ({count} deployments visible)")

    workload = dfw("list-nifi-versions")

    if "error" in workload:
        ok = False

        if verbose:
            print("  workload plane: FAILED")
            print(f"    {workload.get('error')}")
            print(f"    {workload.get('hint', '')}")

    elif verbose:
        versions = workload.get("nifiVersions") or []
        print(f"  workload plane: ok ({len(versions)} NiFi versions)")

        if config.NIFI_VERSION not in versions:
            print(
                f"    warning: configured NIFI_VERSION "
                f"{config.NIFI_VERSION} is not in the available list"
            )

    if verbose:
        print(f"  workload endpoint: {workload_endpoint()}")

    return ok
