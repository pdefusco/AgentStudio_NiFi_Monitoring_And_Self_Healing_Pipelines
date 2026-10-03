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
            "hint": _hint_for(completed.stderr or ""),
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


def _hint_for(stderr: str) -> str:
    """Translate a known CDP CLI failure into an actionable sentence."""

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


def deployment_state(deployment_crn: str) -> str:
    """
    Return the deployment's current state string.

    Returns "UNKNOWN" if the deployment cannot be read, which the
    callers treat the same way as any other non-steady state: do not
    act.
    """

    deployment = describe_deployment(deployment_crn)

    if "error" in deployment:
        return "UNKNOWN"

    return (deployment.get("status") or {}).get("state") or "UNKNOWN"


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


# ---------------------------------------------------------------------
# Mutating calls
# ---------------------------------------------------------------------

def act(
    deployment_crn: str,
    expected_crn: str,
    args: list,
    dry_run: bool = True,
) -> dict:
    """
    Perform a mutating `dfworkload` call, guardrail first.

    The guardrail is applied here rather than at the call sites so that
    no mutation can bypass it, and `dry_run` is checked after it so that
    a dry run exercises the same check an armed run would.

    Returns a dict describing what happened, suitable for the audit log.
    It never contains a credential: `argv` is the argument list this
    module built, and credentials travel in the environment.
    """

    try:
        assert_action_allowed(deployment_crn, expected_crn)

    except NotAllowed as exc:
        return {
            "performed": False,
            "refused": True,
            "reason": str(exc),
            "argv": list(args),
        }

    if dry_run:
        return {
            "performed": False,
            "dry_run": True,
            "argv": ["dfworkload"] + list(args)
            + ["--environment-crn", config.ENVIRONMENT_CRN],
        }

    result = dfw(*args)

    return {
        "performed": "error" not in result,
        "argv": ["dfworkload"] + list(args)
        + ["--environment-crn", config.ENVIRONMENT_CRN],
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
