#!/usr/bin/env python3
"""
Apply a proposed repair: import a flow version, then swap the deployment.

This is the only tool in the template that changes anything, and it
defaults to **not** changing anything: `arm` is False unless explicitly
set, and an unarmed call returns the exact commands it would have run.

What it does when armed, in order:

    df import-flow-definition-version      (a new catalog version)
    df change-flow-version-in-deployment   (point the deployment at it)

Both are `cdp df` — control plane — though the second makes a workload
hop internally, so it must run somewhere that can reach the workload
plane even though the command itself is a control-plane one.

Five refusals stand between a call and a mutation, and each exists
because of a specific way this could go wrong on a shared tenant:

  1. **The repair must be named**, from the vendored whitelist. The LLM
     never authors flow JSON.
  2. **The deployment name must carry the expected prefix.** A typo'd
     CRN belonging to a colleague is refused. The prefix is a module
     constant, not a parameter, so nothing the model says can widen it.
  3. **The flow must be ours.** The catalog flow the new version is
     imported into is re-checked by name after being matched by CRN,
     because the server-side search is a substring match and this is a
     write.
  4. **The flow version must not have moved.** The proposal names the
     version it was computed from; if the deployed version differs now,
     the proposal is stale and is refused rather than replayed against a
     flow it was never computed for.
  5. **The caller must echo back the exact diff** `proposeFlowRepair`
     produced. The edit is re-derived here from the live definition and
     compared; any mismatch is refused. So a proposal cannot be widened
     on its way to being applied.

Then the validator in the vendored region runs over the re-derived edit
independently, refusing anything the repair did not declare.

`--strategy ONLY_RESTART_AFFECTED_COMPONENTS` is deliberate and is the
most consequential choice here. `STOP_AND_EMPTY_QUEUES` is the one call
in the DataFlow surface that genuinely drains a backlog, which makes it
tempting — but it would empty the queue as a side effect, improving the
monitored metric whether or not the repair did anything. A proof that
cannot fail is not a proof. Restarting only the changed processor leaves
the existing backlog in place, so a faster drain has to actually drain
it.

`--kpis` is never passed. The flag's own documentation says that if KPIs
are specified they *replace all existing KPIs*, so omitting it preserves
them — and supplying them would risk erasing the very KPI the monitoring
job watches, blinding the monitor while it continues to report health.
"""

from pydantic import BaseModel, Field
from typing import Any, Optional
import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile


CDP_TIMEOUT_SECONDS = 180

MAX_RAW_OUTPUT_CHARS = 2000

# The guardrail, as a module constant rather than a tool parameter.
#
# If this were a ToolParameters field the model could set it to "" and
# every deployment in the tenant would pass the check. It is not
# configurable from the outside at all: the only way to point this tool
# at a different family of deployments is to edit this line, which is a
# human action reviewable in a diff.
REQUIRED_DEPLOYMENT_PREFIX = "pdf-selfheal"

# The strategy, also fixed. See the module docstring for why
# STOP_AND_EMPTY_QUEUES is excluded despite being the only action that
# would actually drain the backlog.
CHANGE_STRATEGY = "ONLY_RESTART_AFFECTED_COMPONENTS"


class UserParameters(BaseModel):
    """
    Credentials supplied through Agent Studio tool configuration.

    Injected by Agent Studio; never supplied by, or shown to, the LLM.
    """

    CDP_ACCESS_KEY_ID: str
    CDP_PRIVATE_KEY: str


class ToolParameters(BaseModel):
    """
    Arguments supplied by the agent when invoking this tool.
    """

    deployment_crn: str = Field(
        description=(
            "CRN of the deployment to repair. Must be the same one the "
            "proposal was computed for."
        )
    )

    repair: str = Field(
        description=(
            "The repair name from the proposal you are applying. Must "
            "match the proposal exactly."
        )
    )

    expected_flow_version: int = Field(
        description=(
            "The `expected_flow_version` from the proposal's apply_echo. "
            "If the deployed flow has moved to a different version since "
            "the proposal was made, the apply is refused as stale."
        )
    )

    expected_values: dict = Field(
        description=(
            "The `expected_values` object from the proposal's apply_echo, "
            "passed back unchanged. This tool re-computes the edit from "
            "the live flow and refuses to act if the result differs. Do "
            "not edit, trim, or reformat this object — copy it exactly as "
            "the proposal returned it."
        )
    )

    concurrency: Optional[int] = Field(
        default=None,
        description=(
            "The `concurrency` from the proposal's apply_echo, if it had "
            "one. Required for 'speed_up_slow_drain'."
        ),
    )

    arm: bool = Field(
        default=False,
        description=(
            "Leave false to see exactly what would be run without "
            "changing anything — this is the default and the safe "
            "choice. Set true only when a human has reviewed the "
            "proposal and asked for it to be applied. Applying creates a "
            "permanent catalog version that cannot be deleted."
        ),
    )


# ---------------------------------------------------------------------
# CDP control plane helpers
#
# A private copy, as in every other tool in this repo: Agent Studio
# imports a tool as a single file, so `templates/` and `self_healing/`
# share no code in either direction.
# ---------------------------------------------------------------------

def cdp_argv(args: list) -> list:
    """
    The argv a call would use, for echoing in a dry run.

    Built by the same function the executor calls, so the command shown
    unarmed and the command run armed cannot drift apart. Assembling
    those separately is exactly how an audit log comes to describe
    something other than what happened.
    """

    return ["cdp"] + list(args) + ["--output", "json"]


def run_cdp(config: UserParameters, args: list) -> Any:
    """
    Invoke the CDP CLI and return its parsed JSON output.

    On any failure, returns a dict containing an `error` key rather than
    raising. Credentials are passed through the environment so they
    never appear in a command line or process listing.
    """

    # `-c` rather than `-m cdpcli.clidriver`: that module has no
    # `__main__` guard, so running it as a module exits 0 having printed
    # nothing, which is indistinguishable from an empty result.
    command = [
        sys.executable,
        "-c",
        "import sys; from cdpcli.clidriver import main; sys.exit(main())",
    ] + list(args) + ["--output", "json"]

    environment = dict(os.environ)
    environment["CDP_ACCESS_KEY_ID"] = config.CDP_ACCESS_KEY_ID
    environment["CDP_PRIVATE_KEY"] = config.CDP_PRIVATE_KEY

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=CDP_TIMEOUT_SECONDS,
            env=environment,
        )

    except subprocess.TimeoutExpired:

        # Deliberately explicit that this is not a safe failure. A
        # timeout on a mutation means the request may well have been
        # accepted; treating it as "did not happen" and retrying is how
        # one repair becomes two catalog versions.
        return {
            "error": (
                f"The CDP CLI did not complete within "
                f"{CDP_TIMEOUT_SECONDS} seconds."
            ),
            "command": " ".join(args),
            "outcome_unknown": True,
            "hint": (
                "A timeout on a mutating call does not mean the call did "
                "not happen. Do not retry. Read the deployment's current "
                "flow version to find out what actually landed."
            ),
        }

    except Exception as exc:

        return {
            "error": "The CDP CLI could not be executed.",
            "command": " ".join(args),
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }

    if completed.returncode != 0:

        stderr = (completed.stderr or "").strip()

        return {
            "error": "The CDP CLI returned an error.",
            "command": " ".join(args),
            "return_code": completed.returncode,
            "stderr": stderr[:MAX_RAW_OUTPUT_CHARS],
            "hint": hint_for(stderr, completed.returncode),
        }

    stdout = (completed.stdout or "").strip()

    if not stdout:

        # Not an error for every command: some mutations return an empty
        # body on success, so this is reported rather than raised and the
        # caller decides.
        return {
            "empty_response": True,
            "command": " ".join(args),
            "stderr": (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS],
        }

    try:
        return json.loads(stdout)

    except ValueError:

        return {
            "error": "The CDP CLI returned output that was not JSON.",
            "command": " ".join(args),
            "raw_output": stdout[:MAX_RAW_OUTPUT_CHARS],
            "stderr": (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS],
        }


def hint_for(stderr: str, return_code=None) -> str:
    """Choose a diagnosis for a failed CLI call."""

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
            "This is a usage error from the CLI's own argument parsing, "
            "not an authentication or permission failure — the command "
            "never reached the API, so the credentials are not "
            "implicated."
        )

    if "No module named 'cdpcli'" in stderr:
        return (
            "cdpcli is not installed for the interpreter running this "
            "tool. Agent Studio installs it from requirements.txt for a "
            "venv tool."
        )

    if "Private key file" in stderr and "does not exist" in stderr:
        return (
            "CDP_PRIVATE_KEY is read as a file path first and as a "
            "literal key only if no such file exists, so this message "
            "also appears when the key is malformed or truncated."
        )

    if "timed out" in stderr.lower() or "connect" in stderr.lower():
        return (
            "change-flow-version-in-deployment is a control-plane "
            "command that hops to the workload plane internally, and the "
            "workload plane is served by a private load balancer. A "
            "token expiry printed before a timeout means the credentials "
            "and roles are fine and only the network path is missing: "
            "run this from inside the VPC."
        )

    return (
        "Confirm CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY belong to an "
        "active CDP API key with a DataFlow role on this environment."
    )


def deployed_flow_crn(config: UserParameters, deployment_crn: str) -> Any:
    """Resolve the deployed flow's CRN. The listing key is `deployedFlows`."""

    response = run_cdp(
        config,
        ["df", "list-flows-in-deployment", "--deployment-crn", deployment_crn],
    )

    if isinstance(response, dict) and response.get("error"):
        return response

    flows = (response or {}).get("deployedFlows") or []

    if not flows:
        return {
            "error": "The deployment reports no deployed flows.",
            "deployment_crn": deployment_crn,
        }

    crn = flows[0].get("crn")

    if not crn:
        return {
            "error": "The deployed flow listing carried no CRN.",
            "deployment_crn": deployment_crn,
        }

    return crn


def flow_facts(config: UserParameters, deployment_crn: str) -> Any:
    """Return the deployed flow's version, CRNs, state and validActions."""

    resolved = deployed_flow_crn(config, deployment_crn)

    if isinstance(resolved, dict):
        return resolved

    response = run_cdp(
        config,
        [
            "df",
            "describe-flow-in-deployment",
            "--deployment-crn",
            deployment_crn,
            "--deployed-flow-crn",
            resolved,
        ],
    )

    if isinstance(response, dict) and response.get("error"):
        return response

    flow = (response or {}).get("deployedFlow") or {}

    valid_actions = flow.get("validActions") or []

    dangling = [
        action
        for action in valid_actions
        if action in ("RETRY_CHANGE_FLOW_VERSION", "CANCEL_CHANGE_FLOW_VERSION")
    ]

    facts = {
        "deployed_flow_crn": resolved,
        "flow_crn": flow.get("flowCrn"),
        "flow_version": flow.get("flowVersion"),
        "flow_version_crn": flow.get("flowVersionCrn"),
        "valid_actions": valid_actions,
    }

    if dangling:
        facts["change_flow_version_incomplete"] = {"actions_offered": dangling}

    return facts


def definition_for(config: UserParameters, flow_version_crn: str) -> Any:
    """Fetch a flow definition. `flowContents` is at the top level."""

    response = run_cdp(
        config,
        ["df", "get-flow-version", "--flow-version-crn", flow_version_crn],
    )

    if isinstance(response, dict) and response.get("error"):
        return response

    if not isinstance(response, dict) or not response.get("flowContents"):
        return {
            "error": "The flow version carried no flowContents.",
            "flow_version_crn": flow_version_crn,
        }

    return response


# ---------------------------------------------------------------------
# Guardrails
# ---------------------------------------------------------------------

def assert_deployment_allowed(config: UserParameters, deployment_crn: str) -> Any:
    """
    Refuse a deployment whose name does not carry the expected prefix.

    Returns None when allowed and a refusal dict otherwise.

    This exists because the tenant is shared. Twenty deployments by
    different authors live alongside this one, and a CRN is 100
    characters of hex in which a single wrong digit names someone
    else's production flow. The prefix check is what makes that typo a
    refusal instead of an incident.
    """

    if not deployment_crn or not deployment_crn.startswith("crn:"):
        return {
            "refused": "That does not look like a CRN.",
            "deployment_crn": deployment_crn,
        }

    response = run_cdp(
        config,
        ["df", "describe-deployment", "--deployment-crn", deployment_crn],
    )

    if isinstance(response, dict) and response.get("error"):
        return {
            "refused": (
                "The deployment could not be read, so it could not be "
                "confirmed as a permitted target."
            ),
            "detail": response,
        }

    deployment = (response or {}).get("deployment") or {}

    name = deployment.get("name") or ""

    if not name.startswith(REQUIRED_DEPLOYMENT_PREFIX):
        return {
            "refused": (
                f"Deployment {name!r} does not start with "
                f"{REQUIRED_DEPLOYMENT_PREFIX!r}, so this tool will not "
                "modify it. This is a guardrail against acting on "
                "another team's deployment, and it is not overridable "
                "from here."
            ),
            "deployment_crn": deployment_crn,
        }

    return None


def assert_flow_allowed(config: UserParameters, flow_crn: str) -> Any:
    """
    Refuse a catalog flow that is not ours.

    Matched by CRN against a prefix-scoped listing, then re-checked by
    name. The re-check is not redundant: `--search-term` is a
    server-side *substring* search, so it can return a flow whose name
    merely contains the prefix, and the operation this gates is a write
    into a permanent catalog.
    """

    if not flow_crn:
        return {"refused": "The deployed flow reported no catalog flow CRN."}

    response = run_cdp(
        config,
        [
            "df",
            "list-flow-definitions",
            "--search-term",
            REQUIRED_DEPLOYMENT_PREFIX,
        ],
    )

    if isinstance(response, dict) and response.get("error"):
        return {
            "refused": (
                "The flow catalog could not be listed, so the target "
                "flow could not be confirmed as ours."
            ),
            "detail": response,
        }

    # `list-flow-definitions` returns `flows`, not `flowDefinitions`.
    # Guessing that key yields an empty list and a refusal that looks
    # like a permissions problem.
    matches = [
        flow
        for flow in ((response or {}).get("flows") or [])
        if flow.get("crn") == flow_crn
    ]

    if not matches:
        return {
            "refused": (
                "The deployed flow's catalog flow is not among the flows "
                f"whose name matches {REQUIRED_DEPLOYMENT_PREFIX!r}. "
                "Importing a version into a flow we do not own is "
                "refused."
            ),
            "flow_crn": flow_crn,
        }

    name = matches[0].get("name") or ""

    if not name.startswith(REQUIRED_DEPLOYMENT_PREFIX):
        return {
            "refused": (
                f"Catalog flow {name!r} matched by CRN but its name does "
                f"not start with {REQUIRED_DEPLOYMENT_PREFIX!r}. The "
                "search term is a substring match, so this re-check is "
                "what catches it."
            ),
            "flow_crn": flow_crn,
        }

    return None


def latest_version(config: UserParameters, flow_crn: str) -> Any:
    """
    Read back the newest catalog version of a flow.

    Read back rather than taken from the import response, so the CRN
    used for the swap is one the catalog has actually indexed.
    """

    response = run_cdp(
        config,
        ["df", "list-flow-definition-versions", "--flow-crn", flow_crn],
    )

    if isinstance(response, dict) and response.get("error"):
        return response

    versions = (response or {}).get("flowVersions") or []

    if not versions:
        return {"error": "The flow reports no versions.", "flow_crn": flow_crn}

    # `version` is an int32, so max() is well defined and does not
    # depend on the listing's order.
    newest = max(versions, key=lambda item: item.get("version") or 0)

    return newest


# --- BEGIN VENDORED repairs.py ---
#
# Generated by templates/src/vendor_repairs.py. Do not edit this
# region by hand: the next run of that script will overwrite it.
#
# Source:     self_healing/repairs.py (VENDORED REGION)
# sha256:     803b4133aa92e3756bc5b6a15b5eb88955832b53d9dc6047d3dcceabf852b4ef
#
# Agent Studio imports a tool as a single file, so this cannot be
# an import. Edit the source and re-run the script.
#
#
# Everything between this marker and END VENDORED REGION is copied
# verbatim into the Agent Studio repair tools by
# `templates/src/vendor_repairs.py`.
#
# Agent Studio imports a tool as a single file, so a tool cannot import
# this module — the repo's own README states that `templates/` and
# `self_healing/` share no code in either direction, on purpose. The
# copy is therefore unavoidable, but it is generated rather than
# hand-maintained, because a hand copy of 580 lines drifts from this
# file the first time either side is edited and the drift is invisible:
# both would still pass their own tests while disagreeing about what a
# repair means.
#
# After editing anything below, run:
#
#     python templates/src/vendor_repairs.py --check
#
# Nothing in this region may import from the repo, read the filesystem,
# or call an API. `selftest()` deliberately sits outside it: the tools
# do not need it, and it is the one part of this file that does touch
# the filesystem.
# ---------------------------------------------------------------------

# The processor the repairs act on. Named, not indexed: the position of
# a processor in the list is not part of the contract, and matching on
# `name` is what the KPI and every human-readable diff already use.
DRAIN_PROCESSOR = "Slow Drain"

# Bounds on drain concurrency, and the upper one is a measurement rather
# than a guess.
#
# A drain is only measurable if it takes longer than one metric bucket.
# Below that the single falling bucket pair ends at an empty queue,
# `queue_slope()` correctly refuses it, and the verdict is
# `no_drain_observed` — the repair healed the queue in a way the monitor
# cannot see. At 600 files per burst and a 75-second bucket that ceiling
# is 600/75 = 8 files/s, and the drain is ~1 file/s per task, so N = 8
# sits exactly on it.
#
# So the upper bound is not "8 feels safe". It is the largest N whose
# effect this monitor can still prove, and raising it would buy a faster
# drain at the cost of being unable to demonstrate one.
MIN_CONCURRENCY = 1

MAX_CONCURRENCY = 8

# The committed baseline, as field values rather than a file read: this
# module stays pure, and the Agent Studio tool that carries a copy of it
# has no access to the repo's `flow/oscillating_queue.json`. Kept in
# sync with that file by `selftest()`, which compares them when it can
# find it.
BASELINE_DRAIN = {
    "concurrentlySchedulableTaskCount": 2,
    "schedulingPeriod": "1 sec",
    "schedulingStrategy": "TIMER_DRIVEN",
}


class RepairError(Exception):
    """A repair was refused. The message is meant to be shown to a human."""


# ---------------------------------------------------------------------
# Locating things, by name rather than by position
# ---------------------------------------------------------------------

def _contents(definition: dict) -> dict:
    """
    Return `flowContents`, accepting either wrapper shape.

    `get-flow-version` returns `flowContents` at the top level and the
    committed file nests it under a root object. Both appear in this
    codebase, so both are accepted rather than requiring the caller to
    know which one it holds.
    """

    if not isinstance(definition, dict):
        raise RepairError(
            f"the flow definition is {type(definition).__name__}, not an object"
        )

    if "flowContents" in definition:
        contents = definition["flowContents"]

    elif "processors" in definition:
        contents = definition

    else:
        raise RepairError(
            "the flow definition carries neither 'flowContents' nor "
            f"'processors'; top-level keys are {sorted(definition)}"
        )

    if not isinstance(contents, dict):
        raise RepairError("'flowContents' is not an object")

    return contents


def find_processor(definition: dict, name: str) -> dict:
    """
    Return the single processor with this name, or refuse.

    Refusing on a duplicate name matters: a flow with two processors
    called `Slow Drain` would otherwise have one of them silently
    edited, chosen by list order.
    """

    contents = _contents(definition)

    matches = [
        processor
        for processor in (contents.get("processors") or [])
        if isinstance(processor, dict) and processor.get("name") == name
    ]

    if not matches:
        available = sorted(
            str(processor.get("name"))
            for processor in (contents.get("processors") or [])
            if isinstance(processor, dict)
        )

        raise RepairError(
            f"no processor named {name!r} in the flow; it carries {available}"
        )

    if len(matches) > 1:
        raise RepairError(
            f"{len(matches)} processors are named {name!r}, so the one to "
            "edit is ambiguous; this repair refuses rather than guessing"
        )

    return matches[0]


def inventory(definition: dict) -> dict:
    """
    Summarise the flow at the granularity the repairs operate on.

    This is what makes the df API processor-level: every field below
    comes from the flow definition, with no `/nifi-api` involved.
    """

    contents = _contents(definition)

    processors = [
        {
            "name": processor.get("name"),
            "identifier": processor.get("identifier"),
            "type": processor.get("type"),
            "scheduling_strategy": processor.get("schedulingStrategy"),
            "scheduling_period": processor.get("schedulingPeriod"),
            "concurrency": processor.get("concurrentlySchedulableTaskCount"),
            "bundle_version": (processor.get("bundle") or {}).get("version"),
        }
        for processor in (contents.get("processors") or [])
        if isinstance(processor, dict)
    ]

    connections = [
        {
            "name": connection.get("name"),
            "identifier": connection.get("identifier"),
            "source": (connection.get("source") or {}).get("name"),
            "destination": (connection.get("destination") or {}).get("name"),
            "back_pressure_objects": connection.get(
                "backPressureObjectThreshold"
            ),
        }
        for connection in (contents.get("connections") or [])
        if isinstance(connection, dict)
    ]

    return {
        "group_identifier": contents.get("identifier"),
        "group_name": contents.get("name"),
        "processors": processors,
        "connections": connections,
    }


# ---------------------------------------------------------------------
# Paths: addressing a field so a diff can be compared to an allow-list
# ---------------------------------------------------------------------

def _path_of(prefix: str, index: int, item) -> str:
    """
    Address a list element by its identifier where it has one.

    By identifier rather than by index, because an index is not stable:
    two definitions that differ only in the order of their processor
    list would otherwise diff as though every field had changed, and an
    allow-list written against `processors[0]` would authorise edits to
    whichever processor happened to be first.
    """

    if isinstance(item, dict) and item.get("identifier"):
        return f"{prefix}[{item['identifier']}]"

    return f"{prefix}[{index}]"


def flatten(obj, prefix: str = "") -> dict:
    """Flatten a definition to {path: scalar}, for diffing."""

    flat = {}

    if isinstance(obj, dict):
        for key in obj:
            child = f"{prefix}.{key}" if prefix else str(key)
            flat.update(flatten(obj[key], child))

    elif isinstance(obj, list):
        for index, item in enumerate(obj):
            flat.update(flatten(item, _path_of(prefix, index, item)))

    else:
        flat[prefix] = obj

    return flat


def diff_paths(before: dict, after: dict) -> dict:
    """
    Return the paths that changed, were added, or were removed.

    All three categories are reported. A repair that *deletes* a field
    changes behaviour just as much as one that edits it, and a diff that
    only looked at common keys would show that as no change at all.
    """

    flat_before = flatten(before)
    flat_after = flatten(after)

    keys_before = set(flat_before)
    keys_after = set(flat_after)

    changed = sorted(
        path
        for path in keys_before & keys_after
        if flat_before[path] != flat_after[path]
    )

    return {
        "changed": changed,
        "added": sorted(keys_after - keys_before),
        "removed": sorted(keys_before - keys_after),
        "values": {
            path: {"before": flat_before.get(path), "after": flat_after.get(path)}
            for path in changed
        },
    }


# ---------------------------------------------------------------------
# The repairs
# ---------------------------------------------------------------------

def _set_drain_fields(definition: dict, fields: dict) -> tuple:
    """
    Apply field values to the drain processor on a deep copy.

    Returns (after, declared_paths, before_values). Every repair here
    funnels through this, so the declared paths are derived from what
    was actually written rather than listed by hand in two places and
    then allowed to drift apart.
    """

    after = copy.deepcopy(definition)

    processor = find_processor(after, DRAIN_PROCESSOR)
    original = find_processor(definition, DRAIN_PROCESSOR)

    identifier = processor.get("identifier")

    if not identifier:
        raise RepairError(
            f"the {DRAIN_PROCESSOR!r} processor has no 'identifier', so the "
            "edit cannot be addressed or validated"
        )

    root = "flowContents" if "flowContents" in after else ""
    stem = f"{root}.processors[{identifier}]" if root else f"processors[{identifier}]"

    declared = []
    before_values = {}

    for key in sorted(fields):
        path = f"{stem}.{key}"

        # Declared whether or not the value differs. The allow-list
        # describes what this repair is *permitted* to touch, which is a
        # property of the repair; whether it actually moved is the
        # diff's business, and conflating the two would make a no-op
        # edit widen the permissions of the next one.
        declared.append(path)
        before_values[path] = original.get(key)
        processor[key] = fields[key]

    return after, declared, before_values


def _repair_speed_up_slow_drain(definition: dict, params: dict) -> tuple:
    """Raise the drain processor's concurrency to a bounded N."""

    if "concurrency" not in params:
        raise RepairError(
            "speed_up_slow_drain needs a 'concurrency' parameter "
            f"({MIN_CONCURRENCY}-{MAX_CONCURRENCY})"
        )

    raw = params["concurrency"]

    # `isinstance(True, int)` is True in Python, and a bool arriving
    # here means the caller passed a flag where a magnitude belongs.
    # Letting it through would set concurrency to 1.
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        raise RepairError(
            f"concurrency must be an integer, got {type(raw).__name__}"
        )

    try:
        concurrency = int(raw)

    except (TypeError, ValueError):
        raise RepairError(f"concurrency must be an integer, got {raw!r}")

    if not MIN_CONCURRENCY <= concurrency <= MAX_CONCURRENCY:
        raise RepairError(
            f"concurrency {concurrency} is outside the permitted range "
            f"{MIN_CONCURRENCY}-{MAX_CONCURRENCY}. The upper bound is the "
            "largest value whose effect the queue metric can still "
            "measure, not an arbitrary cap."
        )

    return _set_drain_fields(
        definition, {"concurrentlySchedulableTaskCount": concurrency}
    )


def _repair_restore_baseline(definition: dict, params: dict) -> tuple:
    """
    Reset the drain processor to the committed baseline.

    This is mandatory rather than a convenience. A successful repair
    degrades the detector that triggered it: at concurrency 4 the burst
    clears fast enough that the queue sits above the threshold for fewer
    buckets than `BREACH_DEBOUNCE` needs, so the oscillation the whole
    demo rests on stops being detectable. Being able to put it back is
    what makes the repair path safe to exercise repeatedly.
    """

    if params:
        raise RepairError(
            f"restore_baseline takes no parameters, got {sorted(params)}"
        )

    return _set_drain_fields(definition, dict(BASELINE_DRAIN))


REPAIRS = {
    "speed_up_slow_drain": {
        "apply": _repair_speed_up_slow_drain,
        "parameters": {
            "concurrency": (
                f"integer {MIN_CONCURRENCY}-{MAX_CONCURRENCY}, the number of "
                "concurrent tasks for the drain processor"
            )
        },
        "description": (
            f"Raise {DRAIN_PROCESSOR!r}'s concurrentlySchedulableTaskCount so "
            "the queue drains faster. Changes one integer field and nothing "
            "else."
        ),
    },
    "restore_baseline": {
        "apply": _repair_restore_baseline,
        "parameters": {},
        "description": (
            f"Reset {DRAIN_PROCESSOR!r}'s scheduling fields to the committed "
            "baseline, putting the oscillation back the way the demo needs it."
        ),
    },
}


def apply(definition: dict, name: str, params: dict = None) -> tuple:
    """
    Apply a named repair, validate it, and return (after, proposal).

    Validation happens *here*, not only in the caller, so there is no
    code path that produces an edited definition without checking it.
    A caller that forgets to validate is the failure this forecloses.

    The proposal is the agent-facing half: a field-level diff, the
    declared allow-list, and a digest. It contains no flow JSON.
    """

    params = dict(params or {})

    if name not in REPAIRS:
        raise RepairError(
            f"{name!r} is not a known repair. Permitted: "
            f"{sorted(REPAIRS)}. Repairs are named, never authored."
        )

    unexpected = sorted(set(params) - set(REPAIRS[name]["parameters"]))

    if unexpected:
        raise RepairError(
            f"{name} does not take {unexpected}; it takes "
            f"{sorted(REPAIRS[name]['parameters'])}"
        )

    after, declared, before_values = REPAIRS[name]["apply"](definition, params)

    diff = diff_paths(definition, after)

    validate_edit(definition, after, declared)

    return after, {
        "repair": name,
        "parameters": params,
        "declared_paths": declared,
        "changed": diff["changed"],
        "added": diff["added"],
        "removed": diff["removed"],
        "values": diff["values"],
        # An edit that changes nothing must not be imported. Catalog
        # versions are permanent — the df surface has no
        # deleteFlowVersion and delete-flow is all-or-nothing — so a
        # no-op apply leaves an undeletable version behind in a shared
        # catalog and reports success. The caller decides what to do;
        # this flag is how it finds out.
        "no_op": not (diff["changed"] or diff["added"] or diff["removed"]),
        "before_values": before_values,
    }


# ---------------------------------------------------------------------
# The validator
# ---------------------------------------------------------------------

def validate_edit(before: dict, after: dict, declared_paths) -> dict:
    """
    Assert the edit changed only what it declared, and broke nothing.

    Every check below answers a specific way this could go wrong in a
    way the control plane would still report as success. The structural
    ones are not redundant with the path allow-list: a repair could
    declare a legitimate path and still, through a bug in how it copies,
    emit a definition whose identifiers or bundles have shifted.

    Raises `RepairError` on the first problem, because there is no
    partial pass worth acting on. Returns a summary when clean.
    """

    allowed = set(declared_paths or ())

    contents_before = _contents(before)
    contents_after = _contents(after)

    # --- the group's own identity -------------------------------------
    group = contents_after.get("identifier")

    if group != contents_before.get("identifier"):
        raise RepairError(
            "the process group's identifier changed "
            f"({contents_before.get('identifier')} -> {group}); the edit "
            "would import as a different flow"
        )

    # --- component identity: the sets must match exactly --------------
    for kind in ("processors", "connections"):
        ids_before = sorted(
            item.get("identifier")
            for item in (contents_before.get(kind) or [])
            if isinstance(item, dict)
        )
        ids_after = sorted(
            item.get("identifier")
            for item in (contents_after.get(kind) or [])
            if isinstance(item, dict)
        )

        if ids_before != ids_after:
            raise RepairError(
                f"the set of {kind} identifiers changed. A repair may edit a "
                f"field; it may not add, remove or re-identify a component.\n"
                f"  before: {ids_before}\n  after:  {ids_after}"
            )

    # --- parentage: an orphan imports but does not run -----------------
    for processor in contents_after.get("processors") or []:
        if not isinstance(processor, dict):
            continue

        if processor.get("groupIdentifier") != group:
            raise RepairError(
                f"processor {processor.get('name')!r} has groupIdentifier "
                f"{processor.get('groupIdentifier')!r}, which is not the "
                f"process group {group!r}"
            )

    for connection in contents_after.get("connections") or []:
        if not isinstance(connection, dict):
            continue

        if connection.get("groupIdentifier") != group:
            raise RepairError(
                f"connection {connection.get('name')!r} has groupIdentifier "
                f"{connection.get('groupIdentifier')!r}, which is not the "
                f"process group {group!r}"
            )

        for end in ("source", "destination"):
            endpoint = connection.get(end) or {}

            if endpoint.get("groupId") != group:
                raise RepairError(
                    f"connection {connection.get('name')!r} has a {end} "
                    f"groupId of {endpoint.get('groupId')!r}, which is not "
                    f"the process group {group!r}"
                )

    # --- bundles: a version the runtime lacks is rejected at import ----
    def bundles(contents):
        return {
            item.get("identifier"): (item.get("bundle") or {}).get("version")
            for item in (contents.get("processors") or [])
            if isinstance(item, dict)
        }

    if bundles(contents_before) != bundles(contents_after):
        raise RepairError(
            "a processor's bundle version changed. Bundles are pinned to the "
            "runtime's NiFi version and a version it does not carry is "
            "rejected at import."
        )

    # --- connection names: this one protects the monitor's own eyes ----
    #
    # A MetricChart carries no component id, only `componentName`. So
    # the connection's name is the single handle by which the KPI chart
    # is recognised, and renaming it orphans the KPI silently: the chart
    # stops matching, every sample reads `no_chart`, and the verdict is
    # UNKNOWN forever while the job looks healthy. A blind monitor
    # reporting health is the worst outcome available here, so the name
    # is immutable regardless of what any repair declares.
    names_before = sorted(
        connection.get("name")
        for connection in (contents_before.get("connections") or [])
        if isinstance(connection, dict)
    )
    names_after = sorted(
        connection.get("name")
        for connection in (contents_after.get("connections") or [])
        if isinstance(connection, dict)
    )

    if names_before != names_after:
        raise RepairError(
            f"a connection name changed ({names_before} -> {names_after}). "
            "The KPI metric chart identifies the connection by name and has "
            "no component id to fall back on, so this would blind the "
            "monitor while leaving it looking healthy."
        )

    # --- and finally the allow-list ------------------------------------
    diff = diff_paths(before, after)

    undeclared = sorted(set(diff["changed"]) - allowed)

    if undeclared:
        raise RepairError(
            "the edit changed paths the repair did not declare:\n  "
            + "\n  ".join(undeclared)
            + "\n  declared: "
            + (", ".join(sorted(allowed)) or "(none)")
        )

    # Additions and removals are refused wholesale rather than being
    # allow-listable. No repair in this whitelist has a reason to change
    # the *shape* of the definition, and a repair that did would need
    # this validator rewritten rather than widened.
    if diff["added"] or diff["removed"]:
        raise RepairError(
            "the edit changed the definition's shape rather than a field "
            f"value.\n  added: {diff['added']}\n  removed: {diff['removed']}"
        )

    return {
        "ok": True,
        "changed": diff["changed"],
        "values": diff["values"],
        "declared": sorted(allowed),
    }

# --- END VENDORED repairs.py ---


# ---------------------------------------------------------------------
# The tool
# ---------------------------------------------------------------------

def run_tool(config: UserParameters, args: ToolParameters) -> Any:
    """
    Re-derive a proposed repair, then import and swap it when armed.
    """

    if args.repair not in REPAIRS:
        return {
            "refused": f"{args.repair!r} is not a known repair.",
            "allowed_values": sorted(REPAIRS),
        }

    # Guardrail 1: is this deployment ours?
    refusal = assert_deployment_allowed(config, args.deployment_crn)

    if refusal:
        return refusal

    facts = flow_facts(config, args.deployment_crn)

    if facts.get("error"):
        return {"deployment_crn": args.deployment_crn, "flow": facts}

    if facts.get("change_flow_version_incomplete"):
        return {
            "refused": (
                "A flow version change on this deployment is already "
                "half-applied. Applying another on top of it is refused; "
                "this needs a human."
            ),
            "flow": facts,
        }

    # Guardrail 2: is the catalog flow ours?
    refusal = assert_flow_allowed(config, facts.get("flow_crn"))

    if refusal:
        return refusal

    # Guardrail 3: has the flow moved since the proposal?
    if facts.get("flow_version") != args.expected_flow_version:
        return {
            "refused": (
                "The proposal is stale. It was computed from flow "
                f"version {args.expected_flow_version} but the "
                f"deployment is now running version "
                f"{facts.get('flow_version')}. Ask for a fresh proposal "
                "rather than applying this one — the flow has changed "
                "underneath it."
            ),
            "flow": facts,
        }

    before = definition_for(config, facts.get("flow_version_crn"))

    if before.get("error"):
        return {"deployment_crn": args.deployment_crn, "definition": before}

    params = {}

    if args.concurrency is not None:
        params["concurrency"] = args.concurrency

    # Re-derived here rather than trusted from the proposal. The agent
    # hands over scalars and a diff to check against; it never hands
    # over a definition.
    try:
        after, proposal = apply(before, args.repair, params)

    except RepairError as exc:
        return {
            "refused": str(exc),
            "allowed_values": sorted(REPAIRS),
        }

    if proposal.get("no_op"):
        return {
            "refused": (
                "This repair would change nothing, and applying it would "
                "burn a permanent catalog version to achieve nothing. "
                "There is no operation in the DataFlow API to delete a "
                "flow version."
            ),
            "proposal": {"repair": args.repair, "params": params},
        }

    # Guardrail 4: does the re-derived edit match what was proposed?
    if proposal["values"] != args.expected_values:
        return {
            "refused": (
                "The edit re-computed here does not match the "
                "`expected_values` echoed from the proposal, so the "
                "apply is refused. Either the flow changed between the "
                "proposal and now, or the echoed diff was altered. Ask "
                "for a fresh proposal."
            ),
            "expected_values": args.expected_values,
            "recomputed_values": proposal["values"],
        }

    flow_crn = facts.get("flow_crn")

    import_argv = cdp_argv(
        [
            "df",
            "import-flow-definition-version",
            "--flow-crn",
            flow_crn,
            "--file",
            "<the edited definition, written to a temporary file>",
            "--comments",
            f"{args.repair} applied by the nifi_flow_repair workflow",
        ]
    )

    swap_argv = cdp_argv(
        [
            "df",
            "change-flow-version-in-deployment",
            "--deployment-crn",
            args.deployment_crn,
            "--flow-version-crn",
            "<the CRN of the version created by the import above>",
            "--strategy",
            CHANGE_STRATEGY,
        ]
    )

    plan = {
        "deployment_crn": args.deployment_crn,
        "flow_crn": flow_crn,
        "repair": args.repair,
        "params": params,
        "from_flow_version": facts.get("flow_version"),
        "changed_paths": proposal["declared_paths"],
        "values": proposal["values"],
        "strategy": CHANGE_STRATEGY,
        "argv": [import_argv, swap_argv],
        "guardrails_passed": [
            "repair is on the whitelist",
            f"deployment name starts with {REQUIRED_DEPLOYMENT_PREFIX!r}",
            "catalog flow matched by CRN and re-checked by name",
            f"deployed version is still {args.expected_flow_version}",
            "re-derived edit matches the echoed proposal",
            "validate_edit passed",
        ],
    }

    if not args.arm:

        return {
            "armed": False,
            "performed": False,
            "plan": plan,
            "notes": {
                "nothing_changed": (
                    "This was a dry run: no catalog version was created "
                    "and the deployment was not touched. The two commands "
                    "above are what would run. Report this as a plan, not "
                    "as a repair."
                ),
                "to_apply": (
                    "Applying requires arm=true, which a human must ask "
                    "for after reading this plan. Do not set it on your "
                    "own initiative."
                ),
            },
        }

    # ------------------------------------------------------------------
    # Armed from here. Two mutations, and the first is irreversible.
    # ------------------------------------------------------------------

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".json",
        prefix="flow-repair-",
        delete=False,
    )

    try:
        with handle:
            json.dump(after, handle, indent=2)
            handle.flush()

        import_response = run_cdp(
            config,
            [
                "df",
                "import-flow-definition-version",
                "--flow-crn",
                flow_crn,
                "--file",
                handle.name,
                "--comments",
                f"{args.repair} applied by the nifi_flow_repair workflow",
            ],
        )

    finally:
        try:
            os.unlink(handle.name)

        except OSError:
            pass

    if isinstance(import_response, dict) and import_response.get("error"):
        return {
            "armed": True,
            "performed": False,
            "stage": "import",
            "plan": plan,
            "error": import_response,
            "notes": {
                "deployment_untouched": (
                    "The import failed, so no version swap was attempted "
                    "and the running deployment is unchanged."
                )
            },
        }

    created = latest_version(config, flow_crn)

    if isinstance(created, dict) and created.get("error"):
        return {
            "armed": True,
            "performed": False,
            "stage": "read_back",
            "plan": plan,
            "error": created,
            "notes": {
                "version_may_exist": (
                    "The import reported success but the new version "
                    "could not be read back, so it may well exist. The "
                    "deployment was not swapped. Do not retry blindly — "
                    "list the flow's versions first."
                )
            },
        }

    new_version_crn = created.get("crn")
    new_version = created.get("version")

    if new_version_crn == facts.get("flow_version_crn"):
        return {
            "armed": True,
            "performed": False,
            "stage": "read_back",
            "plan": plan,
            "error": (
                "The newest catalog version is still the one already "
                "deployed, so the import created nothing."
            ),
            "notes": {
                "deployment_untouched": (
                    "No swap was attempted. Report this rather than "
                    "retrying: an import that silently created nothing "
                    "is a condition worth a human's attention."
                )
            },
        }

    swap_response = run_cdp(
        config,
        [
            "df",
            "change-flow-version-in-deployment",
            "--deployment-crn",
            args.deployment_crn,
            "--flow-version-crn",
            new_version_crn,
            "--strategy",
            CHANGE_STRATEGY,
        ],
    )

    result = {
        "armed": True,
        "plan": plan,
        "catalog_version_created": {
            "version": new_version,
            "crn": new_version_crn,
        },
    }

    if isinstance(swap_response, dict) and swap_response.get("error"):

        result["performed"] = False
        result["stage"] = "swap"
        result["error"] = swap_response
        result["notes"] = {
            "catalog_version_remains": (
                f"Version {new_version} was created in the catalog and "
                "cannot be deleted — there is no deleteFlowVersion "
                "operation. The deployment was not successfully "
                "repointed. Report both facts: a human needs to know a "
                "version exists that is not deployed."
            )
        }

        return result

    # Read the deployment back rather than trusting the swap's response.
    after_facts = flow_facts(config, args.deployment_crn)

    result["performed"] = True
    result["stage"] = "complete"
    result["flow_after"] = after_facts

    landed = after_facts.get("flow_version") == new_version

    result["version_landed"] = landed

    result["notes"] = {
        "verification_is_not_yours": (
            "The version swap was accepted and the catalog version is "
            f"{new_version}. Whether the repair actually changed the "
            "queue's behaviour is measured by the monitoring job "
            "afterwards, from the KPI, not by this tool and not by you. "
            "Report the predicted effect and say plainly that it has not "
            "been measured."
        )
    }

    if not landed:
        result["notes"]["version_not_yet_visible"] = (
            f"The deployment still reports version "
            f"{after_facts.get('flow_version')} rather than "
            f"{new_version}. A version change takes time to be "
            "reflected, so this may be timing rather than failure. "
            "Report it as unconfirmed; do not retry."
        )

    return result


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Apply a proposed repair to a DataFlow deployment. Dry run "
            "unless arm is true."
        )
    )

    parser.add_argument(
        "--user-params",
        required=True,
        help="JSON object containing CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY.",
    )

    parser.add_argument(
        "--tool-params",
        required=True,
        help=(
            "JSON object containing deployment_crn, repair, "
            "expected_flow_version, expected_values and optionally "
            "concurrency and arm."
        ),
    )

    cli_args = parser.parse_args()

    user_params_dict = json.loads(cli_args.user_params)
    tool_params_dict = json.loads(cli_args.tool_params)

    user_params = UserParameters(**user_params_dict)
    tool_params = ToolParameters(**tool_params_dict)

    output = run_tool(
        config=user_params,
        args=tool_params,
    )

    print(json.dumps(output, indent=2))
