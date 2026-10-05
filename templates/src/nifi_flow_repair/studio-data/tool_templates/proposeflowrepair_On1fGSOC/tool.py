#!/usr/bin/env python3
"""
Propose a bounded, named repair to a deployed flow. Mutates nothing.

This tool answers "what would this repair change?" and nothing else. It
re-fetches the flow definition, applies one whitelisted repair in
memory, runs the independent validator over the result, and returns a
field-level before/after diff. The catalog is untouched and the running
deployment is untouched.

Two properties make it safe to let a model drive:

  1. **The LLM never authors flow JSON.** It picks a repair *name* from
     a fixed whitelist and supplies one bounded scalar. The
     transformation belongs to the vendored code below, which is the
     same code the monitoring job uses.

  2. **The diff is the only thing that crosses back.** The definition
     is not returned, so there is no JSON in the model's context to be
     talked into editing.

The returned `apply_echo` is the contract with `applyFlowRepair`: that
tool refuses to act unless the caller hands back these exact values. So
a proposal cannot be quietly widened on its way to being applied, and a
proposal derived from a flow version that has since changed is refused
rather than replayed against a flow it was never computed for.

A proposal is not an outcome. Nothing in this tool's output means a
repair happened.
"""

from pydantic import BaseModel, Field
from typing import Any, Optional
import argparse
import copy
import json
import os
import subprocess
import sys


CDP_TIMEOUT_SECONDS = 120

MAX_RAW_OUTPUT_CHARS = 2000


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
            "CRN of the deployment to propose a repair for. This is "
            "given to you in the incident; do not search for it."
        )
    )

    repair: str = Field(
        description=(
            "The name of the repair to propose. Must be one of: "
            "'speed_up_slow_drain' (increase the drain processor's "
            "concurrency so a backlog clears faster), or "
            "'restore_baseline' (put the drain processor back to its "
            "committed baseline settings). Never invent a name — an "
            "unrecognised name is refused and the allowed names are "
            "returned to you."
        )
    )

    concurrency: Optional[int] = Field(
        default=None,
        description=(
            "Required for 'speed_up_slow_drain': the number of "
            "concurrent tasks to give the drain processor, between 1 "
            "and 8. Ignored by 'restore_baseline'. The upper bound is a "
            "measurement, not a preference: above 8 the queue drains "
            "faster than the monitoring window can observe, so the "
            "repair could not be proven to have worked."
        ),
    )


# ---------------------------------------------------------------------
# CDP control plane helpers
#
# A private copy, as in every other tool in this repo: Agent Studio
# imports a tool as a single file, so `templates/` and `self_healing/`
# share no code in either direction.
# ---------------------------------------------------------------------

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

        return {
            "error": (
                f"The CDP CLI did not complete within "
                f"{CDP_TIMEOUT_SECONDS} seconds."
            ),
            "command": " ".join(args),
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

        return {
            "error": "The CDP CLI produced no output.",
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
    """
    Choose a diagnosis for a failed CLI call.

    The usage branch is first: a mistyped flag never reaches the API, so
    blaming the credentials sends the reader to investigate a key that
    is working perfectly.
    """

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

    return (
        "Confirm CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY belong to an "
        "active CDP API key with a DataFlow role on this environment."
    )


def deployed_flow_crn(config: UserParameters, deployment_crn: str) -> Any:
    """
    Resolve the deployed flow's CRN.

    The listing returns `deployedFlows`, not `flows`.
    """

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
        facts["change_flow_version_incomplete"] = {
            "actions_offered": dangling,
            "meaning": (
                "A previous flow version change is half-applied. Report "
                "this and propose nothing: a repair stacked on an "
                "unfinished version change is a human's decision."
            ),
        }

    return facts


def definition_for(config: UserParameters, flow_version_crn: str) -> Any:
    """
    Fetch a flow definition from the catalog.

    `flowContents` is at the top level, with no `config` wrapper.
    """

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
    Apply a named repair in memory and return the diff it would make.
    """

    # Validated here rather than with a Literal type, because a pydantic
    # ValidationError's text never reaches the agent: it would see the
    # call fail with no usable explanation and no list of what it should
    # have said instead.
    if args.repair not in REPAIRS:
        return {
            "error": f"{args.repair!r} is not a known repair.",
            "allowed_values": sorted(REPAIRS),
            "hint": (
                "Use one of the allowed values exactly. Do not invent a "
                "repair name and do not describe an edit in prose — only "
                "these named repairs can be applied."
            ),
        }

    facts = flow_facts(config, args.deployment_crn)

    if facts.get("error"):
        return {"deployment_crn": args.deployment_crn, "flow": facts}

    if facts.get("change_flow_version_incomplete"):
        return {
            "deployment_crn": args.deployment_crn,
            "flow": facts,
            "proposal": None,
            "refused": (
                "A flow version change is already half-applied on this "
                "deployment. Report it and stop."
            ),
        }

    version_crn = facts.get("flow_version_crn")

    if not version_crn:
        return {
            "deployment_crn": args.deployment_crn,
            "flow": facts,
            "error": "The deployed flow carried no flow version CRN.",
        }

    before = definition_for(config, version_crn)

    if before.get("error"):
        return {
            "deployment_crn": args.deployment_crn,
            "flow": facts,
            "definition": before,
        }

    params = {}

    if args.concurrency is not None:
        params["concurrency"] = args.concurrency

    try:
        after, proposal = apply(before, args.repair, params)

    except RepairError as exc:

        # A refusal, not a crash. The message is written to be shown to
        # a human and is safe to pass to the agent verbatim.
        return {
            "deployment_crn": args.deployment_crn,
            "flow": facts,
            "proposal": None,
            "refused": str(exc),
            "allowed_values": sorted(REPAIRS),
        }

    if proposal.get("no_op"):
        return {
            "deployment_crn": args.deployment_crn,
            "flow": facts,
            "proposal": proposal,
            "no_op": True,
            "hint": (
                "This repair would change nothing — the flow is already "
                "in the state it describes. Report that and do not go on "
                "to apply it. Applying it would burn a permanent catalog "
                "version to achieve nothing; there is no operation in "
                "the DataFlow API to delete a flow version."
            ),
        }

    return {
        "deployment_crn": args.deployment_crn,
        "flow": facts,
        "proposal": {
            "repair": args.repair,
            "params": params,
            "derived_from_flow_version": facts.get("flow_version"),
            "changed_paths": proposal["declared_paths"],
            "values": proposal["values"],
            # `apply()` runs the validator internally and raises on any
            # failure, so reaching this line is itself the proof that
            # the edit passed every structural check and the allow-list.
            # There is no code path that returns an unvalidated edit.
            "validated": True,
            "added_paths": proposal["added"],
            "removed_paths": proposal["removed"],
        },
        # The contract with applyFlowRepair. It re-derives the edit
        # itself and refuses to act unless these match what it computes,
        # so a proposal cannot be widened in transit and a stale one
        # cannot be replayed against a flow that has since changed.
        "apply_echo": {
            "repair": args.repair,
            "concurrency": args.concurrency,
            "expected_flow_version": facts.get("flow_version"),
            "expected_values": proposal["values"],
        },
        "notes": {
            "this_is_a_proposal": (
                "Nothing has changed. The catalog and the running "
                "deployment are untouched. Present this diff and stop — "
                "do not describe the repair as done, applied, or "
                "working."
            ),
            "flow_definition_withheld": (
                "The flow definition is deliberately not returned. The "
                "diff above is the complete account of what would "
                "change."
            ),
        },
    }


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Propose a bounded repair to a deployed DataFlow flow."
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
            "JSON object containing deployment_crn, repair and "
            "optionally concurrency."
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
