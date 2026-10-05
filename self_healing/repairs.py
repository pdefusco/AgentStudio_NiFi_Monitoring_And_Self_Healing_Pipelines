#!/usr/bin/env python3
"""
The bounded repair whitelist, and the validator that polices it.

This module is pure: it takes a flow definition as a dict, returns a new
one, and touches no API and no filesystem. That is what makes it the
only part of the repair path that can be exhaustively tested offline,
and it is why the dangerous half of the work lives here rather than in
the tool that calls it.

**The LLM never authors flow JSON.** It picks a repair *name* from
`REPAIRS` and a bounded scalar; the transformation belongs to the code.
A model that is shown JSON can be talked into editing it, so it is shown
a diff of named fields instead and the JSON stays on this side of the
boundary.

Two layers of defence, and the second is the one that matters:

  1. Each repair edits only the fields it means to.
  2. `validate_edit()` then re-derives, independently, what actually
     changed between the two definitions and refuses anything the
     repair did not *declare*. So a repair with a bug — or a repair
     added later by someone who did not read this docstring — is caught
     by the layer below it rather than trusted.

The declared-path check is an **allow-list**. A deny-list passes through
whatever it failed to anticipate, which is the same reasoning the repo
already applies to log redaction, and the stakes here are higher: the
output of this module is imported into a shared catalog as a permanent
version that cannot be deleted.

Usage:

    import repairs

    after, proposal = repairs.apply(before, "speed_up_slow_drain", {"concurrency": 4})
    repairs.validate_edit(before, after, proposal["declared_paths"])
"""

import copy


# --- BEGIN VENDORED REGION -------------------------------------------
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


# --- END VENDORED REGION ---------------------------------------------


# ---------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------

def selftest() -> int:
    """
    Exercise the whitelist and the validator against the real baseline.

    Every negative case here is a way a repair could go wrong while the
    control plane still reported success, so they are run rather than
    asserted in a comment.
    """

    import json
    import os

    failures = []

    def check(label, condition, detail=""):
        print(f"  {'ok  ' if condition else 'FAIL'}  {label}{detail}")

        if not condition:
            failures.append(label)

    baseline_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "flow",
        "oscillating_queue.json",
    )

    print(f"reading {baseline_file}")

    with open(baseline_file, "r", encoding="utf-8") as handle:
        before = json.load(handle)

    print("\nthe baseline, as the repairs see it")

    facts = inventory(before)

    for processor in facts["processors"]:
        print(
            f"    {processor['name']!r}: concurrency="
            f"{processor['concurrency']}, period="
            f"{processor['scheduling_period']!r}, bundle="
            f"{processor['bundle_version']}"
        )

    drain = find_processor(before, DRAIN_PROCESSOR)

    print("\nthe committed baseline matches BASELINE_DRAIN")

    for key in sorted(BASELINE_DRAIN):
        check(
            f"{key} == {BASELINE_DRAIN[key]!r}",
            drain.get(key) == BASELINE_DRAIN[key],
            f" (file says {drain.get(key)!r})",
        )

    print("\nspeed_up_slow_drain")

    after, proposal = apply(before, "speed_up_slow_drain", {"concurrency": 4})

    check("exactly one path changed", len(proposal["changed"]) == 1,
          f" ({proposal['changed']})")
    check("it is the concurrency field",
          proposal["changed"][:1] ==
          [f"flowContents.processors[{drain['identifier']}]"
           ".concurrentlySchedulableTaskCount"])
    check("2 -> 4",
          [v["before"] for v in proposal["values"].values()] == [2] and
          [v["after"] for v in proposal["values"].values()] == [4])
    check("nothing added or removed",
          not proposal["added"] and not proposal["removed"])
    check("not reported as a no-op", not proposal["no_op"])
    check("the input was not mutated",
          find_processor(before, DRAIN_PROCESSOR)
          ["concurrentlySchedulableTaskCount"] == 2)

    print("\nbounds")

    for value in (0, 9, -1, 100):
        try:
            apply(before, "speed_up_slow_drain", {"concurrency": value})
            check(f"concurrency {value} refused", False)

        except RepairError:
            check(f"concurrency {value} refused", True)

    try:
        apply(before, "speed_up_slow_drain", {"concurrency": True})
        check("a bool is refused, not silently read as 1", False)

    except RepairError:
        check("a bool is refused, not silently read as 1", True)

    check("the ceiling is the measurability limit",
          MAX_CONCURRENCY == 8,
          f" (600 files / 75s bucket = 8 files/s)")

    print("\nunknown repairs and stray parameters")

    for name, params in (
        ("delete_everything", {}),
        ("speed_up_slow_drain", {"concurrency": 4, "also": "this"}),
        ("restore_baseline", {"concurrency": 4}),
    ):
        try:
            apply(before, name, params)
            check(f"{name} {params} refused", False)

        except RepairError:
            check(f"{name} {params} refused", True)

    print("\nrestore_baseline round-trips")

    restored, restore_proposal = apply(after, "restore_baseline")

    check("concurrency is back to 2",
          find_processor(restored, DRAIN_PROCESSOR)
          ["concurrentlySchedulableTaskCount"] == 2)
    check("the round trip is byte-identical to the original",
          json.dumps(restored, sort_keys=True) ==
          json.dumps(before, sort_keys=True))
    check("restoring an already-baseline flow is flagged no_op",
          apply(before, "restore_baseline")[1]["no_op"])

    print("\nthe validator catches what a buggy repair would do")

    def mutate(fn):
        copy_of = copy.deepcopy(before)
        fn(_contents(copy_of))
        return copy_of

    cases = [
        (
            "a renamed connection",
            mutate(lambda c: c["connections"][0].__setitem__(
                "name", "renamed")),
        ),
        (
            "a changed bundle version",
            mutate(lambda c: c["processors"][0]["bundle"].__setitem__(
                "version", "9.9.9")),
        ),
        (
            "a re-identified processor",
            mutate(lambda c: c["processors"][0].__setitem__(
                "identifier", "00000000-0000-0000-0000-000000000000")),
        ),
        (
            "a removed processor",
            mutate(lambda c: c["processors"].pop()),
        ),
        (
            "an orphaned processor",
            mutate(lambda c: c["processors"][0].__setitem__(
                "groupIdentifier", "00000000-0000-0000-0000-000000000000")),
        ),
        (
            "a repointed connection endpoint",
            mutate(lambda c: c["connections"][0]["source"].__setitem__(
                "groupId", "00000000-0000-0000-0000-000000000000")),
        ),
        (
            "a changed group identifier",
            mutate(lambda c: c.__setitem__(
                "identifier", "00000000-0000-0000-0000-000000000000")),
        ),
        (
            "an undeclared field edit",
            mutate(lambda c: c["processors"][1].__setitem__(
                "schedulingPeriod", "99 sec")),
        ),
    ]

    for label, broken in cases:
        try:
            validate_edit(before, broken, [])
            check(f"{label} refused", False)

        except RepairError:
            check(f"{label} refused", True)

    print("\nthe allow-list is exact, not a prefix")

    drain_stem = f"flowContents.processors[{drain['identifier']}]"

    try:
        # Declaring the concurrency path must not authorise a different
        # field on the same processor.
        validate_edit(
            before,
            mutate(lambda c: c["processors"][1].__setitem__(
                "schedulingPeriod", "99 sec")),
            [f"{drain_stem}.concurrentlySchedulableTaskCount"],
        )
        check("declaring one field does not authorise its sibling", False)

    except RepairError:
        check("declaring one field does not authorise its sibling", True)

    print("\npaths survive processor reordering")

    reordered = copy.deepcopy(before)
    _contents(reordered)["processors"].reverse()

    check("reordering alone is not seen as a change",
          not diff_paths(before, reordered)["changed"],
          f" ({diff_paths(before, reordered)['changed']})")

    print("\nmalformed input")

    for bad in ({}, {"nope": 1}, [], "a string", None):
        try:
            inventory(bad)
            check(f"{bad!r} refused", False)

        except RepairError:
            check(f"{bad!r} refused", True)

    print()

    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")

        return 1

    print("all checks passed")

    return 0


if __name__ == "__main__":
    raise SystemExit(selftest())
