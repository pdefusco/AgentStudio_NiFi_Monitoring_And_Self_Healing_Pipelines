#!/usr/bin/env python3
"""
Emit the synthetic oscillating NiFi flow definition.

The flow exists to misbehave on a predictable schedule, so the
monitoring loop has something to detect without waiting for a real flow
to degrade. It is two processors and one connection:

    GenerateFlowFile "Burst Generator"
        TIMER_DRIVEN, every BURST_PERIOD_SECONDS, Batch Size BURST_SIZE
        |
        |  connection, named so its metric chart is identifiable
        v
    UpdateAttribute "Slow Drain"
        TIMER_DRIVEN, every 1 sec, DRAIN_CONCURRENCY tasks,
        success auto-terminated

UpdateAttribute performs a single `session.get()` per trigger, so at a
one-second scheduling period it removes roughly one flowfile per second
per concurrent task. At the configured concurrency of 2 that is 2/s,
measured exactly: the series steps down 150 per 75-second bucket. A
600-file burst therefore clears in ~300s of a 600s cycle and sits above a
threshold of 100 for ~250 of those seconds — three or four samples at
75-second resolution, comfortably more than the two consecutive breaches
the debounce needs. The margin matters because the metrics API returns one
sample per ~75 seconds at the default window, so a narrow breach window
can be missed entirely by a job run.

The drain runs DRAIN_CONCURRENCY tasks, and that number is the one lesson
this file exists to record. At 1 the drain rate exactly equalled the
generation rate, which measured beautifully — bursts 600s apart, peaks
543/544/546, drain 1.0/s, troughs of ~20 — right up to the first
remediation. A restart adds its frozen backlog plus a fresh burst, and
with zero headroom that addition is permanent: the trough went from ~20 to
~344 and the flow never came back under threshold. At 2 the drain clears a
burst in ~300s of a 600s cycle, so a backlog an action leaves behind is
absorbed within about one cycle and the flow recovers on its own. The
self-check enforces that headroom so this cannot regress quietly.

Two choices worth recording, both about avoiding unverified assumptions:

  - TIMER_DRIVEN rather than CRON_DRIVEN. A Quartz expression would
    express "every ten minutes" more directly, but this NiFi build's
    cron dialect is not something this prototype has verified, a bad
    expression makes the processor invalid at deploy time with a
    confusing error, and a cron fire time that lands during a restart is
    simply skipped.

  - UpdateAttribute rather than ControlRate as the rate limiter. Every
    bundle coordinate and property key here was read off real flows in
    this CDF catalog at this NiFi version. No flow in the catalog uses
    ControlRate, so its property names would have been a guess.

Identifiers are derived with uuid5 from a fixed namespace, so the output
is byte-identical across runs and a re-import produces a reviewable diff
rather than a wall of changed UUIDs. Note that these are *not* the ids a
KPI needs: NiFi assigns its own component ids at deploy time, so the KPI
target has to be discovered after deployment.

Usage:

    python flow/build_flow_definition.py
    python flow/build_flow_definition.py --check
"""

import argparse
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


# ---------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------

# A fixed namespace makes uuid5 deterministic across runs and machines.
NAMESPACE = uuid.UUID("6f3c1e2a-9b4d-5f8e-a1c7-2d0b4e6f8a13")

STANDARD_NAR = "nifi-standard-nar"
UPDATE_ATTRIBUTE_NAR = "nifi-update-attribute-nar"
NAR_GROUP = "org.apache.nifi"

GENERATE_FLOW_FILE_TYPE = "org.apache.nifi.processors.standard.GenerateFlowFile"
UPDATE_ATTRIBUTE_TYPE = "org.apache.nifi.processors.attributes.UpdateAttribute"

GENERATOR_NAME = "Burst Generator"
DRAIN_NAME = "Slow Drain"


def component_id(label: str) -> str:
    """Return a stable UUID for a named component."""

    return str(uuid.uuid5(NAMESPACE, label))


# ---------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------

def processor(
    label: str,
    name: str,
    processor_type: str,
    artifact: str,
    group_id: str,
    scheduling_period: str,
    properties: dict,
    position: tuple,
    auto_terminated: list = None,
    concurrent_tasks: int = 1,
) -> dict:
    """
    Build one versioned processor.

    The field set mirrors what `df get-flow-version` returns for flows
    CDF has already accepted, so nothing here is invented. Only
    `propertyDescriptors` is deliberately left empty: NiFi resolves
    descriptors from the processor class on import, and carrying a copy
    of them would triple the size of this file for no gain.
    """

    return {
        "annotationData": None,
        "autoTerminatedRelationships": auto_terminated or [],
        "backoffMechanism": "PENALIZE_FLOWFILE",
        "bulletinLevel": "WARN",
        "bundle": {
            "artifact": artifact,
            "group": NAR_GROUP,
            "version": config.NIFI_VERSION,
        },
        "comments": "",
        "componentType": "PROCESSOR",
        "concurrentlySchedulableTaskCount": concurrent_tasks,
        "executionNode": "ALL",
        "groupIdentifier": group_id,
        "identifier": component_id(label),
        "instanceIdentifier": None,
        "maxBackoffPeriod": "10 mins",
        "name": name,
        "penaltyDuration": "30 sec",
        "position": {"x": float(position[0]), "y": float(position[1])},
        "properties": properties,
        "propertyDescriptors": {},
        "retriedRelationships": [],
        "retryCount": 10,
        "runDurationMillis": 0,
        "scheduledState": "ENABLED",
        "schedulingPeriod": scheduling_period,
        "schedulingStrategy": "TIMER_DRIVEN",
        "style": {},
        "type": processor_type,
        "yieldDuration": "1 sec",
    }


def endpoint(label: str, name: str, group_id: str) -> dict:
    """Build a connection endpoint reference."""

    return {
        "comments": None,
        "groupId": group_id,
        "id": component_id(label),
        "instanceIdentifier": None,
        "name": name,
        "type": "PROCESSOR",
    }


def build() -> dict:
    """
    Build the complete flow definition.

    The returned structure is the NiFi versioned-flow-snapshot shape that
    `df import-flow-definition` accepts: `flowContents` plus the
    snapshot-level fields. The registry-populated fields
    (`snapshotMetadata`, `bucket`, `flow`) are omitted, since CDF fills
    them in and they read back as null on existing flows.
    """

    group_id = component_id("process-group")

    generator = processor(
        label="generator",
        name=GENERATOR_NAME,
        processor_type=GENERATE_FLOW_FILE_TYPE,
        artifact=STANDARD_NAR,
        group_id=group_id,
        scheduling_period=f"{config.BURST_PERIOD_SECONDS} sec",
        properties={
            # One burst of BURST_SIZE flowfiles per trigger.
            "Batch Size": str(config.BURST_SIZE),
            # Zero-byte flowfiles: the signal is the object count, and
            # keeping the payload empty means the burst costs nothing in
            # storage or throughput. It also makes every byte-based
            # metric flat at zero, which is why the KPI must be
            # connectionAmountQueued and not connectionBytesQueued.
            "File Size": "0B",
            "Unique FlowFiles": "false",
            "Data Format": "Text",
            "character-set": "UTF-8",
        },
        position=(0, 0),
    )

    drain = processor(
        label="drain",
        name=DRAIN_NAME,
        processor_type=UPDATE_ATTRIBUTE_TYPE,
        artifact=UPDATE_ATTRIBUTE_NAR,
        group_id=group_id,
        scheduling_period=config.DRAIN_PERIOD,
        properties={
            "Store State": "Do not store state",
            "canonical-value-lookup-cache-size": "100",
        },
        # Terminating success here is what makes this a sink: the
        # flowfile leaves the queue and the flow needs no downstream.
        auto_terminated=["success"],
        position=(0, 400),
        # Each task does one session.get() per trigger, so the drain rate
        # is this many flowfiles per second. More than the generator
        # produces, which is what lets the queue recover from a backlog a
        # remediation leaves behind instead of plateauing above threshold.
        concurrent_tasks=config.DRAIN_CONCURRENCY,
    )

    connection = {
        "backPressureDataSizeThreshold":
            config.BACK_PRESSURE_DATA_SIZE_THRESHOLD,
        # Well above the burst size. If back pressure engages, NiFi
        # stops the generator mid-burst and the peak never appears.
        "backPressureObjectThreshold":
            config.BACK_PRESSURE_OBJECT_THRESHOLD,
        "bends": [],
        "comments": None,
        "componentType": "CONNECTION",
        "destination": endpoint("drain", DRAIN_NAME, group_id),
        # No expiration, so flowfiles cannot silently vanish from the
        # queue and quietly clear the condition being monitored.
        "flowFileExpiration": "0 sec",
        "groupIdentifier": group_id,
        "identifier": component_id("connection"),
        "instanceIdentifier": None,
        "labelIndex": 1,
        "loadBalanceCompression": "DO_NOT_COMPRESS",
        "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
        # Named explicitly. CDF otherwise generates
        # `<source>_<relationship>_<destination>`, and since a
        # MetricChart carries no component id, this string is the only
        # handle for finding the connection in the KPI scope metadata and
        # for recognising its chart afterwards.
        "name": config.CONNECTION_NAME,
        "partitioningAttribute": None,
        "position": None,
        "prioritizers": [],
        "selectedRelationships": ["success"],
        "source": endpoint("generator", GENERATOR_NAME, group_id),
        "zIndex": 0,
    }

    flow_contents = {
        "comments": (
            "Synthetic oscillating flow for the self-healing prototype. "
            f"{config.BURST_SIZE} flowfiles every "
            f"{config.BURST_PERIOD_SECONDS}s, drained at "
            f"~{config.DRAIN_CONCURRENCY}/s."
        ),
        "componentType": "PROCESS_GROUP",
        "connections": [connection],
        "controllerServices": [],
        "defaultBackPressureDataSizeThreshold":
            config.BACK_PRESSURE_DATA_SIZE_THRESHOLD,
        "defaultBackPressureObjectThreshold":
            config.BACK_PRESSURE_OBJECT_THRESHOLD,
        "defaultFlowFileExpiration": "0 sec",
        "executionEngine": None,
        "flowFileConcurrency": "UNBOUNDED",
        "flowFileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
        "funnels": [],
        "groupIdentifier": None,
        "identifier": group_id,
        "inputPorts": [],
        "instanceIdentifier": None,
        "labels": [],
        "logFileSuffix": None,
        "maxConcurrentTasks": None,
        "name": config.FLOW_NAME,
        "outputPorts": [],
        "parameterContextName": None,
        "position": None,
        "processGroups": [],
        "processors": [generator, drain],
        "remoteProcessGroups": [],
        "scheduledState": None,
        "statelessFlowTimeout": None,
        "versionedFlowCoordinates": None,
    }

    return {
        "externalControllerServices": {},
        "flowContents": flow_contents,
        "flowEncodingVersion": "1.0",
        "latest": False,
        "parameterContexts": {},
        "parameterProviders": {},
    }


# ---------------------------------------------------------------------
# Offline validation
# ---------------------------------------------------------------------

def check(definition: dict) -> list:
    """
    Validate the definition without calling anything.

    Every assertion here corresponds to a way the flow could be accepted
    by CDF and still fail to oscillate, or be rejected for a reason that
    costs a fifteen-minute deployment to discover. Returns a list of
    problems; empty means it passed.
    """

    problems = []
    contents = definition["flowContents"]
    processors = contents["processors"]
    connections = contents["connections"]

    if len(processors) != 2:
        problems.append(f"expected 2 processors, found {len(processors)}")

    if len(connections) != 1:
        problems.append(f"expected 1 connection, found {len(connections)}")

    if not problems:
        by_name = {p["name"]: p for p in processors}
        connection = connections[0]

        ids = {p["identifier"] for p in processors}

        if connection["source"]["id"] not in ids:
            problems.append("connection source id matches no processor")

        if connection["destination"]["id"] not in ids:
            problems.append("connection destination id matches no processor")

        if connection["source"]["id"] == connection["destination"]["id"]:
            problems.append("connection source and destination are the same")

        if connection["name"] != config.CONNECTION_NAME:
            problems.append(
                f"connection name is {connection['name']!r}, expected "
                f"{config.CONNECTION_NAME!r} — the KPI is found by this name"
            )

        if connection["backPressureObjectThreshold"] <= config.BURST_SIZE:
            problems.append(
                "backPressureObjectThreshold "
                f"({connection['backPressureObjectThreshold']}) is not above "
                f"the burst size ({config.BURST_SIZE}): back pressure would "
                "stop the generator mid-burst and hide the peak"
            )

        generator = by_name.get(GENERATOR_NAME)
        drain = by_name.get(DRAIN_NAME)

        if generator is None:
            problems.append(f"no processor named {GENERATOR_NAME!r}")

        elif generator["autoTerminatedRelationships"]:
            problems.append(
                "the generator auto-terminates a relationship, so flowfiles "
                "would never reach the queue"
            )

        if drain is None:
            problems.append(f"no processor named {DRAIN_NAME!r}")

        elif "success" not in drain["autoTerminatedRelationships"]:
            problems.append(
                "the drain does not auto-terminate success, so it would be "
                "invalid and nothing would drain"
            )

        # The breach has to outlast a metric sample interval with room to
        # spare, or a job run can land entirely in the quiet half.
        if generator is not None:
            rate = max(1, config.DRAIN_CONCURRENCY)  # ~1/s per task
            drain_seconds = config.BURST_SIZE / rate
            above = max(0.0, drain_seconds - config.QUEUE_THRESHOLD / rate)

            if above < 3 * 75:
                problems.append(
                    f"the queue only stays above {config.QUEUE_THRESHOLD} for "
                    f"~{above:.0f}s per cycle, which is under three 75s metric "
                    "samples: raise BURST_SIZE, lower QUEUE_THRESHOLD, or "
                    "lower DRAIN_CONCURRENCY"
                )

            # Headroom, learned the hard way: with drain capacity merely
            # equal to generation the queue never recovers from anything
            # an action adds to it, and one remediation ends the
            # oscillation permanently. Require the drain to finish with
            # time to spare inside each cycle.
            if drain_seconds > 0.8 * config.BURST_PERIOD_SECONDS:
                problems.append(
                    f"the drain needs ~{drain_seconds:.0f}s to clear a burst "
                    f"but a cycle is only {config.BURST_PERIOD_SECONDS}s, so "
                    "there is no headroom: a backlog left behind by a "
                    "remediation would never clear and the flow would sit "
                    "above threshold forever. Raise DRAIN_CONCURRENCY or "
                    "lower BURST_SIZE."
                )

    for proc in processors:
        version = proc["bundle"]["version"]

        if version != config.NIFI_VERSION:
            problems.append(
                f"{proc['name']}: bundle version {version} != configured "
                f"NiFi version {config.NIFI_VERSION}"
            )

    problems.extend(f"non-ASCII field name {key!r}" for key in _odd_keys(definition))

    return problems


def _odd_keys(node, path: str = "") -> list:
    """
    Find field names containing non-ASCII characters.

    This catches a failure mode with no symptom: a homoglyph in a key
    (a Cyrillic `у` in `bulletinLevel`, say) produces valid JSON that
    NiFi accepts, ignores as unknown, and silently substitutes a default
    for — so the flow deploys and behaves subtly wrong. Cheap to check,
    invisible otherwise.
    """

    found = []

    if isinstance(node, dict):
        for key, value in node.items():
            if not str(key).isascii():
                found.append(f"{path}.{key}")

            found.extend(_odd_keys(value, f"{path}.{key}"))

    elif isinstance(node, list):
        for index, value in enumerate(node):
            found.extend(_odd_keys(value, f"{path}[{index}]"))

    return found


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

def main() -> None:
    """Build the flow definition, validate it, and write it out."""

    parser = argparse.ArgumentParser(
        description=(
            "Emit the synthetic oscillating NiFi flow definition used by "
            "the self-healing prototype."
        )
    )

    parser.add_argument(
        "--out",
        default=config.FLOW_DEFINITION_FILE,
        help=(
            "Path to write the flow definition JSON to "
            f"(default: {config.FLOW_DEFINITION_FILE})"
        ),
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Validate only; do not write. Exits non-zero if any check "
            "fails."
        ),
    )

    args = parser.parse_args()

    definition = build()
    problems = check(definition)

    if problems:
        print("flow definition FAILED validation:", file=sys.stderr)

        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)

        sys.exit(1)

    contents = definition["flowContents"]

    print(
        f"ok: {len(contents['processors'])} processors, "
        f"{len(contents['connections'])} connection, "
        f"NiFi {config.NIFI_VERSION}"
    )
    print(
        f"    burst {config.BURST_SIZE} every "
        f"{config.BURST_PERIOD_SECONDS}s, drained at "
        f"~{config.DRAIN_CONCURRENCY}/s "
        f"(~{config.BURST_SIZE // max(1, config.DRAIN_CONCURRENCY)}s), "
        f"threshold {config.QUEUE_THRESHOLD}"
    )

    if args.check:
        return

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(definition, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"    wrote {args.out}")


if __name__ == "__main__":
    main()
