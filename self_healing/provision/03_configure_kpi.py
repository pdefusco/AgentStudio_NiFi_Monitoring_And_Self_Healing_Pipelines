#!/usr/bin/env python3
"""
Configure the queue-depth KPI that the monitoring job reads.

This step is **required, not optional**. A fresh deployment publishes only
`Data Out`, `CPU Utilization`, `Memory Utilization` and `Core Allocation`
— there is no queue-depth series by default. The KPI is what creates the
series, so without this the job would run forever and never see a breach.

The sequence, and why it is this shape:

  1. `get-flow-configuration-metadata-in-deployment` for the component id.
     NiFi assigns its own ids at deploy time, so the builder's
     deterministic UUIDs are useless here — the real id has to be
     discovered, and it is addressed as semicolon-joined ancestry,
     `"<processGroupId>;<connectionId>"`.
  2. `get-flow-configuration-in-deployment` for `configurationVersion`,
     read immediately before the write, because anything else touching
     the deployment (the CDF UI included) bumps it.
  3. `update-flow-in-deployment --kpis`, which is a **whole-array
     replace** — so the existing KPIs are read and sent back alongside
     the new one.
  4. Poll until `kpisDirty` is false, which is what says the rule
     actually reached NiFi rather than just being accepted.
  5. Assert the chart is visible through `df list-flow-kpis-in-deployment`
     and record its display-string tuple, because `MetricChart` carries no
     ids and that tuple is the only way the job can recognise it again.

Steps 4 and 5 exist because the failure to avoid is shipping a monitor
that cannot trip.

`update-deployment --kpis` is deliberately **not** used anywhere: it is a
deployment-level whole-array replace with no undo, and against the wrong
CRN it silently erases a colleague's KPIs.

Usage:

    python provision/03_configure_kpi.py
    python provision/03_configure_kpi.py --dry-run
    python provision/03_configure_kpi.py --verify-only
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import df_api
import state


# How long to wait for NiFi to pick up the KPI. `kpisDirty` flipping to
# false is the signal.
APPLY_DEADLINE_SECONDS = 600

APPLY_POLL_SECONDS = 15

# The chart appears only once metrics have been published at least once,
# and publishing is on a 75-second cycle.
CHART_DEADLINE_SECONDS = 420

CHART_POLL_SECONDS = 30


def flow_configuration(deployment_crn: str, flow_crn: str) -> dict:
    """Read the deployed flow's live configuration."""

    result = df_api.require(
        df_api.dfw(
            "get-flow-configuration-in-deployment",
            "--deployment-crn",
            deployment_crn,
            "--deployed-flow-crn",
            flow_crn,
        ),
        "could not read the deployed flow configuration",
    )

    return result.get("deployedFlowConfiguration") or {}


def resolve_component(deployment_crn: str, flow_crn: str) -> tuple:
    """
    Find the NiFi-assigned id of our connection.

    Returns (component_id, unit_id). Exits with the available options
    rather than a bare failure if anything does not match, because the
    useful debugging information is exactly the list of what *is* there.
    """

    result = df_api.require(
        df_api.dfw(
            "get-flow-configuration-metadata-in-deployment",
            "--deployment-crn",
            deployment_crn,
            "--deployed-flow-crn",
            flow_crn,
        ),
        "could not read the flow configuration metadata",
    )

    metadata = result.get("deployedFlowConfigurationMetadata") or {}
    kpi_metadata = metadata.get("kpiMetaData") or {}
    scopes = kpi_metadata.get("kpiScopes") or []

    scope = next(
        (s for s in scopes if s.get("type") == config.KPI_COMPONENT_TYPE),
        None,
    )

    if scope is None:
        sys.exit(
            f"error: no KPI scope of type {config.KPI_COMPONENT_TYPE!r}.\n"
            f"  available: {[s.get('type') for s in scopes]}"
        )

    metric_types = scope.get("metricTypes") or []
    metric = next(
        (m for m in metric_types if m.get("id") == config.KPI_METRIC_ID),
        None,
    )

    if metric is None:
        sys.exit(
            f"error: metric {config.KPI_METRIC_ID!r} is not offered for "
            f"{config.KPI_COMPONENT_TYPE}.\n"
            "  available:\n"
            + "\n".join(
                f"    {m.get('id')}  ({m.get('label')}, default unit "
                f"{m.get('defaultUnitId')})"
                for m in metric_types
            )
        )

    # `scopeComponents[].id` is ALREADY the semicolon-joined ancestry
    # ("<processGroupId>;<connectionId>"), verified live: the connection
    # reads back as 2 segments and the root process group as 1. Joining
    # it to the enclosing contextGroup's id would repeat the first
    # segment and send a componentId matching nothing. Use it as given.
    available = []

    for group in scope.get("contextGroups") or []:
        for component in group.get("scopeComponents") or []:
            available.append(f"{component.get('name')!r} in {group.get('name')!r}")

            if component.get("name") == config.CONNECTION_NAME:
                component_id = component.get("id") or ""

                if not component_id:
                    sys.exit(
                        f"error: the scope component for "
                        f"{config.CONNECTION_NAME!r} has no id."
                    )

                print(f"  connection:    {config.CONNECTION_NAME!r}")
                print(f"  process group: {group.get('name')!r}")
                print(
                    f"  componentId:   {component_id} "
                    f"({len(component_id.split(';'))} ancestry segment(s))"
                )

                return component_id, metric.get("defaultUnitId")

    sys.exit(
        f"error: no {config.KPI_COMPONENT_TYPE} named "
        f"{config.CONNECTION_NAME!r} in this deployment.\n"
        "  available:\n" + "\n".join(f"    {a}" for a in available)
        + "\n  The flow definition sets this name explicitly; if it is "
        "missing, the deployed flow version is not the one this prototype "
        "built."
    )


def build_kpi(component_id: str, unit_id: str) -> dict:
    """Build the KPI to add."""

    return {
        "metricId": config.KPI_METRIC_ID,
        "metricComponentType": config.KPI_COMPONENT_TYPE,
        "componentId": component_id,
        "alert": {
            "thresholdMoreThan": {
                "unitId": unit_id or config.KPI_UNIT_ID,
                "value": config.QUEUE_THRESHOLD,
            },
            # The CLI derives `label` and `abbreviation` from this id
            # before sending (`process_kpis`), so they must not be set
            # here — but `unit.id` must be, or the CLI raises a KeyError.
            "frequencyTolerance": {
                "value": config.KPI_FREQUENCY_TOLERANCE_VALUE,
                "unit": {"id": config.KPI_FREQUENCY_TOLERANCE_UNIT},
            },
        },
    }


def check_resendable(kpis: list) -> None:
    """
    Refuse to re-send KPIs the CLI would crash on.

    `cdpcli.extensions.df.process_kpis` reaches straight into
    `alert['frequencyTolerance']['unit']['id']`, so a stored KPI missing
    that key produces an unhandled KeyError traceback rather than an
    error message. Since `kpis` is a whole-array replace, every existing
    KPI has to survive the round trip.
    """

    for kpi in kpis:
        tolerance = (kpi.get("alert") or {}).get("frequencyTolerance")

        if tolerance is None:
            continue

        if not (tolerance.get("unit") or {}).get("id"):
            sys.exit(
                "error: an existing KPI on this deployment has an alert "
                "frequency tolerance with no unit id, and re-sending it "
                "would crash the CDP CLI.\n"
                f"  KPI: {json.dumps(kpi)[:300]}\n"
                "  Remove or repair that KPI in the CDF UI and re-run."
            )


def already_configured(kpis: list, component_id: str) -> dict:
    """Return our KPI if it is already present, else {}."""

    for kpi in kpis:
        if (
            kpi.get("metricId") == config.KPI_METRIC_ID
            and kpi.get("componentId") == component_id
        ):
            return kpi

    return {}


def wait_for_clean(deployment_crn: str, flow_crn: str) -> bool:
    """
    Poll until `kpisDirty` is false.

    `kpisDirty` is readOnly and means the configured KPIs have not yet
    been applied to the running NiFi. Waiting on it is the difference
    between "CDF accepted the request" and "the rule exists".
    """

    started = time.monotonic()

    while True:
        configuration = flow_configuration(deployment_crn, flow_crn)
        dirty = configuration.get("kpisDirty")
        elapsed = int(time.monotonic() - started)

        if dirty is False:
            print(f"  [{elapsed:>5}s] kpisDirty=false, applied")

            return True

        if elapsed > APPLY_DEADLINE_SECONDS:
            print(f"  [{elapsed:>5}s] still kpisDirty={dirty}")

            return False

        print(f"  [{elapsed:>5}s] kpisDirty={dirty}, waiting")

        time.sleep(APPLY_POLL_SECONDS)


def find_chart(deployment_crn: str, flow_crn: str) -> dict:
    """
    Return the metric chart our KPI created, or {} if not visible yet.

    Both chart surfaces are searched, via the same merge the job uses.
    Which surface a flow-scoped KPI appears in is not documented, so
    verifying against only one risks aborting provisioning over a KPI
    that landed perfectly well on the other.
    """

    merged = df_api.metric_charts(deployment_crn, flow_crn)

    for chart in merged["charts"]:
        if df_api.is_our_connection_chart(chart):
            return chart

    return {}


def wait_for_chart(deployment_crn: str, flow_crn: str) -> dict:
    """Poll until the KPI's chart is published."""

    started = time.monotonic()

    while True:
        chart = find_chart(deployment_crn, flow_crn)
        elapsed = int(time.monotonic() - started)

        if chart:
            print(f"  [{elapsed:>5}s] chart published")

            return chart

        if elapsed > CHART_DEADLINE_SECONDS:
            return {}

        print(f"  [{elapsed:>5}s] no chart yet (published every ~75s)")

        time.sleep(CHART_POLL_SECONDS)


def verify(deployment_crn: str, flow_crn: str) -> dict:
    """Confirm the chart exists and record how to recognise it."""

    print("\nverifying the chart is published:")

    chart = wait_for_chart(deployment_crn, flow_crn)

    if not chart:
        # Print what *is* published rather than only what is missing: if
        # the KPI landed under an unexpected componentName this inventory
        # is the whole diagnosis.
        merged = df_api.metric_charts(deployment_crn, flow_crn)

        sys.exit(
            "error: the KPI was accepted but no matching chart appeared "
            f"within {CHART_DEADLINE_SECONDS}s.\n"
            f"  looking for componentName={config.CONNECTION_NAME!r}\n"
            f"  sources: {merged['sources']}\n"
            "  published charts:\n"
            + "\n".join(
                f"    {c.get('componentType')} / {c.get('componentName')} / "
                f"{c.get('name')}  [{c.get('_source')}]"
                for c in merged["charts"]
            )
            + "\n  The job reads this chart, so it would never detect a "
            "breach. Check the Alerts tab of the deployment in the CDF UI."
        )

    chart_key = {
        "name": chart.get("name"),
        "componentName": chart.get("componentName"),
        "componentType": chart.get("componentType"),
    }

    metrics = chart.get("metrics") or {}
    points = metrics.get("datas") or []

    print(f"  name:          {chart.get('name')!r}")
    print(f"  componentName: {chart.get('componentName')!r}")
    print(f"  componentType: {chart.get('componentType')!r}")
    print(f"  unitType:      {chart.get('unitType')!r}")
    print(f"  read from:     {chart.get('_source')}")
    print(f"  currentValue:  {metrics.get('currentValue')}")
    print(f"  data points:   {len(points)}")

    alert = chart.get("alert")

    if not alert:
        print(
            "\n  warning: the chart has no alert block, so the threshold did "
            "not attach.\n"
            "  The job derives its own breach flag and will still work, but "
            "CDF will not corroborate it."
        )

    else:
        print(f"  alert:         thresholdMoreThan={alert.get('thresholdMoreThan')}")

    current = state.read()
    current["chart_key"] = chart_key
    state.write(current)

    print(f"\nrecorded chart_key in {config.STATE_FILE}")

    return chart


def main() -> None:
    """Discover the connection, write the KPI, and verify it landed."""

    parser = argparse.ArgumentParser(
        description=(
            "Configure the queue-depth KPI that creates the metric series "
            "the monitoring job reads."
        )
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the component and print the KPI payload without writing it.",
    )

    parser.add_argument(
        "--verify-only",
        action="store_true",
        help=(
            "Skip the write; just confirm the chart is published and record "
            "its match key."
        ),
    )

    args = parser.parse_args()

    current = state.read()
    deployment_crn = current.get("deployment_crn")
    flow_crn = current.get("deployed_flow_crn")

    if not deployment_crn or not flow_crn:
        sys.exit(
            "error: the state file has no deployment CRN and deployed-flow "
            "CRN.\n  Run: python provision/02_create_deployment.py"
        )

    # This writes configuration to a deployment, so it passes the same
    # guardrail as a restart. A misconfigured CRN here would reconfigure
    # somebody else's flow.
    try:
        deployment = df_api.assert_action_allowed(deployment_crn, deployment_crn)

    except df_api.NotAllowed as exc:
        sys.exit(f"error: refusing to configure this deployment.\n  {exc}")

    print(f"deployment: {deployment.get('name')!r}")
    print(f"  state: {(deployment.get('status') or {}).get('state')}")

    if args.verify_only:
        verify(deployment_crn, flow_crn)

        return

    print("\nresolving the connection's NiFi-assigned id:")

    component_id, unit_id = resolve_component(deployment_crn, flow_crn)

    configuration = flow_configuration(deployment_crn, flow_crn)
    configuration_version = configuration.get("configurationVersion")
    existing = configuration.get("kpis") or []
    parameter_groups = configuration.get("parameterGroups") or []

    print(f"\n  configurationVersion: {configuration_version}")
    print(f"  existing KPIs:        {len(existing)}")

    # Our synthetic flow declares no parameters. If any are present, this
    # is not the flow we built, and re-sending parameter groups can blank
    # sensitive values whose current value reads back as null.
    if parameter_groups:
        sys.exit(
            "error: this deployed flow has parameter groups, which the "
            "synthetic flow does not.\n"
            f"  groups: {[g.get('name') for g in parameter_groups]}\n"
            "  Refusing, because the target may not be the flow this "
            "prototype built."
        )

    present = already_configured(existing, component_id)

    if present:
        print(
            f"\nthe KPI is already configured "
            f"(id {present.get('id')}); skipping the write."
        )

        verify(deployment_crn, flow_crn)

        return

    check_resendable(existing)

    kpi = build_kpi(component_id, unit_id)
    kpis = existing + [kpi]

    print(f"\nKPI to add:\n{json.dumps(kpi, indent=2)}")
    print(
        f"\n  sending {len(kpis)} KPI(s): `kpis` is a whole-array replace, so "
        f"the {len(existing)} existing one(s) are sent back too."
    )

    arguments = [
        "update-flow-in-deployment",
        "--deployment-crn",
        deployment_crn,
        "--deployed-flow-crn",
        flow_crn,
        "--configuration-version",
        configuration_version,
        "--kpis",
        json.dumps(kpis),
    ]

    outcome = df_api.act(
        deployment_crn,
        deployment_crn,
        arguments,
        dry_run=args.dry_run,
    )

    if outcome.get("refused"):
        sys.exit(f"error: refused by the guardrail.\n  {outcome.get('reason')}")

    if args.dry_run:
        print("\ndry run: nothing was written.")
        print("  " + df_api.command_line(outcome.get("argv") or [], ""))

        return

    if not outcome.get("performed"):
        result = outcome.get("result") or {}

        # A version conflict means something else wrote between the read
        # and the write. One retry with a fresh version is reasonable;
        # looping is not, because a persistent conflict means a human is
        # editing the deployment right now.
        stderr = str(result.get("stderr") or "")

        if "conflict" in stderr.lower() or "version" in stderr.lower():
            print("\nversion conflict; re-reading and retrying once.")

            fresh = flow_configuration(deployment_crn, flow_crn)
            arguments[arguments.index("--configuration-version") + 1] = (
                fresh.get("configurationVersion")
            )

            existing = fresh.get("kpis") or []
            check_resendable(existing)

            if already_configured(existing, component_id):
                print("  the KPI is now present; the other writer added it.")

                verify(deployment_crn, flow_crn)

                return

            arguments[arguments.index("--kpis") + 1] = json.dumps(
                existing + [kpi]
            )

            outcome = df_api.act(
                deployment_crn, deployment_crn, arguments, dry_run=False
            )

        if not outcome.get("performed"):
            df_api.require(
                outcome.get("result") or {"error": "the KPI write failed"},
                "the KPI write was rejected",
            )

    print("\nKPI written. Waiting for NiFi to apply it:")

    if not wait_for_clean(deployment_crn, flow_crn):
        sys.exit(
            "error: the KPI was accepted but kpisDirty did not clear within "
            f"{APPLY_DEADLINE_SECONDS}s.\n"
            "  Check the deployment in the CDF UI before relying on the job."
        )

    verify(deployment_crn, flow_crn)

    print("\nnext: watch the oscillation before arming the job —")
    print("  python job/monitor_and_remediate.py")


if __name__ == "__main__":
    main()
