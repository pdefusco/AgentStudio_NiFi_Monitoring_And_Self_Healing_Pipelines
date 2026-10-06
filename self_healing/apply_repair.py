#!/usr/bin/env python3
"""
Apply one whitelisted repair to the live flow, and measure what it did.

This is verification step 6 as a script rather than a sequence of
hand-typed commands, for one reason: the apply path has no undo. There is
no `deleteFlowVersion` in the df surface, so every run leaves a permanent
catalog version behind. A script that always captures the before-state,
always validates, and refuses on its own when anything looks wrong is
worth more here than the flexibility of doing it by hand.

It deliberately uses the *job-side* modules rather than the Agent Studio
tools. Three reasons:

  - `df_api.change_flow_version()` carries both guardrails, including
    `assert_flow_version_allowed()`, which is the check that stops a
    typo'd version CRN from repointing our deployment at a colleague's
    flow. The tools carry their own copies, but this is the path that
    will run in production.
  - It reads credentials from the ambient CDP profile, so no key material
    has to be passed on a command line or assembled into an environment.
  - `repairs.apply()` is the same code the tools vendor, so the edit this
    produces is the edit the agent would have proposed.

Usage:

    python self_healing/apply_repair.py --measure
    python self_healing/apply_repair.py --concurrency 4
    python self_healing/apply_repair.py --concurrency 4 --arm
    python self_healing/apply_repair.py --restore --arm

Without `--arm` it stops after validating and prints the diff and the
exact commands it would run. `--measure` only reads.

**The workload plane must be reachable.** `change-flow-version-in-deployment`
is a `cdp df` command that hops to the workload plane internally, and that
plane is a private load balancer. Run `df_api.selftest()` first: a
workload token expiry printed *then* a timeout means credentials and roles
are fine and only the network path is missing. Importing happens *before*
the swap and is control-plane, so running this outside the VPC would
create a permanent catalog version and then fail to deploy it — which is
why `--arm` probes the workload plane before it writes anything.
"""

import argparse
import json
import os
import socket
import statistics
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import df_api
import repairs
import state


def slope(charts_result: dict) -> dict:
    """
    The conditional-median drain rate, from a single KPI response.

    Buckets are anchored to request time, so differencing across two
    responses is unsound — the repo measured a 20% error doing it. Only
    pairs that are *falling* and still have work at the end are kept: a
    pair ending at zero is the queue hitting empty partway through the
    bucket, and including it biases the rate toward zero more as the
    drain gets faster, which is backwards for judging a repair whose
    whole purpose is to make it faster.
    """

    ours = [
        chart for chart in charts_result["charts"]
        if df_api.is_our_connection_chart(chart)
    ]

    if not ours:
        return {
            "chart_found": False,
            "names_seen": sorted(
                {c.get("componentName") for c in charts_result["charts"]}
            ),
        }

    chart = ours[0]

    datas = (chart.get("metrics") or {}).get("datas") or chart.get("datas") or []

    points = [(d["timestamp"], d["value"]) for d in datas]

    rates = []

    for (t0, v0), (t1, v1) in zip(points, points[1:]):

        seconds = (t1 - t0) / 1000.0

        if seconds > 0 and v1 < v0 and v1 > 0:
            rates.append((v1 - v0) / seconds)

    return {
        "chart_found": True,
        "series": [int(v) for _, v in points],
        "buckets": len(points),
        "pairs": len(rates),
        "rates": [round(r, 3) for r in rates],
        "median": round(statistics.median(rates), 3) if rates else None,
        "queue_now": int(points[-1][1]) if points else None,
    }


def workload_unreachable(timeout: float = 5.0) -> str:
    """
    Empty string if the workload endpoint accepts a connection.

    Returns a human-readable reason otherwise. This is a network probe,
    not an auth probe: it says nothing about whether the credentials or
    the DataFlow roles are right, only whether there is a path. That
    division is the useful one, because the two failures look identical
    through the CLI — a token expiry printed, then a timeout.
    """

    # `workload_endpoint()` returns a *display* string — the host followed
    # by the token expiry, or `UNRESOLVED (…)` when the token call itself
    # failed. Taking the first field is deliberate rather than tidy:
    # passing the whole string to getaddrinfo raises gaierror, which this
    # function would then report as "does not resolve" — a confident,
    # wrong diagnosis of DNS when the real answer is private addresses
    # that time out. Exactly the misattribution the probe exists to stop.
    display = df_api.workload_endpoint()

    if not display or display.startswith("UNRESOLVED"):
        return (
            "the workload endpoint could not be determined, so "
            f"reachability cannot be established ({display or 'no answer'})"
        )

    host = display.split()[0]

    try:
        addresses = sorted(
            {
                info[4][0]
                for info in socket.getaddrinfo(host, 443, socket.AF_INET)
            }
        )

    except socket.gaierror as error:
        return f"{host} does not resolve ({error})"

    for address in addresses:

        probe = socket.socket()
        probe.settimeout(timeout)

        try:
            probe.connect((address, 443))

            return ""

        except OSError:
            continue

        finally:
            probe.close()

    return (
        f"{host} resolves to {addresses} and none of them accept a "
        f"connection on 443 within {timeout:g}s"
    )


def read_slope(deployment_crn: str, deployed_flow_crn: str) -> dict:
    """One KPI read, reduced to a drain rate."""

    return slope(
        df_api.metric_charts(
            deployment_crn, deployed_flow_crn, config.METRICS_TIME_PERIOD
        )
    )


def report_slope(label: str, measured: dict) -> None:

    print(f"\n{label}")

    if not measured.get("chart_found"):
        print("  CHART NOT FOUND — the monitor is blind.")
        print(f"  names seen: {measured.get('names_seen')}")

        return

    print(f"  series: {measured['series']}")
    print(f"  qualifying pairs: {measured['pairs']}  rates: {measured['rates']}")
    print(f"  median: {measured['median']} files/s   queue now: {measured['queue_now']}")

    if measured["pairs"] == 1:
        print(
            "  NOTE: one pair only. A faster drain clears the burst inside "
            "two buckets,\n        so this is the measurability ceiling "
            "showing, not a bad read. Re-run\n        --measure over "
            "another cycle or two before believing the number."
        )


def main() -> int:

    parser = argparse.ArgumentParser(
        description="Apply one whitelisted repair to the live flow."
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="target concurrentlySchedulableTaskCount for speed_up_slow_drain",
    )

    parser.add_argument(
        "--restore",
        action="store_true",
        help="apply restore_baseline instead, putting the drain back",
    )

    parser.add_argument(
        "--arm",
        action="store_true",
        help=(
            "actually import a version and swap the deployment. Without "
            "this, nothing is written."
        ),
    )

    parser.add_argument(
        "--measure",
        action="store_true",
        help="read the drain rate and exit, changing nothing",
    )

    arguments = parser.parse_args()

    stored = state.read()

    deployment_crn = stored.get("deployment_crn")

    if not deployment_crn:
        return fail(
            "state has no deployment_crn. Run\n"
            "  python self_healing/job/monitor_and_remediate.py --reconcile"
        )

    deployed = df_api.df(
        "list-flows-in-deployment", "--deployment-crn", deployment_crn
    )

    if "error" in deployed:
        return fail(f"could not list deployed flows: {deployed['error']}")

    deployed_flow_crn = (deployed.get("deployedFlows") or [{}])[0].get("crn")

    if arguments.measure:
        report_slope(
            "drain rate", read_slope(deployment_crn, deployed_flow_crn)
        )

        return 0

    # --- before -------------------------------------------------------
    #
    # Both reads are needed and they carry different things.
    # `flow_facts` has the deployed flow's version and `valid_actions`
    # — the half-applied signal — but its `crn` is the *deployed* flow,
    # not the catalog flow. The catalog `flow_crn` the import targets
    # comes from `deployment_facts`, which is one call that also yields
    # the deployment's name for the prefix guard.
    facts = df_api.flow_facts(deployment_crn, deployed_flow_crn)

    deployment = df_api.deployment_facts(deployment_crn)

    flow_crn = deployment.get("flow_crn")

    print("before:")
    print(f"  deployment:   {deployment.get('name')}")
    print(f"  state:        {facts.get('state')}")
    print(f"  flow version: {facts.get('flow_version')}")
    print(f"  validActions: {facts.get('valid_actions')}")
    print(f"  catalog flow: {flow_crn}")

    if not flow_crn:
        return fail(
            "the deployment did not report a catalog flow CRN, so there "
            "is nothing to import a version into."
        )

    # The recorded flow CRN is the independent expectation. Comparing the
    # live one against itself would make the provenance check in
    # change_flow_version() tautological — it exists to catch a version
    # CRN belonging to a different flow, and it can only do that against
    # a value we wrote down before today.
    expected_flow_crn = stored.get("flow_crn") or flow_crn

    if flow_crn != expected_flow_crn:
        return fail(
            "the deployment's catalog flow does not match the one in "
            f"state.\n  live:     {flow_crn}\n  recorded: {expected_flow_crn}\n"
            "  Reconcile deliberately rather than letting this script "
            "pick one."
        )

    if df_api.is_half_applied(facts):
        return fail(
            "a previous version change is half-applied "
            f"({facts.get('valid_actions')}). Resolve it by hand with "
            "retry- or cancel-change-flow-version-in-deployment; this "
            "script will not retry on a shared tenant."
        )

    guard = df_api.assert_action_allowed(deployment_crn, deployment_crn)

    if guard.get("refused"):
        return fail(f"guardrail refused: {guard.get('reason')}")

    before_slope = read_slope(deployment_crn, deployed_flow_crn)

    report_slope("before slope", before_slope)

    if not before_slope.get("chart_found"):
        return fail(
            "the KPI chart is not visible before the change, so a later "
            "'chart missing' could not be attributed to the swap."
        )

    # --- the edit, computed and validated in memory --------------------
    flow_version_crn = facts.get("flow_version_crn")

    definition = df_api.df(
        "get-flow-version", "--flow-version-crn", flow_version_crn
    )

    if "error" in definition:
        return fail(f"could not read the flow definition: {definition['error']}")

    name = "restore_baseline" if arguments.restore else "speed_up_slow_drain"

    params = {} if arguments.restore else {"concurrency": arguments.concurrency}

    try:
        after, proposal = repairs.apply(definition, name, params)

    except repairs.RepairError as error:
        return fail(f"the repair was refused: {error}")

    print(f"\nrepair: {name}  {json.dumps(params)}")
    print(f"  declared paths: {proposal['declared_paths']}")
    print(f"  changed:        {json.dumps(proposal['changed'], indent=2)}")

    if not proposal["changed"]:
        print("\nnothing would change; the flow is already in that state.")

        return 0

    if not arguments.arm:
        print(
            "\nnot armed, so nothing was written. The two commands this "
            "would run:\n"
            f"  cdp df import-flow-definition-version --flow-crn "
            f"{flow_crn} --file <edited.json>\n"
            "  cdp df change-flow-version-in-deployment --deployment-crn "
            "<crn> --flow-version-crn <new> \\\n"
            "      --strategy ONLY_RESTART_AFFECTED_COMPONENTS\n"
            "\nRe-run with --arm to apply. This creates a permanent catalog "
            "version."
        )

        return 0

    # --- armed: probe the workload plane BEFORE writing anything -------
    #
    # The import is control-plane and would succeed outside the VPC; the
    # swap would then time out, leaving a permanent catalog version that
    # nothing is deployed from and no measurement to show for it. So the
    # cheap probe goes first, even though it costs two minutes on failure.
    print("\nprobing the workload plane before writing anything…")

    # A TCP connect first, because it answers in seconds where the CLI
    # takes its full 120s timeout. The workload endpoint's DNS is public
    # but its addresses are RFC1918 — measured 10.10.0.157 and
    # 10.10.1.139 — so from outside the VPC this fails immediately and
    # names the reason precisely, instead of looking like a slow API.
    unreachable = workload_unreachable()

    if unreachable:
        return fail(
            "the workload plane is not reachable, so the swap would fail "
            "after the import had already created a permanent catalog "
            f"version.\n  {unreachable}\n"
            "  Run this from inside the VPC — a Cloudera AI session or job in "
            "the same environment.\n  A VPN is not a workaround: measured "
            "2026-10-05, the tunnel routes those\n  addresses and they still "
            "time out, while the public control plane connects\n  through the "
            "same tunnel in 0.06s. The drop is at the far end."
        )

    probe = df_api.dfw("list-nifi-versions")

    if "error" in probe:
        return fail(
            "the workload plane is not reachable, so the swap would fail "
            "after the import had already created a permanent catalog "
            f"version.\n  {probe.get('error')}\n"
            "  The TCP probe passed but the CLI did not, so this is the "
            "credential or\n  entitlement case rather than the network one. "
            "Check `df_api.selftest()`."
        )

    print("  workload plane: ok")

    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="repair-", delete=False
    )

    try:
        with handle:
            json.dump(after, handle, indent=2)

        imported = df_api.df(
            "import-flow-definition-version",
            "--flow-crn",
            flow_crn,
            "--file",
            handle.name,
            "--comments",
            f"{name} applied by self_healing/apply_repair.py",
        )

    finally:
        try:
            os.unlink(handle.name)

        except OSError:
            pass

    if "error" in imported:
        return fail(f"the import was rejected: {imported['error']}")

    # Read the version back rather than trusting the import response, so
    # the CRN we swap to is one CDF has actually indexed. Same reason
    # provision/01_import_flow.py does it.
    versions = df_api.df(
        "list-flow-definition-versions", "--flow-crn", flow_crn
    )

    newest = max(
        versions.get("flowVersions") or [],
        key=lambda v: v.get("version") or 0,
        default={},
    )

    new_version_crn = newest.get("crn")

    print(f"\nimported version {newest.get('version')}")
    print(f"  {new_version_crn}")

    swap = df_api.change_flow_version(
        deployment_crn=deployment_crn,
        expected_crn=deployment_crn,
        flow_version_crn=new_version_crn,
        expected_flow_crn=expected_flow_crn,
        strategy="ONLY_RESTART_AFFECTED_COMPONENTS",
        dry_run=False,
    )

    print("\nswap:")
    print(json.dumps({k: v for k, v in swap.items() if k != "argv"}, indent=2)[:900])

    if swap.get("refused") or swap.get("error"):
        return fail(
            "the swap was refused or failed, but the catalog version "
            f"{newest.get('version')} now exists permanently and nothing "
            "is deployed from it."
        )

    print(
        f"\nswapped. The drain rate cannot be measured for about two "
        f"buckets (~150s),\nbecause every bucket straddling the change is "
        "discarded. Then:\n"
        "  python self_healing/apply_repair.py --measure"
    )

    return 0


def fail(message: str) -> int:

    print(f"\nerror: {message}", file=sys.stderr)

    return 1


if __name__ == "__main__":
    sys.exit(main())
