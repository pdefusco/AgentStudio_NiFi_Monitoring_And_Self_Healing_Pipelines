#!/usr/bin/env python3
"""
Deploy the synthetic flow to Cloudera DataFlow and wait for it to settle.

This is the step that costs money. An EXTRA_SMALL single-node deployment
bills from the moment provisioning starts until `teardown.py` runs, and
takes roughly fifteen minutes to reach a steady state. It becomes an
additional deployment in a DataFlow service colleagues are already using.

`df create-deployment` does **not** wait: the CLI extension initiates the
deployment and returns the CRN immediately
(`cdpcli/extensions/df/createdeployment.py`). Anything that touches the
deployment next races it, so this script does the waiting that the CLI
does not — first for a steady deployment state, then for the deployed-flow
CRN to appear, which happens later still and which every flow-scoped
operation needs.

Both CRNs are recorded in the state file. That recording is what the
guardrail in `df_api.assert_action_allowed` compares every mutation
against, so this script is also what *arms* the prototype.

Usage:

    python provision/02_create_deployment.py
    python provision/02_create_deployment.py --dry-run
    python provision/02_create_deployment.py --wait-only
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


# Deploying a single EXTRA_SMALL node takes ~15 minutes; this leaves
# generous headroom before giving up, since a slow provision is not a
# failed one.
DEPLOY_DEADLINE_SECONDS = 2400

# The deployed-flow CRN appears after the deployment reaches a steady
# state, not at the same time.
FLOW_CRN_DEADLINE_SECONDS = 600

FLOW_CRN_POLL_SECONDS = 20

MAX_ITEMS = 500


def find_deployment() -> dict:
    """Return our deployment if it already exists, else {}."""

    result = df_api.require(
        df_api.df("list-deployments", "--max-items", MAX_ITEMS),
        "could not list deployments",
    )

    for deployment in result.get("deployments") or []:
        if deployment.get("name") == config.DEPLOYMENT_NAME:
            return deployment

    return {}


def wait_for_deployed_flow(deployment_crn: str) -> str:
    """
    Poll until the deployment exposes a deployed-flow CRN.

    Separate from the deployment-state wait because they complete at
    different times, and because an empty flow list is the normal early
    condition rather than an error.
    """

    started = time.monotonic()

    while True:
        crn = df_api.deployed_flow_crn(deployment_crn)

        if crn:
            return crn

        elapsed = int(time.monotonic() - started)

        if elapsed > FLOW_CRN_DEADLINE_SECONDS:
            return ""

        print(f"  [{elapsed:>5}s] waiting for the deployed flow", flush=True)

        time.sleep(FLOW_CRN_POLL_SECONDS)


def settle(deployment_crn: str) -> tuple:
    """
    Wait for a steady state and a deployed-flow CRN.

    Returns (state, deployed_flow_crn). Either may indicate failure; the
    caller decides, because a deployment stuck in DEPLOYING is a thing to
    report and keep waiting on, not a thing to tear down automatically.
    """

    print("\nwaiting for a steady deployment state:")

    final_state = df_api.wait_for_steady_state(
        deployment_crn,
        DEPLOY_DEADLINE_SECONDS,
        verbose=True,
    )

    if final_state not in df_api.STEADY_STATES:
        return final_state, ""

    print("\nwaiting for the deployed-flow CRN:")

    return final_state, wait_for_deployed_flow(deployment_crn)


def record(deployment_crn: str, flow_crn: str) -> None:
    """Record the CRNs the rest of the prototype is scoped to."""

    current = state.read()
    current["deployment_crn"] = deployment_crn
    current["deployed_flow_crn"] = flow_crn
    state.write(current)

    print(f"\nrecorded in {config.STATE_FILE}")


def main() -> None:
    """Create the deployment if absent, then wait for it to settle."""

    parser = argparse.ArgumentParser(
        description=(
            "Deploy the synthetic oscillating flow and wait for it to reach "
            "a steady state."
        )
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the deployment command and exit without creating it.",
    )

    parser.add_argument(
        "--wait-only",
        action="store_true",
        help=(
            "Do not create anything; wait on the deployment that already "
            "exists and record its CRNs. Use this after an interrupted run."
        ),
    )

    args = parser.parse_args()

    current = state.read()
    flow_version_crn = current.get("flow_version_crn")

    existing = find_deployment()

    if existing:
        deployment_crn = existing.get("crn")

        print(
            f"deployment {config.DEPLOYMENT_NAME!r} already exists:\n"
            f"  {deployment_crn}\n"
            f"  state: {(existing.get('status') or {}).get('state')}"
        )

        final_state, deployed = settle(deployment_crn)

        if not deployed:
            sys.exit(
                f"error: the deployment is in state {final_state} and has no "
                "deployed flow.\n"
                "  Check it in the CDF UI. Re-run with --wait-only to keep "
                "waiting, or run provision/teardown.py to remove it."
            )

        record(deployment_crn, deployed)
        print(f"  deployed flow CRN: {deployed}")
        print("\nnext: python provision/03_configure_kpi.py")

        return

    if args.wait_only:
        sys.exit(
            f"error: no deployment named {config.DEPLOYMENT_NAME!r} exists, "
            "so there is nothing to wait for.\n"
            "  Drop --wait-only to create it."
        )

    if not flow_version_crn:
        sys.exit(
            "error: no flow version CRN is recorded in the state file.\n"
            "  Run: python provision/01_import_flow.py"
        )

    # `--cluster-size` is an object, not a string: the shorthand is
    # `name=EXTRA_SMALL`. Passing the bare size name is silently wrong.
    arguments = [
        "create-deployment",
        "--service-crn",
        config.SERVICE_CRN,
        "--flow-version-crn",
        flow_version_crn,
        "--deployment-name",
        config.DEPLOYMENT_NAME,
        "--cluster-size",
        json.dumps({"name": config.CLUSTER_SIZE}),
        "--static-node-count",
        config.STATIC_NODE_COUNT,
        "--cfm-nifi-version",
        config.NIFI_VERSION,
        "--no-auto-scaling-enabled",
        # Without this the flow is deployed but stopped, nothing is
        # generated, and the queue never fills.
        "--auto-start-flow",
    ]

    print(f"deployment {config.DEPLOYMENT_NAME!r} does not exist.")
    print("\n  " + df_api.command_line(arguments, "cdp df"))

    if args.dry_run:
        print("\ndry run: nothing was created.")

        return

    print(
        f"\ncreating. This bills until provision/teardown.py runs and takes "
        "~15 minutes to settle."
    )

    response = df_api.require(
        df_api.df(*arguments),
        "the deployment request was rejected",
    )

    deployment_crn = response.get("deploymentCrn")

    if not deployment_crn:
        sys.exit(
            "error: the deployment request returned no CRN, so there is "
            "nothing to wait on or tear down.\n"
            f"  response: {json.dumps(response)[:500]}\n"
            "  Check the CDF UI before re-running — a deployment may exist "
            "regardless."
        )

    print(f"\ndeployment CRN: {deployment_crn}")

    # Recorded before the wait, not after. If the wait is interrupted the
    # deployment still exists and is still billing, and teardown.py reads
    # this file to find it.
    record(deployment_crn, "")

    final_state, deployed = settle(deployment_crn)

    if not deployed:
        sys.exit(
            f"error: the deployment reached {final_state} without exposing a "
            "deployed flow.\n"
            f"  The deployment exists and is billing: {deployment_crn}\n"
            "  Re-run with --wait-only to keep waiting, or run "
            "provision/teardown.py to remove it."
        )

    record(deployment_crn, deployed)

    print(f"  deployed flow CRN: {deployed}")
    print(f"\ndeployment is {final_state}.")
    print("next: python provision/03_configure_kpi.py")


if __name__ == "__main__":
    main()
