#!/usr/bin/env python3
"""
Remove everything this prototype created, and stop the billing.

Order matters and is not interchangeable: the deployment is terminated
first and the catalog flow deleted second, because a flow that a
deployment still references cannot be deleted.

Everything is identified by the CRNs recorded at provisioning time, not
by searching for names, because a name search is how a teardown script
ends up deleting a colleague's similarly-named flow. Each target is then
checked again live — prefix, DataFlow service, denylist — so a stale
state file cannot aim this at the wrong thing either.

The one concession to practicality is `--deployment-crn`. If the state
file was lost while a deployment was still billing, insisting on the
state file would mean no way to stop the charge from here at all. The
override keeps every other check and says loudly that one was skipped.

Usage:

    python provision/teardown.py --dry-run
    python provision/teardown.py
    python provision/teardown.py --keep-flow
    python provision/teardown.py --deployment-crn <crn>

Afterwards, confirm nothing is left:

    cdp df list-deployments --output json
    cdp df list-flow-definitions --search-term pdf-selfheal- --output json
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


TERMINATE_DEADLINE_SECONDS = 1800

TERMINATE_POLL_SECONDS = 20

MAX_ITEMS = 500

# States that mean the deployment is already gone or going.
GONE_STATES = frozenset({"TERMINATED", "TERMINATING"})


def deployments_named() -> list:
    """Return every deployment carrying our configured name."""

    result = df_api.require(
        df_api.df("list-deployments", "--max-items", MAX_ITEMS),
        "could not list deployments",
    )

    return [
        d
        for d in result.get("deployments") or []
        if d.get("name") == config.DEPLOYMENT_NAME
    ]


def wait_for_gone(deployment_crn: str) -> str:
    """
    Poll until the deployment is terminated or no longer exists.

    Two end conditions, because they are both normal: the state reads
    TERMINATED, or the deployment stops being readable at all once
    Cloudera DataFlow has finished removing it. A read failure is only
    accepted as "gone" when the deployment has also dropped out of
    `list-deployments` — otherwise a transient API error would be
    reported as a successful teardown, which is the one wrong answer
    that costs money.
    """

    started = time.monotonic()
    last = None

    while True:
        current = df_api.deployment_state(deployment_crn)
        elapsed = int(time.monotonic() - started)

        if current != last:
            print(f"  [{elapsed:>5}s] {current}", flush=True)

            last = current

        if current == "TERMINATED":
            return current

        if current == "UNKNOWN":
            remaining = [
                d for d in deployments_named() if d.get("crn") == deployment_crn
            ]

            if not remaining:
                print(f"  [{elapsed:>5}s] no longer listed", flush=True)

                return "TERMINATED"

        if elapsed > TERMINATE_DEADLINE_SECONDS:
            return current

        time.sleep(TERMINATE_POLL_SECONDS)


def terminate(deployment_crn: str, expected_crn: str, dry_run: bool) -> bool:
    """Terminate the deployment and wait for it to disappear."""

    matches = deployments_named()

    if len(matches) > 1:
        sys.exit(
            f"error: {len(matches)} deployments are named "
            f"{config.DEPLOYMENT_NAME!r}:\n"
            + "\n".join(f"    {d.get('crn')}" for d in matches)
            + "\n  Refusing to guess which one is ours. Remove the extras in "
            "the CDF UI first."
        )

    current = df_api.deployment_state(deployment_crn)

    print(f"deployment: {deployment_crn}")
    print(f"  state: {current}")

    if current in GONE_STATES:
        print("  already terminated or terminating; nothing to request.")

        if current == "TERMINATING" and not dry_run:
            return wait_for_gone(deployment_crn) == "TERMINATED"

        return True

    outcome = df_api.act(
        deployment_crn,
        expected_crn,
        ["terminate-deployment", "--deployment-crn", deployment_crn],
        dry_run=dry_run,
    )

    if outcome.get("refused"):
        sys.exit(
            "error: refusing to terminate this deployment.\n"
            f"  {outcome.get('reason')}"
        )

    if dry_run:
        print("\n  would run: " + df_api.command_line(outcome.get("argv") or [], ""))

        return True

    if not outcome.get("performed"):
        df_api.require(
            outcome.get("result") or {"error": "the terminate request failed"},
            "the terminate request was rejected",
        )

    print("\n  terminating. This is what stops the billing.")

    final = wait_for_gone(deployment_crn)

    if final != "TERMINATED":
        print(
            f"\n  warning: still {final} after {TERMINATE_DEADLINE_SECONDS}s.\n"
            "  The termination was accepted, so it is most likely still in "
            "progress — check the CDF UI. The catalog flow is left alone "
            "because a referenced flow cannot be deleted."
        )

        return False

    print("  terminated.")

    return True


def delete_flow(flow_crn: str, dry_run: bool) -> bool:
    """
    Delete the catalog flow, after confirming it is the one we imported.

    `df delete-flow` is a control-plane call, so it does not pass through
    `df_api.act` and needs its own check. The flow is looked up by the
    recorded CRN within a prefix-filtered listing: the CRN says which
    flow, and the name confirms it is one of ours.
    """

    result = df_api.require(
        df_api.df(
            "list-flow-definitions",
            "--search-term",
            config.PREFIX,
            "--max-items",
            MAX_ITEMS,
        ),
        "could not list the flow catalog",
    )

    match = next(
        (f for f in result.get("flows") or [] if f.get("crn") == flow_crn),
        None,
    )

    if match is None:
        print(
            f"\nflow {flow_crn} is not in the catalog under the "
            f"{config.PREFIX!r} prefix; nothing to delete."
        )

        return True

    name = match.get("name") or ""

    # Belt and braces: the listing was already prefix-filtered, but that
    # filter is a server-side substring search and this is a delete.
    if not name.startswith(config.PREFIX):
        sys.exit(
            f"error: flow {flow_crn} is named {name!r}, which does not carry "
            f"the {config.PREFIX!r} prefix. Refusing to delete it."
        )

    print(f"\nflow: {name!r}")
    print(f"  {flow_crn}")
    print(f"  versions: {match.get('versionCount')}")

    if dry_run:
        print(f"\n  would run: df delete-flow --flow-crn {flow_crn}")

        return True

    response = df_api.df("delete-flow", "--flow-crn", flow_crn)

    if "error" in response:
        # The most likely cause by far is a deployment still referencing
        # this flow, which is worth naming rather than leaving the user
        # with a bare API error.
        print("\n  warning: the flow could not be deleted.")
        print(f"    {response.get('error')}")
        print(f"    {response.get('stderr', '')}")
        print(
            "    A flow still referenced by any deployment cannot be deleted. "
            "Check `cdp df list-deployments` for others using it."
        )

        return False

    print("  deleted.")

    return True


def main() -> None:
    """Terminate the deployment, delete the flow, clear the state file."""

    parser = argparse.ArgumentParser(
        description=(
            "Remove the deployment and catalog flow this prototype created, "
            "and stop the billing."
        )
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be removed and exit without removing it.",
    )

    parser.add_argument(
        "--keep-flow",
        action="store_true",
        help=(
            "Terminate the deployment but leave the catalog flow, so it can "
            "be redeployed without re-importing. The flow itself costs "
            "nothing."
        ),
    )

    parser.add_argument(
        "--deployment-crn",
        default="",
        help=(
            "Terminate this deployment CRN instead of the one in the state "
            "file. For recovering from a lost state file while a deployment "
            "is still billing; the prefix, service and denylist checks still "
            "apply."
        ),
    )

    args = parser.parse_args()

    current = state.read()
    deployment_crn = current.get("deployment_crn") or ""
    flow_crn = current.get("flow_crn") or ""

    if args.deployment_crn:
        print(
            "warning: --deployment-crn was given, so the check that the "
            "target matches the state file is skipped.\n"
            "  The prefix, DataFlow service and denylist checks still apply.\n"
        )

        deployment_crn = args.deployment_crn

    # `expected_crn` is the target itself under the override, which is
    # what makes the state-file comparison a no-op there while leaving
    # the other three checks in force.
    expected_crn = deployment_crn

    if not deployment_crn and not flow_crn:
        print(
            f"nothing recorded in {config.STATE_FILE}; nothing to tear down."
        )

        stray = deployments_named()

        if stray:
            print(
                f"\nwarning: {len(stray)} deployment(s) named "
                f"{config.DEPLOYMENT_NAME!r} exist and are billing:\n"
                + "\n".join(f"    {d.get('crn')}" for d in stray)
                + "\n  Re-run with --deployment-crn <crn> to terminate one."
            )

        return

    terminated = True

    if deployment_crn:
        terminated = terminate(deployment_crn, expected_crn, args.dry_run)

    else:
        print(f"no deployment recorded in {config.STATE_FILE}.")

    deleted = True

    if flow_crn and not args.keep_flow:
        if not terminated and not args.dry_run:
            print(
                "\nskipping the flow deletion: the deployment is not gone "
                "yet, and a referenced flow cannot be deleted. Re-run this "
                "script once it has terminated."
            )

            deleted = False

        else:
            deleted = delete_flow(flow_crn, args.dry_run)

    elif args.keep_flow:
        print(f"\nkeeping catalog flow {flow_crn} (--keep-flow).")

    if args.dry_run:
        print("\ndry run: nothing was removed.")

        return

    # Clearing the state file is part of teardown, not housekeeping. The
    # guardrail authorises actions against the CRN recorded here, so a
    # state file naming a terminated deployment would leave the job
    # trying to restart something that no longer exists on every tick.
    if terminated and deleted:
        cleared = state.default_state()

        if args.keep_flow:
            cleared["flow_crn"] = flow_crn
            cleared["flow_version_crn"] = current.get("flow_version_crn") or ""

        state.write(cleared)

        print(f"\ncleared {config.STATE_FILE}")
        print("\nconfirm nothing is left:")
        print("  cdp df list-deployments --output json")
        print(
            f"  cdp df list-flow-definitions --search-term {config.PREFIX} "
            "--output json"
        )

    else:
        print(
            f"\nleaving {config.STATE_FILE} in place: teardown is incomplete "
            "and the recorded CRNs are how to finish it."
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
