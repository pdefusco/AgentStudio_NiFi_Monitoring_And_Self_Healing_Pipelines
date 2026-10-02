#!/usr/bin/env python3
"""
Import the synthetic flow definition into the Cloudera DataFlow catalog.

This is the first mutating step of the prototype and the cheapest real
test of the generated JSON: a definition CDF rejects fails here, in a few
seconds, instead of fifteen minutes into a deployment.

It is also a write into a catalog shared with about twenty colleagues'
flows, which is why the name carries the configured prefix and the
description says who owns it and that it is disposable.

Idempotent by design. If a flow of this name already exists it adds a new
*version* to it rather than creating a second catalog entry, so re-running
after a tweak to the builder is the normal workflow.

Usage:

    python provision/01_import_flow.py
    python provision/01_import_flow.py --dry-run

`--file` is a path the CDP CLI reads and uploads as an octet-stream, with
the name and description sent as URI-encoded headers (see
`cdpcli/extensions/df/__init__.py`), so the file must exist locally.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import df_api
import state


# Bounds the CLI's auto-pagination. The catalog holds ~20 flows and the
# CLI's default page size is 20, so an unbounded list would paginate and
# emit a plain-text warning ahead of its JSON.
MAX_ITEMS = 500


def find_flow() -> dict:
    """
    Return the catalog flow with our configured name, or {} if absent.

    Matched on an exact name rather than on the server-side search, which
    is a substring filter: the search narrows the page, the equality test
    decides.
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

    for flow in result.get("flows") or []:
        if flow.get("name") == config.FLOW_NAME:
            return flow

    return {}


def latest_version(flow_crn: str) -> dict:
    """Return the highest-numbered version of a catalog flow."""

    result = df_api.require(
        df_api.df("list-flow-definition-versions", "--flow-crn", flow_crn),
        f"could not list versions of {flow_crn}",
    )

    # The response key is `flowVersions`, not `versions`.
    versions = result.get("flowVersions") or []

    if not versions:
        sys.exit(
            f"error: flow {flow_crn} exists but has no versions.\n"
            "  Delete it in the CDF catalog and re-run, or import a version "
            "manually."
        )

    return max(versions, key=lambda v: v.get("version") or 0)


def main() -> None:
    """Import the flow definition, or add a version to the existing flow."""

    parser = argparse.ArgumentParser(
        description=(
            "Import the synthetic oscillating flow into the Cloudera "
            "DataFlow catalog."
        )
    )

    parser.add_argument(
        "--file",
        default=config.FLOW_DEFINITION_FILE,
        help=(
            "Flow definition JSON to upload (default: "
            f"{config.FLOW_DEFINITION_FILE}). Generate it with "
            "flow/build_flow_definition.py."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report what would be imported and exit without writing to the "
            "shared catalog."
        ),
    )

    args = parser.parse_args()

    if not os.path.exists(args.file):
        sys.exit(
            f"error: {args.file} does not exist.\n"
            "  Run: python flow/build_flow_definition.py"
        )

    try:
        with open(args.file, "r", encoding="utf-8") as handle:
            definition = json.load(handle)

    except ValueError as exc:
        sys.exit(f"error: {args.file} is not valid JSON: {exc}")

    contents = definition.get("flowContents") or {}

    print(f"flow definition: {args.file}")
    print(
        f"  {len(contents.get('processors') or [])} processors, "
        f"{len(contents.get('connections') or [])} connections, "
        f"root group {contents.get('name')!r}"
    )

    existing = find_flow()

    if args.dry_run:
        if existing:
            print(
                f"\ndry run: would add a version to the existing flow "
                f"{config.FLOW_NAME!r}\n  {existing.get('crn')}"
            )

        else:
            print(
                f"\ndry run: would create a new catalog flow "
                f"{config.FLOW_NAME!r}"
            )

        return

    if existing:
        flow_crn = existing.get("crn")

        print(
            f"\nflow {config.FLOW_NAME!r} already exists "
            f"({existing.get('versionCount')} version(s)); adding a version."
        )

        df_api.require(
            df_api.df(
                "import-flow-definition-version",
                "--flow-crn",
                flow_crn,
                "--file",
                args.file,
                "--comments",
                "Re-imported by provision/01_import_flow.py",
            ),
            "the flow version import was rejected",
        )

    else:
        print(f"\ncreating catalog flow {config.FLOW_NAME!r}.")

        response = df_api.require(
            df_api.df(
                "import-flow-definition",
                "--file",
                args.file,
                "--name",
                config.FLOW_NAME,
                "--description",
                config.FLOW_DESCRIPTION,
            ),
            "the flow import was rejected",
        )

        flow_crn = response.get("crn")

        if not flow_crn:
            sys.exit(
                "error: the import returned no flow CRN.\n"
                f"  response: {json.dumps(response)[:500]}"
            )

    # Read the version back rather than trusting the import response, so
    # the recorded CRN is one CDF has actually indexed.
    version = latest_version(flow_crn)
    version_crn = version.get("crn")

    print("\nimported.")
    print(f"  flow CRN:         {flow_crn}")
    print(f"  flow version CRN: {version_crn}")
    print(f"  version:          {version.get('version')}")

    current = state.read()
    current["flow_crn"] = flow_crn
    current["flow_version_crn"] = version_crn
    state.write(current)

    print(f"\nrecorded in {config.STATE_FILE}")
    print("next: python provision/02_create_deployment.py")


if __name__ == "__main__":
    main()
