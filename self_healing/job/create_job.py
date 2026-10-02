#!/usr/bin/env python3
"""
Register the monitor as a scheduled Cloudera AI job.

This is a convenience, not a dependency. Everything it does can be done
in the Cloudera AI UI in about a minute — Jobs -> New Job, script
`self_healing/job/monitor_and_remediate.py`, schedule "custom cron"
`* * * * *` — and the README documents that path as the fallback, because
it needs no SDK and no second credential.

Two things make the SDK route fiddly enough to warrant the fallback:

  * `cmlapi` is not on PyPI. Inside a Cloudera AI session it is
    preinstalled; outside, it must be fetched from the workspace itself
    (`<workspace>/api/v2/python.tar.gz`).
  * It authenticates with a **workspace API v2 key**, which is a
    different credential from the `CDP_ACCESS_KEY_ID` /
    `CDP_PRIVATE_KEY` pair the monitor uses. Having one does not imply
    having the other.

The job request model also varies by workspace version, so rather than
hardcoding fields that may not exist on this workspace, this script
**introspects `cmlapi.CreateJobRequest`** and reports what it accepts.
Hardcoding a field the workspace rejects produces an opaque 400; printing
the accepted set makes the mismatch obvious.

The job is created **unarmed** on purpose. Arming is a separate, explicit
edit — see the closing note this script prints.

Usage:

    python job/create_job.py --dry-run
    python job/create_job.py
    python job/create_job.py --arm           # create it already armed
    python job/create_job.py --schedule '*/5 * * * *'
    python job/create_job.py --show-fields   # introspect and exit

Credentials, both read from the environment and never printed:

    CML_API_URL   e.g. https://ml-cbdbb7f3-746.pdf0926.a465-9q4k.cloudera.site
    CML_API_KEY   a workspace API v2 key (User Settings -> API Keys)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


JOB_NAME = f"{config.PREFIX}monitor"

# Relative to the Cloudera AI project root, which is the repository root
# when the repo is cloned as the project.
SCRIPT_PATH = "self_healing/job/monitor_and_remediate.py"

# One minute, as chosen. The oscillation period is ten minutes and
# metrics publish every 75 seconds, so `*/5 * * * *` observes just as
# much for a fifth of the container starts; the README recommends
# switching once the loop is proven.
DEFAULT_SCHEDULE = "* * * * *"

# The smallest sensible allocation. The job shells out to the CDP CLI a
# handful of times and exits.
CPU = 1
MEMORY = 2

# Default workspace URL, from the workspace the user chose for this
# prototype. Overridden by CML_API_URL.
DEFAULT_API_URL = "https://ml-cbdbb7f3-746.pdf0926.a465-9q4k.cloudera.site"


def import_cmlapi():
    """Import cmlapi with an actionable message when it is missing."""

    try:
        import cmlapi

        return cmlapi

    except ImportError:
        url = os.environ.get("CML_API_URL", DEFAULT_API_URL).rstrip("/")

        sys.exit(
            "error: cmlapi is not installed, and it is not on PyPI.\n"
            f"  Inside a Cloudera AI session it is already present.\n"
            f"  Outside one, install it from the workspace:\n"
            f"    pip install {url}/api/v2/python.tar.gz\n"
            "  Or skip this script entirely and create the job in the UI: "
            "Jobs -> New Job, script\n"
            f"    {SCRIPT_PATH}\n"
            f"  schedule (custom cron) {DEFAULT_SCHEDULE!r}."
        )


def show_fields(cmlapi) -> None:
    """
    Print what this workspace's job model actually accepts.

    The field set differs across workspace versions. Reading it from the
    installed SDK is the difference between a clear mismatch and a 400
    with no detail.
    """

    for name in ("CreateJobRequest", "CreateJobRunRequest"):
        model = getattr(cmlapi, name, None)

        if model is None:
            print(f"{name}: not present in this cmlapi build")

            continue

        types = getattr(model, "openapi_types", None) or getattr(
            model, "swagger_types", {}
        )

        print(f"{name} accepts {len(types)} field(s):")

        for field in sorted(types):
            print(f"  {field}: {types[field]}")

        print()


def client_for(cmlapi):
    """Build an authenticated client, or exit saying what is missing."""

    url = os.environ.get("CML_API_URL", DEFAULT_API_URL).rstrip("/")
    key = os.environ.get("CML_API_KEY")

    if not key:
        sys.exit(
            "error: CML_API_KEY is not set. This is a workspace API v2 key "
            "(Cloudera AI -> User Settings -> API Keys), which is a "
            "different credential from CDP_ACCESS_KEY_ID/CDP_PRIVATE_KEY.\n"
            "  Export it, or create the job in the UI instead."
        )

    # The key is passed to the SDK and never printed. Only the host is
    # echoed, so the output is safe to paste into a ticket.
    print(f"workspace: {url}")

    try:
        return cmlapi.default_client(url=url, cml_api_key=key)

    except Exception as exc:
        sys.exit(f"error: could not build a cmlapi client: {exc}")


def resolve_project(client) -> str:
    """
    Find the project this job belongs to.

    Inside Cloudera AI the project id is in the environment. Outside, the
    projects are listed and matched by name, and an ambiguous match is
    refused rather than guessed — creating a job in the wrong project is
    not something to resolve by coin flip.
    """

    for marker in ("CDSW_PROJECT_ID", "CML_PROJECT_ID"):
        if os.environ.get(marker):
            print(f"project: {os.environ[marker]} (from {marker})")

            return os.environ[marker]

    wanted = os.environ.get("CML_PROJECT_NAME", "")

    try:
        projects = client.list_projects(page_size=200).projects or []

    except Exception as exc:
        sys.exit(f"error: could not list projects: {exc}")

    if wanted:
        matches = [p for p in projects if p.name == wanted]

    else:
        matches = [
            p
            for p in projects
            if "nifi" in (p.name or "").lower()
            or "selfheal" in (p.name or "").lower()
        ]

    if not matches:
        print("error: could not identify the project. Available:")

        for project in projects:
            print(f"  {project.name}  ({project.id})")

        sys.exit(
            "  Set CML_PROJECT_NAME to one of the names above and re-run."
        )

    if len(matches) > 1:
        print("error: more than one project matched:")

        for project in matches:
            print(f"  {project.name}  ({project.id})")

        sys.exit("  Set CML_PROJECT_NAME to disambiguate.")

    print(f"project: {matches[0].name} ({matches[0].id})")

    return matches[0].id


def existing_job(client, project_id: str):
    """Return the job of this name if it already exists, else None."""

    try:
        jobs = client.list_jobs(project_id, page_size=200).jobs or []

    except Exception as exc:
        sys.exit(f"error: could not list jobs in {project_id}: {exc}")

    return next((j for j in jobs if j.name == JOB_NAME), None)


def build_request(cmlapi, project_id: str, schedule: str, armed: bool):
    """
    Assemble a CreateJobRequest from the fields this SDK advertises.

    Every optional field is set only if the model declares it, so the same
    script works against workspace versions with different job models.
    """

    types = getattr(cmlapi.CreateJobRequest, "openapi_types", None) or getattr(
        cmlapi.CreateJobRequest, "swagger_types", {}
    )

    arguments = "--arm" if armed else ""

    wanted = {
        "project_id": project_id,
        "name": JOB_NAME,
        "script": SCRIPT_PATH,
        "kernel": "python3",
        "cpu": CPU,
        "memory": MEMORY,
        "schedule": schedule,
        "arguments": arguments,
        # One pass takes seconds. A run still alive after ten minutes has
        # hung, and letting it hang would hold the lock against every
        # subsequent run.
        "timeout": 600,
        "kill_on_timeout": True,
    }

    fields = {k: v for k, v in wanted.items() if k in types and v != ""}
    dropped = sorted(set(wanted) - set(fields))

    if dropped:
        print(
            f"note: this workspace's job model does not accept {dropped}; "
            "those settings are skipped."
        )

        if armed and "arguments" in dropped:
            sys.exit(
                "error: --arm was requested but this job model has no "
                "`arguments` field, so the job would run unarmed and "
                "silently do nothing.\n"
                "  Create the job in the UI, where arguments can be set "
                "explicitly."
            )

    if "runtime_identifier" in types:
        # Left to the workspace default rather than pinned: a runtime
        # identifier that does not exist on this workspace is rejected,
        # and the default is always valid.
        print("note: runtime_identifier left to the workspace default.")

    return cmlapi.CreateJobRequest(**fields), fields


def main() -> int:
    """Create or report the scheduled job."""

    parser = argparse.ArgumentParser(
        description=(
            "Register self_healing/job/monitor_and_remediate.py as a "
            "scheduled Cloudera AI job."
        )
    )

    parser.add_argument(
        "--schedule",
        default=DEFAULT_SCHEDULE,
        help=(
            f"Cron schedule for the job (default {DEFAULT_SCHEDULE!r}). "
            "'*/5 * * * *' matches the oscillation period far better and is "
            "the recommended setting once the loop is proven."
        ),
    )

    parser.add_argument(
        "--arm",
        action="store_true",
        help=(
            "Create the job already armed, so it may remediate. Omitted by "
            "default: an unarmed job observes and logs the action it would "
            "take, which is what should be watched first."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be created, without creating it.",
    )

    parser.add_argument(
        "--show-fields",
        action="store_true",
        help=(
            "Print the job-request fields this workspace's SDK accepts, then "
            "exit. Useful when a create call is rejected."
        ),
    )

    args = parser.parse_args()

    cmlapi = import_cmlapi()

    if args.show_fields:
        show_fields(cmlapi)

        return 0

    client = client_for(cmlapi)
    project_id = resolve_project(client)

    found = existing_job(client, project_id)

    if found:
        # Idempotent, and deliberately not an update: silently rewriting
        # a schedule or an argument list on a job that may be armed is
        # not something a convenience script should do.
        print(
            f"\njob {JOB_NAME!r} already exists ({found.id}).\n"
            "  Nothing was changed. Edit its schedule or arguments in the UI, "
            "or delete it and re-run."
        )

        return 0

    request, fields = build_request(cmlapi, project_id, args.schedule, args.arm)

    print(f"\nwould create job {JOB_NAME!r}:")

    for key in sorted(fields):
        print(f"  {key}: {fields[key]!r}")

    if args.dry_run:
        print("\ndry run; nothing was created.")

        return 0

    try:
        created = client.create_job(request, project_id)

    except Exception as exc:
        sys.exit(
            f"error: create_job failed: {exc}\n"
            "  Run with --show-fields to see what this workspace accepts, or "
            "create the job in the UI."
        )

    print(f"\ncreated: {created.id}")

    if args.arm:
        print(
            "\nThe job is ARMED. It may restart the deployment named "
            f"{config.DEPLOYMENT_NAME!r}, at most "
            f"{config.MAX_ACTIONS_PER_WINDOW} time(s) per "
            f"{config.ACTION_WINDOW_SECONDS}s, after which it disarms itself "
            "and waits for a human."
        )

    else:
        print(
            "\nThe job is UNARMED: it observes, decides, and logs the action "
            "it would take, but changes nothing.\n"
            "  Watch state.json and the audit log across a full ten-minute "
            "oscillation first. To arm it, add `--arm` to the job's arguments "
            "in the UI."
        )

    print(f"  Audit log: {config.STATE_DIR}/audit-YYYYMMDD.jsonl")

    return 0


if __name__ == "__main__":
    sys.exit(main())
