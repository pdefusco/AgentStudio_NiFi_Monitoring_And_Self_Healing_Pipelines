#!/usr/bin/env python3
"""
One-time setup inside the Cloudera AI project: install and prove cdpcli.

A standard Cloudera AI runtime does not ship the CDP CLI, so the monitor
job would die on `cdp: not found`. This installs it into
`/home/cdsw/.local`, which is on the project filesystem and therefore
survives across job runs and session restarts — so the install happens
once here, never on the monitor's one-minute schedule.

Run this as a one-off Cloudera AI job (or in a session terminal) before
creating the scheduled job, and again after any runtime upgrade.

It finishes by calling the monitor's own self-test, because an install
that succeeds and then cannot reach the workload plane is the failure
worth catching here rather than at the first real breach.

Usage:

    python job/setup_runtime.py
    python job/setup_runtime.py --skip-install   # just re-run the checks

Exits non-zero if the environment is not ready, so a failed setup job
looks failed.
"""

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


REQUIREMENTS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "requirements.txt")

USER_BIN = os.path.expanduser("~/.local/bin")

# Set by Cloudera AI inside a session or job. Used only to report where
# the install landed; nothing here requires it.
CAI_MARKERS = ("CDSW_PROJECT_ID", "CDSW_ENGINE_ID", "CML_PROJECT_ID")


def install() -> None:
    """Install the pinned requirements as user packages."""

    print(f"installing {REQUIREMENTS} into ~/.local ...")

    # --user, not a virtualenv: Cloudera AI jobs start a fresh container
    # each run and only the project filesystem persists, so ~/.local is
    # the install location that survives.
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--user",
            "--requirement",
            REQUIREMENTS,
        ],
        check=False,
    )

    if completed.returncode != 0:
        sys.exit(
            f"error: pip install failed with status {completed.returncode}.\n"
            "  If this is an air-gapped workspace, point pip at the internal "
            "index and re-run."
        )


def check_path() -> None:
    """Report whether the installed console scripts are reachable."""

    if not os.path.isdir(USER_BIN):
        sys.exit(
            f"error: {USER_BIN} does not exist after the install, so the "
            "`cdp` console script was not created.\n"
            "  Re-run without --skip-install."
        )

    if USER_BIN in os.environ.get("PATH", ""):
        print(f"PATH: {USER_BIN} is already present")

    else:
        # Not fatal. The monitor prepends this itself, precisely because
        # a job's PATH cannot be relied on.
        print(
            f"PATH: {USER_BIN} is absent. The monitor prepends it at "
            "startup, so this is informational."
        )

    os.environ["PATH"] = USER_BIN + os.pathsep + os.environ.get("PATH", "")


def check_cli() -> None:
    """Prove the CLI is importable and runnable."""

    try:
        import cdpcli

    except ImportError as exc:
        sys.exit(f"error: cdpcli is not importable after the install: {exc}")

    print(f"cdpcli: {getattr(cdpcli, '__version__', 'version unknown')}")

    # The same entry point df_api uses. `python -m cdpcli.clidriver` is
    # not a substitute: that module has no __main__ guard and exits 0
    # having printed nothing, which would make this check pass on a
    # broken install.
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from cdpcli.clidriver import main; sys.exit(main())",
            "--version",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    output = (completed.stdout + completed.stderr).strip()

    if completed.returncode != 0 or not output:
        sys.exit(
            "error: the CDP CLI entry point did not run.\n"
            f"  status {completed.returncode}, output {output!r}"
        )

    print(f"cdp --version: {output}")


def check_credentials() -> None:
    """Report how credentials will be resolved, without printing them."""

    key_id = os.environ.get("CDP_ACCESS_KEY_ID")
    private = os.environ.get("CDP_PRIVATE_KEY")
    credentials_file = os.path.expanduser("~/.cdp/credentials")

    if key_id and private:
        # Only the length is printed. The key itself never appears in
        # output, in the audit log, or on a command line.
        print(
            f"credentials: environment variables set "
            f"(key id {key_id[:4]}…, private key {len(private)} chars)"
        )

        return

    if os.path.exists(credentials_file):
        print(f"credentials: {credentials_file} exists")

        return

    sys.exit(
        "error: no CDP credentials found.\n"
        "  Set CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY as Cloudera AI project "
        "environment variables (Project Settings -> Advanced).\n"
        "  Keep the project private: project environment variables are "
        "readable by every collaborator."
    )


def check_state_dir() -> None:
    """Create the state directory and prove it is writable."""

    os.makedirs(config.STATE_DIR, exist_ok=True)

    probe = os.path.join(config.STATE_DIR, ".write-probe")

    try:
        with open(probe, "w", encoding="utf-8") as handle:
            handle.write("ok")

        os.unlink(probe)

    except OSError as exc:
        sys.exit(
            f"error: {config.STATE_DIR} is not writable: {exc}\n"
            "  The job keeps its state machine and audit log here and cannot "
            "run without it."
        )

    print(f"state dir: {config.STATE_DIR} (writable)")


def main() -> int:
    """Install the CLI and verify the environment."""

    parser = argparse.ArgumentParser(
        description=(
            "Install cdpcli into the Cloudera AI project and verify the "
            "monitor job's prerequisites."
        )
    )

    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip pip and only re-run the verification checks.",
    )

    args = parser.parse_args()

    where = next(
        (f"{m}={os.environ[m]}" for m in CAI_MARKERS if m in os.environ),
        "not a Cloudera AI session (running locally)",
    )

    print(f"environment: {where}")
    print(f"python: {sys.executable}\n")

    if not args.skip_install:
        install()

    print()
    check_path()
    check_cli()
    check_credentials()
    check_state_dir()

    # Imported only now: it asserts the CLI is present at import time, so
    # importing it before the install would fail with a less useful
    # message than the checks above.
    import df_api

    print("\nself-test (both API planes):")

    if not df_api.selftest():
        sys.exit(
            "error: the self-test failed. Detection uses the control plane "
            "and remediation uses the workload plane, so do not create the "
            "scheduled job until both pass.\n"
            "  A workload-plane failure here is usually one of: no egress "
            "from this workspace to the DataFlow endpoint, or a CDP user "
            "without the DataFlow role the action needs."
        )

    print("\nready. Next: python job/create_job.py --dry-run")

    return 0


if __name__ == "__main__":
    sys.exit(main())
