#!/usr/bin/env python3
"""
Persistent state and the single-run lock for the self-healing prototype.

The monitoring job is scheduled every minute but the actions it takes
run for several minutes, so two things have to be true that are not true
of a stateless script:

  - only one run may be in flight at a time. `single_run()` takes a
    non-blocking flock and the loser exits quietly, because a Cloudera
    AI job on a one-minute cron against a runtime that needs 30-60
    seconds to start a container will overlap, and two runs that both
    read IDLE would both issue a restart.

  - what the previous run decided has to survive into the next one.
    `write()` replaces the file atomically, so a run that is killed
    mid-write leaves the previous state intact rather than a truncated
    file that fails to parse.

Usage:

    import state

    with state.single_run() as acquired:
        if not acquired:
            return
        s = state.read()
        ...
        state.write(s)

In Cloudera AI the state directory lives under the project filesystem,
which persists across job runs. It is gitignored: a job writing into the
working tree every minute would otherwise produce a permanently dirty
tree.
"""

import contextlib
import errno
import fcntl
import json
import os
import tempfile
import time

import config


# ---------------------------------------------------------------------
# State machine values
# ---------------------------------------------------------------------

IDLE = "IDLE"
BREACH_PENDING = "BREACH_PENDING"
ACTING = "ACTING"
VERIFYING = "VERIFYING"
COOLING = "COOLING"
ESCALATED = "ESCALATED"
DISARMED = "DISARMED"

# States from which no action may be initiated. ESCALATED and DISARMED
# both need a human; the rest mean something is already under way.
TERMINAL_STATES = frozenset({ESCALATED, DISARMED})


def default_state() -> dict:
    """Return the state a first run starts from."""

    return {
        "state": IDLE,
        "since": time.time(),
        # Set immediately before an action is requested, so a run that
        # overlaps an action in progress can discard every metric sample
        # taken at or before it. Without that, the 30-minute metrics
        # window keeps serving pre-action breach points and the job
        # re-trips on data it has already acted on.
        "last_action_started_at": None,
        "last_action": None,
        "action_count": 0,
        # Timestamps of actions actually performed, pruned to the
        # circuit breaker's rolling window.
        "action_timestamps": [],
        "consecutive_breaches": 0,
        # Recorded by provisioning. The guardrail compares every
        # mutation target against deployment_crn, so an empty value here
        # means nothing can be acted on at all.
        "deployment_crn": "",
        "deployed_flow_crn": "",
        "flow_crn": "",
        "flow_version_crn": "",
        # The (componentType, componentName, name) tuple observed on the
        # real metric chart after the KPI landed. Written by
        # provision/03_configure_kpi.py as a record of what provisioning
        # actually published, and deliberately *not* read by the loop:
        # df_api.is_our_connection_chart() recognises the chart
        # structurally, by component, which survives someone renaming the
        # KPI in the CDF UI. Keep it for diagnosis, not for matching.
        "chart_key": None,
    }


# ---------------------------------------------------------------------
# Reading and writing
# ---------------------------------------------------------------------

def _ensure_dir() -> None:
    """Create the state directory if it does not exist."""

    os.makedirs(config.STATE_DIR, exist_ok=True)


def read() -> dict:
    """
    Load the state file, merged over the defaults.

    Merging means a state file written by an older version of this code
    gains new keys with their default values instead of raising a
    KeyError somewhere further in.
    """

    _ensure_dir()

    merged = default_state()

    if not os.path.exists(config.STATE_FILE):
        return merged

    try:
        with open(config.STATE_FILE, "r", encoding="utf-8") as handle:
            stored = json.load(handle)

    except (ValueError, OSError) as exc:
        raise SystemExit(
            f"error: {config.STATE_FILE} could not be read: {exc}\n"
            "  Inspect it before deleting it — it records which deployment "
            "this prototype is allowed to act on."
        )

    if not isinstance(stored, dict):
        raise SystemExit(
            f"error: {config.STATE_FILE} does not contain a JSON object."
        )

    merged.update(stored)

    return merged


def write(new_state: dict) -> None:
    """
    Replace the state file atomically.

    Written to a temporary file in the same directory and moved into
    place, because `os.replace` is atomic within a filesystem: a reader
    sees either the old file or the new one, never a half-written one.
    """

    _ensure_dir()

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=config.STATE_DIR,
        prefix=".state-",
        suffix=".tmp",
        delete=False,
    )

    try:
        with handle:
            json.dump(new_state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(handle.name, config.STATE_FILE)

    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(handle.name)

        raise


def transition(current: dict, to_state: str, **fields) -> dict:
    """
    Move to a new state, stamping `since`, and persist immediately.

    Persisting inside the transition is deliberate for ACTING: the state
    must be on disk *before* the action is requested, so a run that
    starts while the action is in flight sees ACTING rather than IDLE.
    """

    current["state"] = to_state
    current["since"] = time.time()
    current.update(fields)

    write(current)

    return current


# ---------------------------------------------------------------------
# The circuit breaker
# ---------------------------------------------------------------------

def prune_action_window(current: dict, now: float = None) -> list:
    """Drop recorded actions that have fallen out of the rolling window."""

    now = time.time() if now is None else now

    kept = [
        timestamp
        for timestamp in (current.get("action_timestamps") or [])
        if now - timestamp < config.ACTION_WINDOW_SECONDS
    ]

    current["action_timestamps"] = kept

    return kept


def breaker_tripped(current: dict, now: float = None) -> bool:
    """
    True when the rolling window already holds the maximum actions.

    The breaker exists because no remediation here can clear the
    condition that triggered it, so without a hard cap the only thing
    limiting the number of restarts is how long the demo is left
    running.
    """

    return len(prune_action_window(current, now)) >= config.MAX_ACTIONS_PER_WINDOW


def record_action(current: dict, name: str, now: float = None) -> dict:
    """Record that an action was actually performed."""

    now = time.time() if now is None else now

    prune_action_window(current, now)

    current["action_timestamps"].append(now)
    current["action_count"] = int(current.get("action_count") or 0) + 1
    current["last_action"] = name
    current["last_action_started_at"] = now

    return current


# ---------------------------------------------------------------------
# The lock
# ---------------------------------------------------------------------

@contextlib.contextmanager
def single_run():
    """
    Hold an exclusive non-blocking lock for the duration of one run.

    Yields True if the lock was taken and False if another run holds it.
    The caller is expected to exit quietly on False: an overlapping run
    is normal on a one-minute schedule and is not an error.

    The lock is advisory and process-scoped. It is released when the
    file descriptor closes, including if the process is killed, so a
    crashed run cannot wedge the schedule permanently.
    """

    _ensure_dir()

    descriptor = os.open(config.LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)

    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise

            yield False

            return

        try:
            os.truncate(descriptor, 0)
            os.write(descriptor, f"{os.getpid()} {time.time()}\n".encode())

            yield True

        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)

    finally:
        os.close(descriptor)
