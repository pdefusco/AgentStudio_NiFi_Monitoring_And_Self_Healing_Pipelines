#!/usr/bin/env python3
"""
Configuration for the Cloudera DataFlow self-healing prototype.

Every tunable lives here, and every one can be overridden by an
environment variable of the same name, so the Cloudera AI job needs no
code edit to be retargeted or retuned.

Usage:

    from config import DEPLOYMENT_NAME, QUEUE_THRESHOLD

Nothing in this module reads a credential. `CDP_ACCESS_KEY_ID` and
`CDP_PRIVATE_KEY` are read by `df_api.py` straight from the environment
and passed to the CDP CLI subprocess; they are never stored here, never
placed on a command line, and never written to the audit log.
"""

import os


# ---------------------------------------------------------------------
# Helpers for environment overrides
# ---------------------------------------------------------------------

def _str(name: str, default: str) -> str:
    """Read a string setting, falling back to the module default."""

    return os.environ.get(name, default)


def _int(name: str, default: int) -> int:
    """Read an integer setting, failing loudly on a non-numeric value."""

    raw = os.environ.get(name)

    if raw is None:
        return default

    try:
        return int(raw)

    except ValueError:
        raise SystemExit(
            f"error: {name} must be an integer, got {raw!r}"
        )


# ---------------------------------------------------------------------
# Target: Cloudera DataFlow
# ---------------------------------------------------------------------

# The `se-sandbox-aws` DataFlow service and the environment it runs in.
# Every `dfworkload` command needs the environment CRN; `df_api.dfw()`
# injects it so no caller can forget.
SERVICE_CRN = _str(
    "SELFHEAL_SERVICE_CRN",
    "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233"
    ":service:41519649-07da-49d8-bbdc-0bc6777a7c82",
)

ENVIRONMENT_CRN = _str(
    "SELFHEAL_ENVIRONMENT_CRN",
    "crn:cdp:environments:us-west-1:558bc1d2-8867-4357-8524-311d51259233"
    ":environment:0c31a8ee-56ff-4b72-99bd-08ad97d193da",
)

# Confirmed available in this service by `dfworkload list-nifi-versions`.
# Every processor bundle in the generated flow definition is pinned to
# this exact version, because a bundle version the target runtime does
# not carry is rejected at import.
NIFI_VERSION = _str("SELFHEAL_NIFI_VERSION", "2.6.0.4.12.0.2-1")

CLUSTER_SIZE = _str("SELFHEAL_CLUSTER_SIZE", "EXTRA_SMALL")

STATIC_NODE_COUNT = _int("SELFHEAL_STATIC_NODE_COUNT", 1)


# ---------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------

# The CDF flow catalog and this CDP account are shared with many
# colleagues: 20 flow definitions by different authors, and this
# DataFlow service already hosts two deployments that are not ours.
# Every artifact this prototype creates carries the prefix so it is
# unmistakably owned and trivially greppable, and teardown refuses to
# touch anything without it.
PREFIX = _str("SELFHEAL_PREFIX", "pdf-selfheal-")

FLOW_NAME = f"{PREFIX}oscillator"

DEPLOYMENT_NAME = f"{PREFIX}oscillator"

# The connection carries an explicit name because CDF otherwise
# auto-generates one as `<source>_<relationship>_<destination>`, and a
# MetricChart carries no component id — only `componentName`. This
# string is therefore the single handle for finding the connection in
# the KPI scope metadata and for recognising its chart afterwards.
CONNECTION_NAME = f"{PREFIX}queue"

FLOW_DESCRIPTION = _str(
    "SELFHEAL_FLOW_DESCRIPTION",
    "Throwaway synthetic flow for the self-healing prototype. "
    "Oscillates a queue on purpose. Owner: pauldefusco. "
    "Safe to delete.",
)


# ---------------------------------------------------------------------
# The oscillation
# ---------------------------------------------------------------------

# GenerateFlowFile produces BURST_SIZE flowfiles in one shot every
# BURST_PERIOD_SECONDS; UpdateAttribute downstream does a single
# session.get() per trigger, so at a 1 second scheduling period it
# drains roughly one flowfile per second per concurrent task.
#
# DRAIN_CONCURRENCY is what makes the oscillation survive a remediation,
# and it was added after measuring the alternative. With one task the
# drain rate (1/s) exactly equalled the generation rate (600 per 600s),
# so the two cancelled: the queue returned to ~0 each cycle only because
# nothing perturbed it, and the ~500 flowfiles a restart added never
# cleared. The trough went from ~20 to ~344 and the flow stayed in breach
# permanently — one armed run destroyed the oscillation for good.
#
# At 2 the drain capacity is 1200 per cycle against 600 generated, so any
# backlog an action leaves behind is absorbed within about one cycle.
# 600 at ~2/s drains in ~5 minutes, which with a threshold of 100 keeps
# the queue above threshold for roughly 250 of every 600 seconds — still
# three or four samples at 75-second resolution, comfortably more than
# the two consecutive breaches the debounce needs, and now with a genuine
# healthy stretch rather than a brief dip.
BURST_SIZE = _int("SELFHEAL_BURST_SIZE", 600)

BURST_PERIOD_SECONDS = _int("SELFHEAL_BURST_PERIOD_SECONDS", 600)

DRAIN_PERIOD = _str("SELFHEAL_DRAIN_PERIOD", "1 sec")

DRAIN_CONCURRENCY = _int("SELFHEAL_DRAIN_CONCURRENCY", 2)

# Well above BURST_SIZE. If back pressure engages, NiFi stops the
# generator mid-burst and the peak is never visible.
BACK_PRESSURE_OBJECT_THRESHOLD = _int(
    "SELFHEAL_BACK_PRESSURE_OBJECT_THRESHOLD", 10000
)

BACK_PRESSURE_DATA_SIZE_THRESHOLD = _str(
    "SELFHEAL_BACK_PRESSURE_DATA_SIZE_THRESHOLD", "1 GB"
)


# ---------------------------------------------------------------------
# The KPI and the breach condition
# ---------------------------------------------------------------------

# Verified against a live deployment via
# `dfworkload get-flow-configuration-metadata-in-deployment`:
# kpiMetaData.kpiScopes offers exactly NIFI_CONNECTION, NIFI_PROCESSOR
# and NIFI_PROCESS_GROUP, and the connection metrics are
# connectionAmountQueued (COUNT, unit `count`, labelled "Flow Files
# Queued"), connectionBytesQueued (SIZE) and connectionPercentFull
# (RATIO).
#
# The count is the metric that matters here: the generated flowfiles are
# 0 bytes, which makes every byte-based metric flat at zero.
KPI_METRIC_ID = _str("SELFHEAL_KPI_METRIC_ID", "connectionAmountQueued")

KPI_COMPONENT_TYPE = _str("SELFHEAL_KPI_COMPONENT_TYPE", "NIFI_CONNECTION")

KPI_UNIT_ID = _str("SELFHEAL_KPI_UNIT_ID", "count")

# The threshold the CDF alert rule is configured with, and the same
# number the job compares against itself. The job derives its own
# boolean rather than trusting CDF's alert evaluator; the configured
# alert exists so that the metric chart exists at all, and its firing is
# read as corroboration only.
QUEUE_THRESHOLD = _int("SELFHEAL_QUEUE_THRESHOLD", 100)

KPI_FREQUENCY_TOLERANCE_VALUE = _int(
    "SELFHEAL_KPI_FREQUENCY_TOLERANCE_VALUE", 1
)

KPI_FREQUENCY_TOLERANCE_UNIT = _str(
    "SELFHEAL_KPI_FREQUENCY_TOLERANCE_UNIT", "MINUTES"
)

METRICS_TIME_PERIOD = _str(
    "SELFHEAL_METRICS_TIME_PERIOD", "LAST_THIRTY_MINUTES"
)

# Bounds the event reads. Without an explicit limit the CDP CLI
# auto-paginates and prefixes its JSON with a plain-text warning.
EVENT_COUNT = _int("SELFHEAL_EVENT_COUNT", 25)

# The sample interval is NOT a fixed publish cadence — it is the window
# divided into buckets. Measured 2026-10-01: every window returns exactly
# 25 points spanning it, so the gap is window/24. Hence the familiar 75
# seconds is a consequence of asking for 30 minutes, nothing more.
BUCKET_SECONDS_BY_PERIOD = {
    "LAST_THIRTY_MINUTES": 75,
    "LAST_ONE_HOUR": 150,
    "LAST_TWELVE_HOURS": 1800,
    "LAST_ONE_DAY": 3600,
}

# A data point older than this is treated as UNKNOWN rather than as a
# reading, because the window retains pre-action points and
# `currentValue` immediately after a restart is a value from before it.
MAX_SAMPLE_AGE_SECONDS = _int("SELFHEAL_MAX_SAMPLE_AGE_SECONDS", 300)

# Because both values above are environment-overridable, they can be set
# into a combination that silently disables detection: widen the window
# to LAST_ONE_DAY and the newest bucket is up to an hour old, so every
# sample fails the freshness check and the job reports UNKNOWN forever
# without ever erroring. Catch it at import rather than let a monitor run
# for hours looking healthy because it can see nothing.
_bucket = BUCKET_SECONDS_BY_PERIOD.get(METRICS_TIME_PERIOD)

if _bucket is None:
    raise SystemExit(
        f"error: SELFHEAL_METRICS_TIME_PERIOD={METRICS_TIME_PERIOD!r} is not "
        f"one of {sorted(BUCKET_SECONDS_BY_PERIOD)}."
    )

if _bucket > MAX_SAMPLE_AGE_SECONDS:
    raise SystemExit(
        f"error: {METRICS_TIME_PERIOD} returns one point per ~{_bucket}s, "
        f"which always exceeds the {MAX_SAMPLE_AGE_SECONDS}s freshness "
        "limit, so every sample would be judged stale and no breach could "
        "ever be detected.\n"
        f"  Use a narrower window, or raise "
        f"SELFHEAL_MAX_SAMPLE_AGE_SECONDS above {_bucket}."
    )


# ---------------------------------------------------------------------
# The state machine
# ---------------------------------------------------------------------

# Two consecutive fresh breaches before acting. A single stale or
# borderline sample should not trigger a restart.
BREACH_DEBOUNCE = _int("SELFHEAL_BREACH_DEBOUNCE", 2)

# Neither remediation drains a queue: the flowfile repository is on a
# persistent volume, and during a restart or a flow stop/start *both*
# the generator and the drain processor are stopped, so the backlog is
# frozen at breach level and the breach is still true when the action
# finishes. Only time clears it. The cooldown therefore has to outlast
# the action plus enough oscillation periods for the queue to drain, or
# the next run re-trips on a condition the last action could never have
# fixed.
COOLDOWN_SECONDS = _int("SELFHEAL_COOLDOWN_SECONDS", 1800)

# Absolute deadline for the deployment to return to a steady state after
# an action. On expiry the job goes to ESCALATED rather than pretending
# the action succeeded.
VERIFY_DEADLINE_SECONDS = _int("SELFHEAL_VERIFY_DEADLINE_SECONDS", 1200)

# The circuit breaker. On exhaustion the job moves to the terminal
# DISARMED state and a human has to clear it. This is what stops a
# one-minute schedule plus a self-perpetuating breach from becoming a
# restart storm.
MAX_ACTIONS_PER_WINDOW = _int("SELFHEAL_MAX_ACTIONS_PER_WINDOW", 3)

ACTION_WINDOW_SECONDS = _int("SELFHEAL_ACTION_WINDOW_SECONDS", 86400)

# "alternate" picks actions deterministically from the persisted action
# counter, which keeps the audit log reproducible. "random" is the
# random pick the prototype was originally described with.
ACTION_SELECTION = _str("SELFHEAL_ACTION_SELECTION", "alternate")


# ---------------------------------------------------------------------
# Guardrails
# ---------------------------------------------------------------------

# Every deployment that existed in this account before the prototype was
# built. A mutating call whose target appears here is refused outright,
# regardless of what else matches: a stale CRN in a state file must not
# be able to restart a colleague's deployment.
DENYLIST_DEPLOYMENT_CRNS = frozenset(
    {
        # se-sandbox-aws, not ours
        "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233"
        ":deployment:41519649-07da-49d8-bbdc-0bc6777a7c82"
        "/d6a50727-0f28-4cc4-abc9-895a50bf5a78",  # Ingest-Buddy-Notes-Kafka
        "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233"
        ":deployment:41519649-07da-49d8-bbdc-0bc6777a7c82"
        "/87c7b71e-fef2-48ea-bc79-ca2ea1a7204f",  # buddy-transcripts-2-iceberg
        # geo-hol3-cdp-env
        "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233"
        ":deployment:82b59980-f675-4aa6-80c7-8a78a33e8cd3"
        "/04424a12-666d-400c-bcd0-0ee9e84c9d9a",  # Receive Edge Data
        # marriott-poc-cdp-env, monitored read-only by the templates
        "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233"
        ":deployment:f4a3c788-daf5-45ba-aa7a-374b730beac5"
        "/df7e8fd0-7395-4f67-92fd-b8b6fa30a70f",  # ListenSyslog filter to S3
        "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233"
        ":deployment:f4a3c788-daf5-45ba-aa7a-374b730beac5"
        "/889ad171-3796-4862-b531-01a57bffc314",  # Marriott-rewards-flow
    }
)


# ---------------------------------------------------------------------
# Local paths
# ---------------------------------------------------------------------

# In Cloudera AI, /home/cdsw is the project filesystem and persists
# across job runs, so the default resolves to the repository checkout
# whether the code runs locally or in a job.
STATE_DIR = _str(
    "SELFHEAL_STATE_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "state"),
)

STATE_FILE = os.path.join(STATE_DIR, "state.json")

LOCK_FILE = os.path.join(STATE_DIR, "lock")

FLOW_DEFINITION_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "flow",
    "oscillating_queue.json",
)

CDP_TIMEOUT_SECONDS = _int("SELFHEAL_CDP_TIMEOUT_SECONDS", 120)
