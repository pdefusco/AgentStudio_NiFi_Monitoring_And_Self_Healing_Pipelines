"""
Cloudera DataFlow deployment metrics inspection tool.

This Agent Studio tool reads flow-level metrics for a Cloudera DataFlow
Public Cloud deployment through the Cloudera DataFlow API, using only a
CDP API key pair.

Where the companion `pollNifiFlow` tool asks "is this deployment up",
this tool asks "what are the numbers inside it" — per-processor and
per-connection metric charts, the deployment's configured KPIs and their
alert thresholds, active alerts, and recent event history.


WHY NOT THE NIFI REST API

The companion `pollNifiCanvas` tool reads the same kind of question from
NiFi itself, and in CDF Public Cloud that route does not work. NiFi signs
the bearer tokens its REST API accepts with its own Ed25519 keys, so it
rejects every credential the CDP control plane can mint:

    401 invalid_token
    Signed JWT rejected: Another algorithm expected, or no matching key(s) found

EdDSA expected, RS256 given. A DF workload token is correctly scoped and
correctly audienced and still refused, because the signature is not
NiFi's own. Cloudera documents only interactive browser SSO for reaching
a deployment's NiFi UI, and DataFlow publishes no token-vending endpoint.

The Cloudera DataFlow API is the supported way to the same numbers, and
it needs no second credential. `list-deployment-system-metrics` in
particular returns metric charts scoped by `componentType` and
`componentName` — `Processor`, `Process Group`, `Connection` — which is
genuine flow-level detail rather than deployment-level aggregates.


COMMANDS USED (all read-only)

    df describe-deployment             identity, status, NiFi version
    df list-deployment-system-metrics  per-component metric charts
    df list-deployment-kpis            configured KPI charts + thresholds
    df list-deployment-active-alerts   alerts currently firing
    df list-deployment-events          recent event history

Each is an independent section. Unlike `pollNifiCanvas` there is no
lookup-then-authenticate chain here — every command takes the deployment
CRN directly — so one failing command degrades its own section and
leaves the rest of the report intact.

The only credentials needed are a CDP API key pair, the same pair the
companion tools use:

    CDP_ACCESS_KEY_ID
    CDP_PRIVATE_KEY

The identity also needs a DataFlow role on the environment holding the
deployment: `DFFlowUser` is enough for everything here.

All commands are read-only. This tool cannot start, stop, resize, or
modify anything.
"""

from pydantic import BaseModel, Field
from typing import Any, Optional
import argparse
import json
import os
import subprocess
import sys


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

# Each command is a control plane round trip.
CDP_TIMEOUT_SECONDS = 60

# Truncation limit for CLI stderr and non-JSON output. A control plane
# rejection can be verbose, and the agent does not need all of it to
# understand what went wrong.
MAX_RAW_OUTPUT_CHARS = 2000

# The periods the DataFlow API accepts for a metrics query. Read from
# the CDP CLI's own service model (`cdpcli/data/df/df.yaml`), where it
# is the documented value set for `metricsTimePeriod`.
METRICS_TIME_PERIODS = (
    "LAST_THIRTY_MINUTES",
    "LAST_ONE_HOUR",
    "LAST_TWELVE_HOURS",
    "LAST_ONE_DAY",
)

# Upper bound on events requested. `list-deployment-events` is a
# paginated operation that the CDP CLI follows to the end by default, so
# an unbounded request pulls a deployment's entire history into the
# agent's context.
MAX_EVENT_COUNT = 200


# ---------------------------------------------------------------------
# User / Project Parameters
# ---------------------------------------------------------------------

class UserParameters(BaseModel):
    """
    Credentials supplied through Agent Studio tool configuration.

    These values are injected by Agent Studio and must never be
    supplied by the LLM.
    """

    CDP_ACCESS_KEY_ID: str
    CDP_PRIVATE_KEY: str


# ---------------------------------------------------------------------
# Tool Parameters
# ---------------------------------------------------------------------

class ToolParameters(BaseModel):
    """
    Arguments supplied by the agent when invoking this tool.
    """

    deployment_crn: str = Field(
        description=(
            "CRN of the Cloudera DataFlow deployment whose metrics "
            "should be read."
        ),
    )

    metrics_time_period: str = Field(
        default="LAST_ONE_HOUR",
        description=(
            "Window the metrics cover. One of LAST_THIRTY_MINUTES, "
            "LAST_ONE_HOUR, LAST_TWELVE_HOURS or LAST_ONE_DAY. A "
            "shorter window shows what is happening now; a longer one "
            "shows whether the current value is unusual."
        ),
    )

    component_name_filter: Optional[str] = Field(
        default=None,
        description=(
            "Optional case-insensitive substring matched against each "
            "chart's component name, to narrow the report to one "
            "processor, process group or connection. Leave unset to see "
            "every component the deployment reports."
        ),
    )

    include_kpis: bool = Field(
        default=True,
        description=(
            "Whether to also retrieve the deployment's configured KPIs, "
            "which carry the alert thresholds someone chose for this "
            "flow and are therefore the clearest signal of whether a "
            "value is out of bounds."
        ),
    )

    include_events: bool = Field(
        default=True,
        description=(
            "Whether to also retrieve recent deployment event history, "
            "which gives temporal context for a current problem."
        ),
    )

    event_count: int = Field(
        default=25,
        description=(
            "Maximum number of recent events to retrieve, when "
            "include_events is true. Capped at 200."
        ),
    )

    include_data_points: bool = Field(
        default=False,
        description=(
            "Whether to return every raw time-series data point behind "
            "each metric chart. False, the default, returns each "
            "series' current and average values plus a compact summary "
            "of its points, which is enough to assess health. Set true "
            "only when the shape of a series over time actually matters "
            "— it can be thousands of points per chart."
        ),
    )


# ---------------------------------------------------------------------
# CDP control plane helpers
# ---------------------------------------------------------------------

def run_cdp(config: UserParameters, args: list[str]) -> Any:
    """
    Invoke the CDP CLI and return its parsed JSON output.

    On any failure, returns a dict containing an `error` key rather
    than raising, so the caller can report the problem instead of the
    whole tool call collapsing.

    Credentials are passed through the environment so they never
    appear in a command line or process listing.
    """

    # Invoked through `-c` rather than `-m cdpcli.clidriver`, because
    # cdpcli.clidriver has no `if __name__ == "__main__"` guard: running
    # it as a module is a silent no-op that exits 0 having printed
    # nothing, which looks exactly like a command that returned no data.
    # `main()` is the entry point the installed `cdp` script itself uses.
    command = [
        sys.executable,
        "-c",
        "import sys; from cdpcli.clidriver import main; sys.exit(main())",
    ] + args + ["--output", "json"]

    environment = dict(os.environ)
    environment["CDP_ACCESS_KEY_ID"] = config.CDP_ACCESS_KEY_ID
    environment["CDP_PRIVATE_KEY"] = config.CDP_PRIVATE_KEY

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=CDP_TIMEOUT_SECONDS,
            env=environment,
        )

    except subprocess.TimeoutExpired:

        return {
            "error": (
                f"The CDP CLI did not complete within "
                f"{CDP_TIMEOUT_SECONDS} seconds."
            ),
            "command": " ".join(args),
        }

    except Exception as exc:

        return {
            "error": "The CDP CLI could not be executed.",
            "command": " ".join(args),
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }

    if completed.returncode != 0:

        stderr = (completed.stderr or "").strip()

        # A missing cdpcli is an environment problem, not a credential
        # problem, and saying so saves a pointless key investigation.
        if "No module named 'cdpcli'" in stderr:
            hint = (
                "cdpcli is not installed for the interpreter running this "
                "tool. Agent Studio installs it from the tool's "
                "requirements.txt when the tool runs as a venv tool; when "
                "running standalone, install it into the same interpreter "
                "being used."
            )

        # cdpcli accepts CDP_PRIVATE_KEY as either a path to a key file
        # or the key itself, and reports a malformed key as a missing
        # file, which sends people looking for the wrong problem.
        elif "Private key file" in stderr and "does not exist" in stderr:
            hint = (
                "CDP_PRIVATE_KEY is read as a file path first and as a "
                "literal private key only if no such file exists, so this "
                "message also appears when the key itself is malformed or "
                "truncated. Supply the complete private key, newlines "
                "included."
            )

        else:
            hint = (
                "Confirm CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY belong to "
                "an active CDP API key, and that the user or machine user "
                "has a DataFlow role on this environment. An "
                "AUTHENTICATION_FAILURE naming the access key usually "
                "means the key was deleted or rotated."
            )

        return {
            "error": "The CDP CLI returned an error.",
            "command": " ".join(args),
            "return_code": completed.returncode,
            "stderr": stderr[:MAX_RAW_OUTPUT_CHARS],
            "hint": hint,
        }

    stdout = (completed.stdout or "").strip()

    if not stdout:

        # stderr is included because the CLI can exit 0 while writing a
        # diagnostic there, and that text is the whole diagnosis.
        return {
            "error": "The CDP CLI produced no output.",
            "command": " ".join(args),
            "stderr": (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS],
        }

    try:
        return json.loads(stdout)

    except ValueError:

        return {
            "error": "The CDP CLI returned output that was not JSON.",
            "command": " ".join(args),
            "raw_output": stdout[:MAX_RAW_OUTPUT_CHARS],
            "stderr": (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS],
        }


def describe_deployment(config: UserParameters, deployment_crn: str) -> Any:
    """Look up a deployment through the Cloudera DataFlow control plane."""

    return run_cdp(
        config,
        [
            "df",
            "describe-deployment",
            "--deployment-crn",
            deployment_crn,
        ],
    )


def list_system_metrics(
    config: UserParameters,
    deployment_crn: str,
    metrics_time_period: str,
) -> Any:
    """
    Retrieve the deployment's system metric charts.

    These are the control plane's own flow-level measurements, scoped by
    `componentType` and `componentName`, so they report individual
    processors, process groups and connections rather than only
    deployment-wide totals.
    """

    return run_cdp(
        config,
        [
            "df",
            "list-deployment-system-metrics",
            "--deployment-crn",
            deployment_crn,
            "--metrics-time-period",
            metrics_time_period,
        ],
    )


def list_kpis(
    config: UserParameters,
    deployment_crn: str,
    metrics_time_period: str,
) -> Any:
    """
    Retrieve the deployment's configured KPI charts.

    A KPI is a metric someone deliberately chose to watch on this flow,
    optionally with alert thresholds, which makes these charts a better
    guide to what "unhealthy" means here than the raw system metrics are.
    """

    return run_cdp(
        config,
        [
            "df",
            "list-deployment-kpis",
            "--deployment-crn",
            deployment_crn,
            "--metrics-time-period",
            metrics_time_period,
        ],
    )


def list_active_alerts(config: UserParameters, deployment_crn: str) -> Any:
    """
    Retrieve the alerts currently firing on the deployment.

    Sorted newest first, because when several are active the most recent
    is usually the one that explains the current state.
    """

    return run_cdp(
        config,
        [
            "df",
            "list-deployment-active-alerts",
            "--deployment-crn",
            deployment_crn,
            "--sort",
            "firstOccurrence:desc",
        ],
    )


def list_events(
    config: UserParameters,
    deployment_crn: str,
    event_count: int,
) -> Any:
    """
    Retrieve recent deployment event history.

    `--max-items` is mandatory rather than optional: this operation is
    paginated and the CDP CLI follows pagination to the end by default,
    so leaving it off retrieves the deployment's entire event history.
    """

    return run_cdp(
        config,
        [
            "df",
            "list-deployment-events",
            "--deployment-crn",
            deployment_crn,
            "--max-items",
            str(event_count),
        ],
    )


# ---------------------------------------------------------------------
# Metric chart handling
# ---------------------------------------------------------------------
#
# Every other response in this repo's tools is returned to the agent
# verbatim, so that a question nobody anticipated can still be answered
# from the observation. Metric charts are the one exception, and this
# section is where that exception lives.
#
# A chart's `metrics` and `mirroredMetrics` each carry a `datas` array of
# {timestamp, value} points. Over LAST_ONE_DAY, across a chart per
# processor, that is thousands of points — which would crowd the numbers
# that matter out of the agent's context without telling it anything the
# current value, the average, and the range do not.
#
# So the points are replaced by a summary and every other field is kept,
# thresholds included. `include_data_points` returns them verbatim
# instead, so nothing is permanently unavailable.
# ---------------------------------------------------------------------

def is_number(value: Any) -> bool:
    """
    Whether a value is a JSON number.

    `bool` is a subclass of `int` in Python, so it has to be excluded
    explicitly or a `true` would be compared against a threshold as 1.
    """

    return isinstance(value, (int, float)) and not isinstance(value, bool)


def evaluate_threshold(current_value: Any, alert: Any) -> Optional[dict]:
    """
    Compare a series' current value against its configured thresholds.

    This is a *derived* judgement, not something the API reported, and
    is labelled as such in the output. It is computed here rather than
    left to the model because it is an arithmetic comparison with one
    right answer, and because a configured threshold exists precisely so
    that someone is told when it is crossed.

    Returns None when there is nothing to compare — no numeric current
    value, or no threshold configured. That is deliberately distinct from
    a configured threshold that is simply not breached.
    """

    if not isinstance(alert, dict) or not is_number(current_value):
        return None

    more_than = alert.get("thresholdMoreThan")
    less_than = alert.get("thresholdLessThan")

    if not is_number(more_than) and not is_number(less_than):
        return None

    reasons = []

    if is_number(more_than) and current_value > more_than:
        reasons.append(
            f"current value {current_value} is above the configured "
            f"thresholdMoreThan {more_than}"
        )

    if is_number(less_than) and current_value < less_than:
        reasons.append(
            f"current value {current_value} is below the configured "
            f"thresholdLessThan {less_than}"
        )

    if not reasons:
        return {"breached": False}

    return {"breached": True, "reasons": reasons}


def summarize_series(
    series: Any,
    alert: Any,
    include_data_points: bool,
) -> Any:
    """
    Compact one metric series, keeping every field except the points.
    """

    if not isinstance(series, dict):
        return series

    summarized = {
        key: value for key, value in series.items() if key != "datas"
    }

    points = series.get("datas")
    points = points if isinstance(points, list) else []

    values = [
        point.get("value")
        for point in points
        if isinstance(point, dict) and is_number(point.get("value"))
    ]

    timestamps = [
        point.get("timestamp")
        for point in points
        if isinstance(point, dict) and is_number(point.get("timestamp"))
    ]

    if include_data_points:
        summarized["datas"] = points

    else:
        summarized["datas_summary"] = {
            "point_count": len(points),
            "first_timestamp": timestamps[0] if timestamps else None,
            "last_timestamp": timestamps[-1] if timestamps else None,
            "min_value": min(values) if values else None,
            "max_value": max(values) if values else None,
            "last_value": values[-1] if values else None,
        }

    verdict = evaluate_threshold(series.get("currentValue"), alert)

    if verdict is not None:
        summarized["threshold_breached_derived"] = verdict

    return summarized


def summarize_chart(chart: Any, include_data_points: bool) -> Any:
    """
    Compact one metric chart, preserving its original key order.

    The chart's `alert` is applied only to the primary `metrics` series.
    A `mirroredMetrics` series measures something related but different —
    it exists only on certain system metrics — so the threshold was not
    set on it, and emitting a verdict there would claim a judgement
    nobody configured.
    """

    if not isinstance(chart, dict):
        return chart

    alert = chart.get("alert")

    summarized = {}

    for key, value in chart.items():

        if key == "metrics":
            summarized[key] = summarize_series(
                value,
                alert,
                include_data_points,
            )

        elif key == "mirroredMetrics":
            summarized[key] = summarize_series(
                value,
                None,
                include_data_points,
            )

        else:
            summarized[key] = value

    return summarized


def prepare_chart_response(
    response: Any,
    component_name_filter: Optional[str],
    include_data_points: bool,
) -> Any:
    """
    Compact and optionally filter the charts in a metrics response.

    Error responses and responses with no chart list are passed through
    untouched, so a failure still reaches the agent in the shape every
    other failure in this tool has.

    When a component name filter is set, charts with no component name
    at all — the deployment-wide aggregates — are excluded too: the
    filter was asked for in order to look at one component.
    """

    if not isinstance(response, dict) or "error" in response:
        return response

    charts = response.get("metricCharts")

    if not isinstance(charts, list):
        return response

    needle = (component_name_filter or "").strip().lower()

    kept = []

    for chart in charts:

        if needle:
            name = chart.get("componentName") if isinstance(chart, dict) else None

            if not isinstance(name, str) or needle not in name.lower():
                continue

        kept.append(summarize_chart(chart, include_data_points))

    prepared = dict(response)
    prepared["metricCharts"] = kept

    # Reported so that a filter which matched nothing is distinguishable
    # from a deployment that reported no charts at all.
    counts = {
        "charts_total": len(charts),
        "charts_returned": len(kept),
        "component_name_filter": component_name_filter or None,
        "data_points_included": include_data_points,
    }

    # A filter that matched nothing has two very different causes, and the
    # agent cannot tell them apart from a zero. Either this deployment
    # reports no component-scoped charts at all — the common case, since a
    # chart exists per component only where someone configured a KPI for
    # that component — or it does and the name simply did not match. Say
    # which, so the agent reports the right finding instead of concluding
    # that a processor it can see in NiFi is missing.
    if needle and not kept:

        named = [
            chart.get("componentName")
            for chart in charts
            if isinstance(chart, dict)
            and isinstance(chart.get("componentName"), str)
        ]

        if named:
            counts["filter_hint"] = (
                "No chart's component name contained this filter. Component "
                "names present in this response: " + ", ".join(sorted(set(named)))
            )

        else:
            counts["filter_hint"] = (
                "This deployment reported no component-scoped charts at all — "
                "every chart is deployment-wide, so no component name could "
                "match. Component-scoped charts appear only where a KPI has "
                "been configured against a specific processor, process group "
                "or connection. Retry without component_name_filter to see "
                "the deployment-wide metrics."
            )

    prepared["chart_counts"] = counts

    return prepared


# ---------------------------------------------------------------------
# Tool entry point
# ---------------------------------------------------------------------

def run_tool(config: UserParameters, args: ToolParameters) -> Any:
    """
    Read a Cloudera DataFlow deployment's flow metrics.

    All operations are read-only.

    Returns a dict whose sections hold the actual DataFlow API
    responses. Sections are fetched independently, so any one of them
    that failed carries its own `error` key while the rest still report,
    and `request_status` says plainly whether the data is complete.
    """

    result: dict = {
        "deployment_crn": args.deployment_crn,
    }

    # -----------------------------------------------------------------
    # Validate the time period before spending a round trip on it.
    #
    # Returned as a structured error rather than enforced by a pydantic
    # Literal, because a Literal raises a ValidationError the agent
    # never sees the text of. This way the agent is told what it sent,
    # what is accepted, and can retry.
    # -----------------------------------------------------------------

    period = (args.metrics_time_period or "").strip().upper()

    if period not in METRICS_TIME_PERIODS:

        result["parameters"] = {
            "error": "metrics_time_period is not one of the accepted values.",
            "supplied_value": args.metrics_time_period,
            "allowed_values": list(METRICS_TIME_PERIODS),
        }
        result["request_status"] = {
            "all_requests_succeeded": False,
            "failed_sections": ["parameters"],
        }

        return result

    # Clamped rather than rejected: an over-large request still has an
    # obvious correct interpretation, and the effective value is
    # reported below so the agent can see what it actually got.
    event_count = max(1, min(args.event_count, MAX_EVENT_COUNT))

    # What was asked for, so the agent can state the window its numbers
    # cover instead of implying they are instantaneous.
    result["query"] = {
        "metrics_time_period": period,
        "component_name_filter": args.component_name_filter or None,
        "event_count": event_count if args.include_events else None,
        "include_data_points": args.include_data_points,
    }

    # -----------------------------------------------------------------
    # Five independent sections.
    #
    # Every command takes the deployment CRN directly, so unlike the
    # NiFi API tool there is nothing to resolve first and no section is
    # a prerequisite for another. A failure anywhere costs one section.
    # -----------------------------------------------------------------

    # Identity and control-plane status, so the report can say which
    # deployment answered and what DataFlow thinks of it overall.
    result["deployment"] = describe_deployment(config, args.deployment_crn)

    # The flow-level numbers: a chart per processor, process group and
    # connection the deployment reports.
    result["system_metrics"] = prepare_chart_response(
        list_system_metrics(config, args.deployment_crn, period),
        args.component_name_filter,
        args.include_data_points,
    )

    # The metrics someone chose to watch on this flow, with whatever
    # alert thresholds they set.
    if args.include_kpis:

        result["kpis"] = prepare_chart_response(
            list_kpis(config, args.deployment_crn, period),
            args.component_name_filter,
            args.include_data_points,
        )

    # Alerts currently firing.
    result["active_alerts"] = list_active_alerts(config, args.deployment_crn)

    # Recent history, for temporal context on a current problem.
    if args.include_events:

        result["events"] = list_events(
            config,
            args.deployment_crn,
            event_count,
        )

    # -----------------------------------------------------------------
    # Summarize which sections failed.
    #
    # Every section is still returned in full above. This flag exists
    # so the agent can state plainly whether the data it is reporting
    # is complete, instead of having to infer that from the payload.
    # -----------------------------------------------------------------

    failed_sections = [
        name
        for name, value in result.items()
        if isinstance(value, dict) and "error" in value
    ]

    result["request_status"] = {
        "all_requests_succeeded": not failed_sections,
        "failed_sections": failed_sections,
    }

    return result


# ---------------------------------------------------------------------
# Local / CAI CLI execution
# ---------------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Read a Cloudera DataFlow deployment's flow metrics through "
            "the Cloudera DataFlow API."
        )
    )

    parser.add_argument(
        "--user-params",
        required=True,
        help=(
            "JSON object containing CDP_ACCESS_KEY_ID and "
            "CDP_PRIVATE_KEY."
        ),
    )

    parser.add_argument(
        "--tool-params",
        required=True,
        help=(
            "JSON object containing deployment_crn, and optionally "
            "metrics_time_period, component_name_filter, include_kpis, "
            "include_events, event_count and include_data_points."
        ),
    )

    cli_args = parser.parse_args()

    user_params_dict = json.loads(cli_args.user_params)
    tool_params_dict = json.loads(cli_args.tool_params)

    user_params = UserParameters(**user_params_dict)
    tool_params = ToolParameters(**tool_params_dict)

    output = run_tool(
        config=user_params,
        args=tool_params,
    )

    print(json.dumps(output, indent=2))
