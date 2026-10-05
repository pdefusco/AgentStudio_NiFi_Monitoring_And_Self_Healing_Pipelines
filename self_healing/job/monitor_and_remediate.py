#!/usr/bin/env python3
"""
One pass of the self-healing loop: observe, decide, act, audit.

This is the Cloudera AI job entrypoint. It runs once per invocation and
exits; the schedule, not a loop inside the script, is what makes it
periodic. Nothing here involves Agent Studio — it is a plain script
talking to the DataFlow API.

**Acting requires `--arm`.** A bare run observes, decides, logs what it
*would* do, and changes nothing. That default is deliberate: the schedule
is one minute, the actions take several, and the most likely mistake is
running this armed before the state machine has been watched.

Three facts shape the design, all verified rather than assumed:

  1. **Neither remediation drains the queue.** The flowfile repository is
     on a persistent volume, and a restart or a flow stop/start stops the
     drain processor along with the generator. The backlog is frozen at
     breach level and the breach is *still true* when the action
     finishes. So the loop never re-arms on "condition cleared" — it
     re-arms on a wall-clock cooldown — and a circuit breaker caps how
     many actions a window may contain. Without that, a one-minute
     schedule plus a self-perpetuating breach is a restart storm.

  2. **The two KPI read surfaces return disjoint sets.** A
     component-scoped KPI appears only in `list-flow-kpis-in-deployment`,
     never in `list-deployment-kpis`. Reading one surface would report
     "no breach" forever; `df_api.metric_charts` merges both.

  3. **One sample per ~75 seconds** — which is the requested window split
     into 25 buckets, not a publish cadence, so it widens if the window
     does (`config.py` refuses a window whose buckets outlast the
     freshness limit). The schedule is 60, so consecutive runs routinely
     see the identical latest sample. Staleness is therefore checked
     explicitly and an old sample is `UNKNOWN`, not a reading.

`UNKNOWN` is a third verdict, not a synonym for healthy. A missing chart,
a stale sample, or an unreadable deployment all produce it, and it never
triggers an action and never clears a breach counter toward safety.

Usage:

    python job/monitor_and_remediate.py                 # observe only
    python job/monitor_and_remediate.py --arm            # may act
    python job/monitor_and_remediate.py --selftest
    python job/monitor_and_remediate.py --status
    python job/monitor_and_remediate.py --arm --action restart_deployment
    python job/monitor_and_remediate.py --reset          # clear DISARMED

Exit status is 0 for a completed pass, including one that decided not to
act, so a scheduler does not treat a healthy flow as a failed job.
"""

import argparse
import datetime
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import df_api
import state
import trigger


# A millisecond epoch for any date this code could plausibly run in is
# above this; a seconds epoch is far below it. The metric timestamp is
# documented only as `int64`, and misreading its unit by a factor of 1000
# would make every sample look either ancient or impossibly fresh.
MIN_PLAUSIBLE_MS = 1_000_000_000_000

BREACH = "BREACH"
HEALTHY = "HEALTHY"
UNKNOWN = "UNKNOWN"

# Cloudera AI prepends this when `setup_runtime.py` has installed cdpcli
# as a user package.
USER_BIN = os.path.expanduser("~/.local/bin")


# ---------------------------------------------------------------------
# Observation
# ---------------------------------------------------------------------

def latest_sample(chart: dict, ignore_before: float = None) -> dict:
    """
    Extract the newest usable data point from a metric chart.

    `metrics.currentValue` is deliberately not used as the reading. After
    an action it can still carry a pre-action value, and it has no
    timestamp, so there is no way to tell. The series is read explicitly
    so that age can be checked and so that points predating the last
    action can be discarded.

    Returns a dict with a `status` of ok / no_chart / no_data /
    bad_timestamp / all_pre_action / stale.
    """

    if not chart:
        return {"status": "no_chart"}

    points = (chart.get("metrics") or {}).get("datas") or []

    if not points:
        return {"status": "no_data"}

    newest = points[-1]
    raw = newest.get("timestamp")

    if not isinstance(raw, (int, float)) or raw < MIN_PLAUSIBLE_MS:
        # Refusing to guess the unit is the point: a wrong guess here
        # silently breaks every freshness check downstream.
        return {"status": "bad_timestamp", "timestamp": raw}

    timestamp = raw / 1000.0

    # The metrics window is 30 minutes and retains samples taken before
    # the action. Acting on one of those means acting twice on the same
    # evidence.
    if ignore_before:
        fresh = [
            p
            for p in points
            if isinstance(p.get("timestamp"), (int, float))
            and p["timestamp"] / 1000.0 > ignore_before
        ]

        if not fresh:
            return {
                "status": "all_pre_action",
                "newest_timestamp": timestamp,
                "ignore_before": ignore_before,
                "points_discarded": len(points),
            }

        newest = fresh[-1]
        timestamp = newest["timestamp"] / 1000.0

    age = time.time() - timestamp

    result = {
        "value": newest.get("value"),
        "timestamp": timestamp,
        "observed_at": datetime.datetime.fromtimestamp(
            timestamp, datetime.timezone.utc
        ).isoformat(),
        "age_seconds": round(age, 1),
        "points_in_window": len(points),
    }

    if age > config.MAX_SAMPLE_AGE_SECONDS:
        result["status"] = "stale"

        return result

    result["status"] = "ok"

    return result


def queue_slope(chart: dict, ignore_before: float = None) -> dict:
    """
    How fast the queue drains, in flowfiles per second, negative.

    This is the number that decides whether a repair worked, so how it is
    computed matters more than usual.

    **It takes a chart object, not a CRN, and that signature is the
    enforcement.** A KPI chart divides the requested window into exactly
    25 buckets anchored to *request time*, so two responses sit on two
    different grids and differencing across them is wrong — measured at
    ~20% error in this repo's own notes. Accepting an already-fetched
    chart means there is no code path by which this function could issue a
    second call and make that mistake. It reads the same `metrics.datas`
    list and applies the same `ignore_before` cut as `latest_sample()`, so
    the rate and the depth always describe one response and one
    pre-action filter.

    **Why this is a conditional statistic and not a fit over the last N
    points.** Measured live, one 600s cycle of the oscillator is eight
    75s buckets and its per-bucket rates are:

        -2.00  -1.97  -1.36  0.00  0.00  0.00  +7.33

    The drain is three buckets wide, the floor is three, and the burst is
    one. Any fixed window of more than about two points therefore
    straddles phases, and a line fitted across a V reports a rate near
    zero for a queue that is draining perfectly well. A six-point fit on
    this series measured +0.853 while the queue was refilling — directionally
    right about that instant and useless as an acceptance criterion.

    So select the buckets that describe draining and ignore the rest:

    - **falling** — `value` decreased across the pair. Rising and flat
      buckets are a different regime, not noise to be averaged in.
    - **still had work at the end** — the later value is above zero. The
      `-1.36` bucket above is the queue reaching empty partway through, so
      the drain idles for the remainder and the bucket understates the
      rate by whatever fraction it sat idle. Including it biases every
      measurement toward zero, and biases it *more* as the drain gets
      faster, which is precisely the wrong direction for judging a repair
      that makes the drain faster.

    The median of what survives, because one straddling bucket should not
    move the answer. On the baseline series this selects exactly
    `[-2.00, -1.97]` and returns **-1.985**, reproducing the hand-derived
    -1.99 rather than approximating it.

    `pairs` is returned alongside, and a caller judging a repair should
    read it: at concurrency 4 a cycle yields only about one qualifying
    bucket, so a verdict may be resting on a single measurement.

    Returns a `status` of ok / no_chart / no_data / no_drain_observed.
    """

    if not chart:
        return {"status": "no_chart"}

    points = [
        p
        for p in ((chart.get("metrics") or {}).get("datas") or [])
        if isinstance(p.get("timestamp"), (int, float))
        and p["timestamp"] >= MIN_PLAUSIBLE_MS
        and isinstance(p.get("value"), (int, float))
    ]

    available = len(points)

    if ignore_before:
        points = [p for p in points if p["timestamp"] / 1000.0 > ignore_before]

    if len(points) < 2:
        # Two causes, and they want opposite responses from a reader, so
        # they get different names. A chart that genuinely carries no
        # data is a problem; a chart whose buckets were all discarded as
        # pre-action is this filter working exactly as intended.
        #
        # Measured 2026-10-05: for roughly two buckets after an action
        # the rate is legitimately unknowable, because every bucket that
        # straddles the change has been thrown away on purpose. Reporting
        # that as `no_data` invites someone to go looking for a broken
        # KPI. The wait is bounded and worth stating, since the question
        # being asked is "when will I have a number".
        if ignore_before and available >= 2:
            buckets = config.BUCKET_SECONDS_BY_PERIOD[config.METRICS_TIME_PERIOD]

            return {
                "status": "awaiting_post_action_buckets",
                "points": len(points),
                "discarded": available - len(points),
                "measurable_in_seconds": max(
                    0, int(ignore_before + 2 * buckets - time.time())
                ),
            }

        return {"status": "no_data", "points": len(points)}

    rates = []

    for earlier, later in zip(points, points[1:]):
        seconds = (later["timestamp"] - earlier["timestamp"]) / 1000.0

        if seconds <= 0:
            continue

        drained = float(earlier["value"]) - float(later["value"])

        if drained <= 0 or float(later["value"]) <= 0:
            continue

        rates.append(-drained / seconds)

    if not rates:
        return {
            "status": "no_drain_observed",
            "points": len(points),
            "pairs": 0,
        }

    rates.sort()
    middle = len(rates) // 2

    median = (
        rates[middle]
        if len(rates) % 2
        else (rates[middle - 1] + rates[middle]) / 2.0
    )

    return {
        "status": "ok",
        "per_second": round(median, 3),
        "pairs": len(rates),
        "points": len(points),
        "steepest": round(rates[0], 3),
        "shallowest": round(rates[-1], 3),
        "window_seconds": round(
            (points[-1]["timestamp"] - points[0]["timestamp"]) / 1000.0, 1
        ),
    }


def observe(current: dict) -> dict:
    """
    Read the metric charts and derive the breach verdict.

    The verdict is this job's own comparison, not CDF's alert evaluator.
    The configured KPI exists so that the metric series exists at all; a
    firing CDF alert is read as corroboration and never as the trigger,
    because the evaluator's timing and hysteresis are not under this
    prototype's control.
    """

    deployment_crn = current.get("deployment_crn") or ""
    flow_crn = current.get("deployed_flow_crn") or ""

    merged = df_api.metric_charts(deployment_crn, flow_crn)

    # Logged every run, so a zero-chart condition is visible in the audit
    # trail rather than looking like a quiet healthy flow.
    inventory = [
        {
            "componentType": c.get("componentType"),
            "componentName": c.get("componentName"),
            "name": c.get("name"),
            "source": c.get("_source"),
        }
        for c in merged["charts"]
    ]

    chart = next(
        (c for c in merged["charts"] if df_api.is_our_connection_chart(c)),
        {},
    )

    # The later of the two action clocks. `last_action_started_at` is when
    # the action was *requested*; `incident_applied_at` is when the flow
    # change was first observed, which is later and is the point after
    # which the series describes the repaired flow. Taking the max keeps
    # the filter conservative, so neither a sample nor a slope can be
    # computed from a bucket that straddles the change. Both are read with
    # `or 0` because a fresh state file has neither.
    ignore_before = max(
        current.get("last_action_started_at") or 0,
        current.get("incident_applied_at") or 0,
    )

    sample = latest_sample(chart, ignore_before)

    observation = {
        "chart_sources": merged["sources"],
        "chart_inventory": inventory,
        "chart_found": bool(chart),
        "chart_name": chart.get("name"),
        "sample": sample,
        # Computed on every run, and recorded above the verdict branch on
        # purpose: a trend is only reviewable after the fact if it was
        # logged all along, and the runs where the sample is unusable are
        # exactly the ones where the trend explains why.
        "slope": queue_slope(chart, ignore_before),
        "threshold": config.QUEUE_THRESHOLD,
    }

    if sample["status"] != "ok":
        observation["verdict"] = UNKNOWN
        observation["verdict_reason"] = {
            "no_chart": (
                f"no chart for {config.CONNECTION_NAME!r} in either KPI "
                "surface; the KPI may not be configured"
            ),
            "no_data": "the chart exists but has published no data points yet",
            "bad_timestamp": "the newest data point has an implausible timestamp",
            "all_pre_action": (
                "every data point predates the last action, so there is no "
                "post-action evidence yet"
            ),
            "stale": (
                f"the newest sample is {sample.get('age_seconds')}s old, "
                f"older than the {config.MAX_SAMPLE_AGE_SECONDS}s limit"
            ),
        }.get(sample["status"], sample["status"])

        return observation

    value = sample["value"]
    breached = value is not None and value > config.QUEUE_THRESHOLD

    observation["verdict"] = BREACH if breached else HEALTHY
    observation["verdict_reason"] = (
        f"queue depth {value} {'>' if breached else '<='} "
        f"threshold {config.QUEUE_THRESHOLD}"
    )

    return observation


def gather_context(current: dict) -> dict:
    """
    Read the corroborating signals for the audit log.

    Every read degrades independently: a failing section records its own
    error rather than collapsing the run, because none of this is load
    bearing for the decision. It exists so that a human reading the audit
    log later can reconcile it against the CDF event history.
    """

    deployment_crn = current.get("deployment_crn") or ""
    flow_crn = current.get("deployed_flow_crn") or ""
    context = {}

    reads = {
        "deployment_active_alerts": [
            "list-deployment-active-alerts",
            "--deployment-crn",
            deployment_crn,
        ],
        "system_metrics": [
            "list-deployment-system-metrics",
            "--deployment-crn",
            deployment_crn,
            "--metrics-time-period",
            config.METRICS_TIME_PERIOD,
        ],
        "deployment_events": [
            "list-deployment-events",
            "--deployment-crn",
            deployment_crn,
            "--max-items",
            config.EVENT_COUNT,
        ],
    }

    if flow_crn:
        reads["flow_active_alerts"] = [
            "list-flow-active-alerts-in-deployment",
            "--deployment-crn",
            deployment_crn,
            "--deployed-flow-crn",
            flow_crn,
        ]

    for name, arguments in reads.items():
        result = df_api.df(*arguments)

        if "error" in result:
            context[name] = {"error": result.get("error")}

            continue

        # Both the alert and the event responses carry `eventSummaries`;
        # the system-metrics response carries `metricCharts`.
        summaries = result.get("eventSummaries")
        charts = result.get("metricCharts")

        if summaries is not None:
            context[name] = {
                "count": len(summaries),
                "recent": [
                    {
                        "eventType": s.get("eventType"),
                        "message": s.get("message"),
                        "timestamp": s.get("timestamp"),
                    }
                    for s in summaries[:5]
                ],
            }

        elif charts is not None:
            # Only the current value is kept. The full series for three
            # system charts is several hundred points per run, and this is
            # background colour rather than evidence.
            context[name] = {
                c.get("name"): (c.get("metrics") or {}).get("currentValueLabel")
                for c in charts
            }

        else:
            # Recorded rather than dropped: an unexpected response shape
            # is worth seeing in the log.
            context[name] = {"unrecognised_keys": sorted(result.keys())}

    return context


# ---------------------------------------------------------------------
# Decision
# ---------------------------------------------------------------------

def decide_acting_agent(current: dict, elapsed: float, run: dict) -> dict:
    """
    ACTING, when the action was handing the incident to the workflow.

    ACTING now makes a weaker claim than it used to. For the two direct
    remediations it meant "this job is mid-call"; here it means "a run is
    in flight **in another process**", which this job can only ask about.
    So the question is narrow: has that run reached a terminal state?
    Whether the *repair* worked is not asked here at all — that is
    VERIFYING's job, and it is answered against the deployment rather
    than against the run.

    Two failure modes are separated deliberately, because they need
    different human responses:

      - the launch reported success and **no run ever appeared**, which
        points at the trigger or at Agent Studio;
      - a run appeared and **hung**, which points at the workflow.

    A run this cannot interrogate (`unknown`) is never read as success.
    It simply waits, and `ACTING_DEADLINE_SECONDS` is what resolves it.
    """

    status = (run or {}).get("status") or "unknown"
    detail = (run or {}).get("detail") or ""

    fields = {
        "run_state": status,
        "run_last_polled_at": time.time(),
    }

    if status == "not_found":
        if elapsed > config.RUN_APPEAR_SECONDS:
            return {
                "act": False,
                "to_state": state.ESCALATED,
                "reason": (
                    f"the launch reported success {int(elapsed)}s ago but no "
                    f"run has appeared, past the "
                    f"{config.RUN_APPEAR_SECONDS}s grace period. The workflow "
                    "was never started, so nothing is in flight and nothing "
                    "was repaired. Escalating rather than relaunching."
                ),
                "fields": dict(fields, verify_verdict="RUN_NEVER_STARTED"),
            }

        return {
            "act": False,
            "to_state": None,
            "reason": (
                f"a run was launched {int(elapsed)}s ago and has not appeared "
                f"in a listing yet; allowing "
                f"{config.RUN_APPEAR_SECONDS}s for it to show up."
            ),
            "fields": fields,
        }

    if (run or {}).get("terminal"):
        return {
            "act": False,
            "to_state": state.VERIFYING,
            "reason": (
                f"the run finished ({status}: {detail}). Verifying the "
                "deployment to find out whether anything actually changed — "
                "a run that succeeded is not evidence that the flow was "
                "repaired."
            ),
            "fields": fields,
        }

    if elapsed > config.ACTING_DEADLINE_SECONDS:
        return {
            "act": False,
            "to_state": state.ESCALATED,
            "reason": (
                f"the run has been {status} for {int(elapsed)}s, past the "
                f"{config.ACTING_DEADLINE_SECONDS}s deadline. An Agent Studio "
                "pass that has not finished in that time is hung, not slow. "
                "Escalating; the run may still be holding a half-applied "
                "change, so check it before clearing."
            ),
            "fields": dict(fields, verify_verdict="RUN_HUNG"),
        }

    return {
        "act": False,
        "to_state": None,
        "reason": (
            f"a run is in flight ({status}), {int(elapsed)}s of "
            f"{config.ACTING_DEADLINE_SECONDS}s elapsed."
        ),
        "fields": fields,
    }


def decide_verifying_agent(
    current: dict,
    observation: dict,
    deployment_state: str,
    flow: dict,
    elapsed: float,
) -> dict:
    """
    VERIFYING, when the action was handing the incident to the workflow.

    **The job never concludes a repair happened because the agent said
    so.** The agent writes a report; this function confirms or refutes it
    against the API, and every check below is a control-plane read.

    The checks are ordered, and the order is the design. The two that
    escalate come first, because both describe a world in which carrying
    on would be actively harmful:

      1. **HALF_APPLIED** — the deployed flow is advertising
         `RETRY_CHANGE_FLOW_VERSION` or `CANCEL_CHANGE_FLOW_VERSION`.
         That is the API volunteering that it is half-done, which it can
         only do because the control plane mutates before the workload
         hop. Never retried automatically: this is a shared tenant.

      2. **BLINDED** — the monitor could see its queue before and cannot
         now. `ChangeFlowVersionInDeploymentRequest` accepts a `kpis`
         array with the same whole-array replace semantics already banned
         on `update-deployment`, so a version change that does not
         re-supply the connection KPI erases the monitor's own eyes. The
         symptom is silent: `no_chart`, then `UNKNOWN` forever, while the
         job looks like it is running fine. It escalates rather than
         cools because cooling a blind monitor for thirty minutes is the
         worst available outcome.

    Only then the ordinary outcomes. `flow_version` is an `int32`, so
    `after > before` is a monotonic, non-negotiable fact that nothing the
    agent reports can influence — which is exactly why it, and not the
    agent's prose, is the signal.

    Note what is deliberately *not* here: whether the queue drained.
    Success is a steeper drain rate measured on a queue that was not
    emptied for us, and that comparison needs post-change buckets which
    do not exist yet at this point. This function establishes that the
    change landed and the monitor survived; the drain rate is recorded on
    every run into the audit log and is read there.
    """

    facts = flow or {}
    before = current.get("flow_version_before")
    after = facts.get("flow_version")

    fields = {"flow_version_after": after}

    if df_api.is_half_applied(facts):
        return {
            "act": False,
            "to_state": state.ESCALATED,
            "reason": (
                "the deployed flow is advertising "
                f"{sorted(df_api.HALF_APPLIED_ACTIONS & set(facts.get('valid_actions') or []))}, "
                "which means the version change is half-applied between the "
                "control plane and the workload plane. Escalating and leaving "
                "it alone: retrying or cancelling a dangling change on a "
                "shared tenant is a human's decision."
            ),
            "fields": dict(fields, verify_verdict="HALF_APPLIED"),
        }

    if current.get("chart_present_before") and not observation.get("chart_found"):
        return {
            "act": False,
            "to_state": state.ESCALATED,
            "reason": (
                f"the {config.CONNECTION_NAME!r} chart was present before the "
                "action and is gone now, so the monitor can no longer see the "
                "queue it exists to watch. A version change that does not "
                "re-supply the KPI replaces the whole array. Escalating "
                "rather than cooling: a blind monitor that reports health is "
                "worse than one that stops."
            ),
            "fields": dict(fields, verify_verdict="BLINDED"),
        }

    # A version swap in flight shows on the *deployed flow*, not on the
    # deployment: DeploymentState carries CHANGING_FLOW_VERSION among its
    # x-deprecated-enum-values, so deployment state is not a usable
    # in-flight signal for this operation.
    flow_state = facts.get("state") or ""
    flow_steady = flow_state in df_api.STEADY_STATES

    if isinstance(after, int) and isinstance(before, int) and after > before:
        if deployment_state in df_api.STEADY_STATES and flow_steady:
            return {
                "act": False,
                "to_state": state.COOLING,
                "reason": (
                    f"the flow version moved v.{before} -> v.{after}, the "
                    f"deployment is {deployment_state} and the deployed flow "
                    f"is {flow_state}. The change landed. Whether it was the "
                    "right change is answered by the drain rate in the audit "
                    f"log, not here. Cooling for {config.COOLDOWN_SECONDS}s."
                ),
                "fields": dict(
                    fields,
                    verify_verdict="APPLIED",
                    incident_applied_at=time.time(),
                ),
            }

        if elapsed > config.VERIFY_DEADLINE_SECONDS:
            return {
                "act": False,
                "to_state": state.ESCALATED,
                "reason": (
                    f"the flow version moved v.{before} -> v.{after} but the "
                    f"deployment is {deployment_state} and the flow is "
                    f"{flow_state or 'unknown'} after {int(elapsed)}s, past "
                    f"the {config.VERIFY_DEADLINE_SECONDS}s deadline. The "
                    "repair is not lost, only unconfirmed."
                ),
                "fields": dict(fields, verify_verdict="APPLIED_NOT_STEADY"),
            }

        return {
            "act": False,
            "to_state": None,
            "reason": (
                f"the flow version moved v.{before} -> v.{after}; waiting for "
                f"a steady state (deployment {deployment_state}, flow "
                f"{flow_state or 'unknown'}), {int(elapsed)}s elapsed."
            ),
            "fields": fields,
        }

    # The version did not move. `updated` separates "nothing happened"
    # from "something was changed without bumping the version", and those
    # deserve different answers: the first is a legitimate outcome, the
    # second is a flow in a state this loop did not author.
    updated_before = current.get("flow_updated_before")
    updated_now = facts.get("updated")

    mutated_without_version = (
        updated_before is not None
        and updated_now is not None
        and updated_now != updated_before
    )

    if mutated_without_version and elapsed > config.VERIFY_DEADLINE_SECONDS:
        return {
            "act": False,
            "to_state": state.ESCALATED,
            "reason": (
                f"the flow version is still v.{after} but the deployment's "
                f"`updated` timestamp changed ({updated_before} -> "
                f"{updated_now}), so something was modified without a version "
                "bump. Escalating: that is not a change this loop made and "
                "not one it can verify."
            ),
            "fields": dict(fields, verify_verdict="MUTATED_WITHOUT_VERSION"),
        }

    if not mutated_without_version and elapsed > config.NO_CHANGE_SETTLE_SECONDS:
        return {
            "act": False,
            "to_state": state.COOLING,
            "reason": (
                f"the run finished and the flow is unchanged at v.{after} "
                f"after {int(elapsed)}s. The agent proposed nothing, or "
                "declined to act, which is a valid outcome and not a failure. "
                f"Cooling for {config.COOLDOWN_SECONDS}s; the agent's own "
                "reason is in the incident ledger."
            ),
            "fields": dict(fields, verify_verdict="NO_CHANGE"),
        }

    if elapsed > config.VERIFY_DEADLINE_SECONDS:
        return {
            "act": False,
            "to_state": state.ESCALATED,
            "reason": (
                f"no recognisable outcome after {int(elapsed)}s, past the "
                f"{config.VERIFY_DEADLINE_SECONDS}s deadline: version still "
                f"v.{after}, deployment {deployment_state}, flow "
                f"{flow_state or 'unknown'}. Escalating rather than guessing."
            ),
            "fields": dict(fields, verify_verdict="UNRESOLVED"),
        }

    return {
        "act": False,
        "to_state": None,
        "reason": (
            f"verifying: version still v.{after}, deployment "
            f"{deployment_state}, flow {flow_state or 'unknown'}, "
            f"{int(elapsed)}s elapsed "
            f"(settle at {config.NO_CHANGE_SETTLE_SECONDS}s)."
        ),
        "fields": fields,
    }


def decide(
    current: dict,
    observation: dict,
    deployment_state: str,
    flow: dict = None,
    run: dict = None,
) -> dict:
    """
    Advance the state machine and say whether to act.

    Returns {"act": bool, "to_state": str or None, "reason": str, ...}.
    This function performs no I/O and mutates nothing, so the whole
    decision table is testable without touching the account. `flow` and
    `run` are passed in rather than fetched for exactly that reason:
    verifying an agent action needs the deployed flow's facts and the
    launched run's status, and reading them here would make the decision
    table untestable offline.

    The ordering matters. Terminal and safety checks come first, so no
    amount of breach evidence can act while the breaker is tripped or the
    deployment is gone.
    """

    machine = current.get("state") or state.IDLE
    since = current.get("since") or time.time()
    elapsed = time.time() - since
    verdict = observation.get("verdict")

    if deployment_state in ("TERMINATED", "TERMINATING"):
        return {
            "act": False,
            "to_state": state.DISARMED,
            "reason": (
                f"the deployment is {deployment_state}; there is nothing left "
                "to heal. Disarming."
            ),
        }

    if machine in state.TERMINAL_STATES:
        return {
            "act": False,
            "to_state": None,
            "reason": (
                f"state is {machine}, which is terminal and needs a human. "
                "Clear it with --reset once the cause is understood."
            ),
        }

    if state.breaker_tripped(current):
        return {
            "act": False,
            "to_state": state.DISARMED,
            "reason": (
                f"circuit breaker: {config.MAX_ACTIONS_PER_WINDOW} action(s) "
                f"already performed in the last "
                f"{config.ACTION_WINDOW_SECONDS}s. Disarming rather than "
                "continuing to restart a condition no action here can fix."
            ),
        }

    # Which family of action is in flight decides how it is verified, and
    # that is read from what was actually done rather than from
    # `config.ACTION_MODE`. Flipping the mode between the launch and the
    # next cron tick must not change how an in-flight action is judged.
    agent_action = (current.get("last_action") or "") == trigger.ACTION

    if machine == state.ACTING:
        if agent_action:
            return decide_acting_agent(current, elapsed, run)

        # The direct actions are synchronous `cdp` calls, so reaching a
        # second run while still ACTING means the previous run died
        # before recording the outcome. Treat it as in flight rather than
        # retrying, which is what makes the write-before-act ordering
        # worth having.
        return {
            "act": False,
            "to_state": state.VERIFYING,
            "reason": (
                "a previous run recorded ACTING without completing; moving to "
                "VERIFYING rather than issuing a second action."
            ),
        }

    if machine == state.VERIFYING:
        if agent_action:
            return decide_verifying_agent(
                current, observation, deployment_state, flow, elapsed
            )

        if deployment_state in df_api.STEADY_STATES:
            # Unconditional, and this is the crux: the queue is still
            # above threshold here and always will be, because the action
            # froze it rather than draining it. Requiring the condition to
            # have cleared would hold the machine in VERIFYING until the
            # deadline and then escalate every single time.
            return {
                "act": False,
                "to_state": state.COOLING,
                "reason": (
                    f"the deployment returned to {deployment_state}. The "
                    "action completed; the queue is expected to still be "
                    "above threshold, which is why this does not re-trigger. "
                    f"Cooling for {config.COOLDOWN_SECONDS}s."
                ),
            }

        if elapsed > config.VERIFY_DEADLINE_SECONDS:
            return {
                "act": False,
                "to_state": state.ESCALATED,
                "reason": (
                    f"the deployment has been {deployment_state} for "
                    f"{int(elapsed)}s without returning to a steady state, "
                    f"past the {config.VERIFY_DEADLINE_SECONDS}s deadline. "
                    "Escalating instead of assuming the action worked."
                ),
            }

        return {
            "act": False,
            "to_state": None,
            "reason": (
                f"verifying: deployment is {deployment_state}, "
                f"{int(elapsed)}s elapsed."
            ),
        }

    if machine == state.COOLING:
        if elapsed < config.COOLDOWN_SECONDS:
            return {
                "act": False,
                "to_state": None,
                "reason": (
                    f"cooling: {int(elapsed)}s of "
                    f"{config.COOLDOWN_SECONDS}s elapsed. The breach may well "
                    "still be true; that is expected and is not a reason to "
                    "act again."
                ),
            }

        if deployment_state not in df_api.STEADY_STATES:
            return {
                "act": False,
                "to_state": None,
                "reason": (
                    f"cooldown elapsed but the deployment is "
                    f"{deployment_state}; waiting for a steady state before "
                    "re-arming."
                ),
            }

        return {
            "act": False,
            "to_state": state.IDLE,
            "reason": "cooldown complete and the deployment is steady; re-armed.",
            "reset_breaches": True,
        }

    # IDLE or BREACH_PENDING.
    if verdict == UNKNOWN:
        return {
            "act": False,
            "to_state": None,
            "reason": (
                "verdict UNKNOWN, which is neither a breach nor a clean bill "
                f"of health: {observation.get('verdict_reason')}"
            ),
        }

    if verdict == HEALTHY:
        if (current.get("consecutive_breaches") or 0) > 0:
            return {
                "act": False,
                "to_state": state.IDLE,
                "reason": (
                    "queue is back under threshold before the debounce was "
                    "satisfied; breach counter reset."
                ),
                "reset_breaches": True,
            }

        return {
            "act": False,
            "to_state": None,
            "reason": observation.get("verdict_reason"),
        }

    # A breach.
    consecutive = (current.get("consecutive_breaches") or 0) + 1

    if consecutive < config.BREACH_DEBOUNCE:
        return {
            "act": False,
            "to_state": state.BREACH_PENDING,
            "reason": (
                f"breach {consecutive} of {config.BREACH_DEBOUNCE} required "
                "before acting."
            ),
            "consecutive_breaches": consecutive,
        }

    if deployment_state not in df_api.STEADY_STATES:
        return {
            "act": False,
            "to_state": state.BREACH_PENDING,
            "reason": (
                f"debounce satisfied but the deployment is "
                f"{deployment_state}, so an action is already in flight or "
                "the deployment is not ready. Holding."
            ),
            "consecutive_breaches": consecutive,
        }

    return {
        "act": True,
        "to_state": state.ACTING,
        "reason": (
            f"{consecutive} consecutive fresh breaches and the deployment is "
            f"{deployment_state}. Acting."
        ),
        "consecutive_breaches": consecutive,
    }


# ---------------------------------------------------------------------
# Action
# ---------------------------------------------------------------------

def perform(current: dict, action: str, armed: bool, incident: dict = None) -> dict:
    """
    Carry out one remediation.

    The guardrail is not applied here: it lives inside `df_api.act`, so
    there is no path to a mutation that skips it.

    The agent action is dispatched first and mutates nothing itself — it
    hands an incident to a workflow, and the mutation happens inside that
    workflow's own gated tool. Its guardrail is `trigger`'s ledger and
    the validation of the payload.
    """

    deployment_crn = current.get("deployment_crn") or ""
    flow_crn = current.get("deployed_flow_crn") or ""
    expected = current.get("deployment_crn") or ""

    if action == trigger.ACTION:
        return trigger.launch(incident or {}, dry_run=not armed)

    if action == "stop_then_start_flow":
        return df_api.stop_then_start_flow(
            deployment_crn,
            expected,
            flow_crn,
            dry_run=not armed,
        )

    return df_api.restart_deployment(
        deployment_crn,
        expected,
        dry_run=not armed,
    )


# ---------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------

def audit_path() -> str:
    """Return today's audit log path."""

    day = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")

    return os.path.join(config.STATE_DIR, f"audit-{day}.jsonl")


def audit(record: dict) -> str:
    """
    Append one JSON line describing the whole pass.

    One line per run, appended rather than rewritten, so the history
    survives and is greppable. Credentials never reach here: `df_api`
    passes them through the subprocess environment and the `argv` it
    returns is the argument list this code built.
    """

    os.makedirs(config.STATE_DIR, exist_ok=True)

    path = audit_path()

    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")

    return path


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

def show_status(current: dict) -> None:
    """Print the persisted state without touching the API."""

    print(f"state file: {config.STATE_FILE}\n")

    # Every epoch-seconds key, rendered as a timestamp plus an age. The
    # age is the half that gets read: the question asked of this output
    # is "has the run been in flight too long", and answering it from a
    # raw float means doing the subtraction by hand.
    stamped = (
        "since",
        "last_action_started_at",
        "incident_launched_at",
        "incident_applied_at",
        "run_last_polled_at",
    )

    for key in sorted(current):
        value = current[key]

        if key in stamped and value:
            stamp = datetime.datetime.fromtimestamp(
                value, datetime.timezone.utc
            ).isoformat()
            age = int(time.time() - value)
            print(f"  {key}: {stamp} ({age}s ago)")

        else:
            print(f"  {key}: {value}")

    actions = state.prune_action_window(dict(current))

    print(
        f"\n  circuit breaker: {len(actions)} of "
        f"{config.MAX_ACTIONS_PER_WINDOW} action(s) in the last "
        f"{config.ACTION_WINDOW_SECONDS}s"
    )

    print(f"  audit log: {audit_path()}")


def reconcile() -> int:
    """
    Rewrite the state file's CRNs from the live deployment.

    The state file is what the guardrail compares every mutation target
    against, so it drifts in two directions and both matter. An empty
    `deployment_crn` refuses *every* action, which disarms the loop by
    accident rather than by choice. A stale `flow_version_crn` points at
    a version that is no longer deployed.

    This writes nothing to CDF. But it writes the one file that decides
    what may be mutated, so it applies the same discipline as teardown
    before trusting a name: the listing is filtered by prefix
    server-side, and then the match is re-checked locally, because that
    filter is a substring search and this value ends up authorising
    mutations. A reconcile that wrote a colleague's CRN into the state
    file would arm the loop against their deployment.
    """

    print(f"reconciling {config.STATE_FILE}")
    print(f"  looking for a deployment named {config.DEPLOYMENT_NAME!r}\n")

    listing = df_api.require(
        df_api.df(
            "list-deployments",
            "--search-term",
            config.PREFIX,
        ),
        "could not list deployments",
    )

    matches = [
        d
        for d in (listing.get("deployments") or [])
        if d.get("name") == config.DEPLOYMENT_NAME
    ]

    if not matches:
        sys.exit(
            f"error: no deployment named {config.DEPLOYMENT_NAME!r} exists.\n"
            "  Run the provisioning scripts, or set SELFHEAL_PREFIX to match "
            "an existing one."
        )

    if len(matches) > 1:
        sys.exit(
            f"error: {len(matches)} deployments are named "
            f"{config.DEPLOYMENT_NAME!r}.\n"
            "  Refusing to guess which one the loop may act on. Remove the "
            "duplicate in the CDF UI first."
        )

    deployment = matches[0]
    deployment_crn = deployment.get("crn") or ""

    # Belt and braces, exactly as teardown.delete_flow does it: the
    # listing was already prefix-filtered, but that filter is a
    # server-side substring search and this CRN is about to become the
    # one value that authorises mutations.
    if not (deployment.get("name") or "").startswith(config.PREFIX):
        sys.exit(
            f"error: {deployment.get('name')!r} does not carry the "
            f"{config.PREFIX!r} prefix. Refusing to record it."
        )

    if deployment_crn in config.DENYLIST_DEPLOYMENT_CRNS:
        sys.exit(
            "error: that deployment is on the denylist and must never be "
            "recorded as this loop's target."
        )

    facts = df_api.deployment_facts(deployment_crn)

    if facts.get("error"):
        sys.exit(f"error: could not read the deployment: {facts['error']}")

    flow_crn = df_api.deployed_flow_crn(deployment_crn)

    if not flow_crn:
        print(
            "  warning: the deployment has no deployed flow yet, so "
            "deployed_flow_crn stays empty.\n"
            "  Flow-scoped reads and actions will be unavailable until it "
            "does.\n"
        )

    current = state.read()

    updates = {
        "deployment_crn": deployment_crn,
        "deployed_flow_crn": flow_crn,
        "flow_crn": facts.get("flow_crn") or current.get("flow_crn") or "",
        "flow_version_crn": facts.get("flow_version_crn") or "",
    }

    changed = False

    for key in sorted(updates):
        was = current.get(key)
        now = updates[key]

        if was == now:
            print(f"  {key}: unchanged")

        else:
            print(f"  {key}:\n      was: {was}\n      now: {now}")
            current[key] = now
            changed = True

    print(f"\n  live flow version: v.{facts.get('flow_version')}")
    print(f"  deployment state:  {facts.get('state')}")

    if not changed:
        print("\nnothing to do: the state file already matches the account.")

        return 0

    state.write(current)

    print(f"\nwritten. Confirm with:\n  python {sys.argv[0]} --status")

    return 0


def run_once(args) -> int:
    """Execute one observe/decide/act pass."""

    current = state.read()

    if not current.get("deployment_crn"):
        sys.exit(
            "error: no deployment CRN in the state file, so there is nothing "
            "to monitor and nothing this job is allowed to act on.\n"
            "  Run the provisioning scripts in provision/ first."
        )

    started = time.time()
    machine_before = current.get("state")

    # One call, both uses. `describe-deployment` already carries the flow
    # version, its CRN and `updated`; `deployment_state()` used to read
    # that response and throw all of it away. The before-fingerprint is
    # therefore free rather than an extra API call per minute.
    deployment = df_api.deployment_facts(current["deployment_crn"])
    deployment_state = deployment.get("state")

    observation = observe(current)

    # Fetched only in the two states that need them, so the common idle
    # tick still costs exactly the reads it did before. `decide()` stays
    # pure: it receives these rather than fetching them, which is what
    # keeps the whole decision table testable offline.
    flow = None
    run = None

    if current.get("state") in (state.ACTING, state.VERIFYING):
        if (current.get("last_action") or "") == trigger.ACTION:
            flow = df_api.flow_facts(
                current["deployment_crn"],
                current.get("deployed_flow_crn") or "",
            )
            run = trigger.status(current.get("run_ref") or "")

    decision = decide(current, observation, deployment_state, flow, run)

    print(f"deployment state: {deployment_state}")
    print(f"machine state:    {current.get('state')}")
    print(
        f"charts:           {len(observation['chart_inventory'])} "
        f"({observation['chart_sources']})"
    )

    if not observation["chart_found"]:
        print(
            f"  warning: no chart for {config.CONNECTION_NAME!r}. "
            "Published charts:"
        )

        for chart in observation["chart_inventory"] or [{}]:
            print(
                f"    {chart.get('componentType')} / "
                f"{chart.get('componentName')} / {chart.get('name')}"
            )

    sample = observation["sample"]

    if sample.get("status") == "ok":
        print(
            f"queue depth:      {sample['value']} "
            f"(threshold {config.QUEUE_THRESHOLD}, "
            f"{sample['age_seconds']}s old)"
        )

    else:
        print(f"sample:           {sample.get('status')}")

    slope = observation.get("slope") or {}

    if slope.get("status") == "ok":
        print(
            f"drain rate:       {slope['per_second']:+} files/s "
            f"(median of {slope['pairs']} draining bucket(s), "
            f"{slope['steepest']} to {slope['shallowest']})"
        )

    elif slope.get("status") == "awaiting_post_action_buckets":
        print(
            f"drain rate:       not yet measurable — "
            f"{slope['discarded']} pre-action bucket(s) discarded, "
            f"~{slope['measurable_in_seconds']}s until two post-action "
            "buckets exist"
        )

    else:
        print(f"drain rate:       {slope.get('status')}")

    print(f"verdict:          {observation['verdict']}")
    print(f"decision:         {decision['reason']}")

    performed = None
    action_name = None
    incident = None

    if decision["act"]:
        if args.action:
            action_name = args.action

        elif config.ACTION_MODE == "agent":
            action_name = trigger.ACTION

        else:
            action_name = df_api.choose_action(
                int(current.get("action_count") or 0)
            )

        acting_fields = {
            "consecutive_breaches": decision.get(
                "consecutive_breaches", current.get("consecutive_breaches")
            ),
            "last_action_started_at": time.time(),
            # Recorded here so VERIFYING reads the family that actually
            # acted rather than whatever config says on a later tick.
            "last_action": action_name,
        }

        if action_name == trigger.ACTION:
            # Built from the state as it is about to be written, not as
            # it was read. `decide()` returns the *incremented* breach
            # count in its decision and leaves `current` holding the
            # pre-increment value, so building the incident from
            # `current` alone sent the agent `breach_count: 1` on the
            # same tick whose own reason line said "2 consecutive fresh
            # breaches". Measured 2026-10-05 on a live breach.
            #
            # That matters because `breach_count` exists for exactly one
            # purpose — letting the agent judge severity — and an
            # off-by-one understates it at precisely the moment the
            # threshold to act was crossed.
            incident = trigger.build_incident(
                {**current, **acting_fields}, observation
            )

            # The before-fingerprint, captured from the call already made
            # above. Without it, "the version moved" has nothing to be
            # compared against and BLINDED cannot be distinguished from a
            # KPI that was never there.
            acting_fields.update(
                {
                    "incident_id": incident["incident_id"],
                    "incident_count": int(current.get("incident_count") or 0) + 1,
                    "incident_launch_attempts": 1,
                    "incident_launched_at": time.time(),
                    "incident_applied_at": None,
                    "trigger_backend": config.TRIGGER_BACKEND,
                    "run_ref": None,
                    "run_state": None,
                    "verify_verdict": None,
                    "flow_version_after": None,
                    "flow_version_before": deployment.get("flow_version"),
                    "flow_version_crn_before": deployment.get("flow_version_crn"),
                    "flow_updated_before": deployment.get("updated"),
                    "chart_present_before": bool(observation.get("chart_found")),
                    "credential_source": trigger.credential_source(),
                }
            )

        # ACTING is written *before* the request, so a run that overlaps
        # an action in flight reads ACTING instead of IDLE. Doing this
        # after would leave a window in which two runs both act.
        if args.arm:
            current = state.transition(current, state.ACTING, **acting_fields)

        print(f"action:           {action_name} (armed={bool(args.arm)})")

        if incident is not None:
            print(f"  incident:       {incident['incident_id']}")
            print(
                f"  payload:        queue={incident['queue_depth']} "
                f"drain={incident['drain_rate']}/s "
                f"({incident['drain_rate_pairs']} buckets) "
                f"breaches={incident['breach_count']}"
            )

        performed = perform(current, action_name, bool(args.arm), incident)

        if performed.get("refused"):
            print(f"  REFUSED: {performed.get('reason')}")

        elif args.arm and performed.get("performed"):
            print("  requested.")

            current = state.record_action(current, action_name)

        elif args.arm and performed.get("run_ref"):
            # Accepted but not launched, which is what `record_only`
            # does by design. Reporting this as a failure would be a lie
            # in the one direction that matters: the incident was
            # produced correctly and is on disk.
            print(f"  {performed.get('reason')}")

            current = state.record_action(current, action_name)

        elif args.arm:
            print("  the action failed; see the audit log.")

        else:
            # No state is written on an unarmed decision to act, so the
            # next run reaches this same point again. That is the
            # intended behaviour: an observation-only run should keep
            # showing "would act" for as long as the breach lasts,
            # instead of silently advancing a machine that never acted.
            print("  dry run; nothing was requested. Re-run with --arm to act.")

            for step in performed.get("steps") or [performed]:
                print("    " + df_api.command_line(step.get("argv") or [], ""))

        if args.arm:
            # Even a failed action moves to VERIFYING: the request may
            # have been accepted before the error, and re-issuing it on
            # the next tick is the behaviour to avoid.
            #
            # `run_ref` is carried across here because ACTING and
            # VERIFYING both poll it, and a launch whose reference was
            # not persisted is indistinguishable from one that never
            # happened.
            current = state.transition(
                current,
                state.VERIFYING,
                run_ref=performed.get("run_ref"),
                credential_source=(
                    performed.get("credential_source")
                    or current.get("credential_source")
                ),
            )

    elif decision["to_state"]:
        # Fields the decision asked to be persisted alongside the
        # transition — the verify verdict, the observed version, the run
        # status. Applied generically rather than key by key so a new
        # verdict field does not need a matching branch here.
        fields = dict(decision.get("fields") or {})

        if decision.get("reset_breaches"):
            fields["consecutive_breaches"] = 0

        if "consecutive_breaches" in decision:
            fields["consecutive_breaches"] = decision["consecutive_breaches"]

        if decision["to_state"] != current.get("state") or fields:
            current = state.transition(current, decision["to_state"], **fields)

            print(f"transition:       -> {decision['to_state']}")

            if fields.get("verify_verdict"):
                print(f"verify verdict:   {fields['verify_verdict']}")

    else:
        # No transition, but a decision may still have observations worth
        # persisting — the run status it just polled, for instance, so an
        # operator reading --status sees it without waiting for a state
        # change.
        fields = dict(decision.get("fields") or {})

        if "consecutive_breaches" in decision:
            fields["consecutive_breaches"] = decision["consecutive_breaches"]

        if fields:
            current.update(fields)
            state.write(current)

    record = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 2),
        "armed": bool(args.arm),
        "deployment_crn": current.get("deployment_crn"),
        "deployment_state": deployment_state,
        "machine_state_before": machine_before,
        "machine_state_after": current.get("state"),
        "consecutive_breaches": current.get("consecutive_breaches"),
        "actions_in_window": len(state.prune_action_window(dict(current))),
        "observation": observation,
        "decision": decision,
        "action": action_name,
        "action_outcome": performed,
        "context": gather_context(current),
    }

    path = audit(record)

    print(f"\naudit:            {path}")

    return 0


def main() -> int:
    """Parse arguments and run one pass."""

    parser = argparse.ArgumentParser(
        description=(
            "One pass of the DataFlow self-healing loop. Observes by "
            "default; acting requires --arm."
        )
    )

    parser.add_argument(
        "--arm",
        action="store_true",
        help=(
            "Permit remediation. Without this the job observes, decides and "
            "logs the action it would take, but changes nothing."
        ),
    )

    parser.add_argument(
        "--action",
        choices=df_api.ACTIONS + (trigger.ACTION,),
        help=(
            "Force a specific remediation instead of the one "
            f"SELFHEAL_ACTION_MODE selects (currently {config.ACTION_MODE!r}). "
            f"{trigger.ACTION} hands the incident to the Agent Studio "
            "workflow; the other two are the original direct actions, neither "
            "of which repairs anything. Does not bypass the state machine or "
            "the guardrail."
        ),
    )

    parser.add_argument(
        "--selftest",
        action="store_true",
        help=(
            "Check both API planes are reachable and exit. Detection uses the "
            "control plane and action uses the workload plane, so a read "
            "working proves nothing about an action working."
        ),
    )

    parser.add_argument(
        "--status",
        action="store_true",
        help="Print the persisted state and exit without calling the API.",
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help=(
            "Return a DISARMED or ESCALATED state to IDLE and clear the "
            "circuit breaker. For use after a human has understood the cause."
        ),
    )

    parser.add_argument(
        "--reconcile",
        action="store_true",
        help=(
            "Re-read the live deployment and rewrite the CRNs and flow "
            "version recorded in the state file. Changes nothing in CDF. Use "
            "after provisioning out of band, or when --status shows a null "
            "deployment_crn, which refuses every action."
        ),
    )

    args = parser.parse_args()

    # Cloudera AI runtimes do not ship cdpcli; setup_runtime.py installs
    # it as a user package, whose bin directory is not on PATH by default.
    if os.path.isdir(USER_BIN) and USER_BIN not in os.environ.get("PATH", ""):
        os.environ["PATH"] = USER_BIN + os.pathsep + os.environ.get("PATH", "")

    if args.status:
        show_status(state.read())

        return 0

    if args.selftest:
        print("self-test:")

        return 0 if df_api.selftest() else 1

    if args.reconcile:
        return reconcile()

    if args.reset:
        current = state.read()
        previous = current.get("state")

        current["action_timestamps"] = []
        current["consecutive_breaches"] = 0
        state.transition(current, state.IDLE)

        print(f"reset: {previous} -> {state.IDLE}, circuit breaker cleared.")

        return 0

    # The lock, not the state machine, is what stops two overlapping runs
    # from both acting. A one-minute schedule against a runtime that
    # needs 30-60s to start a container overlaps routinely.
    with state.single_run() as acquired:
        if not acquired:
            print("skipped: a prior run is still in flight.")

            return 0

        return run_once(args)


if __name__ == "__main__":
    sys.exit(main())
