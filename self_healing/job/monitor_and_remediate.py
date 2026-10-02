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

    sample = latest_sample(chart, current.get("last_action_started_at"))

    observation = {
        "chart_sources": merged["sources"],
        "chart_inventory": inventory,
        "chart_found": bool(chart),
        "chart_name": chart.get("name"),
        "sample": sample,
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

def decide(current: dict, observation: dict, deployment_state: str) -> dict:
    """
    Advance the state machine and say whether to act.

    Returns {"act": bool, "to_state": str or None, "reason": str, ...}.
    This function performs no I/O and mutates nothing, so the whole
    decision table is testable without touching the account.

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

    # An action was requested by a previous run that then died before it
    # could record the outcome. Treat it as in flight rather than
    # retrying, which is what makes the ACTING write-before-act ordering
    # worth having.
    if machine == state.ACTING:
        return {
            "act": False,
            "to_state": state.VERIFYING,
            "reason": (
                "a previous run recorded ACTING without completing; moving to "
                "VERIFYING rather than issuing a second action."
            ),
        }

    if machine == state.VERIFYING:
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

def perform(current: dict, action: str, armed: bool) -> dict:
    """
    Carry out one remediation.

    The guardrail is not applied here: it lives inside `df_api.act`, so
    there is no path to a mutation that skips it.
    """

    deployment_crn = current.get("deployment_crn") or ""
    flow_crn = current.get("deployed_flow_crn") or ""
    expected = current.get("deployment_crn") or ""

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

    for key in sorted(current):
        value = current[key]

        if key in ("since", "last_action_started_at") and value:
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

    deployment_state = df_api.deployment_state(current["deployment_crn"])
    observation = observe(current)
    decision = decide(current, observation, deployment_state)

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

    print(f"verdict:          {observation['verdict']}")
    print(f"decision:         {decision['reason']}")

    performed = None
    action_name = None

    if decision["act"]:
        action_name = args.action or df_api.choose_action(
            int(current.get("action_count") or 0)
        )

        # ACTING is written *before* the request, so a run that overlaps
        # an action in flight reads ACTING instead of IDLE. Doing this
        # after would leave a window in which two runs both act.
        if args.arm:
            current = state.transition(
                current,
                state.ACTING,
                consecutive_breaches=decision.get(
                    "consecutive_breaches", current.get("consecutive_breaches")
                ),
                last_action_started_at=time.time(),
            )

        print(f"action:           {action_name} (armed={bool(args.arm)})")

        performed = perform(current, action_name, bool(args.arm))

        if performed.get("refused"):
            print(f"  REFUSED: {performed.get('reason')}")

        elif args.arm and not performed.get("performed"):
            print("  the action failed; see the audit log.")

        elif args.arm:
            print("  requested.")

            current = state.record_action(current, action_name)

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
            current = state.transition(current, state.VERIFYING)

    elif decision["to_state"]:
        fields = {}

        if decision.get("reset_breaches"):
            fields["consecutive_breaches"] = 0

        if "consecutive_breaches" in decision:
            fields["consecutive_breaches"] = decision["consecutive_breaches"]

        if decision["to_state"] != current.get("state") or fields:
            current = state.transition(current, decision["to_state"], **fields)

            print(f"transition:       -> {decision['to_state']}")

    elif "consecutive_breaches" in decision:
        current["consecutive_breaches"] = decision["consecutive_breaches"]
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
        choices=df_api.ACTIONS,
        help=(
            "Force a specific remediation instead of the alternating "
            "selection. Does not bypass the state machine or the guardrail."
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
