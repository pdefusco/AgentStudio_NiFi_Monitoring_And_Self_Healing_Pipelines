#!/usr/bin/env python3
"""
Hand one incident to the Agent Studio repair workflow.

This is the seam between the two processes. The Cloudera AI job detects,
debounces, guardrails and verifies; the workflow reads the flow, proposes
a bounded edit and applies it. An Agent Studio workflow is launched
**once** and makes one pass, so it cannot do the detecting — a tool it
calls samples the metric exactly once per launch, which is not
monitoring.

**The mechanism is isolated here on purpose, because it is unverified.**
One repo doc asserts Agent Studio's screens are "UI operations with no
API"; the top-level glossary says Agent Studio is for "building, running,
and deploying" workflows. Neither claim has a probe behind it. So the
default backend is `record_only`, which writes a complete incident to a
ledger and reports honestly that nothing was launched. That makes the
entire loop testable end to end today, and it is also precisely the
fallback if no programmatic launch turns out to exist: the job detects,
records, and a human opens the workflow with the recorded payload.

Everything here returns the same dict shape `df_api.act()` returns —
`{performed, refused, reason, argv, result}` — so the state machine, the
`--arm` gate, the circuit breaker and the audit log wrap it unchanged.

Credentials follow the repo's conventions without exception: read from
the environment, passed in a subprocess environment or an Authorization
header, never placed on a command line where `ps` can read them, never
returned to a caller, and never written to the audit log.

Usage:

    import trigger

    incident = trigger.build_incident(current, observation)
    outcome = trigger.launch(incident, dry_run=True)
"""

import datetime
import json
import os
import urllib.error
import urllib.request
import uuid

import config


# The action name the job records and the audit log carries. Deliberately
# named for what the job does — launch — rather than for what the agent
# might conclude, because the job can only ever observe the former.
ACTION = "launch_repair_workflow"

# Set by the Cloudera AI runtime in every session and job container. The
# workspace API v2 key is a *different* credential from the
# CDP_ACCESS_KEY_ID / CDP_PRIVATE_KEY pair the monitor already uses, and
# having one does not imply having the other.
API_KEY_VARS = ("CDSW_APIV2_KEY", "CML_API_KEY")


# ---------------------------------------------------------------------
# The payload
# ---------------------------------------------------------------------

def build_incident(current: dict, observation: dict) -> dict:
    """
    Build the scalars-only payload the workflow is launched with.

    **Every field here becomes prompt text.** Agent Studio's workflow
    inputs are the `{placeholder}` tokens in the task description, so
    anything in this dict enters the model's context verbatim. That is
    the whole reason the flow definition is not here: a ~6KB JSON blob in
    the context is a model that can be coaxed into hand-editing flow
    JSON, and the agent must only ever pick a repair *name* and a bounded
    magnitude. The tools fetch the definition themselves.

    So the test each field has to pass is not "is it useful" but "is it
    something the agent cannot look up for itself, and is it a scalar".

    `drain_rate` is the measured conditional median from
    `queue_slope()`, in flowfiles per second and negative. It is the
    number the repair has to improve, and it is passed so the agent can
    state a predicted new rate — the job measures the actual one
    afterwards and never takes the agent's word for it.
    """

    sample = observation.get("sample") or {}
    slope = observation.get("slope") or {}

    return {
        # Correlates the job's audit line with the agent's report, and
        # makes a double launch detectable rather than merely unlikely.
        "incident_id": new_incident_id(),
        "deployment_crn": current.get("deployment_crn") or "",
        "queue_depth": sample.get("value"),
        "drain_rate": slope.get("per_second"),
        # How many buckets the rate rests on. Passed because "-2.0 from
        # one bucket" and "-2.0 from nine" deserve different confidence,
        # and the agent is being asked to reason about severity.
        "drain_rate_pairs": slope.get("pairs"),
        "breach_count": int(current.get("consecutive_breaches") or 0),
        "threshold": observation.get("threshold"),
        # Both forms of the same instant, which is worth the duplication
        # because they have different readers. The ISO string is what
        # lands in the prompt, where an epoch int would be noise the model
        # has to interpret; the epoch int is what arithmetic uses to
        # recognise a stale launch. Named apart so the types cannot be
        # confused — `sample` itself carries `timestamp` as epoch seconds
        # and `observed_at` as ISO, and conflating them is an easy bug.
        "observed_at": sample.get("observed_at") or "",
        "observed_at_epoch": int(sample.get("timestamp") or 0),
    }


def new_incident_id() -> str:
    """
    A fresh incident id: UTC date, then eight random hex.

    Date-prefixed so the ledger directory sorts chronologically and an
    operator can see at a glance when an incident happened without
    opening anything. Random rather than a counter because a counter
    would have to live in the state file, and the ledger has to keep
    working when the state file is reconciled or reset.
    """

    day = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")

    return f"{day}-{uuid.uuid4().hex[:8]}"


def validate_incident(incident: dict) -> str:
    """
    Return an empty string if the payload is launchable, else the reason.

    Checked before anything is recorded or sent, because a launch with a
    null queue depth or a missing deployment CRN produces an agent run
    that reasons confidently about nothing. Refusing is strictly better:
    the breach is still true a minute later and the cron fires again.
    """

    if not incident.get("incident_id"):
        return "the incident has no incident_id"

    if not incident.get("deployment_crn"):
        return (
            "the incident has no deployment_crn; run with --reconcile to "
            "repair the state file"
        )

    for field in ("queue_depth", "drain_rate"):
        if not isinstance(incident.get(field), (int, float)):
            return (
                f"the incident has no measured {field}, so there is nothing "
                "for the workflow to act on"
            )

    return ""


# ---------------------------------------------------------------------
# The ledger
# ---------------------------------------------------------------------

def incident_path(incident_id: str) -> str:
    """Return the ledger path for one incident id."""

    return os.path.join(config.INCIDENT_DIR, f"{incident_id}.json")


def record_incident(incident: dict, extra: dict = None) -> dict:
    """
    Write the incident to the ledger, failing if it is already there.

    `O_CREAT | O_EXCL` makes the filesystem the arbiter of
    "has this incident already been handed over", which is a stronger
    guarantee than a check-then-write against the state file and holds
    across two processes without a lock. It is the mechanism behind the
    plan's "a second run for a recorded id is refused".

    Returns `{recorded: bool, path, reason}`. A collision is not an
    error to raise: it is a meaningful answer, and the caller turns it
    into a refusal.
    """

    os.makedirs(config.INCIDENT_DIR, exist_ok=True)

    path = incident_path(incident["incident_id"])

    record = dict(incident)
    record.update(extra or {})
    record["recorded_at"] = int(
        datetime.datetime.now(datetime.timezone.utc).timestamp()
    )
    record["trigger_backend"] = config.TRIGGER_BACKEND

    try:
        handle = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)

    except FileExistsError:
        return {
            "recorded": False,
            "path": path,
            "reason": (
                f"incident {incident['incident_id']} is already in the "
                "ledger, so it has been handed over once already"
            ),
        }

    with os.fdopen(handle, "w", encoding="utf-8") as stream:
        json.dump(record, stream, indent=2, sort_keys=True, default=str)
        stream.write("\n")

    return {"recorded": True, "path": path, "reason": ""}


# ---------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------

def _api_key() -> tuple:
    """
    Return `(key, source_variable_name)`, or `("", "")` if there is none.

    The name is returned and the key is not, so a caller can record
    *which* credential was used in the audit log — `credential_source` in
    the state file — without the value ever leaving this module.
    """

    for name in API_KEY_VARS:
        value = os.environ.get(name) or ""

        if value:
            return value, name

    return "", ""


def credential_source() -> str:
    """Name the variable a key was found in, for the audit log."""

    return _api_key()[1]


def _launch_environment() -> dict:
    """
    The environment a launch subprocess inherits.

    A copy of the current environment with the API key present under
    both names the Cloudera AI runtime uses, so a child that looks for
    either finds it. The key reaches the child through this dict and not
    through `argv`, because a command line is readable by any process on
    the host via `ps`.
    """

    key, _ = _api_key()

    environment = dict(os.environ)

    if key:
        for name in API_KEY_VARS:
            environment[name] = key

    return environment


# ---------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------

def _launch_record_only(incident: dict, dry_run: bool) -> dict:
    """
    Record the incident and launch nothing.

    Reports `performed: False` with a plain reason, which is the honest
    answer: a complete, correct incident was produced and no workflow
    ran. `argv` carries the request that *would* have been made, so the
    audit log's one factual field stays factual when there is no
    subprocess to quote.
    """

    request = [
        "agent-studio",
        "run-workflow",
        "--workflow",
        "nifi_flow_repair",
        "--incident-id",
        incident["incident_id"],
    ]

    if dry_run:
        return {
            "performed": False,
            "dry_run": True,
            "reason": (
                "dry run: the incident was neither recorded nor launched. "
                "Re-run with --arm to record it."
            ),
            "argv": request,
            "incident": incident,
        }

    written = record_incident(incident)

    if not written["recorded"]:
        return {
            "performed": False,
            "refused": True,
            "reason": written["reason"],
            "argv": request,
            "incident": incident,
        }

    return {
        "performed": False,
        "reason": (
            f"recorded, not launched: TRIGGER_BACKEND is 'record_only', so "
            f"the incident is in {written['path']} and no workflow was "
            "started. A human can run the workflow with this payload."
        ),
        "argv": request,
        "incident": incident,
        "result": {"incident_path": written["path"]},
        "run_ref": f"record_only:{incident['incident_id']}",
    }


def _launch_cml_model_endpoint(incident: dict, dry_run: bool) -> dict:
    """
    POST the incident to a deployed workflow's JSON endpoint.

    Unverified until a Deploy action is shown to produce a CML Model with
    an access key. Written now so that confirming the mechanism is a
    config change rather than a development task, and written to fail
    loudly when its target is unset.
    """

    url = config.TRIGGER_ENDPOINT_URL

    if not url:
        return {
            "performed": False,
            "refused": True,
            "reason": (
                "TRIGGER_BACKEND is 'cml_model_endpoint' but "
                "SELFHEAL_TRIGGER_ENDPOINT_URL is empty, so there is nowhere "
                "to send the incident"
            ),
            "argv": [],
            "incident": incident,
        }

    key, source = _api_key()

    if not key:
        return {
            "performed": False,
            "refused": True,
            "reason": (
                "no workspace API key in the environment (looked for "
                f"{', '.join(API_KEY_VARS)}), so the endpoint cannot be "
                "authenticated against"
            ),
            "argv": [],
            "incident": incident,
        }

    # The URL is the one fact worth quoting; the body is the incident,
    # which is recorded separately, and the header carries a credential
    # and must never be quoted.
    request_note = ["POST", url]

    if dry_run:
        return {
            "performed": False,
            "dry_run": True,
            "reason": "dry run: nothing was posted.",
            "argv": request_note,
            "incident": incident,
            "credential_source": source,
        }

    written = record_incident(incident)

    # Recorded *before* the POST, so a launch that succeeds and then
    # crashes the job cannot be repeated on the next run. The cost of
    # that ordering is a recorded incident that never launched, which is
    # visible and recoverable; the cost of the other ordering is two
    # agents editing one flow.
    if not written["recorded"]:
        return {
            "performed": False,
            "refused": True,
            "reason": written["reason"],
            "argv": request_note,
            "incident": incident,
        }

    body = json.dumps({"request": incident}).encode("utf-8")

    http_request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        },
    )

    try:
        with urllib.request.urlopen(
            http_request, timeout=config.TRIGGER_TIMEOUT_SECONDS
        ) as response:
            raw = response.read().decode("utf-8", "replace")
            status = response.status

    except urllib.error.HTTPError as exc:
        return {
            "performed": False,
            "reason": f"the endpoint returned HTTP {exc.code}",
            "argv": request_note,
            "incident": incident,
            "result": {"status": exc.code},
        }

    except (urllib.error.URLError, OSError) as exc:
        return {
            "performed": False,
            "reason": f"the endpoint could not be reached: {exc}",
            "argv": request_note,
            "incident": incident,
        }

    try:
        parsed = json.loads(raw)

    except ValueError:
        parsed = {"raw": raw[:2000]}

    return {
        "performed": True,
        "reason": f"posted the incident to the workflow endpoint (HTTP {status})",
        "argv": request_note,
        "incident": incident,
        "result": parsed,
        "run_ref": f"cml_model_endpoint:{incident['incident_id']}",
        "credential_source": source,
    }


def _launch_cml_job_run(incident: dict, dry_run: bool) -> dict:
    """
    Start a Cloudera AI job run that invokes the workflow.

    Unverified until Phase B shows there is no endpoint. `cmlapi` is not
    on PyPI — it is served from `<workspace>/api/v2/python.tar.gz` and is
    preinstalled in the workbench — so the import is deliberately local
    to this function and its absence is a refusal rather than a crash at
    module import. The monitor must keep monitoring on a laptop where
    `cmlapi` cannot be installed.
    """

    job_id = config.TRIGGER_JOB_ID

    if not job_id:
        return {
            "performed": False,
            "refused": True,
            "reason": (
                "TRIGGER_BACKEND is 'cml_job_run' but SELFHEAL_TRIGGER_JOB_ID "
                "is empty, so there is no job to run"
            ),
            "argv": [],
            "incident": incident,
        }

    request_note = ["cmlapi", "create_job_run", job_id]

    if dry_run:
        return {
            "performed": False,
            "dry_run": True,
            "reason": "dry run: no job run was created.",
            "argv": request_note,
            "incident": incident,
        }

    try:
        import cmlapi

    except ImportError:
        return {
            "performed": False,
            "refused": True,
            "reason": (
                "cmlapi is not importable, so this backend cannot launch "
                "anything. It is served from <workspace>/api/v2/python.tar.gz "
                "and is preinstalled in the workbench, not on PyPI"
            ),
            "argv": request_note,
            "incident": incident,
        }

    key, source = _api_key()

    if not key:
        return {
            "performed": False,
            "refused": True,
            "reason": (
                "no workspace API key in the environment (looked for "
                f"{', '.join(API_KEY_VARS)})"
            ),
            "argv": request_note,
            "incident": incident,
        }

    written = record_incident(incident)

    if not written["recorded"]:
        return {
            "performed": False,
            "refused": True,
            "reason": written["reason"],
            "argv": request_note,
            "incident": incident,
        }

    project_id = os.environ.get("CDSW_PROJECT_ID") or ""

    if not project_id:
        return {
            "performed": False,
            "refused": True,
            "reason": (
                "CDSW_PROJECT_ID is unset, which means this is not running "
                "inside a Cloudera AI project"
            ),
            "argv": request_note,
            "incident": incident,
        }

    try:
        client = cmlapi.default_client()

        run = client.create_job_run(
            cmlapi.CreateJobRunRequest(
                project_id=project_id,
                job_id=job_id,
                # The incident travels as environment variables rather
                # than arguments for the same reason credentials do, and
                # because a job run has no argv of its own to receive it.
                environment={
                    f"SELFHEAL_INCIDENT_{name.upper()}": str(value)
                    for name, value in incident.items()
                },
            ),
            project_id,
            job_id,
        )

    except Exception as exc:
        return {
            "performed": False,
            "reason": f"create_job_run failed: {type(exc).__name__}: {exc}",
            "argv": request_note,
            "incident": incident,
        }

    run_id = getattr(run, "id", "") or ""

    return {
        "performed": True,
        "reason": f"created job run {run_id or '(no id returned)'}",
        "argv": request_note,
        "incident": incident,
        "result": {"run_id": run_id},
        "run_ref": f"cml_job_run:{run_id}",
        "credential_source": source,
    }


BACKENDS = {
    "record_only": _launch_record_only,
    "cml_model_endpoint": _launch_cml_model_endpoint,
    "cml_job_run": _launch_cml_job_run,
}


# ---------------------------------------------------------------------
# The entry point
# ---------------------------------------------------------------------

def launch(incident: dict, dry_run: bool = True) -> dict:
    """
    Hand one incident to the workflow, by whichever backend is configured.

    Returns the `df_api.act()` contract so every wrapper around an action
    — the state machine, the `--arm` gate, the circuit breaker, the audit
    log — treats this exactly like the two direct remediations.

    `dry_run` defaults to True, matching `df_api.act()`, so a caller that
    forgets the argument gets the safe behaviour.

    The one field this adds beyond that contract is `run_ref`, a
    `"<backend>:<id>"` string. A string rather than a dict because
    `state.read()` merges stored state over the defaults at the top level
    only, so a nested dict in the state file would wholly replace its
    default and a later sub-key would `KeyError`.
    """

    problem = validate_incident(incident)

    if problem:
        return {
            "performed": False,
            "refused": True,
            "reason": f"incident refused: {problem}",
            "argv": [],
            "incident": incident,
        }

    backend = BACKENDS.get(config.TRIGGER_BACKEND)

    # Unreachable while config validates at import, which it does. Kept
    # because this module is importable on its own and a future backend
    # added to one list and not the other should say so rather than
    # raise a TypeError on None.
    if backend is None:
        return {
            "performed": False,
            "refused": True,
            "reason": (
                f"no backend implements TRIGGER_BACKEND="
                f"{config.TRIGGER_BACKEND!r}"
            ),
            "argv": [],
            "incident": incident,
        }

    outcome = backend(incident, dry_run)
    outcome.setdefault("trigger_backend", config.TRIGGER_BACKEND)

    return outcome


def status(run_ref: str) -> dict:
    """
    Ask whether a launched run has finished.

    Returns `{status, terminal, detail}` where `status` is one of
    pending / running / succeeded / failed / unknown / not_found, and
    `terminal` says whether waiting longer could change the answer.

    `record_only` is terminal the moment it is recorded: nothing is
    running, so reporting `pending` would make the job wait out a
    deadline for an event that cannot occur.

    A run this cannot interrogate returns `unknown`, non-terminal — and
    the caller's own deadline is what resolves it. This function never
    guesses that an unobservable run succeeded.
    """

    if not run_ref:
        return {
            "status": "not_found",
            "terminal": False,
            "detail": "no run reference recorded",
        }

    backend, _, reference = run_ref.partition(":")

    if backend == "record_only":
        exists = os.path.exists(incident_path(reference))

        return {
            "status": "succeeded" if exists else "not_found",
            "terminal": exists,
            "detail": (
                "recorded in the ledger; nothing was launched, so there is "
                "no run to wait for"
                if exists
                else "the ledger has no file for this incident"
            ),
        }

    if backend == "cml_model_endpoint":
        # A synchronous POST has already returned by the time a run_ref
        # exists, so there is no run resource to poll. Whether the repair
        # landed is answered by the deployment, not by the launch.
        return {
            "status": "succeeded",
            "terminal": True,
            "detail": (
                "the endpoint call returned; the outcome is verified against "
                "the deployment, not against the launch"
            ),
        }

    if backend == "cml_job_run":
        try:
            import cmlapi

        except ImportError:
            return {
                "status": "unknown",
                "terminal": False,
                "detail": "cmlapi is not importable here",
            }

        project_id = os.environ.get("CDSW_PROJECT_ID") or ""

        if not project_id:
            return {
                "status": "unknown",
                "terminal": False,
                "detail": "CDSW_PROJECT_ID is unset",
            }

        try:
            client = cmlapi.default_client()
            runs = client.list_job_runs(project_id, config.TRIGGER_JOB_ID)

        except Exception as exc:
            return {
                "status": "unknown",
                "terminal": False,
                "detail": f"{type(exc).__name__}: {exc}",
            }

        for run in getattr(runs, "job_runs", None) or []:
            if getattr(run, "id", "") != reference:
                continue

            raw = (getattr(run, "status", "") or "").upper()

            succeeded = "SUCCEED" in raw
            failed = any(
                marker in raw
                for marker in ("FAIL", "STOP", "KILL", "TIMEDOUT", "TIMED_OUT")
            )

            return {
                "status": (
                    "succeeded" if succeeded
                    else "failed" if failed
                    else "running"
                ),
                "terminal": succeeded or failed,
                "detail": raw or "the run reported no status",
            }

        return {
            "status": "not_found",
            "terminal": False,
            "detail": f"no run with id {reference} in the job's run list",
        }

    return {
        "status": "unknown",
        "terminal": False,
        "detail": f"unrecognised run reference backend {backend!r}",
    }


# ---------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------

def selftest() -> int:
    """
    Exercise the module without launching anything or touching CDF.

    Writes to the real ledger directory, because the thing most worth
    proving is that `O_EXCL` actually refuses the second write — and a
    temporary directory would prove that about a temporary directory.
    The incident ids are marked `selftest` so they are obvious in the
    ledger and trivial to delete.
    """

    print(f"backend:          {config.TRIGGER_BACKEND}")
    print(f"ledger:           {config.INCIDENT_DIR}")

    source = credential_source()
    print(f"api key:          {source or '(none in the environment)'}")

    if config.TRIGGER_BACKEND != "record_only" and not source:
        print(
            f"  warning: backend {config.TRIGGER_BACKEND} needs a workspace "
            f"API key and none is set; every launch will be refused."
        )

    failures = []

    # 1. A payload missing its measurements must be refused, not sent.
    refused = launch({"incident_id": "x", "deployment_crn": ""}, dry_run=False)

    print(f"\nempty payload:    refused={refused.get('refused')}")
    print(f"  reason:         {refused.get('reason')}")

    if not refused.get("refused"):
        failures.append("an incident with no deployment_crn was not refused")

    # 2. A dry run must not write to the ledger.
    incident = {
        "incident_id": f"selftest-{uuid.uuid4().hex[:8]}",
        "deployment_crn": "crn:selftest",
        "queue_depth": 520.0,
        "drain_rate": -2.0,
        "breach_count": 2,
    }

    dry = launch(dict(incident), dry_run=True)

    print(f"\ndry run:          performed={dry.get('performed')}")
    print(f"  argv:           {' '.join(dry.get('argv') or []) or '(none)'}")

    if os.path.exists(incident_path(incident["incident_id"])):
        failures.append("a dry run wrote to the ledger")

    # 3. An armed run records, and reports that it launched nothing.
    first = launch(dict(incident), dry_run=False)

    print(f"\nrecorded:         performed={first.get('performed')}")
    print(f"  reason:         {first.get('reason')}")
    print(f"  run_ref:        {first.get('run_ref')}")

    if config.TRIGGER_BACKEND == "record_only":
        if first.get("performed"):
            failures.append("record_only reported performed=True")

        if not first.get("run_ref"):
            failures.append("record_only recorded without a run_ref")

        # 4. The same incident id a second time must be refused by the
        #    filesystem, which is the idempotency guarantee.
        second = launch(dict(incident), dry_run=False)

        print(f"\nreplayed:         refused={second.get('refused')}")
        print(f"  reason:         {second.get('reason')}")

        if not second.get("refused"):
            failures.append(
                "the same incident_id was accepted twice; O_EXCL is not "
                "protecting the ledger"
            )

        # 5. A recorded incident is terminal, not pending.
        state_of_run = status(first.get("run_ref") or "")

        print(f"\nstatus:           {state_of_run['status']}")
        print(f"  terminal:       {state_of_run['terminal']}")

        if not state_of_run["terminal"]:
            failures.append(
                "a recorded incident reported non-terminal, so the job would "
                "wait out a deadline for an event that cannot happen"
            )

        path = incident_path(incident["incident_id"])

        if os.path.exists(path):
            os.unlink(path)
            print(f"\ncleaned up:       {os.path.basename(path)}")

    # 6. Unknown references must not be reported as success.
    unknown = status("no_such_backend:123")

    if unknown["status"] == "succeeded":
        failures.append("an unrecognised run_ref was reported as succeeded")

    if failures:
        print("\nFAILED:")

        for failure in failures:
            print(f"  - {failure}")

        return 1

    print("\nAll trigger self-tests passed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(selftest())
