"""
Apache NiFi canvas inspection tool.

This Agent Studio tool performs read-only requests against the NiFi
REST API of a Cloudera DataFlow Public Cloud deployment, reaching the
NiFi canvas directly rather than going through the Cloudera DataFlow
control plane.

Where the companion `pollNifiFlow` tool asks Cloudera DataFlow "what is
the state of this deployment", this tool asks NiFi itself "what is
happening inside the flow": component run states, queued FlowFiles,
throughput, and active bulletins.

Endpoints used (stable across NiFi 1.x and 2.x):

    GET /nifi-api/flow/about                        NiFi version
    GET /nifi-api/flow/status                       controller-level tallies
    GET /nifi-api/flow/process-groups/{id}/status   process group status
    GET /nifi-api/flow/bulletin-board               active warnings/errors


AUTHENTICATION

The DFX gateway in front of a CDF Public Cloud deployment is an OAuth2
resource server. It expects a signed JWT in an `Authorization: Bearer`
header, and NiFi behind it has no username/password login provider at
all — `GET /nifi-api/access/config` reports `supportsLogin: false`, and
`POST /nifi-api/access/token` answers `409 Username/Password login not
supported by this NiFi.` HTTP Basic authentication with CDP workload
credentials therefore cannot work here, regardless of the credentials
used; the gateway returns 401 without ever evaluating them.

The token the gateway accepts is minted by the CDP control plane:

    cdp iam generate-workload-auth-token \
        --workload-name DF \
        --environment-crn <environment CRN>

This tool does that itself, so the only credentials it needs are a CDP
API key pair, the same pair the companion `pollNifiFlow` tool uses:

    CDP_ACCESS_KEY_ID
    CDP_PRIVATE_KEY

Given a deployment CRN, the tool resolves everything else, chaining the
two monitoring layers together:

    df describe-deployment  ->  deployment.nifiUrl              (API base)
                            ->  deployment.service.environmentCrn
    iam generate-workload-auth-token --workload-name DF
                            ->  token
    GET <nifiUrl minus /nifi>/nifi-api/...
                                with Authorization: Bearer <token>

`nifiUrl` is the deployment's own address and is the field to build on.
`dfxLocalUrl`, also returned, is the base of the shared dfx-local
instance and omits the per-deployment namespace the gateway routes on;
requests built from it are answered 403.

The agent only needs to provide:

    deployment_crn
    process_group_id    (defaults to the root canvas)

Tokens are short lived, so one is minted per tool call and never
persisted. The token value is never returned to the agent or logged;
only its expiry is reported.

All requests are read-only. This tool cannot start, stop, or modify
anything in the flow.
"""

from pydantic import BaseModel, Field
from typing import Any, Optional
import argparse
import json
import os
import subprocess
import sys

import requests


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

REQUEST_TIMEOUT_SECONDS = 30

# The CDP CLI makes a control plane round trip, so it gets a longer
# budget than a single NiFi API call.
CDP_TIMEOUT_SECONDS = 60

# Truncation limit for non-JSON response bodies and CLI stderr. A
# gateway rejection can return an HTML page, and the agent does not
# need the whole document to understand what went wrong.
MAX_RAW_OUTPUT_CHARS = 2000


# ---------------------------------------------------------------------
# User / Project Parameters
# ---------------------------------------------------------------------

class UserParameters(BaseModel):
    """
    Credentials and optional overrides supplied through Agent Studio
    tool configuration.

    These values are injected by Agent Studio and must never be
    supplied by the LLM.
    """

    CDP_ACCESS_KEY_ID: str
    CDP_PRIVATE_KEY: str

    # Optional escape hatches. Normally both are discovered from the
    # deployment CRN, but either can be pinned when the control plane
    # lookup is not available or reports a URL that is not reachable
    # from where this tool runs.
    NIFI_BASE_URL: Optional[str] = None
    DF_ENVIRONMENT_CRN: Optional[str] = None


# ---------------------------------------------------------------------
# Tool Parameters
# ---------------------------------------------------------------------

class ToolParameters(BaseModel):
    """
    Arguments supplied by the agent when invoking this tool.
    """

    deployment_crn: str = Field(
        description=(
            "CRN of the Cloudera DataFlow deployment whose NiFi canvas "
            "should be inspected. Used to look up the deployment's NiFi "
            "endpoint and environment before authenticating."
        ),
    )

    process_group_id: str = Field(
        default="root",
        description=(
            "ID of the NiFi process group to inspect. Use 'root' for "
            "the top-level canvas, which is the default. A specific "
            "process group UUID may be supplied to narrow the scope."
        ),
    )

    recursive: bool = Field(
        default=True,
        description=(
            "Whether to include the status of all nested process "
            "groups. True reports the whole flow beneath the chosen "
            "process group; False reports only that group's own "
            "components."
        ),
    )

    include_bulletins: bool = Field(
        default=True,
        description=(
            "Whether to also retrieve the NiFi bulletin board, which "
            "lists active warnings and errors reported by components."
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


def generate_workload_token(
    config: UserParameters,
    environment_crn: str,
) -> Any:
    """
    Mint a short-lived DataFlow workload authentication token.

    This is the credential the DFX gateway accepts: a signed JWT issued
    by the CDP control plane for the DF workload on a given environment.
    """

    return run_cdp(
        config,
        [
            "iam",
            "generate-workload-auth-token",
            "--workload-name",
            "DF",
            "--environment-crn",
            environment_crn,
        ],
    )


# ---------------------------------------------------------------------
# NiFi API helpers
# ---------------------------------------------------------------------

def normalize_base_url(raw_url: str) -> str:
    """
    Build the `/nifi-api` root from a deployment URL.

    The value this is usually given is the deployment's `nifiUrl`, which
    addresses the NiFi *canvas* — it ends in `/nifi` and may carry a
    query string or a `#` fragment naming a process group. The REST API
    is a sibling of that path, not a child of it, so the `/nifi` segment
    is replaced rather than appended to.

    All of these therefore yield the same root:

        https://dfx.<env>.cloudera.site/<namespace>
        https://dfx.<env>.cloudera.site/<namespace>/
        https://dfx.<env>.cloudera.site/<namespace>/nifi
        https://dfx.<env>.cloudera.site/<namespace>/nifi/
        https://dfx.<env>.cloudera.site/<namespace>/nifi/#/process-groups/abc
        https://dfx.<env>.cloudera.site/<namespace>/nifi-api
    """

    # A fragment is client-side only and a query string selects a view
    # in the canvas; neither belongs in an API path.
    url = raw_url.strip().split("#", 1)[0].split("?", 1)[0].rstrip("/")

    if url.endswith("/nifi-api"):
        return url

    if url.endswith("/nifi"):
        url = url[: -len("/nifi")]

    return f"{url}/nifi-api"


def get_json(
    session: requests.Session,
    api_root: str,
    path: str,
    params: Optional[dict] = None,
) -> Any:
    """
    Perform one read-only GET against the NiFi REST API.

    Returns the parsed JSON body on success. On any failure, returns a
    dict containing an `error` key describing what went wrong, so that
    a single failing endpoint degrades that section of the report
    rather than failing the whole tool call.
    """

    url = f"{api_root}{path}"

    try:

        # Redirects are not followed. A gateway that wants interactive
        # SSO answers by redirecting to its login endpoint, and
        # following that chain yields an HTML login page with a 200
        # status, which is far harder to diagnose than the redirect
        # itself.
        response = session.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )

    except requests.exceptions.SSLError as exc:

        return {
            "error": "TLS verification failed for the NiFi endpoint.",
            "url": url,
            "detail": str(exc),
        }

    except requests.exceptions.ConnectTimeout:

        return {
            "error": (
                f"Connection to the NiFi endpoint timed out after "
                f"{REQUEST_TIMEOUT_SECONDS} seconds."
            ),
            "url": url,
        }

    except requests.exceptions.ReadTimeout:

        return {
            "error": (
                f"The NiFi endpoint did not respond within "
                f"{REQUEST_TIMEOUT_SECONDS} seconds."
            ),
            "url": url,
        }

    except requests.exceptions.RequestException as exc:

        return {
            "error": "The request to the NiFi endpoint failed.",
            "url": url,
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }

    if 300 <= response.status_code < 400:

        return {
            "error": (
                "The gateway redirected the request instead of serving "
                "the NiFi API, which indicates it wants an interactive "
                "browser login rather than a bearer token."
            ),
            "url": url,
            "status_code": response.status_code,
            "redirected_to": response.headers.get("Location", ""),
        }

    # -----------------------------------------------------------------
    # Authentication and authorization failures.
    #
    # The gateway is an OAuth2 resource server, so it explains itself
    # in the WWW-Authenticate header: an expired or malformed token
    # reports `invalid_token` there, while a token that is valid but
    # not permitted reports insufficient scope. That header is far
    # more informative than the status code, so it is passed through.
    # -----------------------------------------------------------------

    if response.status_code in (401, 403):

        return {
            "error": (
                "The gateway rejected the request as unauthenticated or "
                "unauthorized."
            ),
            "url": url,
            "status_code": response.status_code,
            "www_authenticate": response.headers.get("WWW-Authenticate", ""),
            "hint": (
                "A 401, or a 403 whose WWW-Authenticate mentions "
                "invalid_token, means the token itself was refused. A "
                "403 with no WWW-Authenticate header means the token was "
                "accepted and the request was still not permitted, which "
                "has two quite different causes: the URL may address a "
                "path this deployment does not own, or the CDP user may "
                "lack a DataFlow role on it. Check the URL first -- it "
                "must carry the deployment's own namespace segment, as "
                "deployment.nifiUrl does and deployment.dfxLocalUrl does "
                "not. Expiry is not a likely cause, since a token is "
                "minted per tool call. HTTP Basic authentication is not "
                "an option here either: NiFi behind this gateway reports "
                "supportsLogin: false."
            ),
        }

    if response.status_code == 404:

        return {
            "error": "The NiFi endpoint or process group was not found.",
            "url": url,
            "status_code": response.status_code,
            "hint": (
                "Confirm the deployment's base URL and that the process "
                "group ID exists."
            ),
        }

    if not response.ok:

        return {
            "error": "The NiFi REST API returned an error response.",
            "url": url,
            "status_code": response.status_code,
            "raw_output": response.text[:MAX_RAW_OUTPUT_CHARS],
        }

    # -----------------------------------------------------------------
    # Parse the response body
    # -----------------------------------------------------------------

    body = response.text.strip()

    if not body:

        return {
            "error": (
                "The NiFi REST API returned a successful but empty "
                "response."
            ),
            "url": url,
            "status_code": response.status_code,
        }

    try:
        return response.json()

    except ValueError:

        return {
            "error": (
                "The NiFi REST API returned output that could not be "
                "parsed as JSON."
            ),
            "url": url,
            "status_code": response.status_code,
            "raw_output": body[:MAX_RAW_OUTPUT_CHARS],
            "hint": (
                "An HTML body usually means the gateway returned a login "
                "page instead of an API response, which indicates an "
                "authentication problem."
            ),
        }


# ---------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------

def run_tool(config: UserParameters, args: ToolParameters) -> Any:
    """
    Retrieve the current state of a NiFi flow from the NiFi REST API.

    All operations are read-only.

    Returns a dict whose sections hold the actual NiFi API responses.
    Each section is returned verbatim so the agent can inspect every
    field NiFi reported, and any section that failed carries its own
    `error` key.
    """

    result: dict = {
        "deployment_crn": args.deployment_crn,
        "process_group_id": args.process_group_id,
    }

    # -----------------------------------------------------------------
    # Step 1: resolve the deployment through the control plane.
    #
    # This supplies both the NiFi endpoint and the environment CRN the
    # token is scoped to, so the agent only has to know a CRN. It is
    # skipped when both have been pinned in tool configuration.
    # -----------------------------------------------------------------

    base_url = config.NIFI_BASE_URL
    environment_crn = config.DF_ENVIRONMENT_CRN

    if not (base_url and environment_crn):

        described = describe_deployment(config, args.deployment_crn)

        if isinstance(described, dict) and "error" in described:
            result["deployment_lookup"] = described
            result["request_status"] = {
                "all_requests_succeeded": False,
                "failed_sections": ["deployment_lookup"],
            }
            return result

        deployment = (described or {}).get("deployment") or {}

        # Reported for context: the agent should be able to say which
        # deployment answered and what the control plane thinks of it,
        # alongside what NiFi itself reports.
        result["deployment"] = {
            "name": deployment.get("name"),
            "status": deployment.get("status"),
            "cfm_nifi_version": deployment.get("cfmNifiVersion"),
            "cluster_size": deployment.get("clusterSize"),
            "active_error_alert_count": deployment.get("activeErrorAlertCount"),
            "active_warning_alert_count": deployment.get(
                "activeWarningAlertCount"
            ),
        }

        # `nifiUrl`, not `dfxLocalUrl`. Both are returned, and only one
        # is right: dfxLocalUrl is the base of the shared dfx-local
        # instance hosting many deployments, so it omits the
        # per-deployment namespace segment that the gateway routes on.
        # Requests built from it reach the gateway and are answered with
        # 403 — authenticated, but not entitled to that path — which
        # reads exactly like a missing DataFlow role.
        base_url = base_url or deployment.get("nifiUrl")
        environment_crn = environment_crn or (
            (deployment.get("service") or {}).get("environmentCrn")
        )

        missing = [
            name
            for name, value in (
                ("nifiUrl", base_url),
                ("service.environmentCrn", environment_crn),
            )
            if not value
        ]

        if missing:
            result["deployment_lookup"] = {
                "error": (
                    "The deployment description did not include the "
                    "fields needed to reach NiFi."
                ),
                "missing_fields": missing,
                "hint": (
                    "Set NIFI_BASE_URL and DF_ENVIRONMENT_CRN in the tool "
                    "configuration to bypass this lookup."
                ),
            }
            result["request_status"] = {
                "all_requests_succeeded": False,
                "failed_sections": ["deployment_lookup"],
            }
            return result

    # -----------------------------------------------------------------
    # Step 2: mint a workload token for the DF workload.
    #
    # This is the credential the DFX gateway accepts. It is short
    # lived, so it is minted per call and never persisted.
    # -----------------------------------------------------------------

    token_response = generate_workload_token(config, environment_crn)

    if isinstance(token_response, dict) and "error" in token_response:
        result["authentication"] = token_response
        result["request_status"] = {
            "all_requests_succeeded": False,
            "failed_sections": ["authentication"],
        }
        return result

    token = (token_response or {}).get("token")

    if not token:
        result["authentication"] = {
            "error": (
                "The CDP control plane did not return a workload "
                "authentication token."
            ),
            "returned_fields": sorted((token_response or {}).keys()),
        }
        result["request_status"] = {
            "all_requests_succeeded": False,
            "failed_sections": ["authentication"],
        }
        return result

    # The token itself is deliberately not included. Only the fact that
    # one was obtained and when it expires are reported, so a token
    # never reaches the LLM context or a log.
    result["authentication"] = {
        "token_obtained": True,
        "expires_at": token_response.get("expireAt"),
    }

    api_root = normalize_base_url(base_url)
    result["nifi_api_root"] = api_root

    # Also reported unmodified, so that a base URL which normalized into
    # something unexpected can be told apart from one that was wrong to
    # begin with.
    result["nifi_url_source"] = base_url

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })

    # -----------------------------------------------------------------
    # Step 3: read the canvas.
    # -----------------------------------------------------------------

    # NiFi version, so the report states which NiFi answered.
    result["about"] = get_json(session, api_root, "/flow/about")

    # Controller-level tallies: running / stopped / invalid / disabled
    # component counts, active threads, and total queued FlowFiles.
    result["controller_status"] = get_json(session, api_root, "/flow/status")

    # Process group status: per-component run states, queue depths,
    # and throughput for the selected part of the canvas.
    result["process_group_status"] = get_json(
        session,
        api_root,
        f"/flow/process-groups/{args.process_group_id}/status",
        params={"recursive": str(args.recursive).lower()},
    )

    # Active bulletins: the warnings and errors NiFi components are
    # currently reporting.
    if args.include_bulletins:

        result["bulletin_board"] = get_json(
            session,
            api_root,
            "/flow/bulletin-board",
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
        description="Inspect a NiFi flow through the NiFi REST API."
    )

    parser.add_argument(
        "--user-params",
        required=True,
        help=(
            "JSON object containing CDP_ACCESS_KEY_ID and "
            "CDP_PRIVATE_KEY, and optionally NIFI_BASE_URL and "
            "DF_ENVIRONMENT_CRN."
        ),
    )

    parser.add_argument(
        "--tool-params",
        required=True,
        help=(
            "JSON object containing deployment_crn, and optionally "
            "process_group_id, recursive and include_bulletins."
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
