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

Authentication is attempted as HTTP Basic against the DFX gateway,
using CDP workload credentials supplied through Agent Studio tool
configuration:

    NIFI_BASE_URL
    NIFI_USERNAME
    NIFI_PASSWORD

IMPORTANT, UNVERIFIED MECHANISM:

Cloudera documents interactive SSO for reaching the NiFi UI of a
CDF Public Cloud deployment, and documents HTTP Basic authentication
over the DFX gateway only for the Prometheus metrics endpoint, which
uses a dedicated `nifi-metrics` credential rather than a user's
workload password. No Cloudera documentation was found describing a
supported way to call general `/nifi-api` endpoints programmatically
through the gateway.

This tool therefore attempts the most plausible mechanism, and is
written so that an unsupported one fails legibly: a gateway redirect
to SSO is reported as such rather than followed into an HTML login
page, and the 401 path says that the mechanism itself may be
unsupported. If this approach does not work in your environment, the
documented alternatives are the Prometheus metrics endpoint or the
Cloudera DataFlow control plane API.

The agent only needs to provide:

    process_group_id    (defaults to the root canvas)

All requests are read-only. This tool cannot start, stop, or modify
anything in the flow.
"""

from pydantic import BaseModel, Field
from typing import Any, Optional
import argparse
import json

import requests
from requests.auth import HTTPBasicAuth


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

REQUEST_TIMEOUT_SECONDS = 30

# Truncation limit for non-JSON response bodies. A failed gateway auth
# typically returns an HTML login page, and the agent does not need
# the whole document to understand what went wrong.
MAX_RAW_OUTPUT_CHARS = 2000


# ---------------------------------------------------------------------
# User / Project Parameters
# ---------------------------------------------------------------------

class UserParameters(BaseModel):
    """
    Connection details and credentials supplied through Agent Studio
    tool configuration.

    These values are injected by Agent Studio and should not be
    supplied by the LLM.
    """

    NIFI_BASE_URL: str
    NIFI_USERNAME: str
    NIFI_PASSWORD: str


# ---------------------------------------------------------------------
# Tool Parameters
# ---------------------------------------------------------------------

class ToolParameters(BaseModel):
    """
    Arguments supplied by the agent when invoking this tool.
    """

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
# Helpers
# ---------------------------------------------------------------------

def normalize_base_url(raw_url: str) -> str:
    """
    Build the `/nifi-api` root from a user-supplied base URL.

    Accepts the deployment URL with or without a trailing slash, and
    with or without a trailing `/nifi-api`, so that all of these work:

        https://dfx.<env>.cloudera.site/<namespace>
        https://dfx.<env>.cloudera.site/<namespace>/
        https://dfx.<env>.cloudera.site/<namespace>/nifi-api
    """

    url = raw_url.strip().rstrip("/")

    if url.endswith("/nifi-api"):
        return url

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

        # Redirects are not followed. A Knox-style gateway answers an
        # unsupported API login by redirecting to its SSO endpoint, and
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

    # -----------------------------------------------------------------
    # Gateway redirect, which in practice means SSO.
    #
    # Cloudera documents browser SSO for the NiFi UI and does not
    # document a general programmatic auth mechanism for /nifi-api
    # through the DFX gateway. If the gateway bounces this request to
    # an SSO endpoint, HTTP Basic authentication is not accepted here
    # and no credential fix will help — the access method itself has
    # to change. That is worth saying plainly.
    # -----------------------------------------------------------------

    if 300 <= response.status_code < 400:

        location = response.headers.get("Location", "")

        return {
            "error": (
                "The gateway redirected the request instead of serving "
                "the NiFi API, which indicates it requires interactive "
                "SSO rather than HTTP Basic authentication."
            ),
            "url": url,
            "status_code": response.status_code,
            "redirected_to": location,
            "hint": (
                "HTTP Basic authentication with CDP workload "
                "credentials is not an access method Cloudera "
                "documents for /nifi-api through the DFX gateway. "
                "Documented alternatives are the Prometheus metrics "
                "endpoint, which uses a dedicated nifi-metrics "
                "credential, or the Cloudera DataFlow control plane "
                "API. See the repository README."
            ),
        }

    # -----------------------------------------------------------------
    # Authentication and authorization failures.
    #
    # These are the most common setup problems, so they are reported
    # distinctly with a hint rather than as a generic HTTP error.
    # -----------------------------------------------------------------

    if response.status_code in (401, 403):

        return {
            "error": (
                "NiFi rejected the request as unauthenticated or "
                "unauthorized."
            ),
            "url": url,
            "status_code": response.status_code,
            "hint": (
                "Confirm the CDP workload username and workload "
                "password are correct, that a workload password has "
                "been set for the user, and that the user has been "
                "granted a DataFlow role that permits viewing this "
                "deployment in NiFi. Note that HTTP Basic "
                "authentication against /nifi-api through the DFX "
                "gateway is not an access method Cloudera documents, "
                "so a persistent rejection here may mean the "
                "mechanism is unsupported rather than the credentials "
                "being wrong. See the repository README."
            ),
        }

    if response.status_code == 404:

        return {
            "error": (
                "The NiFi endpoint or process group was not found."
            ),
            "url": url,
            "status_code": response.status_code,
            "hint": (
                "Confirm NIFI_BASE_URL points at the deployment root "
                "and that the process group ID exists."
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
                "An HTML body usually means the gateway returned a "
                "login page instead of an API response, which "
                "indicates an authentication problem."
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

    api_root = normalize_base_url(config.NIFI_BASE_URL)

    session = requests.Session()
    session.auth = HTTPBasicAuth(config.NIFI_USERNAME, config.NIFI_PASSWORD)
    session.headers.update({"Accept": "application/json"})

    result: dict = {
        "nifi_api_root": api_root,
        "process_group_id": args.process_group_id,
    }

    # -----------------------------------------------------------------
    # NiFi version, so the report states which NiFi answered.
    # -----------------------------------------------------------------

    result["about"] = get_json(session, api_root, "/flow/about")

    # -----------------------------------------------------------------
    # Controller-level tallies: running / stopped / invalid / disabled
    # component counts, active threads, and total queued FlowFiles.
    # -----------------------------------------------------------------

    result["controller_status"] = get_json(session, api_root, "/flow/status")

    # -----------------------------------------------------------------
    # Process group status: per-component run states, queue depths,
    # and throughput for the selected part of the canvas.
    # -----------------------------------------------------------------

    result["process_group_status"] = get_json(
        session,
        api_root,
        f"/flow/process-groups/{args.process_group_id}/status",
        params={"recursive": str(args.recursive).lower()},
    )

    # -----------------------------------------------------------------
    # Active bulletins: the warnings and errors NiFi components are
    # currently reporting.
    # -----------------------------------------------------------------

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
            "JSON object containing NIFI_BASE_URL, NIFI_USERNAME and "
            "NIFI_PASSWORD."
        ),
    )

    parser.add_argument(
        "--tool-params",
        required=True,
        help=(
            "JSON object optionally containing process_group_id, "
            "recursive and include_bulletins."
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
