"""
Cloudera DataFlow NiFi deployment inspection tool.

This Agent Studio tool performs a read-only `describe-deployment`
request against Cloudera DataFlow Public Cloud using the CDP CLI
Python module.

The tool invokes the CDP CLI through the same Python interpreter
running the Agent Studio tool, entering at `cdpcli.clidriver.main`
rather than running the module:

    python -c 'import sys; from cdpcli.clidriver import main; sys.exit(main())'

Authentication is supplied through Agent Studio project environment
variables:

    CDP_ACCESS_KEY_ID
    CDP_PRIVATE_KEY

The agent only needs to provide:

    deployment_crn

On success, the tool returns the actual JSON response from
Cloudera DataFlow directly to the agent.
"""

from pydantic import BaseModel, Field
from typing import Any
import argparse
import json
import os
import subprocess
import sys


# ---------------------------------------------------------------------
# User / Project Parameters
# ---------------------------------------------------------------------

class UserParameters(BaseModel):
    """
    Credentials supplied through Agent Studio project configuration.

    These values are injected by Agent Studio and should not be
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
            "CRN of the Cloudera DataFlow deployment to inspect. "
            "Example: crn:cdp:df:us-west-1:..."
        )
    )


# ---------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------

def run_tool(config: UserParameters, args: ToolParameters) -> Any:
    """
    Retrieve the current Cloudera DataFlow deployment information.

    This operation is read-only.

    On success, the complete JSON response returned by
    `df describe-deployment` is returned directly to the agent.
    """

    # -----------------------------------------------------------------
    # Build subprocess environment
    # -----------------------------------------------------------------

    env = os.environ.copy()

    env["CDP_ACCESS_KEY_ID"] = config.CDP_ACCESS_KEY_ID
    env["CDP_PRIVATE_KEY"] = config.CDP_PRIVATE_KEY

    # -----------------------------------------------------------------
    # Execute CDP CLI through the same Python interpreter running
    # this Agent Studio tool.
    # -----------------------------------------------------------------

    # Invoked through `-c` rather than `-m cdpcli.clidriver`, because
    # cdpcli.clidriver has no `if __name__ == "__main__"` guard: running
    # it as a module is a silent no-op that exits 0 having printed
    # nothing, which looks exactly like a command that returned no data.
    # `main()` is the entry point the installed `cdp` script itself uses.
    command = [
        sys.executable,
        "-c",
        "import sys; from cdpcli.clidriver import main; sys.exit(main())",
        "df",
        "describe-deployment",
        "--deployment-crn",
        args.deployment_crn,
        "--output",
        "json",
    ]

    try:

        result = subprocess.run(
            command,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # -------------------------------------------------------------
        # CDP CLI returned an error
        # -------------------------------------------------------------

        if result.returncode != 0:

            return {
                "error": "Cloudera DataFlow describe-deployment failed.",
                "deployment_crn": args.deployment_crn,
                "return_code": result.returncode,
                "stderr": result.stderr.strip(),
            }

        # -------------------------------------------------------------
        # Parse actual CDP response
        # -------------------------------------------------------------

        stdout = result.stdout.strip()

        if not stdout:

            # stderr is included because the CLI can exit 0 while writing
            # a diagnostic there, and that text is the whole diagnosis.
            return {
                "error": (
                    "Cloudera DataFlow describe-deployment completed "
                    "successfully but returned an empty response."
                ),
                "deployment_crn": args.deployment_crn,
                "stderr": result.stderr.strip(),
            }

        try:
            response = json.loads(stdout)

        except json.JSONDecodeError:

            return {
                "error": (
                    "Cloudera DataFlow returned output that could not "
                    "be parsed as JSON."
                ),
                "deployment_crn": args.deployment_crn,
                "raw_output": stdout,
            }

        # -------------------------------------------------------------
        # IMPORTANT:
        #
        # Return the actual Cloudera DataFlow API response directly.
        #
        # Do not wrap it inside:
        #
        #     {"success": True, "response": response}
        #
        # The Agent Studio agent can now inspect all fields returned
        # by describe-deployment.
        # -------------------------------------------------------------

        return response

    except subprocess.TimeoutExpired:

        return {
            "error": "Cloudera DataFlow request timed out after 60 seconds.",
            "deployment_crn": args.deployment_crn,
        }

    except Exception as exc:

        return {
            "error": str(exc),
            "error_type": type(exc).__name__,
            "deployment_crn": args.deployment_crn,
        }


# ---------------------------------------------------------------------
# Local / CAI CLI execution
# ---------------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Inspect a Cloudera DataFlow deployment."
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
        help="JSON object containing deployment_crn.",
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
