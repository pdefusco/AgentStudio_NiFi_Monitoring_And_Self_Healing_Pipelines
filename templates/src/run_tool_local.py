#!/usr/bin/env python3
"""
Run a template's tool locally, outside Agent Studio.

Agent Studio injects a tool's `UserParameters` from its configuration.
Locally there is nothing to inject them, and passing them on a command
line means a private key ends up in the shell history and in the process
list. This script reads each `UserParameters` field from an environment
variable of the same name instead, so credentials are never written down
anywhere.

Usage:

    export CDP_ACCESS_KEY_ID=...
    export CDP_PRIVATE_KEY=...        # the key itself, or a path to it

    python templates/src/run_tool_local.py nifi_canvas_monitoring \
        --tool-params '{"deployment_crn":"crn:cdp:df:..."}'

Optional fields that have no environment variable set are left at their
defaults. Missing required fields are reported together, by name, rather
than as a pydantic traceback.
"""

from pathlib import Path
from typing import Any
import argparse
import importlib.util
import json
import os
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "templates" / "src"

MANIFEST_NAME = "workflow_template.json"


def load_tool_module(tool_py: Path):
    """Import a tool.py by path, without it needing to be on sys.path."""

    spec = importlib.util.spec_from_file_location("tool_under_test", tool_py)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def find_tool(source_dir: Path, tool_name: str | None) -> tuple[str, Path]:
    """
    Locate the tool to run, using the template's own manifest as the
    source of truth rather than guessing from the directory layout.
    """

    manifest_path = source_dir / MANIFEST_NAME

    if not manifest_path.is_file():
        sys.exit(f"error: {manifest_path} not found")

    tools = json.loads(manifest_path.read_text()).get("tool_templates", [])

    if not tools:
        sys.exit(f"error: {manifest_path} declares no tools")

    if tool_name:
        matching = [t for t in tools if t["name"] == tool_name]

        if not matching:
            available = ", ".join(sorted(t["name"] for t in tools))
            sys.exit(
                f"error: no tool named {tool_name!r} in this template. "
                f"Available: {available}"
            )

        tool = matching[0]

    elif len(tools) > 1:
        available = ", ".join(sorted(t["name"] for t in tools))
        sys.exit(
            f"error: this template has several tools, pick one with "
            f"--tool: {available}"
        )

    else:
        tool = tools[0]

    tool_py = (
        source_dir
        / tool["source_folder_path"]
        / tool["python_code_file_name"]
    )

    if not tool_py.is_file():
        sys.exit(f"error: {tool_py} not found")

    return tool["name"], tool_py


def build_user_parameters(module) -> Any:
    """
    Populate UserParameters from the environment, one variable per field.

    Required fields that are absent are collected and reported together,
    so a missing credential is one clear message instead of a traceback.
    """

    fields = module.UserParameters.model_fields

    values = {
        name: os.environ[name]
        for name in fields
        if name in os.environ
    }

    missing = [
        name
        for name, field in fields.items()
        if field.is_required() and name not in values
    ]

    if missing:
        sys.exit(
            "error: these environment variables are required by this "
            "tool's UserParameters but are not set:\n"
            + "".join(f"  export {name}=...\n" for name in missing)
        )

    skipped = sorted(set(fields) - set(values))

    if skipped:
        print(
            f"note: using defaults for {', '.join(skipped)}",
            file=sys.stderr,
        )

    return module.UserParameters(**values)


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Run a template's tool locally, taking UserParameters from "
            "the environment."
        )
    )

    parser.add_argument(
        "template",
        help="Directory name under templates/src/, e.g. nifi_canvas_monitoring",
    )

    parser.add_argument(
        "--tool-params",
        default="{}",
        help="JSON object of ToolParameters, i.e. what the agent supplies",
    )

    parser.add_argument(
        "--tool",
        help="Tool name, only needed when a template declares more than one",
    )

    args = parser.parse_args()

    source_dir = SRC_ROOT / args.template

    if not source_dir.is_dir():
        sys.exit(f"error: template not found: {source_dir}")

    tool_name, tool_py = find_tool(source_dir, args.tool)

    module = load_tool_module(tool_py)

    config = build_user_parameters(module)

    try:
        tool_params = json.loads(args.tool_params)

    except ValueError as exc:
        sys.exit(f"error: --tool-params is not valid JSON: {exc}")

    print(f"running {tool_name}", file=sys.stderr)

    output = module.run_tool(
        config=config,
        args=module.ToolParameters(**tool_params),
    )

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
