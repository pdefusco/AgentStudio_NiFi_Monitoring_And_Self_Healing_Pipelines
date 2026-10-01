#!/usr/bin/env python3
"""
Build an Agent Studio workflow template .zip from a source directory.

Agent Studio exports a workflow template as a zip containing a
`workflow_template.json` manifest at the root plus a `studio-data/`
tree holding each tool's `tool.py` and `requirements.txt`. The
templates in this repo keep that tree unpacked under `templates/src/`
so the tool code is reviewable and diffable in git, and this script
packs a source directory back into the layout Agent Studio imports.

Usage:

    python templates/src/build_template.py nifi_canvas_monitoring

    python templates/src/build_template.py nifi_canvas_monitoring \
        --output templates/workflow_template_gvie9za2.zip

Validation performed before writing the zip:

- the manifest parses as JSON
- cross-references between workflow, agents, tasks and tools resolve
- every tool's `source_folder_path` exists and contains the declared
  python and requirements files
"""

from pathlib import Path
import argparse
import json
import sys
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "templates" / "src"

MANIFEST_NAME = "workflow_template.json"

# Empty directories Agent Studio includes in its own exports. They are
# recreated so a hand-built bundle matches an exported one.
ICON_DIRS = [
    "studio-data/dynamic_assets/agent_template_icons/",
    "studio-data/dynamic_assets/mcp_template_icons/",
    "studio-data/dynamic_assets/tool_template_icons/",
]

EXCLUDED_NAMES = {"__pycache__", ".DS_Store"}


def validate(source_dir: Path) -> dict:
    """
    Parse and sanity-check the manifest. Returns it on success;
    exits with a message listing every problem found.
    """

    manifest_path = source_dir / MANIFEST_NAME

    if not manifest_path.is_file():
        sys.exit(f"error: {manifest_path} not found")

    try:
        manifest = json.loads(manifest_path.read_text())

    except json.JSONDecodeError as exc:
        sys.exit(f"error: {manifest_path} is not valid JSON: {exc}")

    problems: list[str] = []

    workflow = manifest.get("workflow_template", {})
    workflow_id = workflow.get("id")

    agents = {a["id"]: a for a in manifest.get("agent_templates", [])}
    tasks = {t["id"]: t for t in manifest.get("task_templates", [])}
    tools = {t["id"]: t for t in manifest.get("tool_templates", [])}

    # Workflow references resolve
    for agent_id in workflow.get("agent_template_ids", []):
        if agent_id not in agents:
            problems.append(f"workflow references unknown agent {agent_id}")

    for task_id in workflow.get("task_template_ids", []):
        if task_id not in tasks:
            problems.append(f"workflow references unknown task {task_id}")

    # Agent references resolve, and backreferences match
    for agent in agents.values():
        if agent.get("workflow_template_id") != workflow_id:
            problems.append(f"agent {agent['name']} has a stale workflow_template_id")

        for tool_id in agent.get("tool_template_ids", []):
            if tool_id not in tools:
                problems.append(
                    f"agent {agent['name']} references unknown tool {tool_id}"
                )

    # Task assignments resolve
    for task in tasks.values():
        if task.get("workflow_template_id") != workflow_id:
            problems.append("a task has a stale workflow_template_id")

        assigned = task.get("assigned_agent_template_id")

        if assigned not in agents:
            problems.append(f"task is assigned to unknown agent {assigned}")

    # Tool source folders exist and hold the declared files
    for tool in tools.values():
        if tool.get("workflow_template_id") != workflow_id:
            problems.append(f"tool {tool['name']} has a stale workflow_template_id")

        folder = source_dir / tool["source_folder_path"]

        if not folder.is_dir():
            problems.append(
                f"tool {tool['name']}: source_folder_path missing: "
                f"{tool['source_folder_path']}"
            )
            continue

        for key in ("python_code_file_name", "python_requirements_file_name"):
            file_name = tool.get(key)

            if file_name and not (folder / file_name).is_file():
                problems.append(f"tool {tool['name']}: {file_name} not found")

    if problems:
        print("error: manifest validation failed", file=sys.stderr)

        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)

        sys.exit(1)

    return manifest


def build(source_dir: Path, output_path: Path) -> None:
    """Pack the source directory into an Agent Studio template zip."""

    manifest = validate(source_dir)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:

        # Directory entries first, mirroring an Agent Studio export.
        archive.writestr("studio-data/", "")
        archive.write(source_dir / MANIFEST_NAME, MANIFEST_NAME)
        archive.writestr("studio-data/dynamic_assets/", "")
        archive.writestr("studio-data/tool_templates/", "")

        for tool in manifest.get("tool_templates", []):

            folder_rel = tool["source_folder_path"].rstrip("/")
            archive.writestr(f"{folder_rel}/", "")

            for path in sorted((source_dir / folder_rel).rglob("*")):

                if any(part in EXCLUDED_NAMES for part in path.parts):
                    continue

                if path.is_file():
                    archive.write(path, str(path.relative_to(source_dir)))

        for icon_dir in ICON_DIRS:
            archive.writestr(icon_dir, "")

    try:
        display_path = output_path.relative_to(REPO_ROOT)

    except ValueError:
        # Output written outside the repo, e.g. to a temp directory.
        display_path = output_path

    print(f"built {display_path}")

    with zipfile.ZipFile(output_path) as archive:
        for info in archive.infolist():
            print(f"  {info.file_size:>7}  {info.filename}")


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Build an Agent Studio workflow template zip."
    )

    parser.add_argument(
        "template",
        help=(
            "Name of the source directory under templates/src/, "
            "e.g. nifi_canvas_monitoring"
        ),
    )

    parser.add_argument(
        "--output",
        help=(
            "Output zip path. Defaults to "
            "templates/workflow_template_<template>.zip"
        ),
    )

    args = parser.parse_args()

    source_dir = SRC_ROOT / args.template

    if not source_dir.is_dir():
        sys.exit(f"error: source directory not found: {source_dir}")

    if args.output:
        output_path = Path(args.output)

        if not output_path.is_absolute():
            output_path = REPO_ROOT / output_path

    else:
        output_path = (
            REPO_ROOT / "templates" / f"workflow_template_{args.template}.zip"
        )

    build(source_dir, output_path)


if __name__ == "__main__":
    main()
