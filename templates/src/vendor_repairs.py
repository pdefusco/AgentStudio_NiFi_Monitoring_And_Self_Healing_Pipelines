#!/usr/bin/env python3
"""
Copy the repair whitelist into the Agent Studio tools that need it.

Agent Studio imports a tool as a single file. A tool therefore cannot
`import repairs`, and the repo's own design note says `templates/` and
`self_healing/` share no code in either direction on purpose. So the
repair logic has to exist twice.

What this script buys is that the second copy is *generated*. A hand
copy of ~580 lines drifts the first time either side is edited, and the
drift is invisible in the worst possible way: `self_healing/repairs.py`
would still pass its 31 self-tests, the tool would still run, and the
two would quietly disagree about what `speed_up_slow_drain` means. One
of them decides what the agent is shown; the other decides what gets
imported into a permanent catalog version. They must be the same code.

The region copied is delimited in the source file by:

    # --- BEGIN VENDORED REGION ---
    # --- END VENDORED REGION ---

and it is spliced into each destination between:

    # --- BEGIN VENDORED repairs.py ---
    # --- END VENDORED repairs.py ---

Usage:

    python templates/src/vendor_repairs.py            # write the copies
    python templates/src/vendor_repairs.py --check    # verify, exit 1 on drift

`--check` is the one that matters. Run it before building a zip: a stale
vendored copy inside a shipped template cannot be detected by
`build_template.py`, which never parses `tool.py` at all.
"""

import argparse
import ast
import hashlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

SOURCE = REPO_ROOT / "self_healing" / "repairs.py"

SOURCE_BEGIN = "# --- BEGIN VENDORED REGION ---"
SOURCE_END = "# --- END VENDORED REGION ---"

DEST_BEGIN = "# --- BEGIN VENDORED repairs.py ---"
DEST_END = "# --- END VENDORED repairs.py ---"

# All three tools carry the copy, including the read-only one.
#
# It is tempting to leave `getDeployedFlow` out on the grounds that a
# tool which only reads has no business holding code that knows how to
# edit a flow. The opposite matters more: `getDeployedFlow` is what the
# agent is *shown*, and `validate_edit()` is what polices the edit. If
# those two derive their view of the flow from different code, the agent
# can be shown a processor inventory that disagrees with the one the
# validator checks against — and a disagreement there is precisely the
# gap an edit could pass through. Sharing `inventory()` makes the view
# the agent reasons about and the view the validator enforces the same
# view by construction. The read tool simply never calls `apply()`.
DESTINATIONS = (
    REPO_ROOT / "templates/src/nifi_flow_repair/studio-data/tool_templates"
    "/getdeployedflow_3mFBJGxl/tool.py",
    REPO_ROOT / "templates/src/nifi_flow_repair/studio-data/tool_templates"
    "/proposeflowrepair_On1fGSOC/tool.py",
    REPO_ROOT / "templates/src/nifi_flow_repair/studio-data/tool_templates"
    "/applyflowrepair_1imeQOp7/tool.py",
)


def extract(text: str, begin: str, end: str, what: str) -> str:
    """
    Return the lines strictly between the two markers.

    Both markers are matched as a line *prefix* rather than by equality,
    because the banner comments carry trailing dashes to the margin and
    those get adjusted by anyone reformatting the file. Missing or
    out-of-order markers are a hard error: silently vendoring nothing
    would produce a tool that imports cleanly and then fails on the
    first call with a NameError.
    """

    lines = text.splitlines()

    starts = [i for i, line in enumerate(lines) if line.startswith(begin)]
    ends = [i for i, line in enumerate(lines) if line.startswith(end)]

    for marker, found in ((begin, starts), (end, ends)):
        if not found:
            raise SystemExit(f"error: {what} has no {marker!r} marker.")

        if len(found) > 1:
            raise SystemExit(
                f"error: {what} has {len(found)} {marker!r} markers; "
                "exactly one is required."
            )

    if starts[0] > ends[0]:
        raise SystemExit(
            f"error: {what} has its markers in the wrong order."
        )

    return "\n".join(lines[starts[0] + 1:ends[0]])


def modules_used(region: str) -> set:
    """
    Names the region uses as `name.attribute` but never binds itself.

    Such a name can only be resolved by an import in the destination, and
    the region cannot carry the import itself: `import copy` sits above
    the source file's BEGIN marker, where it belongs, because a module
    whose imports are scattered through its body is worse than one whose
    vendoring script is slightly cleverer.

    This exists because the first run of the generated tools failed with
    `NameError: name 'copy' is not defined` — on the fifth guardrail, deep
    inside `apply()`, long after the tool had imported cleanly and
    answered three other calls correctly. A missing import in vendored
    code is not a syntax error and not an import error; it is a runtime
    error on one branch. So the dependency has to be checked at vendoring
    time, when it is still cheap to see.

    The test is "used with a dot, never assigned anywhere in the region".
    A local variable would be assigned, so it does not appear here; a
    module reference is not, so it does. This over-reports rather than
    under-reports, which is the right direction for a check whose job is
    to refuse to generate a broken tool.
    """

    tree = ast.parse(region)

    bound = set()

    for node in ast.walk(tree):

        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            bound.add(node.name)

        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)

        elif isinstance(node, ast.arg):
            bound.add(node.arg)

        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                bound.add(alias.asname or alias.name.split(".")[0])

    used = {
        node.value.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
    }

    return used - bound - set(dir(__builtins__))


def modules_imported(text: str) -> set:
    """Top-level module names a destination file imports."""

    imported = set()

    for node in ast.walk(ast.parse(text)):

        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.asname or alias.name.split(".")[0])

        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported.add(alias.asname or alias.name)

    return imported


def banner(source_digest: str) -> str:
    """
    The header written above each vendored copy.

    It carries the digest of the region it was generated from, so a
    reader of the *tool* — who may never see this script — can tell
    whether the copy is current without diffing 580 lines by eye.
    """

    return (
        f"{DEST_BEGIN}\n"
        "#\n"
        "# Generated by templates/src/vendor_repairs.py. Do not edit this\n"
        "# region by hand: the next run of that script will overwrite it.\n"
        "#\n"
        "# Source:     self_healing/repairs.py (VENDORED REGION)\n"
        f"# sha256:     {source_digest}\n"
        "#\n"
        "# Agent Studio imports a tool as a single file, so this cannot be\n"
        "# an import. Edit the source and re-run the script.\n"
        "#"
    )


def splice(destination: Path, region: str, source_digest: str) -> str:
    """Return the destination's full text with the region replaced."""

    text = destination.read_text(encoding="utf-8")

    lines = text.splitlines()

    starts = [i for i, line in enumerate(lines) if line.startswith(DEST_BEGIN)]
    ends = [i for i, line in enumerate(lines) if line.startswith(DEST_END)]

    if len(starts) != 1 or len(ends) != 1:
        raise SystemExit(
            f"error: {destination} must contain exactly one "
            f"{DEST_BEGIN!r} and one {DEST_END!r}."
        )

    if starts[0] > ends[0]:
        raise SystemExit(f"error: {destination} has its markers reversed.")

    rebuilt = (
        lines[:starts[0]]
        + banner(source_digest).splitlines()
        + region.splitlines()
        + [DEST_END]
        + lines[ends[0] + 1:]
    )

    return "\n".join(rebuilt) + "\n"


def main() -> int:

    parser = argparse.ArgumentParser(
        description="Vendor the repair whitelist into the Agent Studio tools."
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Do not write. Exit non-zero if any copy differs from the "
            "source, naming which."
        ),
    )

    arguments = parser.parse_args()

    if not SOURCE.exists():
        raise SystemExit(f"error: {SOURCE} does not exist.")

    region = extract(
        SOURCE.read_text(encoding="utf-8"),
        SOURCE_BEGIN,
        SOURCE_END,
        str(SOURCE),
    )

    digest = hashlib.sha256(region.encode("utf-8")).hexdigest()

    needs = modules_used(region)

    print(f"source: {SOURCE.relative_to(REPO_ROOT)}")
    print(f"  {len(region.splitlines())} lines, sha256 {digest[:16]}…")
    print(f"  requires: {', '.join(sorted(needs)) or 'nothing'}")
    print()

    stale = []
    missing = []

    for destination in DESTINATIONS:

        if not destination.exists():
            raise SystemExit(f"error: {destination} does not exist.")

        wanted = splice(destination, region, digest)

        current = destination.read_text(encoding="utf-8")

        relative = destination.relative_to(REPO_ROOT)

        if current == wanted:
            print(f"  up to date   {relative}")

        else:
            stale.append(relative)

            if arguments.check:
                print(f"  STALE        {relative}")

            else:
                destination.write_text(wanted, encoding="utf-8")

                print(f"  written      {relative}")

        # Checked against `wanted` rather than `current`, so a missing
        # import is reported on the copy as it will exist after this run
        # — including in --check mode, where nothing was written.
        absent = sorted(needs - modules_imported(wanted))

        if absent:
            missing.append((relative, absent))

            print(f"               MISSING IMPORT: {', '.join(absent)}")

    print()

    if missing:
        print(
            "error: the vendored region uses modules the destination does "
            "not import.\nAdd these to the tool's own import block, above "
            f"{DEST_BEGIN!r}:\n"
        )

        for relative, absent in missing:
            for module in absent:
                print(f"  {relative}: import {module}")

        return 1

    if arguments.check and stale:
        print(
            f"error: {len(stale)} vendored copy/copies are stale. Run\n"
            "  python templates/src/vendor_repairs.py\n"
            "and rebuild any affected zip."
        )

        return 1

    print("all vendored copies match the source.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
