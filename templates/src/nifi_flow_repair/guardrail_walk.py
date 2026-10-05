#!/usr/bin/env python3
"""
Exercise applyFlowRepair's refusals with a stubbed CDP CLI.

Every guardrail in that tool is a decision about whether to mutate a
shared tenant, so each one is worth testing deliberately rather than
trusting. Stubbing `run_cdp` makes all of them testable with no
credentials, no VPC, and no catalog versions burned — which matters
because a real test of the apply path leaves a permanent flow version
behind every single run.

Run before building the zip, together with:

    python templates/src/vendor_repairs.py --check

The two catch different things, and this file exists because the first
run proved it. `--check` compares text and verifies the vendored region's
imports; it cannot tell whether `apply()` actually runs. The missing
`import copy` that the first generated tools shipped with was invisible
to every static check: the tools imported cleanly, exposed the right
contract, and answered three guardrail cases correctly before failing on
the fourth. Only executing the repair found it.

It is not packaged into the template — `build_template.py` copies only
`studio-data/` and the manifest — so it costs the shipped tool nothing.
"""

import copy
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent

DEPLOYMENT = (
    "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233:deployment:"
    "707b5faf-765b-44b1-977b-8b25a21aca07/4a6e138d-0e33-4f3f-adf9-25826aca532e"
)

DEPLOYED_FLOW = (
    "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233:deployedFlow:"
    "707b5faf-765b-44b1-977b-8b25a21aca07/d78441aa-fad3-4577-9ac4-96fd83b6bceb"
)

FLOW = (
    "crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233:flow:"
    "pdf-selfheal-oscillator"
)


def load(folder, name="tool.py"):
    """Import a tool by path, the way run_tool_local.py does."""

    spec = importlib.util.spec_from_file_location(
        folder, ROOT / "studio-data/tool_templates" / folder / name
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


BASELINE = json.loads(
    (ROOT.parents[2] / "self_healing/flow/oscillating_queue.json").read_text()
)


class Fake:
    """
    A stubbed control plane.

    Each knob corresponds to one real-world situation the tool has to
    refuse, so a test reads as the situation rather than as a mock
    script.
    """

    def __init__(
        self,
        deployment_name="pdf-selfheal-oscillator",
        catalog_name="pdf-selfheal-oscillator",
        catalog_crn=FLOW,
        flow_version=3,
        valid_actions=None,
        definition=None,
    ):
        self.deployment_name = deployment_name
        self.catalog_name = catalog_name
        self.catalog_crn = catalog_crn
        self.flow_version = flow_version
        self.valid_actions = valid_actions or ["CHANGE_FLOW_VERSION"]
        self.definition = definition or copy.deepcopy(BASELINE)
        self.calls = []

    def __call__(self, config, args):

        self.calls.append(list(args))

        operation = args[1]

        if operation == "describe-deployment":
            return {"deployment": {"name": self.deployment_name}}

        if operation == "list-flows-in-deployment":
            return {"deployedFlows": [{"crn": DEPLOYED_FLOW}]}

        if operation == "describe-flow-in-deployment":
            return {
                "deployedFlow": {
                    "flowCrn": FLOW,
                    "flowVersion": self.flow_version,
                    "flowVersionCrn": f"{FLOW}/v.{self.flow_version}",
                    "validActions": self.valid_actions,
                }
            }

        if operation == "list-flow-definitions":
            return {
                "flows": [{"crn": self.catalog_crn, "name": self.catalog_name}]
            }

        if operation == "get-flow-version":
            return self.definition

        raise AssertionError(f"the stub was asked for {operation!r}")


def case(label, module, fake, params, expect):
    """Run one case and report whether the expected key is present."""

    module.run_cdp = fake

    result = module.run_tool(
        config=module.UserParameters(
            CDP_ACCESS_KEY_ID="stub", CDP_PRIVATE_KEY="stub"
        ),
        args=module.ToolParameters(**params),
    )

    got = expect in result

    detail = result.get(expect)

    if isinstance(detail, str):
        detail = detail.split(".")[0][:78]

    elif expect == "plan":
        detail = f"{len(result['plan']['argv'])} commands, performed=" \
                 f"{result.get('performed')}"

    print(f"  {'ok ' if got else 'FAIL'} {label}")
    print(f"       {expect}: {detail}")

    return got


def main():

    propose = load("proposeflowrepair_On1fGSOC")
    apply_tool = load("applyflowrepair_1imeQOp7")

    passed = []

    print("\nproposeFlowRepair\n" + "-" * 70)

    fake = Fake()
    propose.run_cdp = fake

    good = propose.run_tool(
        config=propose.UserParameters(
            CDP_ACCESS_KEY_ID="stub", CDP_PRIVATE_KEY="stub"
        ),
        args=propose.ToolParameters(
            deployment_crn=DEPLOYMENT, repair="speed_up_slow_drain",
            concurrency=4,
        ),
    )

    echo = good.get("apply_echo") or {}

    print("  ok  a legitimate proposal is produced")
    print(f"       changed: {good['proposal']['changed_paths']}")
    print(f"       values:  {json.dumps(good['proposal']['values'])}")
    passed.append(bool(echo))

    # The definition must never cross back to the model.
    leaked = [
        key for key in good
        if key in ("flowContents", "definition_json", "after")
    ]
    print(f"  {'ok ' if not leaked else 'FAIL'} no flow JSON in the proposal")
    passed.append(not leaked)

    passed.append(
        case(
            "an invented repair name is refused",
            propose, Fake(),
            {"deployment_crn": DEPLOYMENT, "repair": "make_it_faster"},
            "error",
        )
    )

    passed.append(
        case(
            "concurrency above the measurability ceiling is refused",
            propose, Fake(),
            {
                "deployment_crn": DEPLOYMENT,
                "repair": "speed_up_slow_drain",
                "concurrency": 64,
            },
            "refused",
        )
    )

    passed.append(
        case(
            "restore_baseline on an already-baseline flow is a no-op",
            propose, Fake(),
            {"deployment_crn": DEPLOYMENT, "repair": "restore_baseline"},
            "no_op",
        )
    )

    print("\napplyFlowRepair\n" + "-" * 70)

    armed = {
        "deployment_crn": DEPLOYMENT,
        "repair": "speed_up_slow_drain",
        "concurrency": 4,
        "expected_flow_version": echo.get("expected_flow_version"),
        "expected_values": echo.get("expected_values"),
    }

    passed.append(
        case(
            "the happy path, unarmed, returns a plan and performs nothing",
            apply_tool, Fake(), armed, "plan",
        )
    )

    passed.append(
        case(
            "a colleague's deployment is refused on the name prefix",
            apply_tool, Fake(deployment_name="jsmith-prod-ingest"),
            armed, "refused",
        )
    )

    passed.append(
        case(
            "a catalog flow that is not ours is refused",
            apply_tool, Fake(catalog_crn=f"{FLOW}-someone-else"),
            armed, "refused",
        )
    )

    passed.append(
        case(
            "a flow whose name only contains the prefix is refused",
            apply_tool,
            Fake(catalog_name="not-pdf-selfheal-oscillator"),
            armed, "refused",
        )
    )

    passed.append(
        case(
            "a proposal computed from an older version is refused as stale",
            apply_tool, Fake(flow_version=5), armed, "refused",
        )
    )

    widened = dict(armed)
    widened["concurrency"] = 8

    passed.append(
        case(
            "widening concurrency after the proposal is refused",
            apply_tool, Fake(), widened, "refused",
        )
    )

    tampered = dict(armed)
    tampered["expected_values"] = {"anything": {"before": 2, "after": 4}}

    passed.append(
        case(
            "a tampered echo is refused",
            apply_tool, Fake(), tampered, "refused",
        )
    )

    passed.append(
        case(
            "a half-applied version change is refused",
            apply_tool,
            Fake(valid_actions=["RETRY_CHANGE_FLOW_VERSION"]),
            armed, "refused",
        )
    )

    # The dry run's argv is the audit record, so its content matters as
    # much as the refusals.
    apply_tool.run_cdp = Fake()

    plan = apply_tool.run_tool(
        config=apply_tool.UserParameters(
            CDP_ACCESS_KEY_ID="stub", CDP_PRIVATE_KEY="stub"
        ),
        args=apply_tool.ToolParameters(**armed),
    )["plan"]

    print("\nthe dry run's argv\n" + "-" * 70)

    for argv in plan["argv"]:
        print("  " + " ".join(argv))

    flat = " ".join(" ".join(a) for a in plan["argv"])

    checks = [
        ("strategy is ONLY_RESTART_AFFECTED_COMPONENTS",
         "ONLY_RESTART_AFFECTED_COMPONENTS" in flat),
        ("--kpis is absent", "--kpis" not in flat),
        ("--environment-crn is absent (control plane)",
         "--environment-crn" not in flat),
        ("no wait flag alongside a non-default strategy",
         "--wait-for-flow-to-stop-in-minutes" not in flat),
        ("the right deployment is named", DEPLOYMENT in flat),
    ]

    print()

    for label, ok in checks:
        print(f"  {'ok ' if ok else 'FAIL'} {label}")
        passed.append(ok)

    print("\n" + "=" * 70)

    if all(passed):
        print(f"all {len(passed)} checks passed")

        return 0

    print(f"{passed.count(False)} of {len(passed)} checks FAILED")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
