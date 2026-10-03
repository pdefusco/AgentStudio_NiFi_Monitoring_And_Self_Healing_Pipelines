#!/usr/bin/env bash
#
# Rebuild the self-healing prototype in the `pdf0926-cdp-env` environment.
#
# This is README steps 0-4 with the two retargeting variables baked in, in
# one command, behind a reachability gate. It is NOT a new mechanism: every
# step is the same script the README runs, and config.py reads both CRNs
# from the environment precisely so no code edit is needed to retarget.
#
# Usage:
#
#     ./rebuild-pdf0926.sh --dry-run   # free; prints every call it would make
#     ./rebuild-pdf0926.sh             # steps 3+ cost money
#
# Steps 5-7 (observe, one armed run, teardown) stay manual on purpose, and
# Agent Studio (docs/manual-setup/README.md steps 6-7) is a UI operation
# with no API, so it is not scriptable at all.

set -euo pipefail

cd "$(dirname "$0")"

PY="../.venv/bin/python"

export SELFHEAL_SERVICE_CRN="crn:cdp:df:us-west-1:558bc1d2-8867-4357-8524-311d51259233:service:707b5faf-765b-44b1-977b-8b25a21aca07"
export SELFHEAL_ENVIRONMENT_CRN="crn:cdp:environments:us-west-1:558bc1d2-8867-4357-8524-311d51259233:environment:d1b6341e-1a28-4f9a-8e23-9ea762567b11"

DRY=""
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY="--dry-run"
fi

step() { printf '\n\033[1m=== %s ===\033[0m\n' "$1"; }

# ---------------------------------------------------------------------
# 0. The gate.
#
# Detection and action do not share a network path. Metric reads go to the
# public control plane; every mutation -- the KPI in step 4, and every
# remediation later -- goes to this environment's DFX gateway, which is an
# `internal-` ELB private to the environment's own VPC. If the caller has no
# route to that VPC CIDR, every read succeeds and every write connect-times
# out, which is a confusing way to fail.
#
# Gating here is the whole point: without it, step 3 bills for ~15 minutes
# and then step 4 fails, leaving a deployment with no queue-depth chart --
# which is a deployment the monitor can never trip on.
# ---------------------------------------------------------------------
step "0. both API planes reachable?"

if ! $PY job/monitor_and_remediate.py --selftest; then
    cat >&2 <<'EOF'

ABORTING: a plane is unreachable. Nothing was created and nothing billed.

If the CONTROL plane failed, check CDP_ACCESS_KEY_ID / CDP_PRIVATE_KEY.

If the WORKLOAD plane failed with a connect timeout, note what --selftest
printed just above: it reports the workload endpoint *and* its auth token
expiry. A token expiry means the credentials and the DataFlow role are both
fine and the service is enabled -- the only thing left is the network path.
EOF

    # The gateway is a private ELB, so its address is both environment-
    # specific and ephemeral; resolve it rather than hard-coding one that
    # goes stale the next time the load balancer is replaced.
    host="$($PY -c 'import df_api; print(df_api.workload_endpoint().split()[0])' 2>/dev/null || true)"
    elb="$(host "${host:-}" 2>/dev/null | awk '/has address/ {print $NF; exit}')"

    if [[ -n "$elb" ]]; then
        cat >&2 <<EOF

Trace the path to it:

    traceroute -n $elb

If the trace leaves your VPN and then reaches a PUBLIC address, the VPN
carries no route for this environment's VPC CIDR and is falling through to
its default route -- so the packets exit to the internet, where RFC1918 is
unroutable, and never reach AWS at all. That is a route to advertise on the
VPN side, not a security group and not anything fixable here.
EOF
    fi

    exit 1
fi

step "1. build and inspect the flow (offline, free)"
$PY flow/build_flow_definition.py --check

step "2. catalog import (mutates a SHARED catalog)"
$PY provision/01_import_flow.py $DRY

step "3. deploy (THIS STARTS BILLING, ~15 min)"
$PY provision/02_create_deployment.py $DRY

step "4. the queue-depth KPI (not optional)"
$PY provision/03_configure_kpi.py $DRY
$PY provision/03_configure_kpi.py --verify-only

cat <<'EOF'

Done through step 4. Next, by hand:

  # 5. watch a full 10-minute oscillation before trusting the loop
  ../.venv/bin/python job/monitor_and_remediate.py

  # 6. one armed run, at a breach
  ../.venv/bin/python job/monitor_and_remediate.py --arm

  # 7. stop the billing
  ../.venv/bin/python provision/teardown.py
EOF
