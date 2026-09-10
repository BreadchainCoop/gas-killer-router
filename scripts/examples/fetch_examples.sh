#!/usr/bin/env bash
#
# Fetches and builds the public gas-killer/example-contracts library so the `deploy_example`
# binary has Foundry artifacts to deploy from.
#
# Idempotent: clones on the first run, then fetches and re-checks-out the pinned revision on
# every later run. The checkout is gitignored — it is a build input, not part of this repo.
#
# EXAMPLES_REF pins a commit rather than a branch so a rebuild is reproducible. Override it
# with a branch name to track upstream:
#
#   EXAMPLES_REF=hudsonhrh/gas-killer-foundry-examples ./scripts/examples/fetch_examples.sh
#
# The example contracts resolve the Gas Killer SDK, EigenLayer, and OpenZeppelin through
# nested submodules, so the recursive clone pulls a few hundred MB on a cold run.
#
# Requires: git, and Foundry (`forge`) on PATH.

set -euo pipefail

EXAMPLES_REPO="${EXAMPLES_REPO:-https://github.com/gas-killer/example-contracts}"
EXAMPLES_REF="${EXAMPLES_REF:-5b2d9e260d9bd0d41097f8433f6b1ee140475e01}"
EXAMPLES_DIR="${EXAMPLES_DIR:-.examples/example-contracts}"

# The Gas Killer SDK is a submodule of the examples repo, and it carries its own examples
# (array-summation, reentrant-checkpoint) that the manifest also deploys. It builds under its own
# foundry.toml — via_ir = true, versus via_ir = false in the examples repo — so the two trees are
# built separately in place rather than by merging profiles.
SDK_SUBDIR="lib/solidity-sdk"

# Contracts the manifest expects once the build finishes, per tree.
EXPECTED_ARTIFACTS=(
  "OnchainLife.sol/OnchainLife.json"
  "GuardedVault.sol/GuardedVault.json"
  "SortedOracle.sol/SortedOracle.json"
)
EXPECTED_SDK_ARTIFACTS=(
  "ArraySummation.sol/ArraySummation.json"
  "SchnorrArraySummation.sol/SchnorrArraySummation.json"
  "DualSchemeArraySummation.sol/DualSchemeArraySummation.json"
  "ReentrantCheckpoint.sol/ReentrantCheckpoint.json"
  "ReentrantObserver.sol/ReentrantObserver.json"
)

if ! command -v forge >/dev/null 2>&1; then
  echo "❌ forge not found on PATH. Install Foundry: https://getfoundry.sh" >&2
  exit 1
fi

if [ ! -d "$EXAMPLES_DIR/.git" ]; then
  echo "📥 cloning $EXAMPLES_REPO into $EXAMPLES_DIR"
  mkdir -p "$(dirname "$EXAMPLES_DIR")"
  git clone --recurse-submodules "$EXAMPLES_REPO" "$EXAMPLES_DIR"
else
  echo "📥 updating existing checkout at $EXAMPLES_DIR"
fi

echo "🔖 checking out $EXAMPLES_REF"
git -C "$EXAMPLES_DIR" fetch --tags origin
# A pinned SHA is not necessarily reachable by a branch name, so fetch it directly. Tolerated
# if it fails: the ref may already be local, or may be a branch the fetch above resolved.
git -C "$EXAMPLES_DIR" fetch origin "$EXAMPLES_REF" 2>/dev/null || true
git -C "$EXAMPLES_DIR" checkout --detach "$EXAMPLES_REF" 2>/dev/null \
  || git -C "$EXAMPLES_DIR" checkout --detach "origin/$EXAMPLES_REF"

# The SDK remappings resolve through the submodule's own lib/ tree, so this must be recursive
# or the build fails on unresolved EigenLayer and OpenZeppelin imports.
echo "🔗 syncing submodules (recursive)"
git -C "$EXAMPLES_DIR" submodule update --init --recursive

echo "🔨 building the example contracts"
(cd "$EXAMPLES_DIR" && forge build)

echo "🔨 building the SDK's own examples ($SDK_SUBDIR)"
(cd "$EXAMPLES_DIR/$SDK_SUBDIR" && forge build)

missing=0
check_artifacts() {
  local root="$1"; shift
  for artifact in "$@"; do
    if [ ! -f "$root/$artifact" ]; then
      echo "❌ expected artifact missing: $root/$artifact" >&2
      missing=1
    fi
  done
}
check_artifacts "$EXAMPLES_DIR/out" "${EXPECTED_ARTIFACTS[@]}"
check_artifacts "$EXAMPLES_DIR/$SDK_SUBDIR/out" "${EXPECTED_SDK_ARTIFACTS[@]}"
if [ "$missing" -ne 0 ]; then
  echo "The manifest at scripts/examples/examples.toml may be out of date with the checkout." >&2
  exit 1
fi

resolved="$(git -C "$EXAMPLES_DIR" rev-parse HEAD)"
sdk_resolved="$(git -C "$EXAMPLES_DIR/$SDK_SUBDIR" rev-parse HEAD)"
echo
echo "✅ example contracts built at $resolved"
echo "   artifacts: $EXAMPLES_DIR/out"
echo "✅ SDK examples built at $sdk_resolved"
echo "   artifacts: $EXAMPLES_DIR/$SDK_SUBDIR/out"
echo
echo "Next:"
echo "   cargo run -p scripts --bin deploy_example -- --dry-run"
echo "   cargo run -p scripts --bin deploy_example -- --example guardedVault"
