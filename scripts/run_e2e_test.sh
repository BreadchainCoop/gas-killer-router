#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"

# Task submission is authenticated with per-client API keys minted through the admin API, which
# is guarded by ADMIN_KEY. Use a fixed dev value unless the caller overrides it; docker-compose
# reads the same default for the router so the two stay in sync.
export ADMIN_KEY="${ADMIN_KEY:-ci-admin-key}"

# Capture the caller's signature scheme (default: bls) before `cp example.env .env`
# and `source ../.env` can let the example default win. Re-exported after the
# source below so it reaches the deploy binary; docker-compose reads it directly
# from the environment for the node/router containers.
SIGNATURE_SCHEME_CHOICE="${SIGNATURE_SCHEME:-bls}"
export SIGNATURE_SCHEME="$SIGNATURE_SCHEME_CHOICE"
echo "Signature scheme: $SIGNATURE_SCHEME_CHOICE"

# STATE_ENCODING (legacy|canonical|prestate-net), E2E_EXAMPLE
# (array-summation|dual-scheme|reentrant|onchain-life) and GK_SIM_PROFILE (chain|unbounded) follow
# the
# same capture-then-re-export discipline as SIGNATURE_SCHEME so the containers AND the
# host-side deploy/send binaries agree. `reentrant` deploys a ReentrantCheckpoint whose
# task re-enters mid-transition; pair it with `canonical` to prove re-entrancy is safe.
# `onchain-life` deploys an OnchainLife and settles the multi-generation step declared in the
# examples manifest, whose direct execution exceeds a 30M block; it requires
# GK_SIM_PROFILE=unbounded, STATE_ENCODING=prestate-net, and
# ANVIL_EXTRA_ARGS=--disable-block-gas-limit. `dual-scheme` deploys the migration-period base,
# which accepts either scheme's proof, and settles it with whichever fleet is running: the same
# target address settles under both, which is the property an integrator deploys it for.
STATE_ENCODING_CHOICE="${STATE_ENCODING:-legacy}"
export STATE_ENCODING="$STATE_ENCODING_CHOICE"
E2E_EXAMPLE_CHOICE="${E2E_EXAMPLE:-array-summation}"
export E2E_EXAMPLE="$E2E_EXAMPLE_CHOICE"
GK_SIM_PROFILE_CHOICE="${GK_SIM_PROFILE:-chain}"
export GK_SIM_PROFILE="$GK_SIM_PROFILE_CHOICE"
echo "State encoding: $STATE_ENCODING_CHOICE | e2e example: $E2E_EXAMPLE_CHOICE | sim profile: $GK_SIM_PROFILE_CHOICE"

# Track if test passed
TEST_PASSED=false

# Create logs directory
mkdir -p "$LOG_DIR"

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up Docker containers...${NC}"

    # If test didn't pass, dump all container logs for debugging
    if [ "$TEST_PASSED" != "true" ]; then
        echo -e "${YELLOW}=== Dumping all container logs for debugging ===${NC}"
        echo -e "${YELLOW}Ethereum logs:${NC}"
        docker compose logs ethereum 2>/dev/null || true
        echo -e "${YELLOW}Eigenlayer logs:${NC}"
        docker compose logs eigenlayer 2>/dev/null || true
        echo -e "${YELLOW}Router logs:${NC}"
        docker compose logs router 2>/dev/null || true
        echo -e "${YELLOW}Node-1 logs:${NC}"
        docker compose logs node-1 2>/dev/null || true
        echo -e "${YELLOW}Node-2 logs:${NC}"
        docker compose logs node-2 2>/dev/null || true
        echo -e "${YELLOW}Node-3 logs:${NC}"
        docker compose logs node-3 2>/dev/null || true
        echo -e "${YELLOW}Signer logs:${NC}"
        docker compose logs signer 2>/dev/null || true
    fi

    cd "$PROJECT_ROOT"
    docker compose down || true
    echo -e "${GREEN}Cleanup completed${NC}"
}

# Parse flags/env for keeping containers up after script finishes
KEEP_UP=false
for arg in "$@"; do
    case "$arg" in
        --keep-up|--no-cleanup)
            KEEP_UP=true
            ;;
    esac
done

if [ "${KEEP_CONTAINERS:-}" = "1" ] || [ "${KEEP_CONTAINERS:-}" = "true" ]; then
    KEEP_UP=true
fi

# Set trap for cleanup unless explicitly keeping containers up
if [ "$KEEP_UP" = true ]; then
    echo -e "${YELLOW}Skipping auto-cleanup; containers will remain running. Use 'docker compose down' to stop.${NC}"
else
    trap cleanup EXIT INT TERM
fi

echo -e "${GREEN}Starting Gas Killer E2E Test${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Logs directory: $LOG_DIR"

# Step 1: Build scripts and the example contracts they deploy
echo -e "${YELLOW}Step 1: Building scripts...${NC}"
cd "$PROJECT_ROOT/scripts"
cargo build --release -p scripts --bin setup_schnorr_operators
cargo build --release -p scripts --bin deploy_example
cargo build --release -p scripts --bin run_scenario
cargo build --release -p scripts --bin verify_message_hash_parity
cd "$PROJECT_ROOT"

# deploy_example needs Foundry artifacts for the target it deploys, and the example-contracts
# checkout that produces them is gitignored — so a clean tree (any CI runner) has none. Idempotent:
# a warm checkout just re-checks the pinned revision and re-runs two incremental forge builds.
echo -e "${YELLOW}Fetching and building the example contracts...${NC}"
./scripts/examples/fetch_examples.sh

# Step 2: Assume .env already exists and contains required values
echo -e "${YELLOW}Step 2: Using existing .env without modification...${NC}"
if [ ! -f .env ]; then
    cp example.env .env
    echo ".env created from example.env"
else
    echo ".env already exists; leaving it unchanged"
fi

echo "Environment configuration complete"

# Step 3: Pull Docker images
echo -e "${YELLOW}Step 3: Pulling Docker images...${NC}"
docker compose pull

# Step 4: Build service images
echo -e "${YELLOW}Step 4: Building service Docker images...${NC}"
docker compose build

# Step 5: Start Docker Compose services
echo -e "${YELLOW}Step 5: Starting Docker Compose services...${NC}"
docker compose up -d

# Show running containers
docker compose ps

# Step 6: Wait for EigenLayer setup to complete
echo -e "${YELLOW}Step 6: Waiting for EigenLayer setup to complete...${NC}"
timeout=500
elapsed=0

while [ $elapsed -lt $timeout ]; do
    # Check if eigenlayer container has completed setup
    if docker compose logs eigenlayer 2>/dev/null | grep -q "Operator 3 weight in quorum" && [ -f config/.nodes/avs_deploy.json ]; then
        echo -e "${GREEN}EigenLayer setup completed successfully${NC}"
        break
    fi
    
    echo "Waiting for EigenLayer setup... ($elapsed/$timeout seconds)"
    sleep 10
    elapsed=$((elapsed + 10))
done

if [ $elapsed -ge $timeout ]; then
    echo -e "${RED}Timeout waiting for EigenLayer setup${NC}"
    echo "Eigenlayer logs:"
    docker compose logs eigenlayer
    exit 1
fi

# Fix permissions on config/.nodes directory so deploy script can write
echo "Fixing file permissions..."
sudo chmod -R 777 config/.nodes || chmod -R 777 config/.nodes

# Give extra time for nodes to initialize
echo "Waiting for nodes to initialize..."
sleep 30

# Step 7: Deploy Gas Killer example contract (ArraySummation)
echo -e "${YELLOW}Step 7: Deploying Gas Killer example contract (ArraySummation)...${NC}"
cd "$PROJECT_ROOT/scripts"

# Source environment and run deployment
source ../.env
# `source ../.env` may reset these to the example defaults; restore the caller's choices so the
# deploy and trigger binaries pick the right stack, encoding, and example target.
export SIGNATURE_SCHEME="$SIGNATURE_SCHEME_CHOICE"
export STATE_ENCODING="$STATE_ENCODING_CHOICE"
export E2E_EXAMPLE="$E2E_EXAMPLE_CHOICE"
export GK_SIM_PROFILE="$GK_SIM_PROFILE_CHOICE"
export AVS_DEPLOYMENT_PATH="../config/.nodes/avs_deploy.json"

if [ ! -f "$AVS_DEPLOYMENT_PATH" ]; then
    echo -e "${RED}Deployment file not found at $AVS_DEPLOYMENT_PATH${NC}"
    exit 1
fi

# Map the E2E_EXAMPLE selector onto a manifest entry. Under schnorr, array-summation means the
# SchnorrArraySummation variant, which verifies against the stake registry rather than a BLS
# checker; `reentrant` is schnorr-only by construction.
case "$E2E_EXAMPLE:$SIGNATURE_SCHEME" in
    array-summation:schnorr)          MANIFEST_EXAMPLE="schnorrArraySummation" ;;
    array-summation:*)                MANIFEST_EXAMPLE="arraySummation" ;;
    dual-scheme:*|dual-scheme-array-summation:*) MANIFEST_EXAMPLE="dualSchemeArraySummation" ;;
    reentrant:*|reentrant-checkpoint:*) MANIFEST_EXAMPLE="reentrantCheckpoint" ;;
    onchain-life:*|onchainlife:*)     MANIFEST_EXAMPLE="onchainLife" ;;
    *)
        echo -e "${RED}Unknown E2E_EXAMPLE '$E2E_EXAMPLE'${NC}"
        exit 1
        ;;
esac

# The dual-scheme base takes both verifiers in its constructor, so a Schnorr registry has to
# exist even under bls, where the fleet never verifies against it. Provisioning one is what makes
# the operator-set step below do anything under bls; without it `$deploy:schnorrStakeRegistry`
# does not resolve and the deploy fails on an unresolvable placeholder rather than at settlement.
if [ "$MANIFEST_EXAMPLE" = "dualSchemeArraySummation" ] && [ "$SIGNATURE_SCHEME" != "schnorr" ]; then
    export SCHNORR_PROVISION=registry
    echo "Provisioning a Schnorr registry (SCHNORR_PROVISION=registry) for the dual-scheme target"
fi

deploy_failed() {
    echo -e "${RED}$1${NC}"
    echo -e "${YELLOW}Recent ethereum logs:${NC}"
    docker compose logs --tail=100 ethereum || true
    echo -e "${YELLOW}Recent eigenlayer logs:${NC}"
    docker compose logs --tail=100 eigenlayer || true
    exit 1
}

# Both binaries resolve the manifest, the built artifacts, and the generated scenario directory
# relative to the repo root, so they run from there in a subshell — the steps after this one still
# expect the working directory to be scripts/ with its `../`-relative deployment path.
run_from_root() {
    ( cd "$PROJECT_ROOT" \
        && AVS_DEPLOYMENT_PATH="config/.nodes/avs_deploy.json" \
           cargo run --release -p scripts "$@" )
}

# The Schnorr operator set must be registered before any target deploys: every registration
# advances the registry's `effectiveBlock` watermark, and verification fail-closes for reference
# blocks behind it. A no-op under bls unless SCHNORR_PROVISION asked for a registry, so it runs
# unconditionally.
echo "Setting up the Schnorr operator set (no-op under bls without SCHNORR_PROVISION)..."
run_from_root --bin setup_schnorr_operators \
    || deploy_failed "Schnorr operator setup failed"

echo "Deploying the $MANIFEST_EXAMPLE target..."
run_from_root --bin deploy_example -- --example "$MANIFEST_EXAMPLE" \
    || deploy_failed "$MANIFEST_EXAMPLE deployment failed"
echo -e "${GREEN}$MANIFEST_EXAMPLE deployment completed successfully${NC}"

# Advance one block so no contract deployed above was created in the block the task will
# reference. Anvil mines per transaction, so without this the reference block is exactly the last
# deploy's block — and the off-chain replay reads account state at `reference_block - 1`, one block
# behind the trace. Any contract created in the reference block therefore looks code-less to the
# replay: a call to it returns empty data, which a caller decoding a return value surfaces as a
# bare revert with no reason (`reentrantCheckpoint` hits this when its observer reads back
# `counter()`), while a call expecting no return value silently succeeds and hides the skew.
echo "Advancing one block so the task does not reference a deploy block..."
cast rpc evm_mine --rpc-url http://localhost:8545 >/dev/null \
    || deploy_failed "could not mine a block after deploying"

# Extract the deployed target's address from the deployment JSON. deploy_example records
# whichever example it deployed under `gasKillerTarget` (the manifest's `alias`), so this one
# lookup works for every example.
DEPLOY_JSON_PATH="$AVS_DEPLOYMENT_PATH"
if command -v jq >/dev/null 2>&1; then
    TARGET_ADDRESS=$(jq -r '.addresses.gasKillerTarget // empty' "$DEPLOY_JSON_PATH")
else
    TARGET_ADDRESS=$(grep -o '"gasKillerTarget"[[:space:]]*:[[:space:]]*"[^"]*"' "$DEPLOY_JSON_PATH" | head -1 | sed 's/.*:[[:space:]]*"\([^"]*\)"/\1/')
fi

if [ -z "$TARGET_ADDRESS" ]; then
    echo -e "${YELLOW}Warning: Could not determine the target address from $DEPLOY_JSON_PATH${NC}"
else
    echo "Discovered target address: $TARGET_ADDRESS"
    # Set as the default target for Gas Killer trigger helper
    export GAS_KILLER_TARGET_ADDRESS="$TARGET_ADDRESS"
fi

# Step 7a (unbounded profile only): establish the premise — the tracked function cannot be
# executed directly in a real block, because estimating it alone costs more than a block's gas
# limit. Step 10b then shows the same transition landing in one small verifyAndUpdate. Together
# they are the unbounded-mode claim. The estimate itself only completes because the anvil service
# runs with --disable-block-gas-limit (ANVIL_EXTRA_ARGS); against a stock node it would fail as
# out-of-gas.
#
# 30M is an illustrative reference point rather than any chain's live limit (mainnet has since
# risen past it). That direction is safe for both assertions: a workload above 30M is the weaker
# claim to prove in 7a, and staying under 30M is the stricter bar to clear in 10b.
NOMINAL_BLOCK_GAS_LIMIT=30000000
# Anvil's first unlocked dev account, used as the estimate's sender.
ANVIL_FUNDED_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
if [ "$GK_SIM_PROFILE_CHOICE" = "unbounded" ]; then
    echo -e "${YELLOW}Step 7a: Asserting a direct call exceeds the block gas limit...${NC}"
    if [ -z "$TARGET_ADDRESS" ]; then
        echo -e "${RED}No target address resolved; cannot estimate the direct call${NC}"
        exit 1
    fi
    # Estimate the exact calldata the scenario will submit, read out of the generated scenario
    # itself, so the call proven unmineable here and the call settled at step 10 cannot diverge.
    # The last request is the heaviest where an example declares several.
    SCENARIO_FILE="$PROJECT_ROOT/scripts/scenarios/generated/$MANIFEST_EXAMPLE.toml"
    if [ ! -f "$SCENARIO_FILE" ]; then
        echo -e "${RED}$SCENARIO_FILE was not generated by the deploy step${NC}"
        exit 1
    fi
    DIRECT_CALLDATA=$(grep -oE 'call_data[[:space:]]*=[[:space:]]*"0x[0-9a-fA-F]+"' "$SCENARIO_FILE" \
        | tail -1 | sed -E 's/.*"(0x[0-9a-fA-F]+)".*/\1/')
    if [ -z "$DIRECT_CALLDATA" ]; then
        echo -e "${RED}Could not read a call_data value from $SCENARIO_FILE${NC}"
        exit 1
    fi
    # eth_estimateGas rather than `cast estimate`, which takes a signature and arguments and has
    # no option for pre-encoded calldata. `from` is Anvil's first funded account, matching the
    # sender the scenario submits with. The response is a quoted hex string.
    DIRECT_GAS_HEX=$(cast rpc eth_estimateGas \
        "{\"to\":\"$TARGET_ADDRESS\",\"from\":\"$ANVIL_FUNDED_ACCOUNT\",\"data\":\"$DIRECT_CALLDATA\"}" \
        --rpc-url http://localhost:8545) \
        || deploy_failed "eth_estimateGas of the direct call failed"
    # Strip the JSON quotes in-shell; piping to tr would mask cast's exit status above.
    DIRECT_GAS_HEX=${DIRECT_GAS_HEX//\"/}
    DIRECT_GAS=$(cast to-dec "$DIRECT_GAS_HEX") \
        || deploy_failed "could not convert the gas estimate '$DIRECT_GAS_HEX' to decimal"
    echo "Direct call ${DIRECT_CALLDATA:0:10} needs $DIRECT_GAS gas (nominal block limit: $NOMINAL_BLOCK_GAS_LIMIT)"
    if [ -z "$DIRECT_GAS" ] || [ "$DIRECT_GAS" -le "$NOMINAL_BLOCK_GAS_LIMIT" ]; then
        echo -e "${RED}Expected the direct call to exceed the block gas limit; raise the workload in the manifest exercise${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Direct execution cannot fit in a real block ($DIRECT_GAS > $NOMINAL_BLOCK_GAS_LIMIT gas)${NC}"
fi

cd "$PROJECT_ROOT"

# Step 7b: Verify the router's local payload hash matches the contract's getMessageHash
echo -e "${YELLOW}Step 7b: Verifying message-hash parity (build_payload_hash vs on-chain getMessageHash)...${NC}"
cd "$PROJECT_ROOT/scripts"
if ! cargo run --release -p scripts --bin verify_message_hash_parity; then
    echo -e "${RED}❌ Message-hash parity check FAILED — local build_payload_hash diverges from on-chain getMessageHash${NC}"
    cd "$PROJECT_ROOT"
    docker compose logs --tail=100 ethereum || true
    exit 1
fi
echo -e "${GREEN}✅ Message-hash parity verified${NC}"
cd "$PROJECT_ROOT"

# Step 8: Wait for router ingress to be reachable
echo -e "${YELLOW}Step 8: Waiting for router ingress to be ready...${NC}"
ROUTER_HEALTH_URL="http://localhost:8080/healthz"
ROUTER_TIMEOUT=120
ROUTER_INTERVAL=3
elapsed=0
until curl -sf "$ROUTER_HEALTH_URL" > /dev/null 2>&1; do
    if [ "$elapsed" -ge "$ROUTER_TIMEOUT" ]; then
        echo -e "${RED}Timeout: router ingress not ready after ${ROUTER_TIMEOUT}s${NC}"
        docker compose logs --tail=50 router || true
        exit 1
    fi
    echo "Waiting for router ingress... (${elapsed}s)"
    sleep "$ROUTER_INTERVAL"
    elapsed=$((elapsed + ROUTER_INTERVAL))
done
echo -e "${GREEN}Router ingress is ready (${elapsed}s)${NC}"

# Step 9: Brief wait for services to stabilize
echo -e "${YELLOW}Step 9: Waiting briefly for services to stabilize...${NC}"
sleep 5

# Step 9b: Mint an API key so task submission is authenticated. The router requires a valid,
# unrevoked key on /tasks; mint one via the admin API (guarded by ADMIN_KEY) and hand it to
# run_scenario through GAS_KILLER_API_KEY, which it reads when the scenario sets no api_key.
echo -e "${YELLOW}Step 9b: Minting an API key via the admin endpoint...${NC}"
CREATE_RESP=$(curl -s -X POST \
    -H "Authorization: Bearer $ADMIN_KEY" \
    -H "Content-Type: application/json" \
    -d '{"label":"e2e"}' \
    http://localhost:8080/admin/keys)
if command -v jq >/dev/null 2>&1; then
    GAS_KILLER_API_KEY=$(printf '%s' "$CREATE_RESP" | jq -r '.key // empty')
else
    GAS_KILLER_API_KEY=$(printf '%s' "$CREATE_RESP" | grep -o '"key"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*:[[:space:]]*"\([^"]*\)".*/\1/')
fi
case "$GAS_KILLER_API_KEY" in
    gk_*) ;;
    *)
        echo -e "${RED}Failed to mint API key. Admin response: $CREATE_RESP${NC}"
        docker compose logs --tail=50 router || true
        exit 1
        ;;
esac
export GAS_KILLER_API_KEY
echo -e "${GREEN}Minted API key for task submission${NC}"

# Step 10: Trigger the Gas Killer task and verify execution.
#
# The scenario is the one deploy_example rendered from the manifest, so the call submitted here is
# the manifest's own `[[examples.exercise]]` — there is no second definition of the transition to
# keep in step. run_scenario posts the task, submits the rendered payload, and verifies the
# target's stateTransitionCount advanced, which every SDK target exposes regardless of example.
echo -e "${YELLOW}Step 10: Triggering task and verifying execution...${NC}"
SCENARIO="scripts/scenarios/generated/$MANIFEST_EXAMPLE.toml"
if [ ! -f "$PROJECT_ROOT/$SCENARIO" ]; then
    echo -e "${RED}$SCENARIO was not generated by the deploy step${NC}"
    exit 1
fi
echo "Running $SCENARIO..."
# Capture the output so the diagnostics below can trace the settled tx. The `tee` pipeline exits
# 0, so `set -e` does not abort here — the real status comes from PIPESTATUS.
# run_from_root supplies the repo-root cwd and root-relative AVS_DEPLOYMENT_PATH that the
# scenario's `local:` target sentinel resolves against.
TRIGGER_LOG="$(mktemp)"
run_from_root --bin run_scenario -- "$SCENARIO" 2>&1 | tee "$TRIGGER_LOG"
TRIGGER_STATUS=${PIPESTATUS[0]}

# The user-submitted verifyAndUpdate tx hash. Printed by the shared submit helper in
# scripts/src/task_payload.rs, so the format is identical whichever binary drove the submission.
USER_TX_HASH=$(grep -oE 'landed: tx 0x[a-fA-F0-9]{64}' "$TRIGGER_LOG" | sed -E 's/.*tx //' | tail -1)

if [ $TRIGGER_STATUS -eq 0 ]; then
    echo -e "${GREEN}✅ Transition settled successfully - state was updated!${NC}"

    # A settled transition does not by itself prove the re-entrancy example did its job: the
    # target's counter advances whether or not the mid-transition call into the observer executed.
    # `observe()` returns nothing, so against an address with no code the call succeeds silently —
    # the settlement still lands and the leg still passes while having exercised nothing. The
    # observer's own counter is the discriminator; the contract documents it as advancing only when
    # the re-entrant call runs inside a real settlement.
    if [ "$MANIFEST_EXAMPLE" = "reentrantCheckpoint" ]; then
        echo "Verifying the mid-transition re-entrant call actually executed..."
        command -v cast >/dev/null 2>&1 && command -v jq >/dev/null 2>&1 \
            || { echo -e "${RED}cast and jq are required to verify the re-entrancy${NC}"; exit 1; }

        REENTRANT_JSON="$PROJECT_ROOT/config/.nodes/avs_deploy.json"
        OBSERVER_ADDRESS=$(jq -r '.addresses.reentrantObserver // empty' "$REENTRANT_JSON")
        CHECKPOINT_ADDRESS=$(jq -r '.addresses.reentrantCheckpoint // empty' "$REENTRANT_JSON")
        if [ -z "$OBSERVER_ADDRESS" ] || [ -z "$CHECKPOINT_ADDRESS" ]; then
            echo -e "${RED}reentrantObserver/reentrantCheckpoint missing from $REENTRANT_JSON${NC}"
            exit 1
        fi

        cast_uint() {
            cast call "$1" "$2" --rpc-url http://localhost:8545 | tr -d '[:space:]'
        }
        CONFIRMATIONS=$(cast_uint "$OBSERVER_ADDRESS" 'confirmations()(uint256)')
        COUNTER=$(cast_uint "$CHECKPOINT_ADDRESS" 'counter()(uint256)')
        LAST_OBSERVED=$(cast_uint "$CHECKPOINT_ADDRESS" 'lastObserved()(uint256)')
        echo "  observer.confirmations=$CONFIRMATIONS counter=$COUNTER lastObserved=$LAST_OBSERVED"

        if [ "$CONFIRMATIONS" -lt 1 ]; then
            echo -e "${RED}❌ observer.confirmations is $CONFIRMATIONS — the re-entrant call never ran${NC}"
            exit 1
        fi
        # `lastObserved` is written only after the re-entrant call returns, so equality with
        # `counter` is what distinguishes a fully finalized transition from a partial one.
        if [ "$LAST_OBSERVED" != "$COUNTER" ]; then
            echo -e "${RED}❌ lastObserved ($LAST_OBSERVED) != counter ($COUNTER) — transition did not finalize${NC}"
            exit 1
        fi
        echo -e "${GREEN}✅ Re-entrancy verified: observer confirmed the canonical intermediate state${NC}"
    fi

    # Step 10b (unbounded profile only): close the claim step 7a opened. The transition that
    # could not be executed directly in a block has landed as one small verifyAndUpdate, so the
    # receipt's gasUsed is the on-chain cost of an above-block-limit computation.
    if [ "$GK_SIM_PROFILE_CHOICE" = "unbounded" ]; then
        echo -e "${YELLOW}Step 10b: Asserting verifyAndUpdate landed far below the block gas limit...${NC}"
        if [ -z "$USER_TX_HASH" ]; then
            echo -e "${RED}Could not find the verifyAndUpdate tx hash in the scenario output${NC}"
            exit 1
        fi
        VU_GAS=$(cast receipt "$USER_TX_HASH" gasUsed --rpc-url http://localhost:8545 | tr -d '[:space:]')
        # Normalize a possible 0x form to decimal so the comparisons below are arithmetic.
        VU_GAS=$((VU_GAS))
        if [ "$VU_GAS" -le 0 ]; then
            echo -e "${RED}❌ Could not read gasUsed for $USER_TX_HASH${NC}"
            exit 1
        fi
        if [ "$VU_GAS" -ge "$NOMINAL_BLOCK_GAS_LIMIT" ]; then
            echo -e "${RED}❌ verifyAndUpdate used $VU_GAS gas — expected well under $NOMINAL_BLOCK_GAS_LIMIT${NC}"
            exit 1
        fi
        echo -e "${GREEN}✅ Unbounded transition settled: ${VU_GAS} gas on-chain vs ${DIRECT_GAS} gas to execute directly (~$((DIRECT_GAS / VU_GAS))x less)${NC}"
    fi
else
    echo -e "${RED}❌ Array summation verification failed - state was not updated within timeout.${NC}"
    echo -e "${YELLOW}Recent router logs:${NC}"
    docker compose logs --tail=100 router || true
    echo -e "${YELLOW}Recent node logs:${NC}"
    docker compose logs --tail=50 node-1 node-2 node-3 || true
    # If the user-submitted verifyAndUpdate reverted, re-simulate it to surface a revert reason.
    # cast run re-simulates the transaction, so this is best-effort diagnostic output.
    if [ -n "$USER_TX_HASH" ] && command -v cast >/dev/null 2>&1; then
        echo -e "${YELLOW}Execution trace for $USER_TX_HASH:${NC}"
        cast run "$USER_TX_HASH" --rpc-url http://localhost:8545 || true
    fi
    exit 1
fi

# Show recent router logs for confirmation
echo -e "${YELLOW}Recent router logs:${NC}"
docker compose logs --tail=50 router || true

# Print the execution trace of the successful user-submitted verifyAndUpdate for inspection.
# debug_traceTransaction reflects the real mined execution, not a re-simulation.
if [ -n "$USER_TX_HASH" ] && command -v cast >/dev/null 2>&1; then
    echo -e "${YELLOW}Execution trace for $USER_TX_HASH:${NC}"
    cast rpc debug_traceTransaction "$USER_TX_HASH" '{"tracer":"callTracer"}' --rpc-url http://localhost:8545 | jq '.' || true
fi

echo -e "${GREEN}✅ Test passed - Stack is up and array summation completed successfully!${NC}"
TEST_PASSED=true
exit 0