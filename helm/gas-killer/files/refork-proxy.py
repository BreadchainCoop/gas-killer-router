#!/usr/bin/env python3
"""JSON-RPC proxy that re-forks the bundled Anvil to whatever block a request asks for.

Anvil forks at ONE block and never follows the chain. The fleet, however, simulates a task at
the task's anchor block — the chain head when the client submitted — so against a plain fork
every head-anchored task fails enrichment with BlockOutOfRangeError, and after any settlement
the fork's prestate is stale. This sidecar sits between SIM_HTTP_RPC and Anvil: it reads the
block parameter of each request and, when it differs from the block the fork currently sits
on, issues `anvil_reset` to that block before forwarding. Requests at the fork's current block
pass straight through, so the router and every node tracing the same task share one fork.

Concurrency is a readers/writer scheme: forwards at the current block run in parallel; a
reset waits for in-flight forwards to drain (a reset mid-trace would abort the trace) and
blocks new ones until it lands. Tag blocks (`latest`, `pending`, ...) never reset.

Stdlib only, so it runs from a stock python image with the script mounted from a ConfigMap.
"""

import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ANVIL_URL = os.environ.get("ANVIL_URL", "http://127.0.0.1:8545")
FORK_URL = os.environ.get("FORK_URL", "")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8546"))
# A trace of a 2^40-gas call can run for minutes; never cut it off from this side.
FORWARD_TIMEOUT = float(os.environ.get("FORWARD_TIMEOUT_SECONDS", "3600"))
RESET_ATTEMPTS = int(os.environ.get("RESET_ATTEMPTS", "5"))
RESET_BACKOFF = float(os.environ.get("RESET_BACKOFF_SECONDS", "1"))

# Index of the block parameter per method. Anything not listed forwards untouched.
#
# Two classes. EXECUTION methods run the EVM in Anvil, which only has state at the block it
# forked from — any other block is BlockOutOfRangeError — so they need the fork moved whenever
# the requested block differs. STATE reads (storage, code, balance, nonce, proof, headers) at a
# block *behind* the fork point Anvil forwards to the upstream RPC itself (measured: getStorageAt,
# getCode and getBlockByNumber at fork-1 and fork-5 all answer on a bare fork), so those only
# need a reset when they ask for a block *ahead* of the fork. The analyzer reads state at the
# task block's parent alongside tracing at the task block; without this split every task
# thrashed the fork between N and N-1 four times over.
EXECUTION_BLOCK_INDEX = {
    "debug_traceCall": 1,
    "eth_call": 1,
    "eth_estimateGas": 1,
    "eth_createAccessList": 1,
    "eth_simulateV1": 1,
}
STATE_BLOCK_INDEX = {
    "eth_getBalance": 1,
    "eth_getCode": 1,
    "eth_getTransactionCount": 1,
    "eth_getStorageAt": 2,
    "eth_getProof": 2,
    "eth_getBlockByNumber": 0,
    "eth_getBlockTransactionCountByNumber": 0,
}

log = logging.getLogger("refork-proxy")


def parse_block(value):
    """A concrete block number, or None for tags / hashes / absent."""
    if isinstance(value, str):
        v = value.lower()
        if v.startswith("0x"):
            try:
                return int(v, 16)
            except ValueError:
                return None
        return None
    if isinstance(value, dict):
        bn = value.get("blockNumber")
        return parse_block(bn) if bn is not None else None
    return None


def wanted_block(request, fork_block):
    """(block, method) the fork must move to for this request, or (None, None) to pass through.

    Scans a batch for the first entry that needs a move. `fork_block` is where the fork sits now.
    """
    items = request if isinstance(request, list) else [request]
    for item in items:
        if not isinstance(item, dict):
            continue
        method = item.get("method")
        params = item.get("params")
        idx = EXECUTION_BLOCK_INDEX.get(method)
        ahead_only = False
        if idx is None:
            idx = STATE_BLOCK_INDEX.get(method)
            ahead_only = True
        if idx is None or not isinstance(params, list) or len(params) <= idx:
            continue
        block = parse_block(params[idx])
        if block is None or block == fork_block:
            continue
        if ahead_only and fork_block is not None and block < fork_block:
            continue
        return block, method
    return None, None


def rpc(url, method, params, timeout=60.0):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read())
    if "error" in payload:
        raise RuntimeError(f"{method}: {payload['error']}")
    return payload.get("result")


class Fork:
    def __init__(self):
        self.cond = threading.Condition()
        self.block = None      # block the fork currently sits on
        self.readers = 0       # in-flight forwards at self.block
        self.resetting = False
        self.resets = 0

    def init_block(self):
        while True:
            try:
                self.block = int(rpc(ANVIL_URL, "eth_blockNumber", []), 16)
                log.info("fork is at block %d", self.block)
                return
            except Exception as exc:  # anvil still starting
                log.info("waiting for anvil: %s", exc)
                time.sleep(2)

    def acquire(self, request):
        """Move the fork to wherever `request` needs it (if anywhere) and register a reader.

        The move decision is taken under the lock against the fork's *current* block, so a
        state read judged "behind the fork, pass through" cannot be overtaken by a reset that
        drops the fork below it.
        """
        with self.cond:
            while True:
                block, method = wanted_block(request, self.block) if request is not None else (None, None)
                if self.resetting or (block is not None and self.readers > 0):
                    self.cond.wait()
                    continue
                if block is None:
                    self.readers += 1
                    return
                self.resetting = True
                break
        try:
            self._reset(block, method)
        finally:
            with self.cond:
                self.resetting = False
                if self.block == block:
                    self.readers += 1
                self.cond.notify_all()

    def release(self):
        with self.cond:
            self.readers -= 1
            self.cond.notify_all()

    def _reset(self, block, method):
        forking = {"blockNumber": block}
        if FORK_URL:
            forking["jsonRpcUrl"] = FORK_URL
        last = None
        for attempt in range(1, RESET_ATTEMPTS + 1):
            try:
                rpc(ANVIL_URL, "anvil_reset", [{"forking": forking}], timeout=300.0)
                self.block = block
                self.resets += 1
                log.info("re-forked at block %d for %s (reset #%d)", block, method, self.resets)
                return
            except Exception as exc:
                last = exc
                log.warning("anvil_reset to %d failed (attempt %d/%d): %s", block, attempt, RESET_ATTEMPTS, exc)
                time.sleep(RESET_BACKOFF * attempt)
        raise RuntimeError(f"could not re-fork at block {block}: {last}")


FORK = Fork()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet per-request access logs
        pass

    def _send(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/healthz", "/readyz"):
            try:
                rpc(ANVIL_URL, "eth_blockNumber", [], timeout=5.0)
                self._send(200, json.dumps({"ok": True, "forkBlock": FORK.block, "resets": FORK.resets}).encode())
            except Exception as exc:
                self._send(503, json.dumps({"ok": False, "error": str(exc)}).encode())
            return
        self._send(404, b"not found", "text/plain")

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        try:
            request = json.loads(raw) if raw else None
        except ValueError:
            request = None
        try:
            FORK.acquire(request)
        except Exception as exc:
            rid = request.get("id") if isinstance(request, dict) else None
            err = {"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": str(exc)}}
            self._send(200, json.dumps(err).encode())
            return
        try:
            req = urllib.request.Request(
                ANVIL_URL, data=raw,
                headers={"Content-Type": self.headers.get("Content-Type") or "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=FORWARD_TIMEOUT) as resp:
                    self._send(resp.status, resp.read(), resp.headers.get("Content-Type") or "application/json")
            except urllib.error.HTTPError as http_err:
                self._send(http_err.code, http_err.read(), http_err.headers.get("Content-Type") or "application/json")
            except Exception as exc:
                rid = request.get("id") if isinstance(request, dict) else None
                err = {"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": f"upstream anvil: {exc}"}}
                self._send(502, json.dumps(err).encode())
        finally:
            FORK.release()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    FORK.init_block()
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    server.daemon_threads = True
    log.info("listening on :%d, forwarding to %s", LISTEN_PORT, ANVIL_URL)
    server.serve_forever()


if __name__ == "__main__":
    main()
