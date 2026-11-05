#!/usr/bin/env python3
"""Real nonproduction LB -> three-node Raft replication acceptance.

FIDUCIA_E2E_NODE_URLS=http://127.0.0.1:18090,http://127.0.0.1:18091,http://127.0.0.1:18092 \
FIDUCIA_E2E_GATEWAY_URL=http://127.0.0.1:18088 FIDUCIA_E2E_ALLOW_WRITES=1 \
python3 tests/live_peer_replication_e2e.py

All endpoints must belong to the same three-replica Raft quorum. Unlike
single-endpoint smoke tests this checks applied indexes on every actual member.
"""
import json
import os
import time
import urllib.parse
import urllib.request
import uuid


def origins(name, expected):
    values = [v.strip().rstrip("/") for v in os.getenv(name, "").split(",") if v.strip()]
    if len(values) != expected or len(set(values)) != expected:
        raise RuntimeError("%s needs %s independent origins" % (name, expected))
    for value in values:
        u = urllib.parse.urlsplit(value)
        if u.scheme not in ("http", "https") or not u.hostname or u.path not in ("", "/"):
            raise ValueError("%s must contain HTTP origin URLs" % name)
    return values


def request(base, path, method="GET", body=None, internal=False):
    headers = {"accept": "application/json"}
    if internal and os.getenv("FIDUCIA_E2E_INTERNAL_SECRET"):
        headers["x-fiducia-internal-auth"] = os.environ["FIDUCIA_E2E_INTERNAL_SECRET"]
    if not internal and os.getenv("FIDUCIA_E2E_API_KEY"):
        headers["authorization"] = "Bearer " + os.environ["FIDUCIA_E2E_API_KEY"]
    data = None if body is None else json.dumps(body).encode()
    if data is not None:
        headers["content-type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=8) as result:
        assert result.status == 200, (path, result.status)
        return json.load(result)


def status(origin):
    consensus = request(origin, "/v1/status", internal=True)["consensus"]
    shards = {s["shard_id"]: s for s in consensus["shards"]}
    assert shards and len(shards) == len(consensus["shards"]), consensus
    assert not consensus.get("unresponsive_shards"), consensus
    for shard in shards.values():
        assert shard["storage_healthy"] is True, shard
        assert shard["last_applied"] <= shard["commit_index"], shard
    return consensus["node_id"], shards, consensus["shard_count"]


def fnv1a(key):
    # Frozen routing: fiducia-routing.rs uses u32 FNV-1a, not the u64 variant.
    h = 0x811c9dc5
    for byte in key.encode():
        h = ((h ^ byte) * 0x01000193) & 0xffffffff
    return h


def snapshot(nodes):
    rows = [status(node) for node in nodes]
    assert len({name for name, _, _ in rows}) == 3, "endpoints repeat a node"
    assert len({count for _, _, count in rows}) == 1, "shard_count disagreement"
    seen = {}
    for node_id, shards, _ in rows:
        for sid, shard in shards.items():
            if shard["role"] == "leader":
                key = (sid, shard["term"])
                assert key not in seen or seen[key] == node_id, ("dual leader", key)
                seen[key] = node_id
    return rows


def output(response):
    assert response["committed"] is True, response
    return response["result"]["output"]


def main():
    nodes = origins("FIDUCIA_E2E_NODE_URLS", 3)
    gateway = origins("FIDUCIA_E2E_GATEWAY_URL", 1)[0]
    if os.getenv("FIDUCIA_E2E_ALLOW_WRITES") != "1":
        raise RuntimeError("nonproduction write opt-in is required")
    before = snapshot(nodes)
    common = set.intersection(*(set(shards) for _, shards, _ in before))
    assert common, "no shard with three reachable replicas"
    target = min(common)
    count = before[0][2]
    previous = max(shards[target]["commit_index"] for _, shards, _ in before)
    prefix = "e2e/raft/" + uuid.uuid4().hex
    keys = [prefix + "/" + str(i) for i in range(count * 12 + 1)
            if fnv1a(prefix + "/" + str(i)) % count == target][:4]
    assert len(keys) == 4
    try:
        for n, key in enumerate(keys):
            path = "/v1/kv?key=" + urllib.parse.quote(key, safe="")
            ack = output(request(gateway, path, "PUT",
                                 {"value": "revision-" + str(n), "prev_revision": 0}))
            assert ack["ok"] is True and ack["revision"] > 0, ack
            read = request(gateway, path)
            assert read["found"] is True
            assert read["entry"]["value"] == "revision-" + str(n)
        deadline = time.monotonic() + 30
        last = None
        while time.monotonic() < deadline:
            last = [s[target] for _, s, _ in snapshot(nodes)]
            leaders = [s for s in last if s["role"] == "leader"]
            if len(leaders) == 1 and leaders[0]["commit_index"] >= previous + 4:
                committed = leaders[0]["commit_index"]
                if all(s["last_applied"] >= committed for s in last):
                    print("PASS: shard %d, four committed writes applied on 3/3 nodes" % target)
                    return
            time.sleep(0.3)
        raise AssertionError("three-replica apply convergence failed: %r" % last)
    finally:
        for key in keys:
            try:
                request(gateway, "/v1/kv?key=" + urllib.parse.quote(key, safe=""), "DELETE")
            except OSError:
                pass


if __name__ == "__main__":
    main()
