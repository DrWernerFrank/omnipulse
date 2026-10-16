import orjson
from fastapi.testclient import TestClient

from omnipulse.api.app import create_app
from omnipulse.runtime import get_runtime
from omnipulse.schema import RawItem, utcnow
from omnipulse.settings import Settings


def _raw(url, title, summary=""):
    return RawItem(source_id="test_wire", guid=url, url=url, title=title, summary=summary, published_at=utcnow())


def _frames(ws, want: int):
    """Collect envelopes, unpacking BATCH frames."""
    out = []
    while len(out) < want:
        msg = ws.receive_json()
        out.extend(msg["data"] if msg["type"] == "BATCH" else [msg])
    return out


def test_api_end_to_end():
    app = create_app(Settings(), "all")
    with TestClient(app) as client:
        rt = get_runtime()

        # --- live WebSocket with a filter + compact format ------------------------------
        with client.websocket_connect("/v1/stream?format=compact&q=cat:macro") as ws:
            assert ws.receive_json()["type"] == "WELCOME"
            ev = client.portal.call(rt.enricher.process, _raw(
                "https://wire.example/fed", "Federal Reserve raises interest rates by 25 basis points",
                "<p>The FOMC voted to raise the federal funds rate. Chair Jerome Powell said inflation remains "
                "elevated. Treasury yields rose.</p>"))
            client.portal.call(rt.enricher.process, _raw("https://wire.example/apple", "Apple launches new iPhone"))
            frames = _frames(ws, 2)
            types = [f["type"] for f in frames]
            assert "EVENT_CREATED" in types and "CLUSTER_UPDATED" in types
            created = next(f for f in frames if f["type"] == "EVENT_CREATED")
            assert created["data"]["id"] == ev["event_id"] and "Macro" in created["data"]["cat"]

        # --- REST ------------------------------------------------------------------------
        r = client.get("/v1/events", params={"q": "cat:macro", "format": "compact"}).json()
        assert r["count"] == 1 and r["items"][0]["id"] == ev["event_id"]
        assert client.get("/v1/events", params={"format": "headline"}).json()["count"] == 2
        assert client.get(f"/v1/events/{ev['event_id']}").json()["content"]["flash_headline"]
        assert client.get("/v1/events/evt_missing").status_code == 404
        cl = client.get("/v1/clusters", params={"slim": True}).json()
        assert cl["count"] == 2
        detail = client.get(f"/v1/clusters/{ev['cluster_id']}").json()
        assert detail["cluster"]["primary_event_id"] == ev["event_id"]
        hits = client.get("/v1/search", params={"query": "fed interest rate hike"}).json()
        assert hits["items"][0]["event"]["id"] == ev["event_id"]
        assert "Macro" in client.get("/v1/taxonomy").json()["categories"]
        assert client.get("/v1/stats").json()["pipeline"]["counters"]["processed"] == 2

        # --- source registry CRUD --------------------------------------------------------
        assert any(s["id"] == "test_wire" for s in client.get("/v1/sources").json()["items"])
        r = client.post("/v1/sources", params={"validate": False},
                        json={"name": "My Feed", "url": "https://example.com/rss", "type": "rss"})
        assert r.status_code == 201 and r.json()["source"]["id"] == "my_feed"
        assert client.post("/v1/sources", params={"validate": False},
                           json={"name": "My Feed", "url": "https://x", "type": "rss"}).status_code == 409
        assert client.post("/v1/sources/my_feed/pause").json()["enabled"] is False
        assert client.patch("/v1/sources/my_feed", json={"poll_interval": 30}).json()["poll_interval"] == 30
        assert client.delete("/v1/sources/my_feed").json()["deleted"] is True
        assert client.get("/v1/sources/my_feed").status_code == 404
        assert client.post("/v1/sources", params={"validate": False},
                           json={"name": "Bad", "type": "nope"}).status_code == 422

        # --- inbound push (Discord shape) -------------------------------------------------
        r = client.post("/v1/ingest/test_wire", json={"username": "osint-bot", "embeds": [
            {"title": "Power outage reported across Lisbon metro area", "url": "https://x.example/lisbon"}]})
        assert r.json()["accepted"] == 1

        # --- webhooks -----------------------------------------------------------------------
        hook = client.post("/v1/webhooks", json={"url": "https://hooks.example/omni", "q": "u>=8"}).json()
        assert hook["id"].startswith("whk_") and hook["secret"]
        assert client.delete(f"/v1/webhooks/{hook['id']}").json()["deleted"]

        # --- OpenAPI 3.1 + agent docs ---------------------------------------------------------
        spec = client.get("/openapi.json").json()
        assert spec["openapi"].startswith("3.1") and "/v1/events" in spec["paths"]
        assert "MCP" in client.get("/llms.txt").text

        # --- MCP over streamable HTTP (JSON-RPC) --------------------------------------------
        h = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
        init = client.post("/mcp", headers=h, content=orjson.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                       "clientInfo": {"name": "pytest", "version": "1"}}}))
        assert init.status_code == 200, init.text
        assert init.json()["result"]["serverInfo"]["name"] == "OmniPulse"
        tools = client.post("/mcp", headers=h, content=orjson.dumps(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})).json()
        names = {t["name"] for t in tools["result"]["tools"]}
        assert {"get_latest_events", "search_events", "wait_for_events", "list_story_clusters", "add_source"} <= names
        call = client.post("/mcp", headers=h, content=orjson.dumps({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "get_latest_events", "arguments": {"filter": "cat:macro"}}})).json()
        payload = call["result"].get("structuredContent") or orjson.loads(call["result"]["content"][0]["text"])
        assert payload["count"] == 1 and payload["events"][0]["id"] == ev["event_id"]
