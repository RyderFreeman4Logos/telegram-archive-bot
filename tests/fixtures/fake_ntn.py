#!/usr/bin/python3
"""DA05 synthetic ntn protocol fixture. Never contacts Notion."""
import json
import os
import stat
import sys
import time
from pathlib import Path

root = Path(__file__).parent
mode = (root / "mode").read_text()
args = sys.argv[1:]
with (root / "calls").open("a") as out:
    out.write(json.dumps(args) + "\n")
if args == ["--version"]:
    print("ntn 0.1.0" if mode == "bad_version" else "ntn 0.22.10")
    sys.exit(0)
assert args[0] == "api"
assert args[-2:] == ["--notion-version", "2026-03-11"]
assert os.environ["NOTION_API_TOKEN"] == "SYNTHETIC_NOTION_TOKEN"
assert os.environ["NOTION_KEYRING"] == "0"
assert os.environ["NOTION_API_BASE_URL"] == "https://api.notion.com"
assert Path(os.environ["HOME"]).resolve() == Path.cwd()
assert Path(os.environ["NOTION_HOME"]).is_relative_to(Path.cwd())
assert not any(k in os.environ for k in ["NOTION_WORKSPACE_ID", "NOTION_WORKERS_CONFIG_FILE", "HTTP_PROXY", "HTTPS_PROXY"])
path = args[1]
method = args[args.index("-X") + 1] if "-X" in args else "GET"
if mode == "auth":
    print(json.dumps({"object": "error", "status": 401, "code": "unauthorized", "message": os.environ["NOTION_API_TOKEN"]}))
    sys.exit(1)
if mode == "rate":
    print(json.dumps({"object": "error", "status": 429, "code": "rate_limited", "retry_after": 7}))
    sys.exit(1)
if mode == "timeout" and method == "POST":
    time.sleep(10)
if mode == "malformed":
    print("this is not JSON")
    sys.exit(0)
state_path = root / "state.json"
state = json.loads(state_path.read_text()) if state_path.exists() else None
page = "00000000-0000-0000-0000-000000000001"
if "-d" in args:
    body_path = Path(args[args.index("-d") + 1].removeprefix("@"))
    assert body_path.is_relative_to(Path.cwd())
    assert stat.S_IMODE(body_path.stat().st_mode) == 0o600
    raw = body_path.read_bytes()
    assert len(raw) <= 16384
    assert b"SYNTHETIC_NOTION_TOKEN" not in raw
    body = json.loads(raw)
    assert len(body.get("children", [])) <= 100
    for block in body.get("children", []):
        for item in block[block["type"]]["rich_text"]:
            text = item["text"]["content"]
            assert len(text.encode("utf-16-le")) // 2 <= 2000
if method == "POST":
    assert path == "v1/pages"
    assert state is None, "no automatic create retry"
    state = {"object": "page", "id": page, "parent": body["parent"], "properties": body["properties"], "archived": False, "in_trash": False, "blocks": body["children"]}
    state_path.write_text(json.dumps(state))
    print(json.dumps({k: v for k, v in state.items() if k != "blocks"}))
elif method == "PATCH":
    assert path == f"v1/blocks/{page}/children"
    state["blocks"].extend(body["children"])
    state_path.write_text(json.dumps(state))
    print(json.dumps({"object": "list", "results": [], "has_more": False, "next_cursor": None}))
elif path == f"v1/pages/{page}":
    print(json.dumps({k: v for k, v in state.items() if k != "blocks"}))
else:
    assert path == f"v1/blocks/{page}/children"
    blocks = json.loads(json.dumps(state["blocks"]))
    if mode == "corrupt_body":
        # Keep the full metadata/digest markers; corrupt returned captured text only.
        index = next(i for i, b in enumerate(blocks) if b[b["type"]]["rich_text"][0]["text"]["content"] == "Captured text (managed)") + 1
        blocks[index]["paragraph"]["rich_text"][0]["text"]["content"] = "REMOTE EDIT"
    elif mode == "omit":
        blocks.pop(2)
    elif mode == "unexpected":
        blocks.insert(2, {"object": "block", "type": "unsupported", "unsupported": {}})
    elif mode == "annotations":
        blocks.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": "Human annotation, keep me"}}]}})
    for i, block in enumerate(blocks):
        block["id"] = f"10000000-0000-0000-0000-{i:012x}"
        block["has_children"] = False
        for item in block.get(block["type"], {}).get("rich_text", []):
            item["plain_text"] = item["text"]["content"]
            item["annotations"] = {"bold": True}  # harmless Notion-only formatting
    start = int(next((x.split("==", 1)[1] for x in args if x.startswith("start_cursor==")), "0"))
    end = min(start + 17, len(blocks))
    if mode == "truncate":
        blocks = blocks[:3]
        end = len(blocks)
    print(json.dumps({"object": "list", "results": blocks[start:end], "has_more": end < len(blocks), "next_cursor": str(end) if end < len(blocks) else None}))
