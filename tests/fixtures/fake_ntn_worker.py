#!/usr/bin/python3
"""DA06 synthetic multi-page Notion endpoint. No network."""
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import time
import uuid

root = Path(__file__).parent
args = sys.argv[1:]
with (root / "calls").open("a") as f:
    f.write(json.dumps(args) + "\n")
if args == ["--version"]:
    print("ntn 0.0.0" if (root / "mode").read_text() == "unsupported" else "ntn 0.22.10")
    sys.exit(0)
assert args[0] == "api" and args[-2:] == ["--notion-version", "2026-03-11"]
assert os.environ["NOTION_API_TOKEN"] == "SYNTHETIC_NOTION_WORKER_TOKEN"
assert os.environ["NOTION_API_BASE_URL"] == "https://api.notion.com"
assert os.environ["NOTION_KEYRING"] == "0"
assert Path(os.environ["HOME"]).resolve() == Path.cwd()
assert not any(k in os.environ for k in ("HTTP_PROXY", "HTTPS_PROXY", "NOTION_WORKSPACE_ID"))
# A real independent connection proves subprocesses run outside SQLite write transactions.
conn = sqlite3.connect(root / "store" / "archive.sqlite", timeout=0.5)
conn.execute("BEGIN IMMEDIATE")
assert conn.execute("SELECT count(*) FROM notion_queue WHERE state='creating'").fetchone()[0] == 1
assert conn.execute("SELECT count(*) FROM notion_attempts WHERE finished_at IS NULL").fetchone()[0] == 1
conn.rollback()
conn.close()
mode = (root / "mode").read_text()
path, method = args[1], args[args.index("-X") + 1]
body = None
if "-d" in args:
    p = Path(args[args.index("-d") + 1].removeprefix("@"))
    assert p.is_relative_to(Path.cwd()) and stat.S_IMODE(p.stat().st_mode) == 0o600
    raw = p.read_bytes()
    assert len(raw) <= 450000 and b"SYNTHETIC_NOTION_WORKER_TOKEN" not in raw
    body = json.loads(raw)
state_path = root / "remote.json"
state = json.loads(state_path.read_text()) if state_path.exists() else {"pages": {}}
def save():
    pending = root / "remote.new"
    with pending.open("w") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(pending, state_path)
def summary(page):
    return {k: v for k, v in page.items() if k != "blocks"}
if mode in ("auth", "rate"):
    print(json.dumps({"object":"error", "status":401 if mode == "auth" else 429, "retry_after":1, "message":"synthetic"}))
    sys.exit(1)
if path == "v1/pages" and method == "POST":
    assert len(body["children"]) <= 100
    page_id = str(uuid.UUID(int=len(state["pages"]) + 1))
    page = {"object":"page", "id":page_id, "parent":body["parent"], "properties":body["properties"], "archived":False, "in_trash":False, "blocks":body["children"]}
    state["pages"][page_id] = page
    save()
    (root / "accepted").write_text(page_id)
    if mode == "hold_after_create":
        time.sleep(30)
    if mode == "accepted_lost":
        print("accepted but response lost")
    else:
        print(json.dumps(summary(page)))
elif path == "v1/search":
    assert method == "POST" and body["filter"] == {"property":"object", "value":"page"}
    if mode == "search_pages":
        cursor = body.get("start_cursor", "PRIVATE_CURSOR_0")
        number = int(cursor.rsplit("_", 1)[1]) + 1
        # A valid nonmatching page forces real pagination without granting a create.
        print(json.dumps({"object":"list", "results":[{"object":"page", "id":str(uuid.UUID(int=999)), "parent":{"page_id":"11111111-1111-1111-1111-111111111111"}, "properties":{"title":{"title":[{"text":{"content":"PRIVATE_SEARCH_TITLE"}}]}}}], "has_more":True, "next_cursor":f"PRIVATE_CURSOR_{number}"}))
        sys.exit(0)
    if mode in ("search_stdout_limit", "search_stderr_limit", "search_auth_overflow"):
        if mode == "search_stdout_limit":
            print("PRIVATE_RESPONSE " * 2000, flush=True)
        else:
            print(json.dumps({"object":"error", "status":401, "message":"PRIVATE_RESPONSE"}) if mode == "search_auth_overflow" else "{}", flush=True)
            print("PRIVATE_RESPONSE " * 2000, file=sys.stderr, flush=True)
        time.sleep(0.2)
        sys.exit(1)
    if mode == "search_work_limit":
        # Each file stays below RLIMIT_FSIZE; their sum exceeds the workspace cap.
        for name in ("PRIVATE_WORK_FILE_A", "PRIVATE_WORK_FILE_B"):
            with open(name, "wb") as f:
                f.truncate(8_000_001)
        time.sleep(0.2)
        sys.exit(0)
    pages = [summary(p) for p in state["pages"].values() if any(body["query"] in t["text"]["content"] for v in p["properties"].values() for t in v.get("title", []))]
    print(json.dumps({"object":"list", "results":[] if mode == "empty_search" else pages, "has_more":False, "next_cursor":None}))
elif path.startswith("v1/pages/"):
    print(json.dumps(summary(state["pages"][path.split("/")[2]])))
else:
    assert path.startswith("v1/blocks/") and path.endswith("/children")
    page = state["pages"][path.split("/")[2]]
    if method == "PATCH":
        if mode == "hold_before_append":
            (root / "appending").write_text(page["id"])
            time.sleep(30)
        assert len(body["children"]) <= 100
        page["blocks"].extend(body["children"])
        save()
        print(json.dumps({"object":"list", "results":[], "has_more":False, "next_cursor":None}))
    else:
        if mode == "hold_before_readback":
            (root / "reading").write_text(page["id"])
            time.sleep(30)
        blocks = json.loads(json.dumps(page["blocks"]))
        if mode == "remote_edit":
            blocks[2]["paragraph"]["rich_text"][0]["text"]["content"] = "human edit"
        for i, block in enumerate(blocks):
            block["id"] = str(uuid.UUID(int=10000 + i))
            block["has_children"] = False
            for item in block.get(block["type"], {}).get("rich_text", []):
                item["plain_text"] = item["text"]["content"]
        start = int(next((a.split("==", 1)[1] for a in args if a.startswith("start_cursor==")), "0"))
        end = min(start + 100, len(blocks))
        print(json.dumps({"object":"list", "results":blocks[start:end], "has_more":end < len(blocks), "next_cursor":str(end) if end < len(blocks) else None}))
