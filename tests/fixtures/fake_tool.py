#!/usr/bin/python3
"""Synthetic process protocol fixture; never used in production."""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

mode = sys.argv[1]
if mode == "args":
    print(json.dumps(sys.argv[2:]))
elif mode == "env":
    print(json.dumps(dict(os.environ)))
elif mode in {"spawn", "orphan"}:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = subprocess.Popen([
        sys.executable, "-c",
        "import signal,time;from pathlib import Path;signal.signal(signal.SIGTERM,signal.SIG_IGN);Path('child.ready').touch();time.sleep(600)",
    ])
    deadline = time.monotonic() + 3
    while not Path("child.ready").exists():
        assert time.monotonic() < deadline, "child did not install its signal handler"
        time.sleep(0.01)
    Path("child.pid").write_text(str(child.pid))
    Path("parent.pid").write_text(str(os.getpid()))
    if mode == "orphan":
        sys.exit(0)
    time.sleep(600)
elif mode == "flood":
    while True:
        os.write(1, b"x" * 8192)
        os.write(2, b"y" * 8192)
elif mode == "disk":
    for index in range(100):
        Path(f"output-{index}").write_bytes(b"x" * 4096)
elif mode == "nonzero":
    sys.exit(7)
elif mode == "symlink":
    Path("escape").symlink_to("/etc/passwd")
else:
    raise ValueError("unknown fixture mode")
