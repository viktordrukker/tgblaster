"""`python -m blaster.web` as the container runs it: resumes an interrupted
job on start, and on SIGTERM lets the job stop cleanly before exiting."""
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from blaster.jobs import Jobs
from test_jobs import LOOPER, wait_for

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def healthy(port) -> bool:
    try:
        return urllib.request.urlopen(f"http://127.0.0.1:{port}/_stcore/health", timeout=1).status == 200
    except OSError:
        return False


def test_resume_on_start_and_clean_stop_on_sigterm(tmp_path):
    jobs = Jobs(tmp_path / "data" / "jobs")
    log = tmp_path / "data" / "jobs" / "send.log"
    # state left by a container that was killed mid-send
    (jobs.dir / "job.json").write_text(json.dumps({
        "id": "old", "kind": "send", "argv": [sys.executable, "-c", LOOPER], "log": str(log),
        "pid": 2 ** 22 + 1, "want": "running", "started_at": 0, "meta": {"campaign": "dec"}}))
    assert jobs.status().state == "interrupted"

    port = free_port()
    env = {**os.environ, "BLASTER_HOME": str(tmp_path), "UI_PORT": str(port), "PYTHONPATH": str(ROOT)}
    web = subprocess.Popen([sys.executable, "-m", "blaster.web"], cwd=ROOT, env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        assert wait_for(lambda: "started" in jobs.log_tail(), 20)          # resumed by itself
        assert wait_for(lambda: healthy(port), 40)
        web.send_signal(signal.SIGTERM)                                     # docker stop
        web.wait(timeout=60)                                                # exits after the job
    finally:
        if web.poll() is None:
            web.kill()
    out = web.stdout.read()
    assert "продолжаю прерванную операцию" in out
    assert "graceful" in jobs.log_tail()                                    # job stopped cleanly…
    assert jobs.status().state == "interrupted"                             # …and will resume again
