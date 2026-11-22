"""Start the web UI: resume an interrupted job, run Streamlit, and on
shutdown let the running job stop cleanly before the container exits.

    python -m blaster.web            # http://127.0.0.1:8501

Jobs are spawned by Streamlit; if Streamlit exits first they are
reparented to PID 1, and the container would be torn down mid-message.
So this process stays the container's main child: on SIGTERM it signals
Streamlit and the job, then waits for the job before exiting.
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from .common import ctx, home

JOB_STOP_WAIT = 80          # < stop_grace_period in docker-compose.yml


def main() -> int:
    os.chdir(home())
    jobs = ctx().jobs
    job = jobs.resume_if_wanted()
    if job:
        print(f"↻ продолжаю прерванную операцию: {job.kind} {job.meta}", flush=True)
    root = str(Path(__file__).resolve().parents[2])
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [root, os.environ.get("PYTHONPATH")]))}
    ui = subprocess.Popen([
        sys.executable, "-m", "streamlit", "run", str(Path(__file__).with_name("app.py")),
        "--server.address", os.environ.get("UI_ADDRESS", "127.0.0.1"),
        "--server.port", os.environ.get("UI_PORT", "8501"),
        "--server.headless", "true", "--server.fileWatcherType", "none",
        "--browser.gatherUsageStats", "false",
    ], env=env)

    def shutdown(sig, _frame):
        ui.send_signal(sig)
        cur = jobs.current()
        if cur and jobs.status().busy:
            print(f"⏹ останавливаю {cur.kind} — доотправит текущее и выйдет", flush=True)
            os.kill(cur.pid, signal.SIGTERM)       # → resumed on next start
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    code = ui.wait()
    end = time.time() + JOB_STOP_WAIT
    while jobs.status().busy and time.time() < end:
        time.sleep(0.5)
    return code


if __name__ == "__main__":
    sys.exit(main())
