"""One background job at a time (resolve or send), outliving the web page.

    UI ──start──▶ supervisor (python -m blaster.jobs DIR) ──▶ child (python -m blaster …)

* The UI owns ``job.json`` (what to run and what the user wants: running,
  paused, stopped). The supervisor owns ``<id>.exit.json`` (how it ended).
  One writer per file.
* The supervisor stays in the UI's process group, so ``docker stop`` reaches
  it; the child runs in its own session and gets exactly one forwarded
  signal — a second Ctrl+C would make the CLI stop hard.
* A job that ended because of SIGTERM (container shutdown) or vanished
  without an exit record is "interrupted" and is restarted by
  ``resume_if_wanted()`` on the next container start. Paused, stopped and
  finished jobs are not.
"""
from __future__ import annotations

import ctypes
import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

PR_SET_PDEATHSIG = 1


@dataclass
class Job:
    id: str
    kind: str
    argv: list[str]
    log: str
    pid: int
    want: str = "running"            # running | paused | stopped
    started_at: float = 0.0
    meta: dict = field(default_factory=dict)


@dataclass
class Status:
    state: str                        # idle | running | stopping | finished | paused | stopped | interrupted
    job: Job | None = None
    exit_code: int | None = None

    @property
    def meta(self) -> dict:
        return self.job.meta if self.job else {}

    @property
    def busy(self) -> bool:
        return self.state in ("running", "stopping")


def _write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _alive(pid: int) -> bool:
    if pid <= 0:
        return True                            # being spawned right now
    try:
        if os.waitpid(pid, os.WNOHANG) != (0, 0):
            return False                       # our child, just reaped
    except ChildProcessError:
        pass                                   # not our child (e.g. after restart)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:                                       # pid reuse after a restart
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return True
    return b"blaster.jobs" in cmd and stat.split(")")[-1].split()[0] != "Z"


class Jobs:
    def __init__(self, directory: str | Path):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.dir / "job.json"

    # -- reading -----------------------------------------------------------
    def current(self) -> Job | None:
        d = _read_json(self.state_path)
        return Job(**d) if d else None

    def _exit(self, job: Job) -> dict | None:
        return _read_json(self.dir / f"{job.id}.exit.json")

    def status(self) -> Status:
        job = self.current()
        if job is None:
            return Status("idle")
        ex = self._exit(job)
        code = ex["exit_code"] if ex else None
        if _alive(job.pid):
            return Status("running" if job.want == "running" else "stopping", job, code)
        if job.want in ("paused", "stopped"):
            return Status(job.want, job, code)
        if ex is None or ex.get("sigterm"):
            return Status("interrupted", job, code)
        return Status("finished", job, code)

    def log_tail(self, lines: int = 200) -> str:
        job = self.current()
        if job is None or not Path(job.log).is_file():
            return ""
        with open(job.log, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 64 * 1024))
            text = f.read().decode("utf-8", "replace")
        return "\n".join(text.splitlines()[-lines:])

    # -- control -----------------------------------------------------------
    def _spawn(self, kind: str, argv: list[str], meta: dict, log: str | None = None) -> Job:
        now = time.time()
        jid = dt.datetime.fromtimestamp(now).strftime("%Y%m%d-%H%M%S-%f")
        log = log or str(self.dir / f"{jid}-{kind}.log")
        job = Job(id=jid, kind=kind, argv=list(argv), log=log, pid=0, started_at=now, meta=meta)
        _write_json(self.state_path, job.__dict__)
        with open(log, "a", encoding="utf-8") as lf:
            lf.write(f"\n=== {dt.datetime.fromtimestamp(now):%Y-%m-%d %H:%M:%S} ▶ {kind}\n")
        with open(log, "a", encoding="utf-8") as err:     # supervisor tracebacks land in the log
            proc = subprocess.Popen([sys.executable, "-m", "blaster.jobs", str(self.dir)],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=err)
        job.pid = proc.pid
        _write_json(self.state_path, job.__dict__)
        return job

    def start(self, kind: str, argv: list[str], meta: dict | None = None) -> Job:
        if self.status().busy:
            raise RuntimeError("Уже идёт другая операция — дождись её или останови.")
        return self._spawn(kind, argv, meta or {})

    def resume(self) -> Job:
        st = self.status()
        if st.busy:
            raise RuntimeError("Операция уже идёт.")
        if st.job is None:
            raise RuntimeError("Нечего продолжать.")
        return self._spawn(st.job.kind, st.job.argv, st.job.meta, log=st.job.log)

    def resume_if_wanted(self) -> Job | None:
        if self.status().state != "interrupted":
            return None
        return self.resume()

    def _signal(self, want: str) -> None:
        job = self.current()
        if job is None:
            return
        job.want = want
        _write_json(self.state_path, job.__dict__)
        if _alive(job.pid):
            os.kill(job.pid, signal.SIGINT)

    def pause(self) -> None:
        self._signal("paused")

    def stop(self) -> None:
        self._signal("stopped")


# --------------------------------------------------------------------------- #
# supervisor process
# --------------------------------------------------------------------------- #

def _die_with_parent() -> None:
    try:
        ctypes.CDLL(None).prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
    except (OSError, AttributeError):          # not Linux: best effort
        pass


def supervise(directory: str | Path) -> int:
    jobs = Jobs(directory)
    job = jobs.current()
    with open(job.log, "a", encoding="utf-8") as log:
        child = subprocess.Popen(job.argv, stdin=subprocess.DEVNULL, stdout=log,
                                 stderr=subprocess.STDOUT, start_new_session=True,
                                 preexec_fn=_die_with_parent,
                                 env={**os.environ, "PYTHONUNBUFFERED": "1"})
        got: list[int] = []

        def forward(sig, _frame):
            if not got:                        # exactly once: a second one means "hard stop"
                got.append(sig)
                child.send_signal(sig)
        signal.signal(signal.SIGINT, forward)
        signal.signal(signal.SIGTERM, forward)
        code = child.wait()
        log.write(f"=== {dt.datetime.now():%Y-%m-%d %H:%M:%S} ■ код выхода {code}\n")
    _write_json(jobs.dir / f"{job.id}.exit.json",
                {"exit_code": code, "sigterm": got == [signal.SIGTERM], "finished_at": time.time()})
    return code


if __name__ == "__main__":
    sys.exit(supervise(sys.argv[1]))
