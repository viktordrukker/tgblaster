"""Background jobs: a supervisor process runs one command, logs it and
records how it ended, so the UI can show status and resume after a
container restart."""
import os
import signal
import sys
import time

import pytest

from blaster.jobs import Jobs

PY = sys.executable
# Child that stops gracefully on SIGINT/SIGTERM, like `blaster send`.
LOOPER = ("import signal,sys,time\n"
          "signal.signal(signal.SIGINT, lambda *a: (print('graceful', flush=True), sys.exit(2)))\n"
          "signal.signal(signal.SIGTERM, lambda *a: (print('graceful', flush=True), sys.exit(2)))\n"
          "print('started', flush=True)\n"
          "time.sleep(60)\n")


def wait_for(cond, timeout=15.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.1)
    return False


@pytest.fixture
def jobs(tmp_path):
    return Jobs(tmp_path / "jobs")


def test_finished_job_reports_exit_and_log(jobs):
    jobs.start("demo", [PY, "-c", "print('привет')"])
    assert wait_for(lambda: jobs.status().state == "finished")
    st = jobs.status()
    assert st.exit_code == 0
    assert "привет" in jobs.log_tail()
    assert jobs.resume_if_wanted() is None          # finished jobs are not restarted


def test_only_one_job_at_a_time(jobs):
    jobs.start("demo", [PY, "-c", LOOPER])
    assert wait_for(lambda: "started" in jobs.log_tail())
    with pytest.raises(RuntimeError):
        jobs.start("demo", [PY, "-c", "pass"])
    jobs.pause()
    assert wait_for(lambda: jobs.status().state == "paused")


def test_pause_stops_child_gracefully_and_is_not_resumed(jobs):
    jobs.start("send", [PY, "-c", LOOPER], meta={"campaign": "x"})
    assert wait_for(lambda: "started" in jobs.log_tail())
    jobs.pause()
    assert wait_for(lambda: jobs.status().state == "paused")
    assert "graceful" in jobs.log_tail()
    assert jobs.status().exit_code == 2
    assert jobs.status().meta == {"campaign": "x"}
    assert jobs.resume_if_wanted() is None
    jobs.resume()
    assert wait_for(lambda: jobs.status().state == "running")
    jobs.stop()
    assert wait_for(lambda: jobs.status().state == "stopped")


def test_container_shutdown_sigterm_is_resumed(jobs):
    job = jobs.start("send", [PY, "-c", LOOPER])
    assert wait_for(lambda: "started" in jobs.log_tail())
    os.kill(job.pid, signal.SIGTERM)                # what `docker stop` does
    assert wait_for(lambda: jobs.status().state == "interrupted")
    assert "graceful" in jobs.log_tail()
    again = jobs.resume_if_wanted()
    assert again is not None and again.pid != job.pid
    assert wait_for(lambda: jobs.log_tail().count("started") == 2)
    jobs.stop()
    assert wait_for(lambda: jobs.status().state == "stopped")


def test_killed_supervisor_counts_as_interrupted(jobs):
    job = jobs.start("send", [PY, "-c", LOOPER])
    assert wait_for(lambda: "started" in jobs.log_tail())
    os.kill(job.pid, signal.SIGKILL)                # supervisor killed hard
    assert wait_for(lambda: jobs.status().state == "interrupted")
    # the child is told to stop too (parent-death signal), never orphaned
    assert wait_for(lambda: "graceful" in jobs.log_tail())


def test_no_job_yet(jobs):
    assert jobs.status().state == "idle"
    assert jobs.log_tail() == ""
    assert jobs.resume_if_wanted() is None


def test_resume_uses_resume_argv_so_one_shot_flags_are_not_repeated(jobs):
    jobs.start("send", [PY, "-c", LOOPER.replace("started", "first-run")],
               resume_argv=[PY, "-c", "print('resumed-run')"])
    assert wait_for(lambda: "first-run" in jobs.log_tail())
    jobs.pause()
    assert wait_for(lambda: jobs.status().state == "paused")
    jobs.resume()
    assert wait_for(lambda: "resumed-run" in jobs.log_tail())
