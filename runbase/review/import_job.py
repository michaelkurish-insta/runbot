"""Background import with bounded, live output for the review UI."""

from collections import deque
import os
import re
import signal
import subprocess
import threading
import time


class ImportJob:
    def __init__(self, command, cwd, timeout_s=1800):
        self.command = command
        self.cwd = cwd
        self.timeout_s = timeout_s
        self._lock = threading.Lock()
        self._lines = deque(maxlen=250)
        self._started = None
        self._finished = None
        self._state = {"running": False, "success": None, "message": "",
                       "stage": "", "warnings": [], "new_count": 0, "enriched_count": 0, "strava_matched": 0}

    def start(self):
        with self._lock:
            if self._state["running"]:
                return False
            self._lines.clear()
            self._started = time.monotonic()
            self._finished = None
            self._state = {"running": True, "success": None,
                           "message": "Starting import...", "stage": "Starting",
                           "warnings": [], "new_count": 0, "enriched_count": 0, "strava_matched": 0}
        threading.Thread(target=self._run, daemon=True).start()
        return True

    def snapshot(self):
        with self._lock:
            elapsed = ((self._finished or time.monotonic()) - self._started
                       if self._started is not None else 0)
            return {**self._state, "warnings": list(self._state["warnings"]),
                    "output": "\n".join(self._lines), "elapsed_s": int(elapsed)}

    def _record(self, line):
        line = line.strip()
        if not line:
            return
        with self._lock:
            self._lines.append(line[:4000])
            self._state["message"] = line
            if line.startswith("==="):
                self._state["stage"] = line.strip("= ")
            if line.startswith("WARNING:"):
                warning = line.removeprefix("WARNING:").strip()
                if warning not in self._state["warnings"]:
                    self._state["warnings"].append(warning)
            match = re.match(r"(\d+) new, \d+ skipped", line)
            if match:
                self._state["new_count"] = int(match.group(1))
            enrichment = re.match(r"Enrichment complete: (\d+) enriched", line)
            if enrichment:
                self._state["enriched_count"] += int(enrichment.group(1))
            matched = re.search(r"(\d+) Strava matched", line)
            if matched:
                self._state["strava_matched"] = int(matched.group(1))

    def _run(self):
        timed_out = threading.Event()
        success = False
        try:
            with subprocess.Popen(
                self.command, cwd=self.cwd, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, bufsize=1,
                start_new_session=True,
            ) as process:
                def expire():
                    if process.poll() is None:
                        timed_out.set()
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

                timer = threading.Timer(self.timeout_s, expire)
                timer.daemon = True
                timer.start()
                try:
                    for line in process.stdout:
                        self._record(line)
                    success = process.wait() == 0 and not timed_out.is_set()
                finally:
                    timer.cancel()
            if timed_out.is_set():
                self._record(f"Import timed out after {self.timeout_s:g} seconds. "
                             "Completed files were saved; check the last step below and retry.")
        except Exception as exc:
            self._record(f"Import could not finish: {exc}")
        finally:
            with self._lock:
                self._finished = time.monotonic()
                self._state["running"] = False
                self._state["success"] = success
                if success:
                    self._state["message"] = (
                        f"Import complete: {self._state['new_count']} new activities, "
                        f"{self._state['enriched_count']} enriched, "
                        f"{self._state['strava_matched']} Strava matched."
                    )
                elif not self._lines:
                    self._state["message"] = "Import stopped without output. Please retry."
