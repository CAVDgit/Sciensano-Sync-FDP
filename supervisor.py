#!/usr/bin/env python3
"""
supervisor.py

Single entrypoint for the sync container.

Responsibilities:
  - Periodically check for manual sync requests in the "settings FDP"
    (TECHNICAL.syncRequest resources with matching TECHNICAL.syncID).
  - Periodically run the automatic sync pipeline.
  - Ensure that *at most one* sync pipeline runs at any time
    (manual or automatic) using a simple file-based lock.

Configuration (all from config.py / env):
  - URL_SETTINGS_FDP          : root URL of the settings FDP
  - SYNC_ID                   : identifier of the sync configuration to use
  - CHECK_FOR_MANUAL_SYNC     : interval (seconds) to poll for manual requests
  - SYNC_INTERVAL             : interval (seconds) for automatic sync runs
                                (if <= 0, automatic sync is disabled)
"""

import argparse
import json
import subprocess
import sys
import time
import os
from pathlib import Path
from datetime import datetime

import rdflib
import requests
import config


# ------------- small helpers -------------


def _now() -> str:
    """Return current time as ISO-8601 string with seconds precision."""
    return datetime.now().isoformat(timespec="seconds")


# Simple file-based lock to prevent concurrent pipeline runs
LOCK_PATH = "/tmp/sync_pipeline.lock"


class PipelineLock:
    """
    Simple file lock using O_CREAT|O_EXCL.
    If acquisition fails with FileExistsError, some other process in the
    container is already running the sync pipeline.
    """

    def __init__(self, path: str = LOCK_PATH):
        self.path = path
        self.fd = None

    def acquire(self) -> bool:
        try:
            # O_CREAT | O_EXCL -> fail if file already exists
            self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self.fd, str(os.getpid()).encode("utf-8"))
            return True
        except FileExistsError:
            # Lock already held by another process
            return False
        except Exception as e:
            print(f"⚠️  {_now()} Unable to acquire lock {self.path}: {e}")
            return False

    def release(self) -> None:
        if self.fd is not None:
            try:
                os.close(self.fd)
            except Exception:
                pass
            self.fd = None
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"⚠️  {_now()} Unable to remove lock {self.path}: {e}")

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc, tb):
        self.release()


def _fetch_ttl(url: str, timeout: int) -> rdflib.Graph | None:
    """
    Fetch a URL as Turtle and parse it into an rdflib.Graph.
    Returns None on error.
    """
    headers = {"Accept": "text/turtle"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        g = rdflib.Graph()
        g.parse(data=r.text, format="turtle")
        return g
    except Exception as e:
        print(f"⚠️  {_now()} Fetch failed {url}: {e}")
        return None


def get_token() -> str:
    """
    Get a bearer token against the settings FDP.

    Uses:
      - URL_SETTINGS_FDP
      - LOGIN_SETTINGS_FDP
      - PASSWORD_SETTINGS_FDP
    """
    headers = {"Content-type": "application/json", "Accept": "application/json"}

    login = config.LOGIN_SETTINGS_FDP
    password = config.PASSWORD_SETTINGS_FDP
    base_url = config.URL_SETTINGS_FDP

    data = {"email": login, "password": password}
    url_token = f"{base_url.rstrip('/')}/tokens"
    res = requests.post(url_token, data=json.dumps(data), headers=headers, timeout=config.TIMEOUT)
    res.raise_for_status()
    return res.json()["token"]


def _delete_resource(uri: str, token: str) -> tuple[bool, int | None, str | None]:
    """
    DELETE a resource in the settings FDP (used to consume TECHNICAL.syncRequest).
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "*/*"}
    try:
        r = requests.delete(uri, headers=headers, timeout=getattr(config, "TIMEOUT", 30))
        return r.ok, r.status_code, r.text
    except requests.RequestException as e:
        return False, None, str(e)


def check_and_run(URL_SETTINGS_FDP: str, SYNC_ID: str) -> int:
    """
    Manual-trigger checker.

    - Find the TECHNICAL.sync resource with the given SYNC_ID
    - Find a matching TECHNICAL.syncRequest that also has that SYNC_ID
    - DELETE that syncRequest (to avoid re-running)
    - If deletion succeeded (we "own" the request), run sync_pipeline.py

    Return codes:
      0  -> OK (pipeline completed or no-op)
      1+ -> various error situations (network, auth, missing pipeline...)
    """
    # -------- Namespaces --------
    NS = {key: rdflib.Namespace(uri) for key, uri in config.NAMESPACES.items()}
    globals().update(NS)

    if not SYNC_ID:
        print(f"❌ {_now()} SYNC_ID is not set (neither via --sync-id nor config.SYNC_ID).")
        return 2

    timeout = config.TIMEOUT
    root_url = URL_SETTINGS_FDP or config.URL_SETTINGS_FDP

    print(f"\n🔄 {_now()} Fetching Sync settings from {root_url} (sync_id={SYNC_ID})")
    root_g = _fetch_ttl(root_url, timeout=timeout)
    if root_g is None:
        print(f"❌ {_now()} Error fetching sync settings from {root_url}.")
        return 1

    # Collect all candidates that might contain TECHNICAL.sync resources
    candidates = [root_url]
    try:
        for fdp_root in root_g.subjects(RDF.type, FDP.FAIRDataPoint):
            for o in root_g.objects(fdp_root, TECHNICAL.sync):
                candidates.append(str(o))
    except Exception:
        pass

    # de-dup while preserving order
    seen = set()
    candidates = [u for u in candidates if not (u in seen or seen.add(u))]

    found_sync_id = False
    found_sync_request = False
    matching_request_url = None

    # Look for TECHNICAL.sync with our syncID and attached TECHNICAL.syncRequest
    for sync_url in candidates:
        g = _fetch_ttl(sync_url, timeout=timeout)
        if g is None:
            continue

        has_id = any(str(o) == str(SYNC_ID) for _, _, o in g.triples((None, TECHNICAL.syncID, None)))
        if not has_id:
            continue

        found_sync_id = True

        all_sync_requests = {str(o) for o in g.objects(None, TECHNICAL.syncRequest)}
        if not all_sync_requests:
            continue

        for request in all_sync_requests:
            g_req = _fetch_ttl(request, timeout=timeout)
            if g_req is None:
                continue
            has_matching_id = any(str(o) == str(SYNC_ID) for _, _, o in g_req.triples((None, TECHNICAL.syncID, None)))
            if has_matching_id:
                found_sync_request = True
                matching_request_url = request
                print(f"✅ {_now()} Synchronisation request found for syncID='{SYNC_ID}' at {request}")
                break

        if found_sync_request:
            break

    if not found_sync_id:
        print(
            f"ℹ️  {_now()} No synchronisation entry (TECHNICAL.syncID) detected "
            f"for syncID='{SYNC_ID}' under {root_url}."
        )
        return 3

    if not found_sync_request or not matching_request_url:
        # No request -> this is NOT an error, just nothing to do
        interval = config.CHECK_FOR_MANUAL_SYNC
        print(
            f"ℹ️  {_now()} Found documents with syncID='{SYNC_ID}', but no matching TECHNICAL.syncRequest with the same ID."
        )
        print(f"ℹ️  {_now()} Next sync request check in {interval} seconds.")
        return 3

    # ---- DELETE the matching syncRequest BEFORE starting the pipeline ----
    try:
        token = get_token()
    except Exception as e:
        print(f"❌ {_now()} Failed to obtain token: {e}")
        return 6

    ok, status, body = _delete_resource(matching_request_url, token)

    if ok:
        # This worker successfully grabbed and deleted the request → safe to run the pipeline.
        print(f"🗑️  {_now()} syncRequest deleted: {matching_request_url} (status={status})")
    elif status == 404:
        # Another worker has already taken and deleted this request → do not run pipeline again.
        print(
            f"ℹ️  {_now()} syncRequest already removed (404): {matching_request_url}. "
            "Another worker likely already started the pipeline; exiting."
        )
        return 0
    else:
        print(f"❌ {_now()} Failed to delete syncRequest {matching_request_url} (status={status})\n{body}")
        return 6

    # ---- Call the pipeline (we are the owner of this run) ----
    pipeline = Path(__file__).resolve().parent / "sync_pipeline.py"
    if not pipeline.exists():
        print(f"❌ {_now()} sync_pipeline.py not found at {pipeline}")
        return 4

    cmd = [sys.executable, str(pipeline)]
    # Optionally also pass the request URL:
    # cmd += ["--sync-request-url", matching_request_url]

    print(f"🚀 {_now()} Calling synchronisation pipeline (manual request)…")
    try:
        res = subprocess.run(cmd, check=False)
        if res.returncode == 0:
            print(f"🎉 {_now()} Manual synchronisation pipeline completed successfully.")
        else:
            print(f"⚠️  {_now()} Manual synchronisation pipeline exited with code {res.returncode}.")
        return res.returncode
    except Exception as e:
        print(f"❌ {_now()} Failed to start pipeline: {e}")
        return 5


def run_automatic_pipeline() -> int:
    """
    Run sync_pipeline.py for the periodic (automatic) sync.

    This uses the same script and behaviour as manual runs, but is
    triggered purely based on time (SYNC_INTERVAL).
    """
    pipeline = Path(__file__).resolve().parent / "sync_pipeline.py"
    if not pipeline.exists():
        print(f"❌ {_now()} sync_pipeline.py not found at {pipeline}")
        return 4

    cmd = [sys.executable, str(pipeline)]
    print(f"🚀 {_now()} Calling synchronisation pipeline (automatic interval)…")
    try:
        res = subprocess.run(cmd, check=False)
        if res.returncode == 0:
            print(f"🎉 {_now()} Automatic synchronisation pipeline completed successfully.")
        else:
            print(f"⚠️  {_now()} Automatic synchronisation pipeline exited with code {res.returncode}.")
        return res.returncode
    except Exception as e:
        print(f"❌ {_now()} Failed to start automatic pipeline: {e}")
        return 5


# ------------- main supervisor loop -------------


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Supervisor for sync pipeline:\n"
            "  - periodically checks for manual sync requests\n"
            "  - periodically runs automatic sync\n"
            "  - ensures only one pipeline run at a time via a file lock"
        )
    )
    parser.add_argument(
        "--url",
        dest="url",
        help="URL of the Sync Settings FDP root (overrides config.URL_SETTINGS_FDP)",
    )
    parser.add_argument(
        "--sync-id",
        dest="sync_id",
        help="The syncID to look for (overrides config.SYNC_ID)",
    )
    args = parser.parse_args()

    settings_url = args.url or config.URL_SETTINGS_FDP
    sync_id = args.sync_id or config.SYNC_ID

    # Interval for manual-sync polling (required int in config)
    manual_interval = config.CHECK_FOR_MANUAL_SYNC
    # Interval for automatic pipeline (required int in config)
    auto_interval = config.SYNC_INTERVAL

    # Interpret SYNC_INTERVAL <= 0 as "automatic sync disabled"
    if auto_interval <= 0:
        print(f"ℹ️  {_now()} SYNC_INTERVAL <= 0; automatic sync disabled.")
        auto_interval = None

    print("============================================")
    print(f"🕹️  Supervisor started at {_now()}")
    print(f"   Settings FDP URL         : {settings_url}")
    print(f"   syncID                   : {sync_id}")
    print(f"   Manual check interval    : {manual_interval} seconds")
    print(f"   Automatic sync interval  : {auto_interval if auto_interval else 'DISABLED'}")
    print("   Lock file                :", LOCK_PATH)
    print("============================================")

    # Initialize next tick times
    now = time.time()
    next_manual = now + manual_interval
    next_auto = (now + auto_interval) if auto_interval else None

    lock = PipelineLock(LOCK_PATH)

    while True:
        now = time.time()

        # ---- 1) Manual sync check ----
        if now >= next_manual:
            print(f"\n⏰ {_now()} Time to check for manual sync requests…")
            with lock as got_lock:
                if got_lock:
                    # This call will only actually run the pipeline if there *is* a syncRequest.
                    rc = check_and_run(settings_url, sync_id)
                    print(f"ℹ️  {_now()} Manual sync check finished with rc={rc}.")
                else:
                    print(
                        f"⏳ {_now()} Another sync is currently running; manual check skipped this round."
                    )

            # schedule next manual check
            next_manual = time.time() + manual_interval

        # ---- 2) Automatic sync run ----
        if next_auto is not None and now >= next_auto:
            print(f"\n⏰ {_now()} Time for automatic sync run…")
            with lock as got_lock:
                if got_lock:
                    rc = run_automatic_pipeline()
                    print(f"ℹ️  {_now()} Automatic sync finished with rc={rc}.")
                else:
                    print(
                        f"⏳ {_now()} Another sync is currently running; automatic run skipped this round."
                    )
            # schedule next automatic run
            next_auto = time.time() + (auto_interval or 0)

        # ---- 3) Sleep a bit to avoid busy loop ----
        time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
