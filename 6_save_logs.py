#!/usr/bin/env python3
"""
Step 6: Build log dataset TTL from actions, publish it to the logs FDP,
then prune old synchronization logs in the logs catalogue, keeping only
the most recent `config.LAST_LOGS_TO_KEEP`.
"""

import argparse
import json
from datetime import datetime, timezone
from typing import Tuple, Optional

import requests
import config
import rdflib
from pathlib import Path
import shutil

# -------- Namespaces --------
# Load namespaces from config and expose them as globals (RDF, DCAT, DCT, TECHNICAL, …)
NS = {key: rdflib.Namespace(uri) for key, uri in config.NAMESPACES.items()}
globals().update(NS)


# ---------- HTTP (FDP auth + publish logs) ----------

def get_token() -> str:
    """
    Get an authentication token for the *logs* FDP, using the dedicated
    LOGIN_SETTINGS_FDP / PASSWORD_SETTINGS_FDP credentials.
    """
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    data = {"email": config.LOGIN_SETTINGS_FDP, "password": config.PASSWORD_SETTINGS_FDP}
    URL_TOKEN_LOG_FDP = f"{config.URL_SETTINGS_FDP.rstrip('/')}/tokens"
    res = requests.post(URL_TOKEN_LOG_FDP, data=json.dumps(data), headers=headers, timeout=config.TIMEOUT)
    res.raise_for_status()
    return res.json()["token"]


def create_logs_ttl(token: str, rdf_content: str) -> Tuple[bool, Optional[int], str, Optional[str]]:
    """
    POST the log dataset TTL to the logs FDP.

    Assumption: the logs FDP exposes an LDP container at <URL_SETTINGS_FDP>/logs
    that behaves like /catalog, /dataset, etc. on a standard FDP.
    """
    url = f"{str(config.URL_SETTINGS_FDP).rstrip('/')}/logs"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/turtle",
        "Accept": "text/turtle",
    }
    try:
        r = requests.post(
            url,
            data=rdf_content.encode("utf-8"),
            headers=headers,
            timeout=getattr(config, "TARGET_TIMEOUT", config.TIMEOUT),
        )
        uri = r.headers.get("Location")
        return r.ok, r.status_code, r.text, uri
    except requests.RequestException as e:
        return False, None, str(e), None


def publish_logs(uri: str, token: str) -> Tuple[bool, Optional[int], Optional[str]]:
    """
    PUT /meta/state to mark the log dataset as PUBLISHED.
    """
    url = f"{uri}/meta/state"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    data = '{"current": "PUBLISHED"}'
    try:
        r = requests.put(
            url,
            headers=headers,
            data=data,
            timeout=getattr(config, "TARGET_TIMEOUT", config.TIMEOUT),
        )
        status = None
        if r.ok and r.text:
            try:
                status = json.loads(r.text).get("current")
            except json.JSONDecodeError:
                status = None
        return r.ok, r.status_code, status
    except requests.RequestException as e:
        return False, None, str(e)


# ---------- Small HTTP helpers for pruning ----------

def _fetch_ttl(uri: str, token: str | None = None) -> tuple[bool, str | None]:
    """
    GET a resource as text/turtle. Optional token if the logs FDP is protected.
    """
    headers = {"Accept": "text/turtle"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        r = requests.get(uri, headers=headers, timeout=getattr(config, "TARGET_TIMEOUT", config.TIMEOUT))
        if r.ok:
            return True, r.text
        return False, None
    except requests.RequestException:
        return False, None


def _delete_resource(uri: str, token: str) -> tuple[bool, int | None, str | None]:
    """
    DELETE a resource on the logs FDP.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "*/*"}
    try:
        r = requests.delete(uri, headers=headers, timeout=getattr(config, "TARGET_TIMEOUT", config.TIMEOUT))
        return r.ok, r.status_code, r.text
    except requests.RequestException as e:
        return False, None, str(e)


def _parse_iso_dt(s: str):
    """
    Robust ISO-8601 parser. Returns timezone-aware datetime or None.
    """
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# ---------- Prune logic ----------

def prune_old_logs(keep: int) -> int:
    """
    Delete the oldest logs so that only `keep` remain.

    Discovery strategy:
      1. Fetch the logs catalogue (currently config.URL_SETTINGS_FDP).
      2. Find all log datasets via:
           - TECHNICAL.logs     (link from catalogue → datasets), OR
           - RDF.type TECHNICAL.Logs on the dataset.
      3. For each log, fetch its TTL and read dct:created (or technical:modified).
      4. Sort by date (newest first); delete the oldest beyond 'keep'.
    Returns: number of logs deleted.
    """
    try:
        # NOTE: if you later define a dedicated LOG_LOCATION_URL in config,
        # this is the place to switch from URL_SETTINGS_FDP to LOG_LOCATION_URL.
        logs_catalogue = config.URL_SETTINGS_FDP
    except Exception:
        print("ℹ️ No config.URL_SETTINGS_FDP configured; skipping prune.")
        return 0

    ok, cat_ttl = _fetch_ttl(logs_catalogue, token=None)
    if not ok or not cat_ttl:
        print(f"⚠️ Cannot fetch logs catalogue {logs_catalogue}; skipping prune.")
        return 0

    g = rdflib.Graph()
    try:
        g.parse(data=cat_ttl, format="turtle")
    except Exception:
        print("⚠️ Cannot parse logs catalogue TTL; skipping prune.")
        return 0

    # Either explicit predicate technical:logs, or any subject typed as TECHNICAL.Logs
    log_uris = [str(o) for o in g.objects(None, TECHNICAL.logs)]
    if not log_uris:
        log_uris = [str(s) for s in g.subjects(RDF.type, TECHNICAL.Logs)]

    if not log_uris:
        print("ℹ️ No log found to prune.")
        return 0

    # Extract created/modified timestamps per log dataset
    items: list[tuple[str, datetime | None]] = []
    for du in log_uris:
        ok, dt_ttl = _fetch_ttl(du, token=None)
        created_dt = None
        if ok and dt_ttl:
            try:
                dg = rdflib.Graph()
                dg.parse(data=dt_ttl, format="turtle")
                created = next((str(o) for o in dg.objects(rdflib.URIRef(du), DCT.created)), None)
                if not created:
                    created = next((str(o) for o in dg.objects(rdflib.URIRef(du), TECHNICAL.modified)), None)
                created_dt = _parse_iso_dt(created) if created else None
            except Exception:
                created_dt = None
        items.append((du, created_dt))

    # Sort: newest first, undated last
    def _sort_key(pair: tuple[str, datetime | None]):
        _uri, dt = pair
        return (0, -dt.timestamp()) if dt else (1, 0)

    items_sorted = sorted(items, key=_sort_key)
    to_delete = items_sorted[keep:]

    if not to_delete:
        print(f"ℹ️ Nothing to prune — total {len(items_sorted)}, keep {keep}.")
        return 0

    # Auth once for deletions
    try:
        token = get_token()
    except Exception as e:
        print(f"❌ Auth failed for pruning: {e}")
        return 0

    deleted = 0
    for uri, _ in to_delete:
        ok, code, _body = _delete_resource(uri, token)
        if ok:
            deleted += 1
            print(f"🗑️  Pruned old log: {uri}")
        else:
            print(f"⚠️ Failed to delete old log ({code}): {uri}")

    kept = min(keep, len(items_sorted))
    print(f"✅ Prune complete — kept {kept}, deleted {deleted}.")
    return deleted

def prune_sync_folders(sync_dir: str, keep: int) -> int:
    """
    Delete the oldest local sync run folders so that only `keep` remain.

    Expects a base directory containing subfolders named like:
      YYYY-MM-DD_HH-MM-SS

    Returns: number of folders deleted.
    """
    base = Path(sync_dir)
    if not base.exists() or not base.is_dir():
        print(f"ℹ️ Sync folder {base} does not exist or is not a directory; skipping local prune.")
        return 0

    # Only consider subdirectories (each run is a folder)
    runs = [d for d in base.iterdir() if d.is_dir()]
    if not runs:
        print(f"ℹ️ No sync runs to prune in {base}.")
        return 0

    # Folder names are timestamped like YYYY-MM-DD_HH-MM-SS:
    # lexicographic order == chronological order, so we can sort by name.
    runs_sorted = sorted(runs, key=lambda p: p.name, reverse=True)  # newest first
    to_delete = runs_sorted[keep:]

    if not to_delete:
        print(f"ℹ️ Nothing to prune in sync folder — total {len(runs_sorted)}, keep {keep}.")
        return 0

    deleted = 0
    for folder in to_delete:
        try:
            shutil.rmtree(folder)
            deleted += 1
            print(f"🗑️  Pruned old sync run folder: {folder}")
        except Exception as e:
            print(f"⚠️ Failed to delete sync run folder {folder}: {e}")

    kept = min(keep, len(runs_sorted))
    print(f"✅ Local sync prune complete — kept {kept}, deleted {deleted}.")
    return deleted



# ---------- RDF build ----------

def build_log_dataset_ttl(actions_json_path: str, base_iri: str,
                          status: Optional[str], note: Optional[str]) -> str:
    """
    Build a TECHNICAL.Logs dataset as Turtle, describing:
      - the sync run (identifier, created, status, note)
      - all actions (create/update/delete) with planned and actual outcome.

    Input:
      actions_json_path: actionsOnTargetFDP.json (list of actions)
      base_iri:          base IRI for log + action nodes
      status:            optional high-level run status ("OK", "FAILED:PREPARE", ...)
      note:              optional free-text note
    """
    with open(actions_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Support both shapes:
    #  - list[actions]
    #  - {"actions": [...]}
    actions = data["actions"] if isinstance(data, dict) and "actions" in data else (
        data if isinstance(data, list) else []
    )
    total = len(actions)

    g = rdflib.Graph()

    # Namespaces for this run
    BASE = rdflib.Namespace(base_iri.rstrip("/") + "/")
    ACT  = rdflib.Namespace(str(BASE) + "action/")

    # Main log dataset URI = <base>/logs/<timestamp>
    now_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    log_uri = rdflib.URIRef(str(BASE) + "logs/" + now_ts)

    # Main log dataset type + core metadata
    g.add((log_uri, RDF.type, TECHNICAL.Logs))
    g.add((log_uri, DCT.identifier, rdflib.Literal(now_ts)))
    g.add((log_uri, DCT.title, rdflib.Literal(f"sync-{now_ts}")))
    g.add((log_uri, DCT.created, rdflib.Literal(now_ts, datatype=XSD.dateTime)))
    g.add((log_uri, RDFS.label, rdflib.Literal(f"Sync FDP Dataset run {now_ts}")))

    # Parent logs catalogue (point to the logs FDP root or dedicated catalogue)
    try:
        log_catalogue_uri = config.URL_SETTINGS_FDP
        g.add((log_uri, DCT.isPartOf, rdflib.URIRef(log_catalogue_uri)))
    except Exception:
        pass

    # Summary metadata about this run
    g.add((log_uri, TECHNICAL.totalActions, rdflib.Literal(total, datatype=XSD.integer)))
    if status:
        g.add((log_uri, TECHNICAL.status, rdflib.Literal(status)))
    if note:
        g.add((log_uri, TECHNICAL.note, rdflib.Literal(note)))

    # Convenience function to add literals only when non-empty
    def add_lit(subject, pred, val, datatype=None):
        if val is None:
            return
        if isinstance(val, str) and not val.strip():
            return
        if datatype:
            g.add((subject, pred, rdflib.Literal(val, datatype=datatype)))
        else:
            g.add((subject, pred, rdflib.Literal(str(val))))

    # One TECHNICAL.Action node = one entry in actions JSON
    for idx, a in enumerate(actions, start=1):
        action_uri = rdflib.URIRef(str(ACT) + str(idx))
        g.add((action_uri, RDF.type, TECHNICAL.Action))
        g.add((log_uri, TECHNICAL.hasAction, action_uri))
        g.add((action_uri, TECHNICAL.plannedOrder, rdflib.Literal(idx, datatype=XSD.integer)))

        # Planned attributes
        add_lit(action_uri, TECHNICAL.action,          a.get("action"))
        add_lit(action_uri, TECHNICAL.entityType,      a.get("type"))
        add_lit(action_uri, TECHNICAL.sourceUri,       a.get("source_uri"))
        add_lit(action_uri, TECHNICAL.targetUri,       a.get("target_uri"))
        add_lit(action_uri, TECHNICAL.parentSourceUri, a.get("parent_source_uri"))
        add_lit(action_uri, TECHNICAL.parentTargetUri, a.get("parent_target_uri"))
        add_lit(action_uri, TECHNICAL.contentFile,     a.get("content"))

        # Sync messages: keep both original "sync" and "sync_note" if present
        add_lit(action_uri, TECHNICAL.syncMessage,     a.get("sync"))
        add_lit(action_uri, TECHNICAL.syncMessage,     a.get("sync_note"))
        add_lit(action_uri, TECHNICAL.actionNote,      a.get("action_note"))

        # Outcome fields
        # NOTE: TECHNICAL.syncStatus is used twice:
        #   - once for any explicit sync_status
        #   - once for the boolean "success"
        # If you prefer, you could split into TECHNICAL.success vs TECHNICAL.syncStatus later.
        add_lit(action_uri, TECHNICAL.syncStatus,      a.get("sync_status"))
        add_lit(action_uri, TECHNICAL.syncStatus,      a.get("success"))
        add_lit(action_uri, TECHNICAL.syncedAt,        a.get("sync_time"))
        add_lit(action_uri, TECHNICAL.contentFetchedAt, a.get("content_fetched_at"), XSD.dateTime)

        # Booleans:
        #   - action_delayed : payload ready but parent not yet known
        #   - ignored        : action was deliberately skipped
        for pred, key in [
            (TECHNICAL.actionDelayed, "action_delayed"),
            (TECHNICAL.actionIgnored, "ignored"),   # ✅ aligned with your choice (no more action_ignored)
        ]:
            if key in a:
                g.add((action_uri, pred, rdflib.Literal(bool(a.get(key)), datatype=XSD.boolean)))

    return g.serialize(format="turtle")


# ---------- CLI ----------

def parse_args():
    ap = argparse.ArgumentParser(
        description="Step 6: Build logs TTL, publish to FDP, then prune old logs."
    )
    ap.add_argument("--actions", required=True, help="Path to actionsOnTargetFDP.json")
    ap.add_argument(
        "--base",
        default="http://healthdataportal.eu/syncFDP/",
        help="Base IRI for run/action resources (default: http://healthdataportal.eu/syncFDP/)",
    )
    ap.add_argument("--no-publish", action="store_true", help="Do not publish to FDP; only print TTL")
    ap.add_argument("--status", default=None, help="Short status tag to embed (e.g., OK, NO_ACTIONS, ERROR)")
    ap.add_argument("--note", default=None, help="Optional free-text note to embed in the log")
    ap.add_argument("--prune", action="store_true", help="Prune old logs after publishing")
    ap.add_argument(
        "--keep",
        type=int,
        default=getattr(config, "LAST_LOGS_TO_KEEP", 10),
        help="How many latest logs to keep when pruning (default from config.LAST_LOGS_TO_KEEP)",
    )
    ap.add_argument(
        "--sync-dir",
        default=None,
        help="Optional path to local sync runs folder (e.g. data/sync) to prune alongside FDP logs.",
    )
    return ap.parse_args()



def main():
    args = parse_args()

    # Build TTL for this run
    ttl = build_log_dataset_ttl(args.actions, args.base, args.status, args.note)

    if args.no_publish:
        # Just print TTL to stdout (debug / development mode)
        print(ttl)
        return 0

    # Publish to FDP
    try:
        token = get_token()
    except Exception as e:
        print(f"❌ Auth failed: {e}")
        return 1

    ok_draft, code_draft, body_draft, new_uri = create_logs_ttl(token, ttl)
    if not ok_draft or not new_uri:
        print(f"❌ Create failed ({code_draft}): {body_draft[:300] if isinstance(body_draft, str) else body_draft}")
        return 1

    ok_pub, code_pub, state = publish_logs(new_uri, token)
    if ok_pub and state == "PUBLISHED":
        print(f"✅ Logs dataset published at {new_uri}")

        # Optional prune pass
        if args.prune:
            keep = max(0, int(args.keep))
            print(f"🧹 Pruning logs: keeping last {keep}")
            prune_old_logs(keep)

            # NEW: also prune local sync folders if provided
            if args.sync_dir:
                print(f"🧹 Pruning local sync runs in {args.sync_dir}: keeping last {keep}")
                prune_sync_folders(args.sync_dir, keep)

        return 0

    else:
        print(f"⚠️ Created but not published ({code_pub}): state={state}, uri={new_uri}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
