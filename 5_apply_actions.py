#!/usr/bin/env python3
"""
Step 5: Apply actions

This step:
  - Reads actionsOnTargetFDP.json produced by Step 3 (and possibly enriched by Step 4)
  - Applies the actions on the TARGET FDP (create / update / delete)
  - Uses prepared payloads (TTL files) from Step 4
  - Marks results (success, sync_note, sync_time, etc.) back into the actions file

Semantics of flags:
  - ignored = True  → action is deliberately skipped (e.g. cascade delete, parent not expected)
  - action_delayed  → payload ready, but parent_target_uri is not yet available; skip for now
"""

import json
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Tuple

import requests
import rdflib
import config

# -------- Namespaces --------
# Load RDF namespaces from config, make them directly available as globals (DCAT, DCT, etc.)
NS = {key: rdflib.Namespace(uri) for key, uri in config.NAMESPACES.items()}
globals().update(NS)

# -------- Utils --------

def save_actions_atomic(path: Path, data: list):
    """
    Atomically write the updated actions list to disk:
    write to .tmp, then rename → avoids partial writes.
    """
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)  # atomic on POSIX

def norm(s): 
    """Lowercase, strip string; safe on None."""
    return (s or "").strip().lower()

def type_bucket(t: str) -> str:
    """
    Normalize type values into a small fixed set so we can:
      - sort deterministically
      - map them to FDP endpoints (/catalog, /dataset, ...)
    """
    if t == "catalog": return "catalog"
    if t == "dataset": return "dataset"
    if t == "distribution": return "distribution"
    if t == "sample": return "sample"
    if t == "analytics": return "analytics"
    return "other"

# Order in which actions will be applied: delete → create → update
ACTION_ORDER = {"delete": 0, "create": 1, "update": 2}
# Within each action type, order resources: catalog → dataset → children
TYPE_ORDER   = {"catalog": 0, "dataset": 1, "distribution": 2, "sample": 3, "analytics": 4}

def sort_key(a: dict):
    """
    Sorting key for actions:
      1. By action type (delete, create, update)
      2. By resource type (catalog, dataset, distribution, ...)
    """
    act = norm(a.get("action"))
    typ = type_bucket(a.get("type"))
    return (ACTION_ORDER.get(act, 99), TYPE_ORDER.get(typ, 2))

# -------- HTTP to Target FDP --------

def get_token():
    """
    Get an auth token for the TARGET FDP using LOGIN_TARGET_FDP / PASSWORD_TARGET_FDP.
    """
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    data = {"email": config.LOGIN_TARGET_FDP, "password": config.PASSWORD_TARGET_FDP}
    URL_TOKEN_TARGET_FDP  = f"{config.URL_TARGET_FDP.rstrip('/')}/tokens"
    res = requests.post(URL_TOKEN_TARGET_FDP, data=json.dumps(data), headers=headers, timeout=config.TIMEOUT)
    res.raise_for_status()
    return res.json()["token"]

def create_metadata(class_metadata: str, token: str, rdf_content: str) -> Tuple[bool, int | None, str, str | None]:
    """
    POST a new resource to the TARGET FDP:
      - class_metadata = 'catalog', 'dataset', ...
      - returns (ok, status_code, response_text, new_location_uri)
    """
    url = f"{str(config.URL_TARGET_FDP).rstrip('/')}/{class_metadata.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/turtle",
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

def publish_metadata(uri: str, token: str) -> Tuple[bool, int | None, str | None]:
    """
    PUT /meta/state to set the current state to PUBLISHED.
    """
    url = f'{uri}/meta/state'
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

def update_metadata(uri: str, token: str, rdf_content: str):
    """
    PUT RDF back to an existing metadata URI.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/turtle"
    }
    try:
        r = requests.put(
            uri,
            data=rdf_content.encode("utf-8"),
            headers=headers,
            timeout=getattr(config, "TARGET_TIMEOUT", config.TIMEOUT),
        )
        return r.ok, r.status_code, r.text
    except requests.RequestException as e:
        return False, None, str(e)

def delete_metadata(uri: str, token: str):
    """
    DELETE an existing metadata resource on target.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "*/*"
    }
    try:
        r = requests.delete(
            uri,
            headers=headers,
            timeout=getattr(config, "TARGET_TIMEOUT", config.TIMEOUT),
        )
        return r.ok, r.status_code, r.text
    except requests.RequestException as e:
        return False, None, str(e)

# -------- Payload helpers --------

def load_payload(actions_item: dict, payloads_dir: Path):
    """
    Resolve and read the TTL payload file for a given action.

    Payloads are expected under:
      payloads_dir/<type>/<filename>.ttl
    """
    filename = actions_item.get("content")
    typ = type_bucket(actions_item.get("type", ""))
    if not filename:
        return None, f"No content filename set for {typ} ({actions_item.get('source_uri')})"
    ttl_path = payloads_dir / typ / filename
    if not ttl_path.exists():
        return None, f"TTL file not found: {ttl_path}"
    try:
        return ttl_path.read_text(encoding="utf-8"), None
    except Exception as e:
        return None, f"Read error for {ttl_path}: {e}"

def update_isPartOf_in_rdf(actions_item: dict, payloads_dir: Path) -> bool:
    """
    Patch a payload TTL to inject a dct:isPartOf triple for the main resource
    when a parent_target_uri is known only after the parent was created on target.

    This is used when:
      - Step 3 couldn't resolve parent_target_uri earlier, and
      - Step 5 just created the parent and propagates the new URI to children.
    """
    delayed = actions_item.get("action_delayed")
    parent_target_uri = actions_item.get("parent_target_uri")
    if not parent_target_uri:
        print(f"[isPartOf] Skip: no parent_target_uri for {actions_item.get('source_uri')}")
        return False

    ttl_text, err = load_payload(actions_item, payloads_dir)
    if err:
        print(f"[isPartOf] Skip: failed to load payload for {actions_item.get('content')}: {err}")
        return False

    g = rdflib.Graph()
    try:
        g.parse(data=ttl_text, format="turtle")
    except Exception as e:
        print(f"[isPartOf] Skip: TTL parse error in {actions_item.get('content')}: {e}")
        return False

    parent = rdflib.URIRef(parent_target_uri)
    candidates = set()
    # These are the RDF types we accept as "main" resource to add isPartOf to
    for t in (DCAT.Dataset, DCAT.Catalog, DCAT.Distribution):
        for s in g.subjects(RDF.type, t):
            candidates.add(s)

    if not candidates:
        print(f"[isPartOf] Skip: no dcat:Dataset/Distribution found in {actions_item.get('content')}")
        return False

    added = False
    for s in candidates:
        if (s, DCT.isPartOf, None) not in g:
            g.add((s, DCT.isPartOf, parent))
            added = True

    if not added:
        print(f"[isPartOf] Skip: all candidates in {actions_item.get('content')} already have dct:isPartOf")
        return False

    # Overwrite payload file in-place
    typ = type_bucket(actions_item.get("type", ""))
    ttl_path = payloads_dir / typ / actions_item["content"]

    try:
        g.serialize(destination=str(ttl_path), format="turtle")
    except Exception as e:
        print(f"[isPartOf] ERROR: failed to write patched TTL {ttl_path}: {e}")
        return False

    actions_item["action_delayed"] = False
    print(f"[isPartOf] OK: added dct:isPartOf → {parent_target_uri} in {actions_item.get('content')}")
    return True

# -------- Actions wrappers --------

def action_delete(a, token):
    """
    Execute a DELETE action on the target FDP and update the action dict.
    """
    uri = a.get("target_uri")
    ts = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    ok, code, body = delete_metadata(uri, token)
    if ok:
        a["sync_note"] = "Successfully deleted on target"
        a["sync_time"] = ts
        a["success"] = True
        return True
    else:
        a["sync_note"] = f"Delete failed ({code}): {body[:200] if isinstance(body,str) else body}"
        a["success"] = False
        return False

def action_create(a, token, payloads_dir: Path):
    """
    Execute a CREATE action:
      - POST TTL to /<type>
      - PUBLISH via /meta/state
    """
    typ = type_bucket(a.get("type"))
    ts = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    ttl_text, err = load_payload(a, payloads_dir)
    if err:
        a["sync_note"] = f"Missing payload: {err}"
        a["success"] = False
        return False

    ok_draft, code_draft, body_draft, new_uri = create_metadata(typ, token, ttl_text)
    if not ok_draft or not new_uri:
        a["sync_note"] = f"Create failed ({code_draft}): {body_draft[:200] if isinstance(body_draft,str) else body_draft}"
        a["success"] = False
        return False

    ok_pub, code_pub, state = publish_metadata(new_uri, token)
    if ok_pub and state == "PUBLISHED":
        a["target_uri"] = new_uri
        a["sync_note"] = "Successfully published on target"
        a["sync_time"] = ts
        a["success"] = True
        return True
    else:
        a["target_uri"] = new_uri
        a["sync_note"] = f"Creation in DRAFT only ({code_pub}): {state}"
        a["sync_time"] = ts
        a["success"] = False
        return False

def action_update(a, token, payloads_dir: Path):
    """
    Execute an UPDATE action on an existing target URI.
    """
    uri = a.get("target_uri")
    ts = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    ttl_text, err = load_payload(a, payloads_dir)
    if err:
        a["sync_note"] = f"Missing payload: {err}"
        a["success"] = False
        return False

    ok, code, body = update_metadata(uri, token, ttl_text)
    if ok:
        a["sync_note"] = "Successfully updated on target"
        a["sync_time"] = ts
        a["success"] = True
        return True
    else:
        a["sync_note"] = f"Update failed ({code}): {body[:200] if isinstance(body,str) else body}"
        a["success"] = False
        return False

# -------- CLI / Main --------

def parse_args():
    """
    CLI:
      --actions  : path to actionsOnTargetFDP.json
      --payloads : path to payloads dir created by Step 4
      --stop-on-error : stop at first failure
      --dry-run      : simulate without calling the FDP
    """
    ap = argparse.ArgumentParser(description="Step 5: apply actions on target using prepared payloads.")
    ap.add_argument("--actions", required=True, help="Path to actionsOnTargetFDP.json produced by step 3/4")
    ap.add_argument("--payloads", required=True, help="Path to payloads directory produced by step 4")
    ap.add_argument("--stop-on-error", action="store_true", help="Stop pipeline on first failure")
    ap.add_argument("--dry-run", action="store_true", help="Only print what would be done")
    return ap.parse_args()

def main():
    args = parse_args()
    actions_path = Path(args.actions)
    payloads_dir = Path(args.payloads)

    # Basic sanity checks
    if not actions_path.exists():
        print(f"❌ Actions file not found: {actions_path}")
        sys.exit(1)

    if not payloads_dir.exists():
        print(f"❌ Payloads dir not found: {payloads_dir}")
        # Mark all actions as failed with a clear note
        try:
            actions = json.loads(actions_path.read_text(encoding="utf-8"))
            for a in actions:
                a.setdefault("success", False)
                a.setdefault("sync_note", f"Payloads dir missing: {payloads_dir}")
            save_actions_atomic(actions_path, actions)
            print(f"💾 Saved updates to {actions_path} (payloads dir missing).")
        except Exception:
            pass
        sys.exit(1)

    # Load actions
    try:
        actions = json.loads(actions_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"❌ Cannot read actions: {e}")
        # Write a minimal actions array so Step 6 can log the failure
        try:
            save_actions_atomic(actions_path, [{
                "action": "unknown",
                "type": "unknown",
                "success": False,
                "sync_note": f"Cannot read actions: {e}"
            }])
            print(f"💾 Wrote minimal {actions_path} for logging.")
        except Exception:
            pass
        sys.exit(1)

    # Authenticate (unless dry-run)
    token = None
    if not args.dry_run:
        try:
            token = get_token()
            print("✅ Auth successful")
        except Exception as e:
            print(f"❌ Auth failed: {e}")
            try:
                for a in actions:
                    a["success"] = False
                    a["sync_note"] = f"Auth failed: {e}"
                save_actions_atomic(actions_path, actions)
                print(f"💾 Saved updates to {actions_path} (auth failure).")
            except Exception:
                pass
            sys.exit(1)

    # Sort for deterministic order (delete → create → update; catalog → dataset → children)
    actions_sorted = sorted(actions, key=sort_key)

    # Totals per action type (for progress display)
    total_create = sum(1 for x in actions_sorted if norm(x.get("action")) == "create")
    total_update = sum(1 for x in actions_sorted if norm(x.get("action")) == "update")
    total_delete = sum(1 for x in actions_sorted
                       if norm(x.get("action")) == "delete" and not x.get("ignored"))

    counters = {"create": 0, "update": 0, "delete": 0}

    def _progress_tag(kind: str) -> str:
        """
        Build a progress tag like: [create 3/10]
        """
        if kind == "create":
            return f"[create {counters['create']+1}/{total_create}]"
        if kind == "update":
            return f"[update {counters['update']+1}/{total_update}]"
        if kind == "delete":
            return f"[delete {counters['delete']+1}/{total_delete}]"
        return "[?]"

    # Aggregate counts for summary
    counts = {"create": 0, "update": 0, "delete": 0}
    failed = 0

    # 1) DELETEs first (ungated)
    for a in actions_sorted:
        if norm(a.get("action")) != "delete":
            continue

        # Respect ignored deletes (cascade / not needed)
        if a.get("ignored"):
            reason = a.get("ignored_reason") or a.get("action_note") or "ignored"
            msg = f"Skipped delete ({reason})"
            a["sync_note"] = msg
            a["success"] = True
            if args.dry_run:
                print(f"⏭️  (dry-run) {msg}: {a.get('target_uri')}")
            else:
                print(f"⏭️  {msg}: {a.get('target_uri')}")
            continue

        if not a.get("target_uri"):
            a["sync_note"] = "No target_uri for delete"
            a["success"] = False
            failed += 1
            if args.stop_on_error:
                break
            continue

        if args.dry_run:
            print(f"🗑️  {_progress_tag('delete')} (dry-run) {a.get('target_uri')}")
            a["success"] = True
            counts["delete"] += 1
            counters["delete"] += 1
            continue

        print(f"🗑️  {_progress_tag('delete')} {a.get('target_uri')}")
        ok = action_delete(a, token)
        counts["delete"] += 1 if ok else 0
        counters["delete"] += 1
        failed += 0 if ok else 1

        if args.stop_on_error and not ok:
            break

    # 2) CREATE / UPDATE (only when parent is available or not required)
    if failed == 0 or not args.stop_on_error:
        for a in actions_sorted:
            act = norm(a.get("action"))
            if act not in {"create", "update"}:
                continue

            typ = type_bucket(a.get("type"))
            parent_uri = a.get("parent_target_uri")
            delayed = bool(a.get("action_delayed"))

            # Gate: require known parent for non-root resources
            if typ != "catalog" and (parent_uri is None or delayed):
                a["sync_note"] = "Skipped: parent metadata not yet published on target (action_delayed)."
                a["success"] = False
                continue

            if args.dry_run:
                if act == "create":
                    print(
                        f"➕ {_progress_tag('create')} (dry-run) {typ} → {config.URL_TARGET_FDP}/{typ}  "
                        f"(from {a.get('source_uri')})"
                    )
                    a["success"] = True
                    counts["create"] += 1
                    counters["create"] += 1
                else:
                    print(f"📝 {_progress_tag('update')} (dry-run) {a.get('target_uri')}")
                    a["success"] = True
                    counts["update"] += 1
                    counters["update"] += 1
                continue

            if act == "create":
                # Log with progress and source URI (target URI unknown until Location header)
                print(
                    f"➕ {_progress_tag('create')} CREATE {typ} → {config.URL_TARGET_FDP}/{typ}  "
                    f"(from {a.get('source_uri')})"
                )
                ok = action_create(a, token, payloads_dir)

                if ok:
                    print(f"✅ {_progress_tag('create').replace('create', 'created')} {a.get('target_uri')}")

                    # ---- propagate parent target URI to DIRECT children & patch payload TTLs ----
                    parent_target_uri = a.get("target_uri")
                    parent_type = type_bucket(a.get("type"))
                    parent_source_uri = a.get("source_uri")
                    parent_group_value = a.get("group_value_uri")

                    if parent_target_uri:
                        for child in actions_sorted:
                            if child is a:
                                continue  # don't treat self as child

                            # Already resolved or not a create/update child → skip
                            if child.get("parent_target_uri") is not None:
                                continue

                            child_type = type_bucket(child.get("type"))

                            # -------- Catalog → Dataset (theme catalogs) --------
                            if parent_type == "catalog":
                                # only datasets belong under catalogs
                                if child_type != "dataset":
                                    continue
                                # for group-by catalogs we must match the theme
                                if not parent_group_value:
                                    # defensive: a theme catalog SHOULD have group_value_uri
                                    continue
                                if child.get("group_value_uri") != parent_group_value:
                                    continue

                            # -------- Dataset → distribution/sample/analytics --------
                            elif parent_type == "dataset":
                                if child_type not in {"distribution", "sample", "analytics"}:
                                    continue
                                # only children whose parent_source_uri is this dataset
                                if child.get("parent_source_uri") != parent_source_uri:
                                    continue

                            else:
                                # No other parent relationships are managed here
                                continue

                            # At this point, this child *logically* belongs to this parent
                            child["parent_target_uri"] = parent_target_uri
                            child["action_delayed"] = False
                            print(
                                f"🔗 Updated relation for child {child.get('source_uri')} "
                                f"→ new dct:isPartOf {parent_target_uri}"
                            )
                            try:
                                updated = update_isPartOf_in_rdf(child, payloads_dir)
                                if updated:
                                    print(f"   ↳ Patched dct:isPartOf in payload {child.get('content')}")
                                else:
                                    print(
                                        f"   ↳ No dct:isPartOf patch for {child.get('content')} "
                                        "(see [isPartOf] reason above)"
                                    )
                            except Exception as e:
                                print(f"   ↳ ⚠️ dct:isPartOf patch failed for {child.get('content')}: {e}")

                else:
                    print(
                        f"❌ {_progress_tag('create').replace('create', 'created')} FAILED "
                        f"(src={a.get('source_uri')})  note={a.get('sync_note')}"
                    )

                counts["create"] += 1 if ok else 0
                counters["create"] += 1
                failed += 0 if ok else 1
                if args.stop_on_error and not ok:
                    break


            elif act == "update":
                if not a.get("target_uri"):
                    a["sync_note"] = "No target_uri for update"
                    a["success"] = False
                    failed += 1
                    counters["update"] += 1  # still advance the progress line
                    print(
                        f"❌ {_progress_tag('update')} skipped (missing target_uri) "
                        f"(src={a.get('source_uri')})"
                    )
                    if args.stop_on_error:
                        break
                    continue

                print(f"📝 {_progress_tag('update')} {a.get('target_uri')}")
                ok = action_update(a, token, payloads_dir)

                if ok:
                    print(f"✅ {_progress_tag('update').replace('update', 'updated')} {a.get('target_uri')}")
                else:
                    print(
                        f"❌ {_progress_tag('update').replace('update', 'updated')} FAILED "
                        f"{a.get('target_uri')}  note={a.get('sync_note')}"
                    )

                counts["update"] += 1 if ok else 0
                counters["update"] += 1
                failed += 0 if ok else 1
                if args.stop_on_error and not ok:
                    break

    # Persist updated actions back to disk
    for a in actions_sorted:
        if "success" not in a:
            # Default to False if something slipped through without being processed
            a["success"] = False
    save_actions_atomic(actions_path, actions_sorted)
    print(f"\n💾 Saved updates to {actions_path}")

    # Summary & exit code
    print("\n================ SUMMARY ================")
    print(
        f"Create:  {counts['create']}/{total_create}  "
        f"Update: {counts['update']}/{total_update}  "
        f"Delete: {counts['delete']}/{total_delete}"
    )
    print(f"Failures: {failed}")
    print("=========================================")

    sys.exit(1 if failed > 0 else 0)

if __name__ == "__main__":
    main()
