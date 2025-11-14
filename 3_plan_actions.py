#!/usr/bin/env python3
"""
Step 3: Plan actions (create/update/delete) to align the target FDP with the source FDP
in a group-by catalog setup.

Input:
- sourceFDP.json: harvested metadata from the SOURCE FDP (Step 2)
- targetFDP.json: harvested metadata from the TARGET FDP (Step 2)
- syncSettings.json: sync configuration from Step 1 (group-by property & values)

Output:
- actionsOnTargetFDP.json: list of actions to be applied on target (Step 4/5)
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import argparse
import sys
import config

# ----------------------------- utils -----------------------------

def load_json_list(path: Path):
    """
    Load a JSON file and ensure it is a list (array).
    Raises ValueError if the root is not a list.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return data

def parse_iso_dt(s: str):
    """
    Parse ISO-8601 datetimes robustly.
    Returns a timezone-aware datetime or None if parsing fails.

    - Replaces 'Z' with '+00:00'
    - If only the date is provided, appends 'T00:00:00+00:00'
    """
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = datetime.fromisoformat(s + "T00:00:00+00:00")
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def norm(u):
    """
    Normalise a URI-like string:
    - strip whitespace
    - remove trailing slash
    """
    return u.strip().rstrip("/") if isinstance(u, str) else u


def load_sync_settings(path: Path) -> dict:
    """
    Load Step-1 sync settings JSON (if present) and extract the group-by settings.

    Returns a dict with:
      - "group_by_property": IRI of the group-by property (or None)
      - "group_by_values": list of entries:
          [{ "value_uri": "...", "titleEn": "...", "titleFr": "...", "titleNl": "...", ... }]
      - "last_modified_settings": ISO datetime string (or None)
    """
    out = {
        "group_by_property": None,
        "group_by_values": [],  # [{value_uri, titleEn, titleFr, titleNl, ...}]
        "last_modified_settings": None,
    }
    if not path or not path.exists():
        return out
    try:
        j = json.loads(path.read_text(encoding="utf-8"))
        out["group_by_property"] = j.get("group_by_property")
        out["group_by_values"] = j.get("group_by_values", []) or []
        out["last_modified_settings"] = j.get("last_modified_settings")
    except Exception as e:
        print(f"⚠️ Could not read sync settings {path}: {e}")
    return out

def _expected_groups(sync_settings: dict):
    """
    From Step-1: list of group-by entries (dicts).

    Keep only those entries that have a non-empty value_uri.
    Returned shape: [ { "value_uri": "...", "titleEn": "...", ... }, ...]
    """
    out = []
    for e in sync_settings.get("group_by_values", []):
        vu = e.get("value_uri")
        if vu:
            out.append(e)
    return out

def _titles_from_group_entry(ge: dict):
    """
    Convert a group_by entry (with titleEn/titleFr/titleNl/...)
    into a language-keyed dict: {"en": "...", "fr": "...", "nl": "..."}.
    """
    t = {}
    if ge.get("titleEn"): t["en"] = ge["titleEn"]
    if ge.get("titleFr"): t["fr"] = ge["titleFr"]
    if ge.get("titleNl"): t["nl"] = ge["titleNl"]
    return t

def _postpass_reparent(actions, source_dataset_groups, group_prop):
    """
    Post-processing step for datasets:

    If a dataset is currently planned under a catalog that is going to be deleted,
    we:
      - clear its parent_target_uri
      - ensure it carries group_by information (group_value_uri/group_property_iri)
        so that downstream (Step 4/5) can attach it to the correct group-by catalog.
    """
    deleted_catalogs = {
        a.get("target_uri")
        for a in actions
        if (a.get("action") == "delete" and str(a.get("type","")).lower() == "catalog")
        and a.get("target_uri")
    }

    for a in actions:
        if str(a.get("type","")).lower() != "dataset":
            continue
        if not a.get("parent_target_uri"):
            continue
        if a["parent_target_uri"] in deleted_catalogs:
            # Force group-by parenting
            a["parent_target_uri"] = None
            if not a.get("group_value_uri"):
                # pick first mapped group-by value if we have any
                vu_list = source_dataset_groups.get(a.get("source_uri"), []) or []
                a["group_value_uri"] = vu_list[0] if vu_list else None
            if not a.get("group_property_iri"):
                a["group_property_iri"] = group_prop
            note = (a.get("action_note") or "").strip()
            suffix = "Reparented: catalog scheduled for deletion"
            a["action_note"] = f"{note} | {suffix}" if note else suffix



# ----------------------------- core: NEW plan_actions -----------------------------

def plan_actions(source, target, sync_settings: dict, only_type=None):
    """
    Plans actions to align the target FDP with the source FDP in a group-by catalog setup.

    Decision logic:

      A) CATALOG:
         - Delete catalogs on target that are not group-by compliant.
         - Ensure all expected group-by catalogs exist (create missing).
         - Deduplicate catalogs (keep newest per group-by value).

      B) DATASET:
         - Create/Update *public* datasets that have valid group-by values.
         - Attach datasets under the correct group-by catalog(s).
         - If a dataset's group-by values change on source:
             -> delete old dataset on target
             -> create dataset under the new catalog(s).
         - Delete datasets on target that are non-public on source or missing on source.

      C) CHILDREN (distribution / sample / analytics):
         - Align children with source (create/update/delete), respecting parent presence:
           - If parent is deleted or not expected on target → child actions are ignored.
    """

    # -----------------------------
    # Inputs & pre-processing
    # -----------------------------
    group_prop = sync_settings.get("group_by_property")
    # Expect entries like {"value_uri": "...", "titleEn": "...", ...}
    expected_entries = _expected_groups(sync_settings)
    expected_set = {e["value_uri"] for e in expected_entries}
    exp_by_value = {e["value_uri"]: e for e in expected_entries}

    settings_dt = parse_iso_dt(sync_settings.get("last_modified_settings"))

    # Normalize 'only_type' filter to lower-case or None
    only_type = (only_type or "").strip().lower() or None

    # -----------------------------
    # 1) Index existing catalogs on target + detect invalid ones
    # -----------------------------
    tgt_catalogs = [e for e in target if str(e.get("type", "")).lower() == "catalog"]

    # Map: group value_uri -> [catalog entries on target...]
    by_value_lists = {vu: [] for vu in expected_set}

    # Collect catalogs that should be removed (not group-by compliant)
    invalid_catalogs = []

    for cat in tgt_catalogs:
        # Accept both shapes from Step 2: ["uri", ...] or [{"value_uri": "uri"}, ...]
        gvals = cat.get("groupBy") or cat.get("groupByValues") or []
        val_uris = []
        for gv in gvals:
            if isinstance(gv, dict) and gv.get("value_uri"):
                val_uris.append(gv["value_uri"])
            elif isinstance(gv, str):
                val_uris.append(gv)
        # de-dup, keep order
        val_uris = list(dict.fromkeys(val_uris))

        # A) INVALID if: no group-by values at all
        if not val_uris:
            invalid_catalogs.append((cat, "No groupBy on catalog"))
            continue

        # A) INVALID if: none of the values are expected per sync settings
        expected_hits = [vu for vu in val_uris if vu in expected_set]
        if not expected_hits:
            invalid_catalogs.append((cat, "Catalog groupBy not in expected set"))
            continue

        # Valid catalog: attach it to all matching group values (rare case: multi-valued)
        for vu in expected_hits:
            by_value_lists.setdefault(vu, []).append(cat)

    actions = []
    # Order key for final sorting
    order = {"delete": 0, "create": 1, "update": 2}

    # -----------------------------
    # A) Delete invalid catalogs
    # -----------------------------
    if only_type in (None, "catalog"):
        for cat, reason in invalid_catalogs:
            actions.append({
                "action": "delete",
                "type": "catalog",
                "target_uri": cat.get("target_uri"),
                "source_uri": None,
                "parent_target_uri": cat.get("isPartOf"),
                "action_note": f"Remove non-group catalog: {reason}"
            })

    # -----------------------------
    # B) Ensure expected catalogs exist & deduplicate
    #     - Keep newest by `modified`
    #     - Force UPDATE if catalog older than last_modified_settings
    # -----------------------------
    canonical_by_value = {}  # value_uri -> kept catalog (or None if to be created)

    if only_type in (None, "catalog"):
        for vu in expected_set:
            candidates = by_value_lists.get(vu, [])
            if not candidates:
                # Missing catalog for this expected value → CREATE
                ge = exp_by_value[vu]
                titles = _titles_from_group_entry(ge)  # {"en":..., "fr":..., "nl":...}
                actions.append({
                    "action": "create",
                    "type": "catalog",
                    "target_uri": None,
                    "source_uri": None,
                    "parent_source_uri": None,
                    "group_value_uri": vu,
                    "group_property_iri": group_prop,
                    "catalogue_title": titles,
                    "action_note": "Create group-by catalog for value_uri."
                })
                # Mark as not yet existing; datasets will use `group_value_uri`
                canonical_by_value[vu] = None
            else:
                # Deduplicate: keep newest
                def _m(cat):
                    return parse_iso_dt(cat.get("modified")) or datetime.min.replace(tzinfo=timezone.utc)
                candidates_sorted = sorted(candidates, key=_m, reverse=True)
                keep = candidates_sorted[0]
                canonical_by_value[vu] = keep

                # Delete extras
                for dup in candidates_sorted[1:]:
                    actions.append({
                        "action": "delete",
                        "type": "catalog",
                        "target_uri": dup.get("target_uri"),
                        "source_uri": dup.get("source_uri", None),
                        "parent_target_uri": dup.get("isPartOf"),
                        "action_note": f"Duplicate group-by catalog for {vu}; keeping newest {keep.get('target_uri')}"
                    })

                # NEW: force UPDATE if catalog is older than last_modified_settings
                if settings_dt:
                    tgt_dt = parse_iso_dt(keep.get("modified"))
                    need_update = False
                    if tgt_dt is None:
                        need_update = True
                        reason = "Target catalog has no modified timestamp."
                    elif settings_dt > tgt_dt:
                        need_update = True
                        reason = "Sync settings are newer than target catalog (forced resync)."
                    else:
                        reason = ""

                    if need_update:
                        # Optional time-diff note
                        time_diff_note = ""
                        if tgt_dt:
                            diff = settings_dt - tgt_dt
                            total_seconds = diff.total_seconds()
                            days = int(total_seconds // 86400)
                            hours = int((total_seconds % 86400) // 3600)
                            minutes = int((total_seconds % 3600) // 60)
                            if days > 0:
                                time_diff_note = f" (~{days} day{'s' if days > 1 else ''} newer)"
                            elif hours > 0:
                                time_diff_note = f" (~{hours} hour{'s' if hours > 1 else ''} newer)"
                            elif minutes > 0:
                                time_diff_note = f" (~{minutes} minute{'s' if minutes > 1 else ''} newer)"
                            else:
                                time_diff_note = " (<1 minute difference)"

                        ge = exp_by_value[vu]
                        titles = _titles_from_group_entry(ge)

                        actions.append({
                            "action": "update",
                            "type": "catalog",
                            "target_uri": keep.get("target_uri"),
                            "source_uri": None,
                            "parent_target_uri": keep.get("isPartOf"),
                            "group_value_uri": vu,
                            "group_property_iri": group_prop,
                            "catalogue_title": titles,
                            "action_note": (
                                reason if not time_diff_note else f"{reason}{time_diff_note}"
                            ),
                        })


    # Quick lookup for canonical target catalogs (existing ones only, already on target)
    tgt_by_val = {}
    for vu in expected_set:
        cat = canonical_by_value.get(vu)
        if cat and cat.get("target_uri"):
            tgt_by_val[vu] = cat

    # -----------------------------
    # C) Prepare dataset indexes
    # -----------------------------
    # Map target datasets by their "internal" source_uri (string or list)
    target_by_internal = {}
    for e in target:
        internals = e.get("source_uri")
        if not internals:
            continue
        if isinstance(internals, list):
            for iu in internals:
                if iu:
                    target_by_internal[norm(iu)] = e
        else:
            target_by_internal[norm(internals)] = e

    # Build source indexes for datasets:
    #  - source_all_uris: all datasets seen on source (any status)
    #  - source_public_by_uri: only public datasets (candidates to create/update)
    #  - source_not_target_uris: non-public datasets (candidates to delete on target)
    #  - source_dataset_groups: dataset_uri -> [groupBy value_uris...]
    source_all_uris = set()
    source_public_by_uri = {}
    source_not_target_uris = set()
    source_dataset_groups = {}

    for e in source:
        if str(e.get("type", "")).lower() != "dataset":
            continue  # ignore non-datasets in the dataset indexing step

        uri = norm(e.get("source_uri"))
        if not uri:
            continue

        source_all_uris.add(uri)
        status = str(e.get("status", "")).lower()
        if status == "public":
            source_public_by_uri[uri] = e
        else:
            source_not_target_uris.add(uri)

        # Collect group-by values from Step 2 (accept both keys for robustness)
        gvals = e.get("groupBy") or e.get("groupByValues") or []
        values = []
        for gv in gvals:
            if isinstance(gv, str):
                values.append(gv)
            elif isinstance(gv, dict) and gv.get("value_uri"):
                values.append(gv["value_uri"])

        # Keep only values declared in sync settings (expected_set)
        # If you want to allow "any value" when expected_set is empty,
        # this is the place to relax that condition.
        values = [v for v in dict.fromkeys(values) if v in expected_set]
        source_dataset_groups[uri] = values

    # -----------------------------
    # C) Create/Update public datasets with valid group-by
    #     + handle group_by change (delete old + create new)
    # -----------------------------
    if only_type in (None, "dataset"):
        for uri, se in source_public_by_uri.items():
            ds_values = source_dataset_groups.get(uri, [])
            if not ds_values:
                # Public dataset but no valid group-by value per sync settings → ignore
                continue

            # Lookup existing target dataset for this source_uri (if any)
            te = target_by_internal.get(uri)

            # --- Detect group-by change (source vs target) ---
            if te:
                src_groups = set(ds_values)
                tgt_groups_raw = te.get("groupBy") or te.get("groupByValues") or []
                tgt_groups = {
                    (g.get("value_uri") if isinstance(g, dict) else str(g))
                    for g in tgt_groups_raw if g
                }
                if src_groups != tgt_groups:
                    # Group-by changed:
                    #   - delete old dataset from its current catalog
                    #   - then force CREATE(s) under the new catalog(s)
                    actions.append({
                        "action": "delete",
                        "type": "dataset",
                        "target_uri": te.get("target_uri"),
                        "source_uri": uri,
                        "parent_target_uri": te.get("isPartOf"),
                        "action_note": (
                            f"Group-by changed on source: {tgt_groups or 'none'} → {src_groups}. "
                            "Remove old dataset before recreating under new group-by catalog(s)."
                        )
                    })
                    te = None  # important: fall through to CREATE(s)

            # For each group value, attach to the corresponding canonical target catalog
            for vu in ds_values:
                tcat = tgt_by_val.get(vu)
                parent_target_uri = tcat.get("target_uri") if tcat else None

                if not te:
                    # CREATE (dataset not present on target yet, or group-by changed)
                    actions.append({
                        "action": "create",
                        "type": "dataset",
                        "target_uri": None,
                        "source_uri": uri,
                        "parent_target_uri": parent_target_uri,  # may be None if catalog will be created
                        "group_value_uri": vu,
                        "group_property_iri": group_prop,
                        "action_note": (
                            "Create dataset under group-by catalog, " +
                            ("catalog already exists." if parent_target_uri else "(catalog will be created.)")
                        )
                    })
                else:
                    # UPDATE if source is newer (or target has no modified)
                    src_dt = parse_iso_dt(se.get("modified"))
                    tgt_dt = parse_iso_dt(te.get("modified"))
                    if tgt_dt is None or (src_dt and src_dt > tgt_dt):
                        # Build a human-readable time diff (optional)
                        time_diff_note = ""
                        if src_dt and tgt_dt:
                            diff = src_dt - tgt_dt
                            total_seconds = diff.total_seconds()
                            days = int(total_seconds // 86400)
                            hours = int((total_seconds % 86400) // 3600)
                            minutes = int((total_seconds % 3600) // 60)
                            if days > 0:
                                time_diff_note = f" (~{days} day{'s' if days > 1 else ''} newer)"
                            elif hours > 0:
                                time_diff_note = f" (~{hours} hour{'s' if hours > 1 else ''} newer)"
                            elif minutes > 0:
                                time_diff_note = f" (~{minutes} minute{'s' if minutes > 1 else ''} newer)"
                            else:
                                time_diff_note = " (<1 minute difference)"

                        actions.append({
                            "action": "update",
                            "type": "dataset",
                            "target_uri": te.get("target_uri"),
                            "source_uri": uri,
                            "parent_target_uri": parent_target_uri,
                            "group_value_uri": vu,
                            "group_property_iri": group_prop,
                            "action_note": (
                                "Target has no modified timestamp."
                                if tgt_dt is None
                                else f"Source metadata is newer than target{time_diff_note}."
                            )
                        })

    # -----------------------------
    # D) Delete datasets that should not be on target
    # -----------------------------
    if only_type in (None, "dataset"):
        for te in target:
            if str(te.get("type", "")).lower() != "dataset":
                continue

            internals = te.get("source_uri")
            if not internals:
                # Target dataset with no source mapping → delete if configured
                if config.DELETE_ORPHAN_METADATA_ON_TARGET:
                    actions.append({
                        "action": "delete",
                        "type": "dataset",
                        "target_uri": te.get("target_uri"),
                        "source_uri": None,
                        "parent_target_uri": te.get("isPartOf"),
                        "action_note": "Orphan on target (no source mapping); remove."
                    })
                continue

            vals = internals if isinstance(internals, list) else [internals]
            vals = [norm(v) for v in vals if v]

            # Delete if dataset exists on target but is non-public on source
            matched_internal = [v for v in vals if v in source_not_target_uris]
            if matched_internal:
                for v in matched_internal:
                    actions.append({
                        "action": "delete",
                        "type": "dataset",
                        "target_uri": te.get("target_uri"),
                        "source_uri": v,
                        "parent_target_uri": te.get("isPartOf"),
                        "action_note": "Source dataset is not public; delete on target."
                    })
                continue

            # Delete if none of the internals exist on source anymore
            if vals and all(v not in source_all_uris for v in vals):
                actions.append({
                    "action": "delete",
                    "type": "dataset",
                    "target_uri": te.get("target_uri"),
                    "source_uri": vals[0],
                    "parent_target_uri": te.get("isPartOf"),
                    "action_note": "Dataset doesn't exist on source; cleanup."
                })

    # ---------------------------------------
    # E) CHILD CLASSES: Distribution / Sample / Analytics
    # ---------------------------------------

    if only_type in (None, "distribution", "sample", "analytics"):
        # --- 1) Discover parents being deleted on target in this run (cascade case) ---
        parents_deleted_target_uris = {
            a.get("target_uri")
            for a in actions
            if (a.get("action") == "delete" and str(a.get("type","")).lower() == "dataset")
            and a.get("target_uri")
        }

        # Map source dataset URI -> target dataset entry (used to attach new children)
        target_datasets_by_source = {}
        for e in target:
            if str(e.get("type","")).lower() == "dataset":
                su = e.get("source_uri")
                if not su:
                    continue
                if isinstance(su, list):
                    for u in su:
                        if u:
                            target_datasets_by_source[norm(u)] = e
                else:
                    target_datasets_by_source[norm(su)] = e

        # --- 2) Index target children by their source_uri ---
        target_child_by_internal = {}
        for e in target:
            t = str(e.get("type","")).lower()
            if t not in ("distribution", "sample", "analytics"):
                continue
            internals = e.get("source_uri")
            if not internals:
                continue
            if isinstance(internals, list):
                for iu in internals:
                    if iu:
                        target_child_by_internal[norm(iu)] = e
            else:
                target_child_by_internal[norm(internals)] = e

        # --- 3) Build source child indexes (public/non-public/all) + parent link ---
        source_child_all_uris = set()
        source_child_public_by_uri = {}      # only public children considered for create/update
        source_child_not_target_uris = set() # non-public → delete on target if present
        child_parent_source = {}             # child source_uri -> parent dataset source_uri

        for e in source:
            t = str(e.get("type","")).lower()
            if t not in ("distribution", "sample", "analytics"):
                continue

            cu = norm(e.get("source_uri"))
            if not cu:
                continue
            source_child_all_uris.add(cu)

            # Parent is a dataset source URI
            parent_uri = norm(e.get("isPartOf"))
            if parent_uri:
                child_parent_source[cu] = parent_uri

            status = str(e.get("status","")).lower()
            if status == "public":
                source_child_public_by_uri[cu] = e
            else:
                source_child_not_target_uris.add(cu)

        # --- 4) Plan CREATE/UPDATE for public children, with parent checks/cascade ignore ---
        for cu, se in source_child_public_by_uri.items():
            parent_src = child_parent_source.get(cu)
            # Map to parent on target (where to attach)
            parent_tgt_entry = target_datasets_by_source.get(parent_src) if parent_src else None
            parent_tgt_uri = parent_tgt_entry.get("target_uri") if parent_tgt_entry else None

            # If parent exists on target but is planned for deletion → delete child (ignored)
            actions.append  # (no-op; just to keep the context clear in your head :) )

            if parent_tgt_uri and parent_tgt_uri in parents_deleted_target_uris:
                actions.append({
                    "action": "delete",
                    "type": str(se.get("type")).lower(),
                    "target_uri": target_child_by_internal.get(cu, {}).get("target_uri"),
                    "source_uri": cu,
                    "parent_target_uri": parent_tgt_uri,
                    "ignored": True,
                    "action_note": "Ignored: parent not expected on target (cascade delete)."
                })
                continue

            # If parent is missing on target:
            # - We still plan a CREATE for the child, attaching via parent_source_uri.
            # - The final cascade guard (section F) will ignore this create if
            #   the parent isn't planned for creation.
            if not parent_tgt_uri:
                actions.append({
                    "action": "create",
                    "type": str(se.get("type")).lower(),
                    "target_uri": None,
                    "source_uri": cu,
                    "parent_source_uri": parent_src,   # no parent_target_uri yet
                    "action_note": "Create child (parent not yet on target); will attach after parent creation."
                })
                continue

            te = target_child_by_internal.get(cu)

            if not te:
                # CREATE child under the known parent dataset on target
                actions.append({
                    "action": "create",
                    "type": str(se.get("type")).lower(),
                    "target_uri": None,
                    "source_uri": cu,
                    "parent_target_uri": parent_tgt_uri,
                    "action_note": "Create child under parent dataset on target."
                })
            else:
                # UPDATE if source newer than target (or target missing modified timestamp)
                src_dt = parse_iso_dt(se.get("modified"))
                tgt_dt = parse_iso_dt(te.get("modified"))
                if tgt_dt is None or (src_dt and src_dt > tgt_dt):
                    actions.append({
                        "action": "update",
                        "type": str(se.get("type")).lower(),
                        "target_uri": te.get("target_uri"),
                        "source_uri": cu,
                        "parent_target_uri": parent_tgt_uri,
                        "action_note": (
                            "Target has no modified timestamp."
                            if tgt_dt is None
                            else "Source child is newer than target."
                        )
                    })

        # --- 5) Plan DELETEs for children that should not be on target ---
        for te in target:
            tt = str(te.get("type","")).lower()
            if tt not in ("distribution", "sample", "analytics"):
                continue

            internals = te.get("source_uri")

            # Orphan child on target (no mapping) → delete if configured
            if not internals:
                if config.DELETE_ORPHAN_METADATA_ON_TARGET:
                    actions.append({
                        "action": "delete",
                        "type": tt,
                        "target_uri": te.get("target_uri"),
                        "source_uri": None,
                        "parent_target_uri": te.get("isPartOf"),
                        "action_note": "Orphan child on target (no source mapping); remove."
                    })
                continue

            vals = internals if isinstance(internals, list) else [internals]
            vals = [norm(v) for v in vals if v]

            # If any internal exists but is non-public on source → delete
            matched_internal = [v for v in vals if v in source_child_not_target_uris]
            if matched_internal:
                for v in matched_internal:
                    actions.append({
                        "action": "delete",
                        "type": tt,
                        "target_uri": te.get("target_uri"),
                        "source_uri": v,
                        "parent_target_uri": te.get("isPartOf"),
                        "action_note": "Source child is not public; delete on target."
                    })
                continue

            # If none of the internal URIs exist on source anymore → delete
            if vals and all(v not in source_child_all_uris for v in vals):
                actions.append({
                    "action": "delete",
                    "type": tt,
                    "target_uri": te.get("target_uri"),
                    "source_uri": vals[0],
                    "parent_target_uri": te.get("isPartOf"),
                    "action_note": "Child no longer exists on source; cleanup."
                })


    # ---------------------------------------
    # F) CASCADE GUARD for children whose parent dataset isn't expected on target
    #    (inspired by your old code)
    # ---------------------------------------

    # Parents that will be DELETED on target (by target_uri)
    parents_deleted_target_uris = {
        a.get("target_uri")
        for a in actions
        if a.get("action") == "delete" and str(a.get("type","")).lower() == "dataset" and a.get("target_uri")
    }

    # Parents that will be CREATED on target (by source_uri)
    parents_created_source_uris = {
        a.get("source_uri")
        for a in actions
        if a.get("action") == "create" and str(a.get("type","")).lower() == "dataset" and a.get("source_uri")
    }

    for a in actions:
        if a.get("action") not in ("create", "update"):
            continue
        t = str(a.get("type","")).lower()
        if t not in ("distribution", "sample", "analytics"):
            continue

        # Prefer source-based parenting if present (case where parent isn’t on target yet)
        parent_src = a.get("parent_source_uri")
        parent_tgt = a.get("parent_target_uri")

        # If the action refers to a target parent that is scheduled for delete → ignore (cascade delete)
        if parent_tgt and parent_tgt in parents_deleted_target_uris:
            a["ignored"] = True
            a["action_note"] = (a.get("action_note") + " | " if a.get("action_note") else "") + \
                               "Ignored: parent scheduled for deletion on target."
            continue

        # If we only have parent_source_uri (parent not on target yet):
        #   - Keep if that parent is planned for creation
        #   - Otherwise mark ignored (parent not expected on target)
        if parent_src and not parent_tgt:
            if parent_src not in parents_created_source_uris:
                a["ignored"] = True
                a["action_note"] = (a.get("action_note") + " | " if a.get("action_note") else "") + \
                                   f"Ignored: parent not expected on target ({parent_src})."
            # else: parent will be created this run; keep the child action as valid



    # -----------------------------
    # Sort & final re-parenting pass
    # -----------------------------
    actions.sort(key=lambda a: (
        order.get(a.get("action"), 99),
        str(a.get("type", "")),
        a.get("target_uri", a.get("source_uri", ""))
    ))

    # If a dataset was planned under a catalog that will be deleted,
    # remove that parenting and force group-by parenting instead.
    _postpass_reparent(actions, source_dataset_groups, group_prop)

    return actions


# ----------------------------- main -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Plan actions (create/update/delete) to sync target with source.")
    parser.add_argument("--in-source", required=True, help="Path to sourceFDP.json produced by step 2")
    parser.add_argument("--in-target", required=True, help="Path to targetFDP.json produced by step 2")
    parser.add_argument("--out",       required=True, help="Where to write actionsOnTargetFDP.json")
    parser.add_argument("--only-type", choices=["catalog", "dataset", "distribution", "sample", "analytics"],
                        help="Optional: restrict planning to a single type")
    parser.add_argument("--sync-settings", help="Path to syncSettings.json produced by step 1 (optional but required for group-by mode)")
    args = parser.parse_args()

    source_path = Path(args.in_source)
    target_path = Path(args.in_target)
    out_path    = Path(args.out)
    sync_path   = Path(args.sync_settings) if args.sync_settings else None

    if not source_path.exists() or not target_path.exists():
        print(f"❌ Missing input file(s): {source_path} / {target_path}")
        sys.exit(1)

    # Load inputs
    print(f"🔍 Loading source: {source_path}")
    print(f"🔍 Loading target: {target_path}")
    source = load_json_list(source_path)
    target = load_json_list(target_path)

    # Load group-by sync settings from Step 1
    sync_settings = load_sync_settings(sync_path)

    print(f"🧭 Group-by {sync_settings.get('group_by_property')})")
    actions = plan_actions(source, target, sync_settings, only_type=args.only_type)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(actions, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary for human output
    counts = {"create": 0, "update": 0, "delete": 0}
    ignored = 0
    for a in actions:
        k = str(a.get("action"))
        counts[k] = counts.get(k, 0) + 1
        if a.get("ignored"):
            ignored += 1

    print(f"✅ Planned actions -> create: {counts['create']}, update: {counts['update']}, "
          f"delete: {counts['delete']}, ignored: {ignored}")
    print(f"🧾 Actions saved to {out_path.resolve()}")

if __name__ == "__main__":
    main()
