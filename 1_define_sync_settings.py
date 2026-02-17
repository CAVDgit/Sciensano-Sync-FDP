#!/usr/bin/env python3
"""
Step 1: Resolve sync settings from an FDP containing TECHNICAL:sync resources.

- Fetches RDF (Turtle) from a "sync settings FDP".
- Locates a TECHNICAL:sync resource with the given TECHNICAL:syncID.
- Extracts:
    * catalogues to sync or skip on the **source**
    * optional group-by property and values
    * optional TECHNICAL:conformsTo filters (list of profiles/standards URIs)

Outputs a JSON file consumed by later steps in the pipeline.
"""

import requests
import rdflib
import config
import argparse, json
from pathlib import Path
import sys
import re

_MOJI_MARKERS = ("Ã", "Â", "â€", "â€™", "â€œ", "â€�", "â€“", "â€”", "ï¿½")

def fix_mojibake(s: str) -> str:
    """
    Repair common mojibake patterns caused by UTF-8 bytes decoded as latin-1/cp1252.

    Strategy:
      - If string contains typical mojibake markers, try latin-1 -> utf-8 roundtrip.
      - If that fails, return original.
    """
    if not isinstance(s, str) or not s:
        return s
    if not any(m in s for m in _MOJI_MARKERS):
        return s
    try:
        repaired = s.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
        return repaired
    except Exception:
        return s

def deep_fix(obj):
    """
    Recursively apply fix_mojibake to strings inside dict/list structures.
    """
    if isinstance(obj, str):
        return fix_mojibake(obj)
    if isinstance(obj, list):
        return [deep_fix(x) for x in obj]
    if isinstance(obj, dict):
        return {k: deep_fix(v) for k, v in obj.items()}
    return obj

def _decode_response_bytes(r: requests.Response) -> str:
    """
    Decode HTTP response bytes into text robustly, preferring declared encoding,
    then requests guesses, then utf-8. Also apply mojibake repair if needed.
    """
    raw = r.content or b""

    # 1) Respect explicit charset if provided
    enc = None
    ctype = (r.headers.get("Content-Type") or "")
    m = re.search(r"charset=([^\s;]+)", ctype, flags=re.I)
    if m:
        enc = m.group(1).strip().strip('"').strip("'")

    # 2) Else requests' detected encoding (may be wrong but we try)
    if not enc:
        enc = r.encoding or None

    # 3) Decode with fallback
    tried = []
    for candidate in [enc, "utf-8", "windows-1252", "latin-1"]:
        if not candidate:
            continue
        if candidate in tried:
            continue
        tried.append(candidate)
        try:
            txt = raw.decode(candidate, errors="strict")
            return fix_mojibake(txt)
        except Exception:
            continue

    # 4) Last resort: permissive utf-8
    return fix_mojibake(raw.decode("utf-8", errors="replace"))

def write_json_atomic(path: Path, data: dict):
    """
    Atomic UTF-8 JSON write (prevents partial files and keeps encoding correct).
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    txt = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    tmp.write_text(txt, encoding="utf-8", newline="\n")
    tmp.replace(path)



def define_sync_settings(URL_SETTINGS_FDP, SYNC_ID):
    """
    Fetch sync settings only from RDF. No fallback to config here.

    Hard-fail if SYNC_ID is not found under any TECHNICAL.sync resource.

    Returns a 4-tuple:
        (override_found, SYNC_ID, settings_dict, exit_code)

    Where settings_dict contains:
      - 'catalogue_to_sync_in_source' : set[str]
      - 'catalogue_to_skip_in_source' : set[str]
      - 'group_by_property'           : str | None
      - 'group_by_values'             : list[dict]
      - 'conforms_to'                 : set[str]
      - 'last_modified_settings'      : str | None
    """

    # Build an rdflib Namespace instance for each configured prefix.
    # This will give you globals like RDF, FDP, TECHNICAL, etc.
    NS = {key: rdflib.Namespace(uri) for key, uri in config.NAMESPACES.items()}
    globals().update(NS)

    # SYNC_ID must always be provided (either via CLI or config).
    if not SYNC_ID:
        err = "SYNC_ID is not set (neither via --sync-id nor config.SYNC_ID)."
        print(f"❌ {err}")
        return None, None, {
            "error": err,
            "catalogue_to_sync_in_source": [],
            "catalogue_to_skip_in_source": [],
            "group_by_property": None,
            "group_by_values": [],
            "conforms_to": [],
            "last_modified_settings": None,
        }, 2

    print(f'\n🔄 Starting fetching Sync settings from {URL_SETTINGS_FDP} (sync_id={SYNC_ID})')

    headers = {'Accept': 'text/turtle'}

    def _fetch_ttl(url: str) -> rdflib.Graph | None:
        """
        Fetch a Turtle document from `url` and parse it into an rdflib.Graph.
        Returns None if the request or parse fails.

        Important: decode bytes robustly to avoid mojibake (UTF-8 decoded as latin-1).
        """
        try:
            r = requests.get(url, headers=headers, timeout=getattr(config, "TIMEOUT", 30))
            r.raise_for_status()

            ttl_text = _decode_response_bytes(r)

            g = rdflib.Graph()
            g.parse(data=ttl_text, format="turtle")
            return g
        except Exception as e:
            print(f"⚠️ Fetch failed {url}: {e}")
            return None

    # Fetch the root sync FDP document.
    root_g = _fetch_ttl(URL_SETTINGS_FDP)
    if root_g is None:
        err = f"Error fetching sync settings from {URL_SETTINGS_FDP}."
        print(f"❌ {err}")
        return None, SYNC_ID, {
            "error": err,
            "catalogue_to_sync_in_source": [],
            "catalogue_to_skip_in_source": [],
            "group_by_property": None,
            "group_by_values": [],
            "conforms_to": [],
            "last_modified_settings": None,
        }, 2

    # Candidates that may contain TECHNICAL:sync resources:
    candidates = [URL_SETTINGS_FDP]
    try:
        for fdp_root in root_g.subjects(RDF.type, FDP.FAIRDataPoint):
            for o in root_g.objects(fdp_root, TECHNICAL.sync):
                candidates.append(str(o))
    except Exception:
        pass

    # De-duplicate candidate URLs while preserving order
    seen = set()
    candidates = [u for u in candidates if not (u in seen or seen.add(u))]

    # RDF-only defaults
    sync_settings = {
        'catalogue_to_sync_in_source': set(),
        'catalogue_to_skip_in_source': set(),
        'group_by_property': None,
        'group_by_values': [],
        'conforms_to': set(),            # ✅ NEW
        'last_modified_settings': None,  # ✅ normalize default
    }

    # Whether RDF defines explicit include/skip lists
    override_config_sync_resource = False
    # Whether we found at least one TECHNICAL:sync with the requested SYNC_ID
    found_sync_id = False

    # Iterate over all candidate docs and look for a TECHNICAL:sync with the given SYNC_ID
    for sync_url in candidates:
        g = _fetch_ttl(sync_url)
        if g is None:
            continue

        # Find subjects that carry the matching TECHNICAL:syncID
        subjects = [
            s for (s, p, o) in g.triples((None, TECHNICAL.syncID, None))
            if str(o) == str(SYNC_ID)
        ]
        if not subjects:
            # No matching syncID in this graph; move to next candidate
            continue

        found_sync_id = True
        sync_res = subjects[0]

        # URIs of catalogues to include/skip on the source FDP
        URIToSyncAtSource = {str(o) for o in g.objects(sync_res, TECHNICAL.resourceToSyncAtSource)}
        URIToSkipAtSource = {str(o) for o in g.objects(sync_res, TECHNICAL.resourceToSkipAtSource)}

        if URIToSyncAtSource or URIToSkipAtSource:
            sync_settings['catalogue_to_sync_in_source'] = URIToSyncAtSource
            sync_settings['catalogue_to_skip_in_source'] = URIToSkipAtSource
            override_config_sync_resource = True
            print(f"🔧 Using sync include/skip from {sync_url}")

        # ✅ NEW: capture all TECHNICAL:conformsTo values from settings
        conforms_to = {str(o) for o in g.objects(sync_res, TECHNICAL.conformsTo)}
        if conforms_to:
            sync_settings['conforms_to'] = conforms_to
            print(f"📌 Found {len(conforms_to)} TECHNICAL.conformsTo value(s) in {sync_url}")

        last_modified_settings = next(g.objects(sync_res, TECHNICAL.modified), None)
        if last_modified_settings is not None:
            sync_settings['last_modified_settings'] = str(last_modified_settings)

        # --- Extract groupBy property + values if defined ---

        # TECHNICAL.groupBy points to the property IRI used for grouping,
        # e.g. technical:sciensanoTheme
        group_prop = next(g.objects(sync_res, TECHNICAL.groupBy), None)
        if group_prop:
            sync_settings['group_by_property'] = str(group_prop)

            # We expect triples like:
            #   sync_res  <group_prop>  _:node
            # for each group-by entry.
            for node in g.objects(sync_res, group_prop):
                entry = {}

                # Attempt to resolve the underlying "value" of the group:
                # - Either using the same property (node, <group_prop>, value)
                # - Or a generic RDF.value fallback (node, rdf:value, value)
                value_iri = (
                    next(g.objects(node, group_prop), None) or
                    next(g.objects(node, RDF.value), None)
                )
                if value_iri:
                    entry["value_uri"] = str(value_iri)

                # Optional multilingual titles if present on the node
                for lang_key, pred in [
                    ("titleEn", TECHNICAL.titleEn),
                    ("titleFr", TECHNICAL.titleFr),
                    ("titleNl", TECHNICAL.titleNl),
                    # Add more if you define them in the TECHNICAL namespace
                ]:
                    lit = next(g.objects(node, pred), None)
                    if lit is not None:
                        entry[lang_key] = str(lit)

                sync_settings['group_by_values'].append(entry)
        else:
            print("ℹ️ No TECHNICAL.groupBy found on the sync resource.")

    # If we never saw any TECHNICAL:sync with the requested SYNC_ID
    if not found_sync_id:
        err = f"No TECHNICAL.sync with TECHNICAL.syncID='{SYNC_ID}' found under {URL_SETTINGS_FDP}."
        print(f"❌ {err}")
        # Return empty lists and an error; non-zero exit code signals failure to caller
        return False, SYNC_ID, {
            "error": err,
            "catalogue_to_sync_in_source": [],
            "catalogue_to_skip_in_source": [],
            "group_by_property": None,
            "group_by_values": [],
            "conforms_to": [],
            "last_modified_settings": None,
        }, 2

    # We found a matching sync resource but it didn't define sync/skip lists:
    if not override_config_sync_resource:
        print("ℹ️ Matching TECHNICAL.sync found but no include/skip lists; using empty lists (RDF-only policy).")

    # Success: override_config_sync_resource may be True or False, but we found a sync resource.
    return override_config_sync_resource, SYNC_ID, sync_settings, 0


def main():
    """
    CLI wrapper:
      - resolves sync URL and ID (CLI overrides config)
      - calls define_sync_settings(...)
      - writes a JSON file suitable for later pipeline steps
    """
    ap = argparse.ArgumentParser(
        description="Resolve sync settings from RDF and write JSON (no config fallbacks)."
    )
    ap.add_argument("--out", required=True, help="Path to write sync_settings.json")
    ap.add_argument("--sync-url", default=None, help="Override URL_SETTINGS_FDP")
    ap.add_argument("--sync-id", default=None, help="Override SYNC_ID")
    args = ap.parse_args()

    # Determine sync_url:
    #   1. use --sync-url if provided
    #   2. else use config.URL_SETTINGS_FDP
    sync_url = None
    if isinstance(args.sync_url, str) and args.sync_url.strip():
        sync_url = args.sync_url.strip()
    elif hasattr(config, "URL_SETTINGS_FDP") and isinstance(config.URL_SETTINGS_FDP, str):
        sync_url = config.URL_SETTINGS_FDP.strip() or None
    elif hasattr(config, "URL_SETTINGS_FDP"):
        # config.URL_SETTINGS_FDP exists but is not a string
        raise TypeError("config.URL_SETTINGS_FDP must be a single string URL.")

    if not sync_url:
        print("❌ Missing sync URL (provide --sync-url or set config.URL_SETTINGS_FDP). Aborting.")
        sys.exit(1)

    # Determine sync_id:
    #   1. use --sync-id if provided
    #   2. else use config.SYNC_ID (if present)
    sync_id = (
        args.sync_id.strip()
        if isinstance(args.sync_id, str) and args.sync_id.strip()
        else getattr(config, "SYNC_ID", None)
    )

    # Call core logic
    override, _sync_id, settings, rc = define_sync_settings(sync_url, sync_id)


    # Normalise settings into a JSON-serialisable dict for output
    out = {
        "override": bool(override) if override is not None else False,
        "sync_id": _sync_id,
        "catalogue_to_sync_in_source": sorted(list(settings.get("catalogue_to_sync_in_source", []))),
        "catalogue_to_skip_in_source": sorted(list(settings.get("catalogue_to_skip_in_source", []))),
        "group_by_property": settings.get("group_by_property"),
        "group_by_values": settings.get("group_by_values", []),
        "conforms_to": sorted(list(settings.get("conforms_to", []))),  # ✅ NEW
        "last_modified_settings": settings.get("last_modified_settings"),
    }
    if "error" in settings:
        out["error"] = settings["error"]

    # Write JSON to disk
    out = deep_fix(out)
    write_json_atomic(Path(args.out), out)
    print(f"🧩 Wrote sync settings to {args.out}")

    # Exit code mirrors define_sync_settings
    sys.exit(rc if rc is not None else 0)


if __name__ == "__main__":
    main()
