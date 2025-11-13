#!/usr/bin/env python3
"""
Step 2: Harvest source/target FDPs and produce JSON files consumed by later steps.

- Fetches FDP root, catalogues, datasets, and child resources (distribution, sample, analytics).
- For the source:
    * Uses CatalogRecords + TECHNICAL:metadataStatus to determine status of dataset/children.
    * Respects include/skip catalogue lists coming from sync_settings.json.
- For the target:
    * Collects catalogues, datasets, and children with timestamps for comparison.

Output (per role) is a JSON array (list of dicts) with extracted metadata.
"""

import requests
import rdflib
from urllib.parse import urlparse
import re
import json
import config
import argparse
import sys
from pathlib import Path
from collections import defaultdict


# ------------------------- small utils -------------------------

# Sanitize file/folder names (e.g., for backups)
def sanitize_filename(name):
    name = re.sub(r'@.*', '', name)
    name = re.sub(r'[^A-Za-z0-9-_ ]', '', name)
    name = name.replace(' ', '_')
    return name.strip().lower()


UUID_RE = re.compile(
    r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',
    re.I
)

def _safe_segment(u: str) -> str:
    """Return a URL-safe segment derived from a string (hostname, path, etc.)."""
    return re.sub(r'[^A-Za-z0-9._-]+', '_', (u or '').strip())

def extract_uuid(uri: str):
    """Extract a UUID from a URI if present, else None."""
    m = UUID_RE.search(uri or '')
    return m.group(0).lower() if m else None


# ------------------------- top-level harvest per role -------------------------

def harvest_role_to_file(
    url: str,
    role: str,
    out_path: str,
    backup_metadata: bool,
    max_workers: int = 8,
    backup_root: str | None = None,
    sync_settings: dict | None = None
) -> bool:
    """
    Harvest a single FDP URL for a given role ("source" or "target")
    and write the result as a JSON list to out_path.

    Returns True if harvest was successful, False otherwise.
    """
    items = []

    if not isinstance(url, str) or not url.strip():
        print("⚠️ No valid FDP URL provided.")
        Path(out_path).write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
        return False

    res = harvest_fdp(
        url.strip(),
        role,
        sync_settings or {}
    )
    items.extend(res.get("metadata", []))

    Path(out_path).write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"🧾 Wrote: {out_path}")

    return bool(res.get("ok"))


# ------------------------- core FDP harvesting -------------------------

def harvest_fdp(
    fdpURL,
    fdp_role: str = "source",
    sync_settings = {},
):
    """
    Harvest an FDP.

    :param fdp_role: "source" or "target"
        Used to decide:
         - whether to read metadataStatus from CatalogRecords (source)
         - or only gather structural info (target).
    :param sync_settings: dict from Step 1 (sync_settings.json), e.g.:
        {
          "catalogue_to_sync_in_source": set([...]),
          "catalogue_to_skip_in_source": set([...]),
          "group_by_property": "http://technical.fdp/sciensanoTheme",
          "group_by_values": [...]
        }
    :param backup_metadata: (future) whether to store local TTL backups (currently unused)
    :param backup_root: (future) root folder for backups (currently unused)

    Returns a summary dict including `metadata` (list of harvested entries).
    """
    summary = {
        "ok": True,
        "fdp_url": fdpURL,
        "role": fdp_role.strip().lower(),
        "catalogues": 0,
        "datasets": 0,
        "distributions": 0,
        "samples": 0,
        "analytics": 0,
        "backup_path": None,
        "errors": [],
        "metadata": [],  # ← final rows returned to caller
    }

    # Namespaces from config.NAMESPACES (RDF, FDP, DCAT, DCT, TECHNICAL, etc.)
    NS = {key: rdflib.Namespace(uri) for key, uri in config.NAMESPACES.items()}
    globals().update(NS)

    # GroupBy property IRI (e.g., technical:sciensanoTheme)
    group_by_property = sync_settings.get('group_by_property')

    role = fdp_role.strip().lower()
    if role not in ("source", "target"):
        summary["ok"] = False
        summary["errors"].append(f"Invalid role: {fdp_role}")
        return summary

    print(f'\n🔄 Starting {role} harvest for {fdpURL}')

    headers = {'Accept': 'text/turtle'}
    try:
        res = requests.get(fdpURL, headers=headers, timeout=config.TIMEOUT)
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Error fetching root FDP from {fdpURL}: {e}")
        summary["ok"] = False
        summary["errors"].append(f"Root fetch failed: {e}")
        return summary

    fdpStore = rdflib.Graph()
    fdpStore.parse(data=res.text, format="turtle")

    # Retrieve catalogues linked from the FDP root
    allCatalogues = []
    for item in fdpStore.subjects(RDF.type, FDP.FAIRDataPoint):
        allCatalogues += list(fdpStore.objects(item, FDP.metadataCatalog))

    print(f'📦 Found {len(allCatalogues)} catalogues.')
    summary["catalogues"] = len(allCatalogues)

    # ---- Sync settings: include/skip catalogues on the SOURCE only ----
    catalogue_to_sync_in_source = set(sync_settings.get('catalogue_to_sync_in_source') or [])
    catalogue_to_skip_in_source = set(sync_settings.get('catalogue_to_skip_in_source') or [])

    metadata = []

    for index, catalogue_uri in enumerate(allCatalogues):
        metadataStatus = {}

        # -- Filter catalogues to sync for SOURCE role only --
        if role == 'source':
            if (
                # If we have an explicit "sync" list and this catalogue is not in it → skip
                (catalogue_to_sync_in_source and str(catalogue_uri) not in catalogue_to_sync_in_source)
                # If we have no "sync" list but we have a "skip" list, and this catalogue *is* in it → skip
                or (not catalogue_to_sync_in_source and catalogue_to_skip_in_source and str(catalogue_uri) in catalogue_to_skip_in_source)
            ):
                continue

        # Fetch catalogue metadata
        try:
            resCatalogue = requests.get(catalogue_uri, headers=headers, timeout=config.TIMEOUT)
            resCatalogue.raise_for_status()
            catalogueStore = rdflib.Graph()
            catalogueStore.parse(data=resCatalogue.text, format="turtle")
        except Exception as e:
            print(f"⚠️ Skipping catalogue {catalogue_uri}: {e}")
            summary["errors"].append(f"Catalogue skip {catalogue_uri}: {e}")
            continue

        # Title of the catalogue (prefer English)
        catalogTitles = [
            t for s in catalogueStore.subjects(RDF.type, DCAT.Catalog)
            for t in catalogueStore.objects(s, DCT.title)
        ]
        catalogTitle_en = next((str(t) for t in catalogTitles if getattr(t, 'language', None) == 'en'), None)
        catalogTitleDefault = catalogTitle_en or (str(catalogTitles[0]) if catalogTitles else None)

        # Capture list of (linked) datasets from the catalogue.
        allDatasets = list(catalogueStore.objects(None, DCAT.dataset))
        summary["datasets"] += len(allDatasets)

        # ---------- SOURCE: read statuses from CatalogRecords ----------
        if role == "source":
            # One metadataStatus mapping per *catalogue*:
            # {
            #   "published": {"dataset": {uri1, ...}, "child": {...}},
            #   "draft": {...},
            #   ...
            # }
            metadataStatus = defaultdict(lambda: {"dataset": set(), "child": set()})

            # Collect all catalogRecords belonging to this catalogue
            catalogRecords = [
                str(t)
                for s in catalogueStore.subjects(RDF.type, DCAT.Catalog)
                for t in catalogueStore.objects(s, DCAT.record)
            ]

            for catalogRecord in catalogRecords:
                try:
                    resCatalogueRecord = requests.get(catalogRecord, headers=headers, timeout=config.TIMEOUT)
                    resCatalogueRecord.raise_for_status()
                    catalogueRecordStore = rdflib.Graph()
                    catalogueRecordStore.parse(data=resCatalogueRecord.text, format="turtle")
                except Exception as e:
                    print(f"⚠️ Skipping catalogue record {catalogRecord}: {e}")
                    summary["errors"].append(f"Catalogue record skip {catalogRecord}: {e}")
                    continue

                record_uri = rdflib.URIRef(catalogRecord)

                # Entities referenced by the record:
                #  - FOAF.primaryTopic: dataset
                #  - TECHNICAL.childfOfPrimaryTopic: distributions/samples/analytics
                dataset_uris = list(catalogueRecordStore.objects(record_uri, FOAF.primaryTopic))
                child_uris   = list(catalogueRecordStore.objects(record_uri, TECHNICAL.childfOfPrimaryTopic))

                dataset_set = {str(u) for u in dataset_uris}
                child_set   = {str(u) for u in child_uris}

                for ent in dataset_uris + child_uris:
                    ent_str = str(ent)

                    # Decide which bucket to use
                    if ent_str in dataset_set:
                        bucket = "dataset"
                    elif ent_str in child_set:
                        bucket = "child"
                    else:
                        # Should not happen; skip rather than crash
                        continue

                    # TECHNICAL.metadataStatus may be set on the dataset/child.
                    statuses = {
                        str(o)
                        for o in catalogueRecordStore.objects(ent, TECHNICAL.metadataStatus)
                    }
                    if not statuses:
                        # Default to "draft" when no status found
                        statuses = {"draft"}

                    for st in statuses:
                        metadataStatus[st][bucket].add(ent_str)

            # Convert sets to sorted lists (for deterministic output if needed)
            metadataStatus = {
                st: {k: sorted(v) for k, v in buckets.items()}
                for st, buckets in metadataStatus.items()
            }

        # ---------- TARGET: collect catalogue-level info (timestamps, groupBy) ----------
        if role == "target":
            print(f"📁 Catalogue: {catalogTitleDefault} ({len(allDatasets)} datasets)")

            # Last modified of the catalogue: dct:modified or fallback FDP:metadataModified
            metadataModified = [
                str(t)
                for s in catalogueStore.subjects(RDF.type, DCAT.Catalog)
                for t in catalogueStore.objects(s, DCT.modified)
            ]
            _vals = list(catalogueStore.objects(None, FDP.metadataModified))
            metadataModified_failback = str(_vals[0]) if _vals else None

            catalogue_lastUpdated = metadataModified[0] if metadataModified else metadataModified_failback

            # GroupBy values for the catalogue, if a group_by_property is defined
            catalogue_groupByValues = []
            if group_by_property:
                prop_iri = rdflib.URIRef(group_by_property)
                catalogue_groupByValues = [
                    str(t)
                    for s in catalogueStore.subjects(RDF.type, DCAT.Catalog)
                    for t in catalogueStore.objects(s, prop_iri)
                ]

            metadata.append({
                "target_uri": str(catalogue_uri),
                "type": "catalog",
                "title": catalogTitleDefault,
                "source_uri": None,
                "modified": catalogue_lastUpdated,
                "isPartOf": fdpURL,
                "groupBy": catalogue_groupByValues
            })

        # ---------- Both roles: get list of datasets to actually process ----------
        if role == "source":
            # For source, we recompute allDatasets from metadataStatus,
            # which ensures that only datasets referenced by CatalogRecords
            # are considered.
            allDatasets = []
            for st, groups in metadataStatus.items():
                allDatasets.extend(groups.get("dataset", []))
            allDatasets = sorted(set(allDatasets))

        # ---------- Loop over datasets ----------
        for dataset in allDatasets:
            try:
                resDataset = requests.get(dataset, headers=headers, timeout=config.TIMEOUT)
                resDataset.raise_for_status()
                datasetStore = rdflib.Graph()
                datasetStore.parse(data=resDataset.text, format='turtle')
            except Exception as e:
                print(f"⚠️ Skipping dataset {dataset}: {e}")
                summary["errors"].append(f"Dataset skip {dataset}: {e}")
                continue

            # Dataset title (prefer English)
            datasetTitles = [
                t for s in datasetStore.subjects(RDF.type, DCAT.Dataset)
                for t in datasetStore.objects(s, DCT.title)
            ]
            datasetTitle_en = next((str(t) for t in datasetTitles if getattr(t, 'language', None) == 'en'), None)
            datasetTitleDefault = datasetTitle_en or (str(datasetTitles[0]) if datasetTitles else None)

            dataset_uri = str(dataset)
            dataset_uuid = extract_uuid(dataset_uri)
            dataset_filename = f"dataset_{dataset_uuid}.ttl"

            metadataModified = [
                str(t)
                for s in datasetStore.subjects(RDF.type, DCAT.Dataset)
                for t in datasetStore.objects(s, DCT.modified)
            ]
            _vals = list(datasetStore.objects(None, FDP.metadataModified))
            metadataModified_failback = str(_vals[0]) if _vals else None

            dataset_lastUpdated = metadataModified[0] if metadataModified else metadataModified_failback
            dataset_internalURI = sorted({
                str(notation)
                for id_node in datasetStore.objects(None, ADMS.identifier)
                for notation in datasetStore.objects(id_node, SKOS.notation)
            })

            if role == "source":
                # Determine dataset status from metadataStatus buckets
                dataset_status = "draft"
                for st, groups in metadataStatus.items():
                    if dataset_uri in groups.get("dataset", []):
                        dataset_status = st
                        break

            # GroupBy values for the dataset, if a group_by_property is defined
            dataset_groupByValues = []
            if group_by_property:
                prop_iri = rdflib.URIRef(group_by_property)
                dataset_groupByValues = [
                    str(t)
                    for s in datasetStore.subjects(RDF.type, DCAT.Dataset)
                    for t in datasetStore.objects(s, prop_iri)
                ]

            print(f"🕒 {datasetTitleDefault}")
            if role == "source":
                metadata.append({
                    "type": "dataset",
                    "title": datasetTitleDefault,
                    "source_uri": dataset_uri,
                    "filename": dataset_filename,
                    "modified": dataset_lastUpdated,
                    "status": dataset_status,
                    "isPartOf": str(catalogue_uri),
                    "groupBy": dataset_groupByValues
                })
            elif role == "target":
                metadata.append({
                    "type": 'dataset',
                    "title": datasetTitleDefault,
                    "source_uri": dataset_internalURI,
                    "target_uri": dataset_uri,
                    "modified": dataset_lastUpdated,
                    "isPartOf": str(catalogue_uri),
                    "groupBy": dataset_groupByValues
                })

            # ---------- Child resources (distribution, sample, analytics) ----------
            allDistributions = list(datasetStore.objects(None, DCAT.distribution))
            allSamples = list(datasetStore.objects(None, ADMS.sample))
            allAnalytics = list(datasetStore.objects(None, HEALTHDCATAP.analytics))

            for subclass_type, allSubClass, rdf_type in [
                ("distribution", allDistributions, DCAT.Distribution),
                ("sample", allSamples, ADMS.Sample),
                ("analytics", allAnalytics, HEALTHDCATAP.Analytics)
            ]:
                if subclass_type == "distribution":
                    summary["distributions"] += len(allSubClass)
                elif subclass_type == "sample":
                    summary["samples"] += len(allSubClass)
                elif subclass_type == "analytics":
                    summary["analytics"] += len(allSubClass)

                for subclass in allSubClass:
                    try:
                        subRes = requests.get(subclass, headers=headers, timeout=config.TIMEOUT)
                        subRes.raise_for_status()
                        subclassStore = rdflib.Graph()
                        subclassStore.parse(data=subRes.text, format="turtle")
                        subclass_uri = str(subclass)

                        subclassTitles = list(subclassStore.objects(None, DCT.title))
                        subclassTitle_en = next((str(t) for t in subclassTitles if getattr(t, 'language', None) == 'en'), None)
                        subclassTitleDefault = subclassTitle_en or (str(subclassTitles[0]) if subclassTitles else None)

                        if subclassTitleDefault:
                            print(f"   ➕ {subclass_type.capitalize()}: {subclassTitleDefault}")

                        subclass_uuid = extract_uuid(subclass_uri)
                        subclass_filename = f"{subclass_type}_{subclass_uuid}.ttl"

                        metadataModified = [
                            str(t)
                            for s in subclassStore.subjects(RDF.type, rdf_type)
                            for t in subclassStore.objects(s, DCT.modified)
                        ]
                        _vals = list(subclassStore.objects(None, FDP.metadataModified))
                        metadataModified_failback = str(_vals[0]) if _vals else None
                        subclass_lastUpdated = metadataModified[0] if metadataModified else metadataModified_failback

                        subclass_internalURI = sorted({
                            str(notation)
                            for id_node in subclassStore.objects(None, ADMS.identifier)
                            for notation in subclassStore.objects(id_node, SKOS.notation)
                        })

                        if role == "source":
                            # Determine child status from metadataStatus buckets
                            subclass_status = "draft"
                            for st, groups in metadataStatus.items():
                                if subclass_uri in groups.get("child", []):
                                    subclass_status = st
                                    break

                        if role == "source":
                            metadata.append({
                                "source_uri": subclass_uri,
                                "type": subclass_type,
                                "title": subclassTitleDefault,
                                "filename": subclass_filename,
                                "modified": subclass_lastUpdated,
                                "status": subclass_status,
                                "isPartOf": dataset_uri
                            })
                        else:
                            metadata.append({
                                "target_uri": subclass_uri,
                                "type": subclass_type,
                                "title": subclassTitleDefault,
                                "source_uri": subclass_internalURI,
                                "modified": subclass_lastUpdated,
                                "isPartOf": dataset_uri
                            })

                    except Exception as e:
                        print(f"   ⚠️ Failed {subclass_type} {subclass}: {e}")
                        summary["errors"].append(f"{subclass_type} failed {subclass}: {e}")

    summary["metadata"] = metadata
    return summary


# ------------------------- CLI entrypoint -------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Harvest source/target FDPs into JSON for pipeline step 2."
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output folder where sourceFDP.json and targetFDP.json will be written"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Parallel workers for per-URL harvesting (currently unused)"
    )
    parser.add_argument(
        "--backup-metadata",
        action="store_true",
        help="Also serialize RDF TTL backups (currently not implemented)"
    )
    parser.add_argument(
        "--backup-root",
        help="Root folder where backups are stored (e.g. data/backup/<RUN_ID>)"
    )
    parser.add_argument(
        "--sync-settings",
        help="Path to sync_settings.json from Step 1"
    )
    args = parser.parse_args()

    def _normalize_to_set(x):
        """Convert a list (or None) to a set, safely."""
        if not x:
            return set()
        return set(x)

    # start empty; only fill from --sync-settings if provided
    sync_settings = {
        "catalogue_to_sync_in_source": set(),
        "catalogue_to_skip_in_source": set(),
        "group_by_property": None,
        "group_by_values": [],
    }
    group_by_map = {}  # value_uri -> {titleEn, titleFr, titleNl,...} (for later use)

    if args.sync_settings and Path(args.sync_settings).exists():
        try:
            j = json.loads(Path(args.sync_settings).read_text(encoding="utf-8"))

            # include/skip (ONLY these are sets)
            sync_settings["catalogue_to_sync_in_source"] = _normalize_to_set(
                j.get("catalogue_to_sync_in_source")
            )
            sync_settings["catalogue_to_skip_in_source"] = _normalize_to_set(
                j.get("catalogue_to_skip_in_source")
            )

            # grouping (preserve original types)
            sync_settings["group_by_property"] = j.get("group_by_property")
            sync_settings["group_by_values"] = j.get("group_by_values", []) or []

            for e in sync_settings["group_by_values"]:
                vu = e.get("value_uri")
                if vu:
                    group_by_map[vu] = {
                        k: v for k, v in e.items() if k not in ("node", "value_uri")
                    }
            print(f"🔧 Using sync include/skip and grouping from {args.sync_settings}")
        except Exception as e:
            print(
                f"⚠️ Could not read sync overrides ({args.sync_settings}): {e}. "
                "Proceeding with empty filters."
            )

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    backup_root = args.backup_root

    source_out = out_dir / "sourceFDP.json"
    target_out = out_dir / "targetFDP.json"

    print(f"📂 Output directory: {out_dir}")

    ok_source = harvest_role_to_file(
        getattr(config, "URL_SOURCE_FDP", ""),
        "source",
        str(source_out),
        backup_metadata=args.backup_metadata,
        max_workers=args.max_workers,
        backup_root=backup_root,
        sync_settings={**sync_settings, "group_by_map": group_by_map},  # NOTE: group_by_map currently unused
    )
    print(f"🧾 Wrote: {source_out}  (ok={ok_source})")

    ok_target = harvest_role_to_file(
        getattr(config, "URL_TARGET_FDP", ""),
        "target",
        str(target_out),
        backup_metadata=args.backup_metadata,
        max_workers=args.max_workers,
        backup_root=backup_root,
        sync_settings={**sync_settings, "group_by_map": group_by_map},
    )
    print(f"🧾 Wrote: {target_out}  (ok={ok_target})")

    if not (ok_source or ok_target):
        print("❌ Harvest failed for both source and target.")
        sys.exit(1)

    print("✅ Harvest step completed.")
