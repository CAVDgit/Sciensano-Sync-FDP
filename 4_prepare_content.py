#!/usr/bin/env python3
"""
Step 4: Prepare content

- Reads actionsOnTargetFDP.json (planned actions from Step 3)
- For each create/update action with a source_uri:
    * fetch the TTL from the source FDP
    * clean the RDF (remove FDP-O noise, rewire isPartOf, etc.)
    * save cleaned TTL into payload files OR inline into JSON
- For synthetic group-by catalogs, generates a TTL from sync settings
- Optionally marks delete actions as 'ignored' when the parent is also deleted
  (to let the FDP cascade deletes instead of doing redundant child deletes)
"""

import json
import config
import time
from datetime import datetime
import rdflib
import os
from urllib.parse import urlparse
from pathlib import Path
import argparse
import sys
import requests

# ---------------- CLI ----------------

def parse_args():
    """
    Parse command-line arguments:
      --actions    : path to actionsOnTargetFDP.json from Step 3
      --out-dir    : directory where TTL payloads will be written
      --target-url : base URL of the TARGET FDP (for parent resolution)
      --inline     : if set, TTL is stored inline in JSON instead of files
    """
    p = argparse.ArgumentParser(
        description="Step 4: fetch TTLs from source for planned actions and clean them into payloads."
    )
    p.add_argument("--actions", required=True, help="Path to actionsOnTargetFDP.json (from step 3)")
    p.add_argument("--out-dir", required=True, help="Directory to write cleaned TTL payloads")
    p.add_argument("--target-url", default=None,
                   help="Target FDP base URL (defaults to config.URL_TARGET_FDP)")
    p.add_argument("--publishable-source-uris", default=None,
                   help="Optional path to publishableSourceUris.json from step 3")
    p.add_argument("--inline", action="store_true",
                   help="Store cleaned TTL inline in actions JSON instead of external files")
    return p.parse_args()

# -------------- Namespaces --------------
# Build rdflib.Namespace objects from config.NAMESPACES
NS = {key: rdflib.Namespace(uri) for key, uri in config.NAMESPACES.items()}
globals().update(NS)

# -------------- HTTP helpers --------------

def read_metadata(uri: str, token: str = None):
    """
    Perform a single GET on a metadata URI, requesting Turtle.
    Token is there for future use (Bearer auth), currently unused.
    """
    headers = {"Accept": "text/turtle", "Content-type": "text/turtle"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    res = requests.get(uri, headers=headers, timeout=config.TIMEOUT)
    return res

def fetch_metadata_with_retries(uri: str, token: str = None, retries: int = 3, backoff_sec: float = 1.0):
    """
    Fetch metadata with simple exponential backoff.

    Returns: (ok: bool, text: str|None, content_type: str|None, error_msg: str|None)
    """
    for attempt in range(1, retries + 1):
        try:
            res = read_metadata(uri, token=token)
            ct = res.headers.get("Content-Type", "text/turtle")
            if res.status_code == 200:
                raw = res.content
                text = raw.decode("utf-8", errors="replace")  # ✅ force UTF-8
                return True, text, ct, None
            else:
                raw = res.content or b""
                snippet = raw.decode("utf-8", errors="replace")[:300]
                err = f"HTTP {res.status_code} for {uri}: {snippet}"
        except Exception as e:
            err = f"Exception for {uri}: {e}"

        if attempt < retries:
            # exponential backoff: 1, 2, 4, ...
            time.sleep(backoff_sec * (2 ** (attempt - 1)))
        else:
            return False, None, None, err

# -------------- File helpers --------------

def _uuid_from_uri(uri: str) -> str:
    """
    Heuristic to derive a filename-friendly ID from a URI:
    take the last path segment, or 'unknown'.
    """
    return urlparse(uri or "").path.rstrip("/").split("/")[-1] or "unknown"

def _choose_filename(action: dict) -> str:
    """
    Choose a payload filename for a given action.

    Priority:
      1) If 'filename' is set on the action, use it (ensuring '.ttl' extension).
      2) For synthetic group-by catalogs (no source_uri + group_value_uri),
         use 'catalog_<value>.ttl'.
      3) Fallback: '<type>_<uuid_from_source_uri>.ttl'
    """
    fn = action.get("filename")
    if isinstance(fn, str) and fn.strip():
        return fn if fn.endswith(".ttl") else (os.path.splitext(fn)[0] + ".ttl")

    typ = (action.get("type") or "object")

    # ✅ Handle group-by catalogs (no source_uri, only group_value_uri)
    if not (action.get("source_uri") or "").strip() and action.get("group_value_uri"):
        vu = urlparse(action["group_value_uri"]).path.split("/")[-1]
        return f"catalog_{vu}.ttl"

    return f"{typ}_{_uuid_from_uri(action.get('source_uri') or '')}.ttl"

def _save_ttl_file(ttl_text: str, out_base_dir: str, action: dict) -> str:
    """
    Save TTL text under out_base_dir/<type>/<filename>.ttl and
    return the filename only (not the full path).
    """
    typ = (action.get("type") or "object")
    fname = _choose_filename(action)
    out_dir = os.path.join(out_base_dir, typ)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
        f.write(ttl_text)
    return fname

# -------------- Mapping (target <-> source identifiers) --------------

def find_parents_class(fdpURL):
    """
    Build a mapping { target_resource_uri : [adms:identifier skos:notation values] }
    for all catalogs, datasets and their children on the TARGET FDP.

    This mapping is later used to:
      - re-point isPartOf to the correct target parents
      - re-point child links during updates
    """
    headers = {'Accept': 'text/turtle'}
    try:
        res = requests.get(fdpURL, headers=headers, timeout=config.TIMEOUT)
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Error fetching root FDP from {fdpURL}: {e}")
        return {}

    fdpStore = rdflib.Graph()
    fdpStore.parse(data=res.content.decode("utf-8", errors="replace"), format="turtle")

    allCatalogues = []
    for catalogue in fdpStore.subjects(RDF.type, FDP.FAIRDataPoint):
        allCatalogues += list(fdpStore.objects(catalogue, FDP.metadataCatalog))

    source_target_mapping = {}

    for catalogue_uri in allCatalogues:
        # --- Catalog-level identifiers ---
        try:
            resCatalogue = requests.get(catalogue_uri, headers=headers, timeout=config.TIMEOUT)
            resCatalogue.raise_for_status()
            catalogueStore = rdflib.Graph()
            catalogueStore.parse(data=resCatalogue.content.decode("utf-8", errors="replace"), format="turtle")
        except Exception as e:
            print(f"⚠️ Skipping catalogue {catalogue_uri}: {e}")
            continue

        identifiers = [
            str(notation)
            for cat in catalogueStore.subjects(RDF.type, DCAT.Catalog)
            for id_node in catalogueStore.objects(cat, ADMS.identifier)
            for notation in catalogueStore.objects(id_node, SKOS.notation)
        ]
        if identifiers:
            source_target_mapping[str(catalogue_uri)] = identifiers

        # --- Dataset-level identifiers ---
        allDatasets = list(catalogueStore.objects(None, DCAT.dataset))

        for dataset in allDatasets:
            try:
                resDataset = requests.get(dataset, headers=headers, timeout=config.TIMEOUT)
                resDataset.raise_for_status()
                datasetStore = rdflib.Graph()
                datasetStore.parse(data=resDataset.content.decode("utf-8", errors="replace"), format="turtle")
            except Exception as e:
                print(f"⚠️ Skipping dataset {dataset}: {e}")
                continue

            datasetIdentifiers = [
                str(notation)
                for cat in datasetStore.subjects(RDF.type, DCAT.Dataset)
                for id_node in datasetStore.objects(cat, ADMS.identifier)
                for notation in datasetStore.objects(id_node, SKOS.notation)
            ]
            if datasetIdentifiers:
                source_target_mapping[str(dataset)] = datasetIdentifiers

            # --- Children (distribution/sample/analytics) identifiers ---
            subclasses = [
                ("distribution", DCAT.Distribution, datasetStore.objects(None, DCAT.distribution)),
                ("sample", ADMS.Sample, datasetStore.objects(None, ADMS.sample)),
                ("analytics", HEALTHDCATAP.Analytics, datasetStore.objects(None, HEALTHDCATAP.analytics)),
            ]

            for subclass_type, rdf_type, allSubClass in subclasses:
                for subclass in allSubClass:
                    try:
                        subRes = requests.get(subclass, headers=headers, timeout=config.TIMEOUT)
                        subRes.raise_for_status()
                        subclassStore = rdflib.Graph()
                        subclassStore.parse(data=subRes.content.decode("utf-8", errors="replace"), format="turtle")
                    except Exception as e:
                        print(f"   ⚠️ Failed {subclass_type} {subclass}: {e}")
                        continue

                    subclassIdentifiers = [
                        str(notation)
                        for cat in subclassStore.subjects(RDF.type, rdf_type)
                        for id_node in subclassStore.objects(cat, ADMS.identifier)
                        for notation in subclassStore.objects(id_node, SKOS.notation)
                    ]
                    if subclassIdentifiers:
                        source_target_mapping[str(subclass)] = subclassIdentifiers

    return source_target_mapping


def is_source_fdp_uri(uri: str) -> bool:
    """
    True if the given URI belongs to the SOURCE FDP base URL.
    """
    base = str(getattr(config, "URL_SOURCE_FDP", "") or "").rstrip("/")
    return bool(base) and str(uri).startswith(base)


def resolve_target_uri_from_source(source_uri: str, source_target_mapping: dict) -> str | None:
    """
    Resolve a SOURCE FDP URI to the corresponding TARGET FDP URI
    using the existing mapping structure:
        {target_uri: [identifier1, identifier2, ...]}
    """
    src = str(source_uri)
    for target_uri, identifiers in (source_target_mapping or {}).items():
        if src in [str(v) for v in (identifiers or [])]:
            return str(target_uri)
    return None


def load_publishable_source_uris(path: Path | None) -> set[str]:
    """
    Load the companion JSON emitted by Step 3 and return a set of source URIs
    expected to exist on target in this run.
    """
    if not path or not path.exists():
        return set()
    try:
        j = json.loads(path.read_text(encoding="utf-8"))
        vals = j.get("publishable_source_uris", []) if isinstance(j, dict) else []
        return {str(v).strip().rstrip("/") for v in vals if isinstance(v, str) and v.strip()}
    except Exception as e:
        print(f"⚠️ Could not read publishable source URIs {path}: {e}")
        return set()


def merge_source_target_mappings(base_mapping: dict, extra_mapping: dict) -> dict:
    """
    Merge two mappings of the shape:
        {target_uri: [source_uri_1, source_uri_2, ...]}

    Values are de-duplicated and normalized with trailing slash removed.
    """
    merged: dict[str, list[str]] = {}

    def _add(target_uri, values):
        tgt = str(target_uri).strip()
        if not tgt:
            return
        bucket = merged.setdefault(tgt, [])
        seen = {str(v) for v in bucket}
        for v in (values or []):
            sv = str(v).strip()
            if not sv or sv in seen:
                continue
            bucket.append(sv)
            seen.add(sv)

    for k, vals in (base_mapping or {}).items():
        _add(k, vals)
    for k, vals in (extra_mapping or {}).items():
        _add(k, vals)
    return merged


def load_target_harvest_mapping(path: Path | None) -> dict:
    """
    Load targetFDP.json harvested in Step 2 and convert it to the same mapping shape as
    find_parents_class():
        {target_uri: [source_uri_1, source_uri_2, ...]}

    This helps resolve pre-existing target resources that may not be discoverable via
    live traversal of the target FDP in find_parents_class().
    """
    if not path or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️ Could not read harvested target mapping {path}: {e}")
        return {}

    out: dict[str, list[str]] = {}
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict):
            continue
        target_uri = str(item.get("target_uri") or "").strip()
        if not target_uri:
            continue

        vals = item.get("source_uri")
        if isinstance(vals, str):
            values = [vals]
        elif isinstance(vals, list):
            values = [v for v in vals if isinstance(v, str)]
        else:
            values = []

        values = [v.strip().rstrip("/") for v in values if v.strip()]
        if values:
            out[target_uri] = values

    return out

# -------------- RDF cleaning --------------

def prepare_rdf(class_metadata: str,
              action: str,
              ttl_text: str,
              now_iso: str,
              source_target_mapping: dict,
              target_base_url: str | None) -> tuple[str, str | None]:
    """
    Clean a raw TTL payload coming from the SOURCE FDP.

    Steps:
      1. Remove FDP-O specific noise (LDP containers, SIO_000628, FDPO predicates…)
      2. Detect the main resource (catalog/dataset/distribution)
      3. Rewrite / drop problematic predicates:
           - DCT.isPartOf re-pointed to target parents when known,
             or dropped if unresolved (dataset/distribution)
           - DCT.isPartOf of catalog → attach to target root FDP
           - remove FDP profiles, bare bnode publishers/rights, etc.
      4. On create: use a fresh BNode as subject.
         On update: re-map to existing target resource when possible.
      5. Add an ADMS identifier (original URI) and TECHNICAL.modified timestamp.
    """
    g = rdflib.Graph()
    g.parse(data=ttl_text, format="turtle")

    # --------- First pass: global removals (class-agnostic) ----------
    direct_containers = set(g.subjects(rdflib.RDF.type, LDP.DirectContainer))
    targets_628 = set(o for _, _, o in g.triples((None, SIO.SIO_000628, None)))

    tmp = rdflib.Graph()
    for s, p, o in g:
        # Drop everything involving LDP.DirectContainer
        if s in direct_containers or o in direct_containers:
            continue
        # Drop SIO_000628 relations and their targets
        if p == SIO.SIO_000628:
            continue
        if s in targets_628 or o in targets_628:
            continue
        # Drop FDP-O predicates entirely
        if str(p).startswith(str(FDPO)):
            continue
        tmp.add((s, p, o))

    # --------- Identify main class URI ----------
    meta = (class_metadata or "").lower()
    if meta in ("catalog",):
        candidate_types = [DCAT.Catalog]
    elif meta == "dataset":
        candidate_types = [DCAT.Dataset]
    elif meta in ("distribution", "sample", "analytics"):
        candidate_types = [DCAT.Distribution]
    else:
        # fallback: try several likely classes
        candidate_types = [DCAT.Dataset, DCAT.Distribution, DCAT.Catalog]

    classURI, class_type = None, None
    for t in candidate_types:
        s = next(tmp.subjects(RDF.type, t), None)
        if s is not None:
            classURI, class_type = s, t
            break

    # If still no obvious main resource, pick the most "connected" non-BNode subject
    if classURI is None:
        counts = {}
        for s, p, o in tmp:
            if isinstance(s, rdflib.BNode):
                continue
            counts[s] = counts.get(s, 0) + 1
        if counts:
            classURI = max(counts, key=counts.get)
            class_type = next(tmp.objects(classURI, RDF.type), DCAT.Resource)

    # If we truly cannot identify anything, just return the cleaned graph as-is
    if classURI is None:
        return tmp.serialize(format="turtle"), None

    resolved_parent_target: str | None = None

    # Helper nodes for FDP-specific accessRights/identifier patterns
    fdp_access_rights_uri = rdflib.URIRef(f"{classURI}#accessRights")
    fdp_identifier_uri = rdflib.URIRef(f"{classURI}#identifier")

    # ConformsTo /profile/ nodes
    fdp_profile_uris = {
        o for o in tmp.objects(classURI, DCT.conformsTo)
        if "/profile/" in str(o)
    }

    # Protect adms:identifier traceability structures from URI translation
    identifier_nodes = set(tmp.objects(None, ADMS.identifier))

    # Identify bare bnodes used for publisher/rights (only rdf:type)
    bare_pub_rights = set()

    def _is_bare_bnode(node: rdflib.term.Node) -> bool:
        if not isinstance(node, rdflib.BNode):
            return False
        for p, _ in tmp.predicate_objects(node):
            if p != RDF.type:
                return False
        return True

    for pred in (DCT.publisher, DCT.rights):
        for o in tmp.objects(classURI, pred):
            if _is_bare_bnode(o):
                bare_pub_rights.add(o)

    # --------- Predicate removals / rewrites ----------
    filtered = rdflib.Graph()
    for s, p, o in tmp:
        # Remove dedicated FDP accessRights node and its link
        if fdp_access_rights_uri is not None:
            if s == fdp_access_rights_uri or o == fdp_access_rights_uri:
                continue
            if s == classURI and p == DCT.accessRights and o == fdp_access_rights_uri:
                continue

        # Remove dedicated FDP identifier node
        if s == fdp_identifier_uri and (
            p == DCT.identifier or (p == RDF.type and str(o) == "http://purl.org/spar/datacite/Identifier")
        ):
            continue

        # Drop /profile/ labels
        if s in fdp_profile_uris and p == RDFS.label:
            continue

        # Drop classURI → (publisher/rights) → bare bnode
        if s == classURI and p in (DCT.publisher, DCT.rights) and o in bare_pub_rights:
            continue

        # Drop the orphan bnode's type triples
        if s in bare_pub_rights:
            continue

        # --------- Class-specific logic for the main resource ---------
        if s == classURI:
            # Drop conformsTo /profile/ URIs
            if p == DCT.conformsTo and "/profile/" in str(o):
                continue
            # Drop language / license defaults that you don't want to propagate
            if p == DCT.language and str(o) == "http://id.loc.gov/vocabulary/iso639-1/en":
                continue
            if p == DCT.license and str(o) == "http://rdflicense.appspot.com/rdflicense/cc-by-nc-nd3.0":
                continue
            # Drop RDFS.label on main resource
            if p == RDFS.label:
                continue

            # Rewrite/validate isPartOf to target URIs
            if p == DCT.isPartOf and class_type in (DCAT.Dataset, DCAT.Distribution):
                # prefer parent decided in Step 3 (group-by or explicit)
                explicit_parent = globals().get("CURRENT_ACTION_PARENT_TARGET_URI")
                parent = None

                if explicit_parent:
                    parent = explicit_parent
                else:
                    # Fallback: try to map from source isPartOf using identifier mapping
                    parent_in_target = next(
                        (k for k, v in source_target_mapping.items() if str(o) in v),
                        None
                    )
                    if parent_in_target:
                        parent = parent_in_target

                if parent:
                    resolved_parent_target = parent
                    o = rdflib.URIRef(parent)
                else:
                    # Can't resolve → drop triple (and mark as delayed in main)
                    continue

            elif p == DCT.isPartOf and class_type == DCAT.Catalog:
                # Root catalog belongs to target FDP root (no fallback to config list)
                base = target_base_url
                if base:
                    resolved_parent_target = base
                    o = rdflib.URIRef(base)
                else:
                    # No known target base; drop triple
                    continue

            # On create/update drop child links; on update try to re-point to target
            if p in (DCAT.dataset, DCAT.distribution, ADMS.sample, HEALTHDCATAP.analytics):
                if action.lower() == "create":
                    # New catalogs/datasets should not carry children links
                    continue
                elif action.lower() == "update":
                    if class_type == DCAT.Catalog:
                        pass
                    else:
                        # Try to re-point child links to the existing target child via mapping
                        child_in_target = next(
                            (k for k, v in source_target_mapping.items() if str(o) in v),
                            None
                        )
                        if child_in_target:
                            o = rdflib.URIRef(child_in_target)
                        else:
                            # Unknown child on target; drop the link
                            continue

        # Generic immediate translation of internal SOURCE FDP object URIs
        # - only object URIs
        # - only if they start with URL_SOURCE_FDP
        # - never inside adms:identifier traceability structures
        # - only when the referenced source URI already has a target URI
        if (
            isinstance(o, rdflib.URIRef)
            and p != ADMS.identifier
            and s not in identifier_nodes
            and is_source_fdp_uri(str(o))
        ):
            mapped_target = resolve_target_uri_from_source(str(o), source_target_mapping)
            if mapped_target:
                o = rdflib.URIRef(mapped_target)

        filtered.add((s, p, o))

    # --------- Replace main subject on create/update ---------
    if action.lower() == "create":
        # Create: new blank node (target will assign final URI)
        finalURI = rdflib.BNode()
    else:  # update
        # Update: try to map to an existing target resource via ADMS identifier mapping
        class_in_target = next(
            (k for k, v in source_target_mapping.items() if str(classURI) in v),
            None
        )
        finalURI = rdflib.URIRef(class_in_target) if class_in_target else rdflib.BNode()

    finalg = rdflib.Graph()
    for s, p, o in filtered:
        # Replace references to original classURI with finalURI (subject or object)
        s2 = finalURI if s == classURI else s
        o2 = finalURI if o == classURI else o
        finalg.add((s2, p, o2))

    # Preserve type of main resource
    finalg.add((finalURI, rdflib.RDF.type, class_type))


    # Track original URI as adms:Identifier
    id_node_iri = rdflib.URIRef(f"{str(classURI)}#admsIdentifier")
    finalg.add((finalURI, ADMS.identifier, id_node_iri))
    finalg.add((id_node_iri, RDF.type, ADMS.Identifier))
    finalg.add((id_node_iri, SKOS.notation, rdflib.Literal(str(classURI), datatype=XSD.anyURI)))

    # technical:modified timestamp (editor-level timestamp)
    finalg.remove((finalURI, TECHNICAL.modified, None))
    finalg.add((finalURI, TECHNICAL.modified, rdflib.Literal(now_iso, datatype=XSD.dateTime)))

    ttl_bytes = finalg.serialize(format="turtle", encoding="utf-8")
    ttl = ttl_bytes.decode("utf-8")
    return ttl, resolved_parent_target

# -------------- main ----------------

def main():
    args = parse_args()
    actions_path = Path(args.actions)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Optional companion file from Step 3 listing source URIs expected on target
    publishable_path = Path(args.publishable_source_uris) if args.publishable_source_uris else (actions_path.parent / "publishableSourceUris.json")
    publishable_source_uris = load_publishable_source_uris(publishable_path)

    # Determine target URL (used for catalog isPartOf and mapping)
    target_url = args.target_url or (config.URL_TARGET_FDP if getattr(config, "URL_TARGET_FDP", "") else None)

    # Build TARGET mapping once (for updates / parent resolution)
    source_target_mapping = {}
    if target_url:
        source_target_mapping = find_parents_class(target_url)

    # Supplement the live mapping with the harvested target JSON from Step 2.
    # This helps resolve pre-existing target resources that are present in targetFDP.json
    # but are not discovered by live traversal in find_parents_class().
    target_harvest_path = actions_path.parent / "targetFDP.json"
    harvested_target_mapping = load_target_harvest_mapping(target_harvest_path)
    if harvested_target_mapping:
        source_target_mapping = merge_source_target_mappings(source_target_mapping, harvested_target_mapping)
        print(f"🧭 Loaded {len(harvested_target_mapping)} harvested target mapping entr{'y' if len(harvested_target_mapping)==1 else 'ies'} from {target_harvest_path}")

    if publishable_source_uris:
        print(f"🧷 Loaded {len(publishable_source_uris)} publishable source URI(s) from {publishable_path}")

    # Load actions JSON
    try:
        with open(actions_path, "r", encoding="utf-8") as f:
            actions = json.load(f)
    except Exception as e:
        print(f"❌ Cannot read actions file {actions_path}: {e}")
        sys.exit(1)

    updated_any = False
    now_iso = datetime.now().astimezone().isoformat(timespec="seconds")

    # --------- Per-action processing (create/update + synthetic catalogs) ---------
    for action in actions:
        act = (action.get("action") or "").lower()
        typ = (action.get("type") or "")
        source_uri = action.get("source_uri")
        action.pop("unresolved_publishable_refs", None)
        action.pop("unresolved_publishable_refs_scan_error", None)

        # If Step 3 attached this dataset to a catalog that is now scheduled for deletion,
        # clear the parent and mark the action as delayed so that Step 3's group-by logic
        # can re-parent correctly on the next run.
        if typ == "dataset" and action.get("parent_target_uri"):
            deleting = any(
                a.get("action") == "delete" and a.get("target_uri") == action["parent_target_uri"]
                for a in actions
            )
            if deleting:
                print(f"⚠️ Skipping dataset under deleted catalog: {action['parent_target_uri']}")
                action["parent_target_uri"] = None
                action["action_delayed"] = True

        print(f"→ Preparing {typ:<12} for {act.upper():<7} {source_uri or '-'}")

        # --- Synthetic group-by catalogs (no source_uri, created from sync settings) ---
        if act == "create" and typ == "catalog" and not source_uri and action.get("group_value_uri"):
            vu = action["group_value_uri"]
            titles = action.get("catalogue_title") or {}
            group_prop = action.get("group_property_iri")
            base = action.get("parent_target_uri") or (config.URL_TARGET_FDP or "").rstrip("/")

            # Build a minimal TTL for the new catalog:
            #   - type dcat:Catalog
            #   - dct:isPartOf target FDP root
            #   - groupBy property with the value_uri
            #   - multilingual titles
            cat_uri = rdflib.BNode()
            g = rdflib.Graph()
            g.add((cat_uri, RDF.type, DCAT.Catalog))
            g.add((cat_uri, DCT.isPartOf, rdflib.URIRef(base)))
            g.add((cat_uri, rdflib.URIRef(group_prop), rdflib.URIRef(vu)))

            for lang, lit in titles.items():
                if lit:
                    g.add((cat_uri, DCT.title, rdflib.Literal(lit, lang=lang)))

            now_iso_local = datetime.now().astimezone().isoformat(timespec="seconds")
            g.add((cat_uri, TECHNICAL.modified, rdflib.Literal(now_iso_local, datatype=XSD.dateTime)))

            ttl_text = g.serialize(format="turtle")
            fname = _save_ttl_file(ttl_text, str(Path(args.out_dir)), action)
            action["content"] = fname
            print(f"   ↳ generated new group-by catalog for {vu}")
            updated_any = True
            continue  # skip normal fetch/clean logic for synthetic catalog

        # --- Update group-by catalogs: reuse clean_rdf and then replace titles from sync settings ---
        if act == "update" and typ == "catalog" and action.get("group_value_uri"):
            vu = action["group_value_uri"]
            titles = action.get("catalogue_title") or {}
            target_uri = action.get("target_uri")

            if not target_uri:
                print("   ↳ no target_uri for catalog update, skipping")
                continue

            # Fetch the current catalog metadata from TARGET FDP
            ok, raw_ttl, content_type, err = fetch_metadata_with_retries(target_uri)
            action["content_fetched_at"] = now_iso

            if not ok or not raw_ttl:
                action["content"] = None
                action["content_error"] = err or "fetch failed for catalog update"
                updated_any = True
                print(f"   ↳ failed to fetch catalog {target_uri} for update: {err}")
                continue

            try:
                # Prepare a mapping so clean_rdf keeps the same catalog URI on update
                mapping_for_clean = dict(source_target_mapping) if source_target_mapping else {}
                vals = list(mapping_for_clean.get(target_uri, []))
                if target_uri not in vals:
                    vals.append(target_uri)
                mapping_for_clean[target_uri] = vals

                # Expose current action context for isPartOf handling
                globals()["CURRENT_ACTION_PARENT_TARGET_URI"] = action.get("parent_target_uri")
                globals()["CURRENT_ACTION_GROUP_VALUE_URI"] = vu
                globals()["CURRENT_ACTION_GROUP_PROPERTY_IRI"] = action.get("group_property_iri")

                # Reuse clean_rdf to strip FDP/SIO/etc. noise
                cleaned_ttl, parent_target = prepare_rdf(
                    "catalog",
                    "update",
                    raw_ttl,
                    now_iso,
                    mapping_for_clean,
                    target_url,
                )

                # Cleanup globals to avoid leaking context
                globals().pop("CURRENT_ACTION_PARENT_TARGET_URI", None)
                globals().pop("CURRENT_ACTION_GROUP_VALUE_URI", None)
                globals().pop("CURRENT_ACTION_GROUP_PROPERTY_IRI", None)

                # If clean_rdf resolved a parent on target, keep it on the action
                if parent_target is not None:
                    action["parent_target_uri"] = parent_target

                # Now override titles (and refresh TECHNICAL.modified) on the CLEANED graph
                g = rdflib.Graph()
                g.parse(data=cleaned_ttl, format="turtle")

                # Find the main catalog resource
                cats = list(g.subjects(RDF.type, DCAT.Catalog))
                if cats:
                    cat_uri = cats[0]
                else:
                    cat_uri = rdflib.URIRef(target_uri)

                # Replace all dct:title with the ones from sync settings
                g.remove((cat_uri, DCT.title, None))
                for lang, lit in titles.items():
                    if lit:
                        g.add((cat_uri, DCT.title, rdflib.Literal(lit, lang=lang)))

                ttl_text = g.serialize(format="turtle")

                if args.inline:
                    action["content"] = ttl_text
                    action.pop("content_error", None)
                else:
                    fname = _save_ttl_file(ttl_text, str(Path(args.out_dir)), action)
                    action["content"] = fname
                    action.pop("content_error", None)

                print(f"   ↳ cleaned and updated group-by catalog titles for {vu}")
                updated_any = True

            except Exception as e:
                action["content"] = None
                action["content_error"] = f"catalog title update failed: {e}"
                updated_any = True
                print(f"   ↳ catalog title update failed: {e}")

            # Skip normal source-based fetch/clean logic for this catalog
            continue



        # --- Regular create/update from SOURCE FDP ---
        if act in {"create", "update"} and source_uri:
            ok, raw_ttl, content_type, err = fetch_metadata_with_retries(source_uri)
            action["content_fetched_at"] = now_iso

            if ok and raw_ttl:
                try:
                    # Make current action context available to clean_rdf (mainly parent_target_uri)
                    globals()["CURRENT_ACTION_PARENT_TARGET_URI"] = action.get("parent_target_uri")
                    globals()["CURRENT_ACTION_GROUP_VALUE_URI"] = action.get("group_value_uri")
                    globals()["CURRENT_ACTION_GROUP_PROPERTY_IRI"] = action.get("group_property_iri")

                    cleaned_ttl, parent_target = prepare_rdf(
                        typ, act, raw_ttl, now_iso, source_target_mapping, target_url
                    )

                    # Detect internal SOURCE FDP URIs that are publishable in this run
                    # but do not yet have a corresponding target URI.
                    unresolved_publishable_refs = []
                    try:
                        g_check = rdflib.Graph()
                        g_check.parse(data=cleaned_ttl, format="turtle")
                        identifier_nodes_check = set(g_check.objects(None, ADMS.identifier))
                        seen_refs = set()
                        for s_chk, p_chk, o_chk in g_check:
                            if not isinstance(o_chk, rdflib.URIRef):
                                continue
                            o_str = str(o_chk)
                            if p_chk == ADMS.identifier or s_chk in identifier_nodes_check:
                                continue
                            if not is_source_fdp_uri(o_str):
                                continue
                            if not publishable_source_uris:
                                continue
                            if o_str not in publishable_source_uris:
                                continue
                            if resolve_target_uri_from_source(o_str, source_target_mapping):
                                continue
                            key = (str(p_chk), o_str)
                            if key in seen_refs:
                                continue
                            seen_refs.add(key)
                            unresolved_publishable_refs.append({
                                "predicate": str(p_chk),
                                "source_object_uri": o_str
                            })
                    except Exception as e:
                        action["unresolved_publishable_refs_scan_error"] = str(e)

                    if unresolved_publishable_refs:
                        action["unresolved_publishable_refs"] = unresolved_publishable_refs

                    # Cleanup globals to avoid leaking context
                    globals().pop("CURRENT_ACTION_PARENT_TARGET_URI", None)
                    globals().pop("CURRENT_ACTION_GROUP_VALUE_URI", None)
                    globals().pop("CURRENT_ACTION_GROUP_PROPERTY_IRI", None)

                    # If clean_rdf resolved a parent on target, update the action accordingly
                    if parent_target is not None:
                        action["parent_target_uri"] = parent_target
                    else:
                        # Only mark as delayed if we DON'T already have a parent from step 3.
                        if typ in ("dataset", "distribution", "sample", "analytics") and not action.get("parent_target_uri"):
                            action["parent_target_uri"] = None
                            action["action_delayed"] = True
                    if action.get("unresolved_publishable_refs"):
                        print(f"   ↳ fetched and cleaned ({len(action['unresolved_publishable_refs'])} unresolved publishable ref(s) kept as source URI for now)")
                    else:
                        print("   ↳ fetched and cleaned")
                except Exception as e:
                    # Transform errors are stored on the action
                    action["transform_error"] = f"transform failed: {e}"
                    print(f"   ↳ transform failed: {e}")
                    cleaned_ttl = None

                # Save cleaned TTL either inline or into a file
                if cleaned_ttl is not None:
                    if args.inline:
                        action["content"] = cleaned_ttl
                        action.pop("content_error", None)
                    else:
                        try:
                            fname = _save_ttl_file(cleaned_ttl, str(out_dir), action)
                            action["content"] = fname
                            action.pop("content_error", None)
                        except Exception as e:
                            action["content"] = None
                            action["content_error"] = f"save failed: {e}"
                    updated_any = True
            else:
                # Fetch failed → mark error, no content
                action["content"] = None
                action["content_error"] = err or "fetch failed"
                updated_any = True
                print("   ↳ failed to fetch")

    # --------- Cascade optimisation for DELETE actions ---------
    # If a catalog or dataset is deleted, children that also have a delete action
    # with that parent_target_uri can be marked 'ignored' so that Step 5 can skip
    # redundant calls (the FDP will cascade deletes anyway).
    parent_delete_set = {
        a.get("target_uri")
        for a in actions
        if (a.get("action") or "").lower() == "delete"
        and (a.get("type") or "").lower() in {"catalog", "dataset"}
        and a.get("target_uri")
    }
    cascade_msg = "Ignored: parent scheduled for deletion; FDP will cascade."
    for a in actions:
        if (a.get("action") or "").lower() != "delete":
            continue
        parent_t = a.get("parent_target_uri")
        if parent_t and parent_t in parent_delete_set:
            changed = False
            if not a.get("ignored"):
                a["ignored"] = True
                changed = True
            note = (a.get("action_note") or "").strip()
            if cascade_msg not in note:
                if note.endswith("|"):
                    note = note[:-1].strip()
                a["action_note"] = f"{note} | {cascade_msg}" if note else cascade_msg
                changed = True
            if changed:
                updated_any = True

    # --------- Persist updated actions ---------
    if updated_any:
        try:
            with open(actions_path, "w", encoding="utf-8") as f:
                json.dump(actions, f, ensure_ascii=False, indent=2)
            print(f"✅ Updated actions with cleaned content: {actions_path}")
            print(f"📦 Payloads written to: {out_dir.resolve()}")
        except Exception as e:
            print(f"❌ Failed writing updated actions: {e}")
            sys.exit(1)
    else:
        print("ℹ️ No eligible actions (create/update with source_uri); file left unchanged.")

if __name__ == "__main__":
    main()
