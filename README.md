# Sciensano Sync FDP

A Dockerized, fully automated synchronisation service between a **source FAIR Data Point (FDP)** and a **target FDP**.

This software is provided as-is and is intended to support FAIR metadata
synchronisation workflows. It does not provide any warranty and does not
replace institutional governance processes.

It supports:

- A **6-step RDF synchronisation pipeline** (Python)
- **RDF-driven sync configuration** hosted on a dedicated **Sync Settings FDP**
- Optional **group-by catalogues** (derived catalogues on the target FDP)
- Parent–child consistency (dataset → distributions/samples/analytics)
- Internal URI reconciliation between source and target metadata
- Second-pass fixup updates for publishable internal cross-references
- Optional **orphan cleanup** on the target FDP
- A published **RDF log dataset** for each run + **retention pruning**
- A **supervisor** process for:
  - scheduled runs
  - manual trigger detection via RDF
  - file-locking to prevent overlapping executions

---

## How it works

Each run creates a timestamped folder under:

```
data/sync/<RUN_ID>/
```

Where `<RUN_ID>` is `YYYY-MM-DD_HH-MM-SS`.

Artifacts produced per run:

- `syncSettings.json` (resolved from RDF sync settings, Step 1)
- `sourceFDP.json` (harvested source metadata, Step 2)
- `targetFDP.json` (harvested target metadata, Step 2)
- `actionsOnTargetFDP.json` (planned actions, Step 3)
- `publishableSourceUris.json` (source URIs planned for create/update, Step 3)
- `payloads/` (cleaned Turtle payloads, Step 4)

---

## The 6-step pipeline

The orchestrator (`sync_pipeline.py`) runs the following steps:

1. **Resolve sync settings** from RDF (`1_define_sync_settings.py`)
2. **Harvest** source + target FDPs into JSON (`2_harvest.py`)
3. **Plan actions** create/update/delete on target (`3_plan_actions.py`)
4. **Prepare content** TTL payloads (fetch + clean) (`4_prepare_content.py`)
5. **Apply actions** against target FDP (`5_apply_actions.py`)
6. **Publish logs** + prune old logs and old local run folders (`6_save_logs.py`)

---

## RDF-driven sync settings (hosted on an FDP)

This service is configured primarily through RDF hosted on a **Sync Settings FDP** (configured via `URL_SETTINGS_FDP`).

The pipeline does **not** hard-code sync scope logic. Instead, at runtime it:

- fetches Turtle from the Sync Settings FDP
- finds a `technical:sync` resource matching your `SYNC_ID`
- resolves the settings into `syncSettings.json`
- uses those settings in Steps 2 and 3

### `technical:sync` resource

A sync configuration is described as an RDF resource with:

- `technical:syncID` (string identifier matched against `SYNC_ID`)
- optional include / exclude lists for source catalogues:
  - `technical:resourceToSyncAtSource`
  - `technical:resourceToSkipAtSource`
- optional group-by configuration:
  - `technical:groupBy` → points to the property IRI used for grouping
  - per-group value nodes may carry:
    - `rdf:value` (or the same groupBy property)
    - `technical:titleEn`, `technical:titleFr`, `technical:titleNl`, …
- optional profile filter:
  - `technical:conformsTo`

If no include/skip lists are defined in RDF, the pipeline follows an RDF-only policy (no fallback include list).

---

## Publication policy (source status → target behavior)

The pipeline treats the target FDP as a “sticky public mirror”:

- **PUBLIC**: publish to target (create if missing, update if source is newer)
- **DRAFT**: do not publish changes, do not delete if already published
- **REVIEW**: same as DRAFT
- **INTRANET**: remove from target (delete, including children when applicable)

---

## HealthDCAT-AP compliance requirement (dct:conformsTo)

In addition to the publication status policy, this synchronisation service can apply a profile filter for public datasets.

Only datasets that:
- have status PUBLIC, and
- match the configured `technical:conformsTo` values from the sync settings

are eligible for synchronisation to the target FDP.

How it works:

- The pipeline reads `dct:conformsTo` from the **CatalogRecord** associated with the dataset, not from the dataset resource itself.
- Step 1 resolves the allowed `technical:conformsTo` values from the sync settings RDF.
- Step 3 only plans `create` / `update` actions for PUBLIC datasets whose catalog record conforms to at least one allowed profile.
- If no `technical:conformsTo` values are configured, this extra profile filter is not applied.

This allows the same source FDP to expose multiple metadata profiles while a given sync configuration mirrors only the intended public profile(s).

---

## Internal URI reconciliation between source and target

A dataset or child resource may contain RDF links to other metadata resources hosted on the **source** FDP, for example a dataset pointing to another dataset, distribution, sample, analytics resource, or source catalogue.

The synchronisation pipeline now distinguishes three situations for object URIs found in metadata payloads:

1. **External URI**
   - If the object URI does not start with `URL_SOURCE_FDP`, it is left unchanged.

2. **Internal source URI already known on target**
   - If the object URI starts with `URL_SOURCE_FDP` and the referenced source URI already has a corresponding resource on the target FDP, the URI is immediately rewritten to the target URI during Step 4.

3. **Internal source URI planned for publication but not yet known on target**
   - If the object URI starts with `URL_SOURCE_FDP`, is planned for `create` or `update` in the current run, but its target URI is not yet known, the link is temporarily kept as the source URI and recorded for a later fixup pass.

This behaviour prevents payloads from keeping stale source links when an equivalent target resource already exists, while still allowing the pipeline to progress when target URIs are created later in the same run.

### Traceability is preserved

URI rewriting is **never** applied inside the `adms:identifier` traceability structure.

This means the original source URI remains preserved in triples such as:

```turtle
?resource adms:identifier ?idNode .
?idNode skos:notation "https://intranet-fdp.sciensano.be/..."^^xsd:anyURI .
```

These identifiers are used to map source resources back to their corresponding target resources and must remain stable.

---

## New behaviour by step

### Step 3: publishable source URI index

In addition to `actionsOnTargetFDP.json`, Step 3 now writes:

```text
publishableSourceUris.json
```

This file contains the list of all `source_uri` values that are planned for non-ignored `create` or `update` actions in the current run.

It is used later to detect **publishable internal references** that may need a second-pass fixup once the corresponding target resource has been created.

### Step 4: immediate rewrite + unresolved publishable reference detection

Step 4 still fetches and cleans Turtle payloads, but now also:

- immediately rewrites internal source URIs to target URIs when the target resource already exists
- excludes all `adms:identifier` traceability triples from this rewrite logic
- records unresolved internal references in the action JSON when they point to source resources that are planned for publication in the same run

These unresolved references are stored on the action as:

```json
"unresolved_publishable_refs": [
  {
    "predicate": "...",
    "source_object_uri": "https://intranet-fdp.sciensano.be/..."
  }
]
```

At this stage, unresolved publishable references are **recorded but not blocked**. The payload can still be created and applied.

### Step 5: second-pass fixup update

After normal create/update/delete processing, Step 5 performs an extra reconciliation pass:

- it rebuilds a `source_uri -> target_uri` map from the actions that now have successful target resources
- it reopens payloads for actions carrying `unresolved_publishable_refs`
- it rewrites any now-resolvable source URIs to their final target URIs
- it sends a follow-up **update** to the target FDP when the fixup payload changed successfully

This avoids deadlocks where two publishable resources reference each other and neither one initially has a target URI.

In practice, the run behaves like this:

1. create/update resources as far as possible
2. collect newly assigned target URIs
3. patch remaining publishable internal links
4. send follow-up updates when needed

---

## Group-by catalogues

The pipeline supports optional **derived target catalogues** driven by RDF sync settings.

When group-by mode is enabled:

- Step 1 resolves the grouping property and expected value list from RDF
- Step 3 creates, updates, or deletes derived target catalogues as needed
- public datasets are attached to the canonical target catalogue corresponding to their group value
- Step 4 can generate synthetic catalog Turtle payloads when a grouped catalogue does not yet exist on target

If a dataset changes group value on the source side, the old target dataset can be removed and recreated under the new derived target catalogue.

---

## Parent-child consistency

Child resources (`distribution`, `sample`, `analytics`) follow the same publication policy as their parent dataset.

The pipeline preserves consistency by:

- resolving `dct:isPartOf` to the correct target parent when known
- delaying or re-parenting child actions when the parent target URI is not yet available
- letting the target FDP handle cascade deletes when a parent is removed

---

## Supervisor

The supervisor runs continuously inside the container and handles:

- Scheduled synchronisation (`SYNC_INTERVAL`)
- Manual sync trigger detection (`CHECK_FOR_MANUAL_SYNC`)
- Locking to prevent overlapping runs (`/tmp/sync_pipeline.lock`)

---

## Retention & pruning

Retention is controlled by:

```
LAST_LOGS_TO_KEEP
```

It applies to:

- published RDF log datasets
- local sync run folders under `data/sync/`

---

## Configuration (environment variables)

All configuration is provided via environment variables (typically via a `.env` file).

Important variables include:

- `URL_SOURCE_FDP`
- `URL_TARGET_FDP`
- `URL_SETTINGS_FDP`
- `SYNC_ID`
- `LAST_LOGS_TO_KEEP`
- `SYNC_INTERVAL`
- `CHECK_FOR_MANUAL_SYNC`

`URL_SOURCE_FDP` is also used by the payload preparation logic to decide whether an object URI should be treated as an **internal source FDP URI** eligible for reconciliation.

---

## Build & deploy with Docker

### Build image

```bash
docker build -t sciensano-sync-fdp:latest .
```

### Run container

```bash
docker run -d \
  --name fdp-sync \
  --restart unless-stopped \
  --env-file .env \
  -v "$(pwd)/data:/app/data" \
  sciensano-sync-fdp:latest
```

---

## Docker Compose (recommended)

```yaml
services:
  fdp-sync:
    build: .
    restart: unless-stopped
    env_file:
      - .env
    volumes:
      - ./data:/app/data
```

Run:

```bash
docker compose up -d --build
```

---

## License

This project is licensed under the **Apache License, Version 2.0**.

You are free to use, modify, and distribute this software, including for
commercial purposes, under the terms of the license.

See the `LICENSE` file for details.
