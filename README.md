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

In addition to the publication status policy, this synchronisation service applies a HealthDCAT-AP profile filter for public datasets.

Only datasets that:
- have status PUBLIC, and
- have a dct:conformsTo value containing "healthdcat"

are eligible for synchronisation to the target FDP.

How it works

The pipeline reads dct:conformsTo from the CatalogRecord associated with the dataset (not from the dataset resource itself).
A dataset is considered HealthDCAT-compliant if at least one dct:conformsTo value contains the substring: ```healthdcat```

The match is case-insensitive. For example, the following value is accepted:

```<https://healthdataeu.pages.code.europa.eu/healthdcat-ap/releases/release-6/>```

Behaviour

- PUBLIC datasets without a HealthDCAT-related dct:conformsTo value are not synchronised.
- PUBLIC datasets that previously conformed but no longer do so may be removed from the target (depending on configuration).
- DRAFT / REVIEW / INTRANET datasets follow the standard publication policy described above.

This ensures that the target FDP acts as a HealthDCAT-AP public mirror, and does not publish datasets that do not explicitly declare compliance with the HealthDCAT-AP profile.

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
