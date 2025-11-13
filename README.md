# FDP Synchronisation Pipeline – Docker Service
A fully automated synchronisation service between a **source FAIR Data Point (FDP)** and a **target FDP**, including group-by catalogs, metadata reconciliation, payload cleaning, parent–child linking, publishing, orphan pruning, and full audit logging.

This repository provides:

- A complete **6-step RDF synchronisation pipeline** (Python)
- A **supervisor** process that continuously:
  - Checks for **manual synchronisation requests** on a dedicated Sync Settings FDP
  - Runs the full sync pipeline automatically at user-configured intervals
  - Ensures **no overlapping** sync runs using file locking
- A Docker environment enabling the service to run continuously and fault-tolerantly

## Overview

This service keeps a target FDP synchronized with a source FDP by executing the following steps:

1. Resolve sync settings
2. Harvest metadata from both source and target FDPs  
3. Plan required actions (create, update, delete)
4. Prepare & clean metadata payloads
5. Apply actions on the target FDP (REST publish/update/delete)
6. Publish a log dataset + prune old logs

The pipeline produces a directory containing:
- `sourceFDP.json`
- `targetFDP.json`
- `actionsOnTargetFDP.json`
- Cleaned RDF payloads
- A published log dataset

## Supervisor

The supervisor handles:

### 1. Automatic periodic sync  
Configured via:
```
SYNC_INTERVAL
```

### 2. Detection of manual sync requests  
Polling interval:
```
CHECK_FOR_MANUAL_SYNC
```

When a `technical:syncRequest` containing the configured SYNC_ID appears in the Sync Settings FDP, it is deleted atomically and the pipeline runs immediately.

### 3. Locking
A file lock (`/tmp/sync_pipeline.lock`) ensures:
- No two syncs run simultaneously  
- Manual sync requests wait until automatic sync finishes (and vice‑versa)

## Environment Configuration

All configuration is done through environment variables.

Required:
```
DELETE_ORPHAN_METADATA_ON_TARGET
URL_SOURCE_FDP
URL_TARGET_FDP
LOGIN_TARGET_FDP
PASSWORD_TARGET_FDP

URL_SETTINGS_FDP
LOGIN_SETTINGS_FDP
PASSWORD_SETTINGS_FDP
SYNC_ID

LAST_LOGS_TO_KEEP
TIMEOUT
CHECK_FOR_MANUAL_SYNC
SYNC_INTERVAL
NAMESPACES_JSON
```

## Docker Usage

### Dockerfile
```
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "supervisor.py"]
```

### docker-compose.yml
```
services:
  fdp-sync:
    build: .
    restart: unless-stopped
    env_file:
      - .env
    volumes:
      - ./data:/app/data
```

## Manual Sync Trigger

Create an RDF resource containing:
```
technical:syncRequest  <request-uri> .
<request-uri> technical:syncID "my-sync-id" .
```

The supervisor will detect it, delete it, and run the pipeline.

## Logs

- Available via `docker logs -f fdp-sync`
- An RDF log dataset is also published automatically
- Old logs are pruned using:
```
LAST_LOGS_TO_KEEP
```

