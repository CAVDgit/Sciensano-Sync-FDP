#!/usr/bin/env python3
"""
Main sync pipeline orchestrator.

This script wires the 6 individual steps together:

  1_define_sync_settings.py  → Resolve sync configuration from RDF
  2_harvest.py               → Harvest source & target FDPs into JSON
  3_plan_actions.py          → Compute actions (create/update/delete) on target
  4_prepare_content.py       → Fetch + clean TTL payloads for each action
  5_apply_actions.py         → Execute actions against target FDP
  6_save_logs.py             → Publish a log dataset + prune old logs

Each run gets its own timestamped folder under data/sync/<RUN_ID>.
"""

import sys, subprocess
from pathlib import Path
from datetime import datetime
import config
import json

# Always resolve all paths relative to this script's location so the pipeline
# works no matter from which directory it is invoked.
SCRIPT_DIR = Path(__file__).resolve().parent

# Individual pipeline step scripts (all in the same directory as this file)
STEP1 = SCRIPT_DIR / "1_define_sync_settings.py"
STEP2 = SCRIPT_DIR / "2_harvest.py"
STEP3 = SCRIPT_DIR / "3_plan_actions.py"
STEP4 = SCRIPT_DIR / "4_prepare_content.py"
STEP5 = SCRIPT_DIR / "5_apply_actions.py"
STEP6 = SCRIPT_DIR / "6_save_logs.py"

# Base folder for all data produced by the pipeline
BASE = (SCRIPT_DIR / "data").resolve()

# Folder for synchronisation runs
SYNC_BASE = BASE / "sync"

# Run identifier used as folder name: YYYY-MM-DD_HH-MM-SS
RUN_ID = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
RUN_DIR = SYNC_BASE / RUN_ID

# Folder where TTL payloads (step 4) are written
PAYLOADS_DIR = RUN_DIR / "payloads"

# Standard file names inside a run folder
SYNC_JSON   = RUN_DIR / "syncSettings.json"       # Step 1 output
SOURCE_JSON = RUN_DIR / "sourceFDP.json"          # Step 2 output (source)
TARGET_JSON = RUN_DIR / "targetFDP.json"          # Step 2 output (target)
ACTIONS_JSON= RUN_DIR / "actionsOnTargetFDP.json" # Step 3 output (actions)


def run_step(name, cmd, must_pass=True):
    """
    Helper to run a single step as a subprocess.

    Args:
        name:      Human-readable step name (for logs).
        cmd:       Command list passed to subprocess.call([python, stepX.py, ...]).
        must_pass: If True, the entire pipeline stops when this step fails.

    Returns:
        The subprocess return code (int).
    """
    print(f"\n▶️  {name}")
    print("   ", " ".join(map(str, cmd)))
    rc = subprocess.call(cmd)
    if rc != 0:
        print(f"❌ {name} failed (exit={rc}).")
        if must_pass:
            print("Stopping.")
            sys.exit(rc)
    else:
        print(f"✅ {name} done.")
    return rc


def main():
    # Create run + payloads directories if they don't exist
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    PAYLOADS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"🏁 Pipeline run: {RUN_ID}")
    print(f"📂 Sync folder: {RUN_DIR.resolve()}")

    # ------------------------------------------------------------------
    # Step 1: Resolve sync settings (non-blocking)
    #
    # Reads sync configuration from RDF in the sync FDP and writes
    # syncSettings.json. If it fails, we still continue with defaults.
    # ------------------------------------------------------------------
    rc1 = run_step(
        "Step 1: Resolve sync settings",
        [sys.executable, STEP1, "--out", str(SYNC_JSON)],
        must_pass=False
    )
    sync_settings_failed = (rc1 != 0)

    # ------------------------------------------------------------------
    # Step 2: Harvest (source & target)
    #
    # Harvests both FDPs (source + target) into:
    #   - sourceFDP.json
    #   - targetFDP.json
    #
    # We pass syncSettings.json if it exists (e.g. to restrict catalogues).
    # This step is mandatory: without it we cannot plan actions.
    # ------------------------------------------------------------------
    cmd2 = [sys.executable, STEP2, "--out", str(RUN_DIR), "--max-workers", "4"]
    if SYNC_JSON.exists():
        cmd2 += ["--sync-settings", str(SYNC_JSON)]
    rc2 = run_step("Step 2: Harvest (source & target)", cmd2, must_pass=True)

    # ------------------------------------------------------------------
    # Step 3: Plan actions
    #
    # Uses sourceFDP.json + targetFDP.json (+ optional syncSettings.json)
    # to produce actionsOnTargetFDP.json, which contains all create/update/
    # delete operations to apply on the target FDP.
    # ------------------------------------------------------------------
    cmd3 = [
        sys.executable, STEP3,
        "--in-source", str(SOURCE_JSON),
        "--in-target", str(TARGET_JSON),
        "--out",       str(ACTIONS_JSON),
    ]
    if SYNC_JSON.exists():
        cmd3 += ["--sync-settings", str(SYNC_JSON)]

    rc3 = run_step("Step 3: Plan actions", cmd3, must_pass=True)

    # ------------------------------------------------------------------
    # Determine if there are any actions at all.
    #
    # If the actions file is missing or invalid, we treat that as "no actions"
    # but still produce an empty file so that Step 6 can log something.
    # ------------------------------------------------------------------
    try:
        with open(ACTIONS_JSON, "r", encoding="utf-8") as f:
            actions = json.load(f)
        no_actions = (not actions)
    except Exception as e:
        print(f"❌ Could not read actions file {ACTIONS_JSON}: {e}")
        Path(ACTIONS_JSON).write_text("[]", encoding="utf-8")
        no_actions = True

    rc4 = rc5 = 0

    if no_actions:
        # We skip Step 4 & 5 but still run Step 6 to register a log with status=NO_ACTIONS
        print("\nℹ️ No actions to perform. Skipping Steps 4 & 5, but will still create a log RDF.")
    else:
        # ------------------------------------------------------------------
        # Step 4: Prepare content (non-blocking)
        #
        # For each create/update action, fetch the source TTL, clean it,
        # and store payloads under RUN_DIR/payloads/<type>/*.ttl.
        # If this fails, we do not stop the pipeline (Step 6 will still run),
        # but Step 5 may be skipped depending on rc4.
        # ------------------------------------------------------------------
        rc4 = run_step(
            "Step 4: Prepare content",
            [sys.executable, STEP4,
             "--actions", str(ACTIONS_JSON),
             "--out-dir", str(PAYLOADS_DIR),
             "--target-url", config.URL_TARGET_FDP],
            must_pass=False
        )

        # ------------------------------------------------------------------
        # Step 5: Apply actions
        #
        # Only executed if Step 4 completed successfully (rc4 == 0).
        # This step POST/PUT/DELETEs on the target FDP using the payloads
        # prepared by Step 4, and updates actionsOnTargetFDP.json with
        # success flags and notes.
        # ------------------------------------------------------------------
        if rc4 == 0:
            rc5 = run_step(
                "Step 5: Apply actions",
                [sys.executable, STEP5,
                 "--actions", str(ACTIONS_JSON),
                 "--payloads", str(PAYLOADS_DIR)],
                must_pass=False
            )
        else:
            print("⚠️ Skipping Step 5 because Step 4 failed.")

    # Make sure actions file exists so Step 6 always has something to log
    if not Path(ACTIONS_JSON).exists():
        Path(ACTIONS_JSON).write_text("[]", encoding="utf-8")

    # ------------------------------------------------------------------
    # Compute global status flag for the log dataset
    #
    # This is a coarse run-level status:
    #   - FAILED:SYNC_SETTINGS  : Step 1 failed
    #   - NO_ACTIONS            : Nothing to do on target
    #   - FAILED:PREPARE        : Failure in Step 4
    #   - FAILED:APPLY          : Failure in Step 5
    #   - OK                    : All relevant steps succeeded
    # ------------------------------------------------------------------
    if sync_settings_failed:
        status = "FAILED:SYNC_SETTINGS"
    elif no_actions:
        status = "NO_ACTIONS"
    else:
        if rc4 != 0:
            status = "FAILED:PREPARE"
        elif rc5 != 0:
            status = "FAILED:APPLY"
        else:
            status = "OK"

    # ------------------------------------------------------------------
    # Step 6: Build & publish log + prune old logs (always run)
    #
    # Even if previous steps failed, we still try to:
    #   - generate a log dataset from actionsOnTargetFDP.json
    #   - POST it to the logs FDP
    #   - prune old logs to keep only LAST_LOGS_TO_KEEP
    #   - prune old local sync folders under data/sync
    #
    # Errors here do not abort the pipeline (must_pass=False).
    # ------------------------------------------------------------------
    run_step(
        "Step 6: Publish logs and prune old ones",
        [
            sys.executable, STEP6,
            "--actions", str(ACTIONS_JSON),
            "--base", "http://healthdataportal.eu/syncFDP/",
            "--status", status,
            "--prune",
            "--keep", str(config.LAST_LOGS_TO_KEEP),
            "--sync-dir", str(SYNC_BASE),
        ],
        must_pass=False
    )

    # Final summary of artefacts for this run
    print("\n🎉 Pipeline finished (logs attempted regardless of prior failures).")
    print(f"🧾 Artifacts:\n  - {SOURCE_JSON}\n  - {TARGET_JSON}\n  - {ACTIONS_JSON}\n  - {PAYLOADS_DIR}")

if __name__ == "__main__":
    main()
