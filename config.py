# config.py
"""
Configuration of the synchronisation tool between a source and a target FDP.
This version reads all variables from environment variables (.env or docker-compose).
No hardcoded defaults — the script will raise an error if something required is missing.
"""
# Global version of the code
VERSION = "1.0.0"

import os
import json
from urllib.parse import urlparse
from pathlib import Path

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            if not line.strip() or line.strip().startswith("#"):
                continue
            k, _, v = line.strip().partition("=")
            os.environ[k.strip()] = v.strip()

 # ------


# ---------- helpers ----------

def _get_env(name: str, required: bool = True) -> str:
    """Fetch an environment variable or fail."""
    v = os.getenv(name)
    if v is None or v.strip() == "":
        if required:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return None
    return v.strip()

def _get_bool(name: str, required: bool = True) -> bool:
    v = _get_env(name, required)
    if v is None:
        return None
    return v.lower() in {"1", "true", "yes", "on", "y"}

def _get_int(name: str, required: bool = True) -> int:
    v = _get_env(name, required)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        raise RuntimeError(f"Invalid integer value for {name}: {v}")

def _get_json(name: str, required: bool = True):
    v = _get_env(name, required)
    if v is None:
        return None
    try:
        return json.loads(v)
    except Exception as e:
        raise RuntimeError(f"Invalid JSON for {name}: {e}")

def _require_url(name: str) -> str:
    u = _get_env(name)
    p = urlparse(u)
    if not (p.scheme and p.netloc):
        raise RuntimeError(f"{name} must be a valid absolute URL, got: {u!r}")
    return u

def _parse_times_list(s: str | None) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts




# ---------- required config variables ----------

DELETE_ORPHAN_METADATA_ON_TARGET = _get_bool("DELETE_ORPHAN_METADATA_ON_TARGET")

URL_SOURCE_FDP = _require_url("URL_SOURCE_FDP")
URL_TARGET_FDP = _require_url("URL_TARGET_FDP")

LOGIN_TARGET_FDP = _get_env("LOGIN_TARGET_FDP")
PASSWORD_TARGET_FDP = _get_env("PASSWORD_TARGET_FDP")

SYNC_ID = _get_env("SYNC_ID")
URL_SETTINGS_FDP = _require_url("URL_SETTINGS_FDP")
LOGIN_SETTINGS_FDP = _get_env("LOGIN_SETTINGS_FDP")
PASSWORD_SETTINGS_FDP = _get_env("PASSWORD_SETTINGS_FDP")

LAST_LOGS_TO_KEEP = _get_int("LAST_LOGS_TO_KEEP")
TIMEOUT = _get_int("TIMEOUT")
CHECK_FOR_MANUAL_SYNC = _get_int("CHECK_FOR_MANUAL_SYNC")
SYNC_SCHEDULE_TIMES = _parse_times_list(os.getenv("SYNC_SCHEDULE_TIMES", ""))


# Default namespaces
NAMESPACES_DEFAULT = {
    "RDF": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "FDP": "https://w3id.org/fdp/fdp-o#",
    "DCAT": "http://www.w3.org/ns/dcat#",
    "ADMS": "http://www.w3.org/ns/adms#",
    "LDP": "http://www.w3.org/ns/ldp#",
    "SIO": "http://semanticscience.org/resource/",
    "FDPO": "https://w3id.org/fdp/fdp-o#",
    "DCT": "http://purl.org/dc/terms/",
    "RDFS": "http://www.w3.org/2000/01/rdf-schema#",
    "SKOS": "http://www.w3.org/2004/02/skos/core#",
    "XSD": "http://www.w3.org/2001/XMLSchema#",
    "FOAF": "http://xmlns.com/foaf/0.1/",
    "HEALTHDCATAP": "http://healthdataportal.eu/ns/health#",
    "TECHNICAL": "http://technical.fdp/",
}

# Read JSON override (optional)
NAMESPACES_OVERRIDE = _get_json("NAMESPACES_JSON", required=False) or {}

# Merge: env overrides / adds keys
NAMESPACES = {**NAMESPACES_DEFAULT, **NAMESPACES_OVERRIDE}

# ---------- optional debug helper ----------

def dump_effective_config(mask_secrets: bool = True):
    def _mask(s: str) -> str:
        if not s or not mask_secrets:
            return s
        return s[:2] + "****" + s[-2:] if len(s) > 4 else "****"
    return {
        "DELETE_ORPHAN_METADATA_ON_TARGET": DELETE_ORPHAN_METADATA_ON_TARGET,
        "URL_SOURCE_FDP": URL_SOURCE_FDP,
        "URL_TARGET_FDP": URL_TARGET_FDP,
        "LOGIN_TARGET_FDP": _mask(LOGIN_TARGET_FDP),
        "PASSWORD_TARGET_FDP": _mask(PASSWORD_TARGET_FDP),
        "URL_SETTINGS_FDP": URL_SETTINGS_FDP,
        "SYNC_ID": SYNC_ID,
        "LOGIN_SETTINGS_FDP": _mask(LOGIN_SETTINGS_FDP),
        "PASSWORD_SETTINGS_FDP": _mask(PASSWORD_SETTINGS_FDP),
        "LAST_LOGS_TO_KEEP": LAST_LOGS_TO_KEEP,
        "TIMEOUT": TIMEOUT,
        "CHECK_FOR_MANUAL_SYNC": CHECK_FOR_MANUAL_SYNC,
        "SYNC_SCHEDULE_TIMES": SYNC_SCHEDULE_TIMES,
        "NAMESPACES_keys": sorted(list(NAMESPACES)),
    }
