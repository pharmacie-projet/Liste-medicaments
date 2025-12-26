import os
import time
import json
from typing import List, Dict, Optional

import requests

# ==================================================
# CONFIGURATION
# ==================================================

CIS_URL_DEFAULT = (
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
)

CIS_URL = os.getenv("CIS_URL", CIS_URL_DEFAULT)
DOWNLOAD_PATH = os.getenv("DOWNLOAD_PATH", "data/CIS_bdpm.txt")

AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

BATCH_SIZE = 10
REQUEST_SLEEP_SECONDS = 0.25


# ==================================================
# ENV CHECK
# ==================================================

def require_env():
    missing = []
    if not AIRTABLE_API_TOKEN:
        missing.append("AIRTABLE_API_TOKEN")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if not AIRTABLE_CIS_TABLE_NAME:
        missing.append("AIRTABLE_CIS_TABLE_NAME")

    if missing:
        raise SystemExit(
            "❌ Variables d'environnement manquantes : "
            + ", ".join(missing)
        )


# ==================================================
# AIRTABLE HELPERS
# ==================================================

def airtable_base_url() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_CIS_TABLE_NAME}"


def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json",
    }


def airtable_get(url: str, params=None):
    r = requests.get(url, headers=airtable_headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def airtable_delete(record_ids: List[str]):
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i + 10]
        params = [("records[]", rid) for rid in batch]
        r = requests.delete(
            airtable_base_url(),
            headers=airtable_headers(),
            params=params,
            timeout=60,
        )
        r.raise_for_status()
        time.sleep(REQUEST_SLEEP_SECONDS)


def airtable_create(records: List[Dict[str, str]]):
    payload = {"records": [{"fields": r} for r in records]}
    r = requests.post(
        airtable_base_url(),
        headers=airtable_headers(),
        data=json.dumps(payload),
        timeout=60,
    )
    r.raise_for_status()


# ==================================================
# STEP 1 — CLEAR TABLE COMPLETELY
# ==================================================

def clear_airtable_table():
    print("🧹 Suppression complète de la table Airtable…")
    offset = None
    total_deleted = 0

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        data = airtable_get(airtable_base_url(), params=params)
        records = data.get("records", [])

        if not records:
            break

        record_ids = [r["id"] for r in records]
        airtable_delete(record_ids)
        total_deleted += len(record_ids)

        offset = data.get("offset")
        if not offset:
            break

    print(f"✅ {total_deleted} lignes supprimées")


# ==================================================
# DOWNLOAD
# ==================================================

def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def download_file(url: str, dest_path: str):
    ensure_parent_dir(dest_path)

    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    print(f"⬇️ Fichier CIS téléchargé ({os.path.getsize(dest_path)} octets)")


# ==================================================
# PARSING (ACCENTS OK)
# ==================================================

def parse_tsv_line(line: str) -> Optional[Dict[str, str]]:
    if not line.strip():
        return None

    parts = line.split("\t")
    if len(parts) < 6:
        return None

    return {
        "Code cis": parts[0].strip(),
        "Spécialité": parts[1].strip(),
        "Forme": parts[2].strip(),
        "Voie d'administration": parts[3].strip(),
        "Laboratoire": parts[-2].strip(),  # avant-dernière colonne
    }


def iter_records_from_file(filepath: str):
    with open(filepath, "rb") as f:
        raw = f.read()

    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            print(f"✅ Décodage : {enc}")
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("latin-1")
        print("⚠️ Décodage forcé latin-1")

    for line in text.splitlines():
        rec = parse_tsv_line(line)
        if rec:
            yield rec


# ==================================================
# MAIN
# ==================================================

def main():
    require_env()

    # 1) Nettoyage total de la table
    clear_airtable_table()

    # 2) Téléchargement du fichier
    download_file(CIS_URL, DOWNLOAD_PATH)

    # 3) Parsing
    records = list(iter_records_from_file(DOWNLOAD_PATH))
    print(f"📄 Lignes parsées : {len(records)}")

    # 4) Import par batch
    print("🚀 Réécriture complète de la table…")
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        airtable_create(batch)
        print(f"➡️ Importées : {i + len(batch)}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print("🎉 Table Airtable entièrement reconstruite")


if __name__ == "__main__":
    main()
