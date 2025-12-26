import os
import time
import json
from typing import List, Dict, Optional

import requests

# --------------------------------------------------
# Configuration
# --------------------------------------------------

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


# --------------------------------------------------
# Sécurité / vérification des variables
# --------------------------------------------------

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


# --------------------------------------------------
# Airtable helpers
# --------------------------------------------------

def airtable_url() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_CIS_TABLE_NAME}"


def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json",
    }


def post_with_retry(url: str, payload: dict, max_retries: int = 6) -> requests.Response:
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.post(
                url,
                headers=airtable_headers(),
                data=json.dumps(payload),
                timeout=60,
            )

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep((2 ** attempt) * 0.5)
                continue

            return r
        except Exception as e:
            last_exc = e
            time.sleep((2 ** attempt) * 0.5)

    raise RuntimeError(f"Échec API après retries : {last_exc}")


# --------------------------------------------------
# Download
# --------------------------------------------------

def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def download_file(url: str, dest_path: str) -> None:
    ensure_parent_dir(dest_path)

    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    size = os.path.getsize(dest_path)
    if size < 1000:
        raise RuntimeError("Fichier téléchargé trop petit – échec probable")

    print(f"✅ Fichier téléchargé : {dest_path} ({size} octets)")


# --------------------------------------------------
# Parsing CIS_bdpm.txt (gestion accents robuste)
# --------------------------------------------------

def parse_tsv_line(line: str) -> Optional[Dict[str, str]]:
    if not line.strip():
        return None

    parts = line.split("\t")
    if len(parts) < 6:
        return None

    code_cis = parts[0].strip()
    if not code_cis:
        return None

    return {
        "Code cis": code_cis,
        "Spécialité": parts[1].strip(),
        "Forme": parts[2].strip(),
        "Voie d'administration": parts[3].strip(),
        "Laboratoire": parts[-2].strip(),  # avant-dernière colonne
    }


def iter_records_from_file(filepath: str):
    # Lecture binaire puis décodage avec fallback
    with open(filepath, "rb") as f:
        raw = f.read()

    text = None
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            print(f"✅ Décodage réussi avec : {enc}")
            break
        except UnicodeDecodeError:
            continue

    if text is None:
        text = raw.decode("latin-1")
        print("⚠️ Décodage forcé en latin-1")

    for line in text.splitlines():
        rec = parse_tsv_line(line)
        if rec:
            yield rec


# --------------------------------------------------
# Upload Airtable
# --------------------------------------------------

def chunked(items: List[Dict], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def upsert_batch(batch_fields: List[Dict[str, str]]) -> None:
    payload = {
        "performUpsert": {"fieldsToMergeOn": ["Code cis"]},
        "records": [{"fields": fields} for fields in batch_fields],
    }

    r = post_with_retry(airtable_url(), payload)
    if r.status_code >= 300:
        raise RuntimeError(r.text)


def create_batch(batch_fields: List[Dict[str, str]]) -> None:
    payload = {"records": [{"fields": fields} for fields in batch_fields]}
    r = post_with_retry(airtable_url(), payload)
    if r.status_code >= 300:
        raise RuntimeError(r.text)


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    require_env()

    print("⬇️ Téléchargement du fichier CIS BDPM…")
    download_file(CIS_URL, DOWNLOAD_PATH)

    records = list(iter_records_from_file(DOWNLOAD_PATH))
    print(f"📄 Lignes parsées : {len(records)}")

    # Dédoublonnage par Code cis
    dedup = {}
    for r in records:
        dedup[r["Code cis"]] = r
    records = list(dedup.values())
    print(f"🧬 Après dédoublonnage : {len(records)}")

    total = 0
    for batch in chunked(records, BATCH_SIZE):
        try:
            upsert_batch(batch)
        except Exception:
            # fallback si performUpsert non supporté
            create_batch(batch)

        total += len(batch)
        print(f"➡️ Importés : {total}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print("🎉 Import CIS BDPM terminé avec succès")


if __name__ == "__main__":
    main()
