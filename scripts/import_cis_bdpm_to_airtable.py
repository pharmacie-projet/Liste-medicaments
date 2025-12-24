import os
import time
import json
import requests
from typing import List, Dict, Optional

# Optional .env support
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "").strip()

# Path to CIS_bdpm.txt
INPUT_FILE = os.getenv("INPUT_FILE", "CIS_bdpm.txt")

# Airtable API limits
BATCH_SIZE = 10
REQUEST_SLEEP_SECONDS = 0.25  # gentle pacing


def require_env():
    missing = []
    if not AIRTABLE_API_TOKEN:
        missing.append("AIRTABLE_API_TOKEN")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if not AIRTABLE_TABLE_NAME:
        missing.append("AIRTABLE_TABLE_NAME")
    if missing:
        raise SystemExit(
            f"❌ Variables d'environnement manquantes: {', '.join(missing)}\n"
            f"➡️  Exemple:\n"
            f"   AIRTABLE_API_TOKEN=pat_xxx\n"
            f"   AIRTABLE_BASE_ID=appXXXXXXXXXXXXXX\n"
            f"   AIRTABLE_TABLE_NAME=\"Liste médicaments\"\n"
        )


def airtable_url() -> str:
    # Table name can contain spaces; requests will handle quoting if we pass it as part of URL
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"


def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json",
    }


def parse_tsv_line(line: str) -> Optional[Dict[str, str]]:
    """
    Expected mapping (1-indexed):
      1 -> Code cis
      2 -> Spécialité
      3 -> Forme
      4 -> Voie d'administration
      last-1 -> Laboratoire  (avant-dernière colonne)
    """
    line = line.rstrip("\n")
    if not line.strip():
        return None

    parts = line.split("\t")
    if len(parts) < 6:
        # Too short to contain the required fields
        return None

    code_cis = parts[0].strip()
    specialite = parts[1].strip()
    forme = parts[2].strip()
    voie = parts[3].strip()
    laboratoire = parts[-2].strip()  # avant-dernière

    if not code_cis:
        return None

    return {
        "Code cis": code_cis,
        "Spécialité": specialite,
        "Forme": forme,
        "Voie d'administration": voie,
        "Laboratoire": laboratoire,
    }


def iter_records_from_file(filepath: str):
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            rec = parse_tsv_line(line)
            if rec:
                yield rec


def chunked(items: List[Dict], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def post_with_retry(url: str, payload: dict, max_retries: int = 6) -> requests.Response:
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=airtable_headers(), data=json.dumps(payload), timeout=60)

            # Airtable rate limit / transient errors
            if r.status_code in (429, 500, 502, 503, 504):
                wait = (2 ** attempt) * 0.5
                time.sleep(wait)
                continue

            return r
        except Exception as e:
            last_exc = e
            wait = (2 ** attempt) * 0.5
            time.sleep(wait)

    raise RuntimeError(f"Échec API après retries. Dernière erreur: {last_exc}")


def upsert_batch(batch_fields: List[Dict[str, str]]) -> None:
    """
    Uses Airtable 'performUpsert' (if enabled for your base) to merge on Code cis.
    If your Airtable plan/base does not support upsert in API, you can switch to simple create.
    """
    url = airtable_url()
    payload = {
        "performUpsert": {
            "fieldsToMergeOn": ["Code cis"]
        },
        "records": [{"fields": fields} for fields in batch_fields],
    }

    r = post_with_retry(url, payload)
    if r.status_code >= 300:
        raise RuntimeError(f"❌ Airtable error {r.status_code}: {r.text}")


def create_batch(batch_fields: List[Dict[str, str]]) -> None:
    url = airtable_url()
    payload = {"records": [{"fields": fields} for fields in batch_fields]}
    r = post_with_retry(url, payload)
    if r.status_code >= 300:
        raise RuntimeError(f"❌ Airtable error {r.status_code}: {r.text}")


def main():
    require_env()

    if not os.path.exists(INPUT_FILE):
        raise SystemExit(f"❌ Fichier introuvable: {INPUT_FILE}")

    records = list(iter_records_from_file(INPUT_FILE))
    print(f"✅ Lignes parsées: {len(records)}")

    # De-dup by Code cis (keep last occurrence)
    dedup = {}
    for r in records:
        dedup[r["Code cis"]] = r
    records = list(dedup.values())
    print(f"✅ Après dédoublonnage (Code cis): {len(records)}")

    total = 0
    for batch in chunked(records, BATCH_SIZE):
        # Try UPSERT first; if Airtable rejects, fallback to CREATE
        try:
            upsert_batch(batch)
        except RuntimeError as e:
            msg = str(e)
            # Some bases may not support performUpsert; fallback to create
            if "performUpsert" in msg or "UNKNOWN_FIELD_NAME" in msg or "Invalid request" in msg:
                print("⚠️ Upsert non supporté / rejeté, fallback en création simple (doublons possibles).")
                create_batch(batch)
            else:
                raise

        total += len(batch)
        print(f"➡️  Importés: {total}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print("🎉 Import terminé.")


if __name__ == "__main__":
    main()
