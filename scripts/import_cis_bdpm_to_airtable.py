import os
import time
import json
from typing import List, Dict, Optional, Tuple

import requests

# ==================================================
# CONFIGURATION
# ==================================================

CIS_URL_DEFAULT = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
CIS_CPD_URL_DEFAULT = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"

CIS_URL = os.getenv("CIS_URL", CIS_URL_DEFAULT).strip()
CIS_CPD_URL = os.getenv("CIS_CPD_URL", CIS_CPD_URL_DEFAULT).strip()

DOWNLOAD_CIS_PATH = os.getenv("DOWNLOAD_CIS_PATH", "data/CIS_bdpm.txt").strip()
DOWNLOAD_CPD_PATH = os.getenv("DOWNLOAD_CPD_PATH", "data/CIS_CPD_bdpm.txt").strip()

AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

# Airtable limits
BATCH_SIZE = 10
REQUEST_SLEEP_SECONDS = 0.25

# Airtable field names (must match EXACTLY)
FIELD_CPD = "Conditions de prescription et délivrance"
FIELD_RCP = "Lien vers RCP"


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
        raise SystemExit("❌ Variables d'environnement manquantes : " + ", ".join(missing))


# ==================================================
# AIRTABLE HELPERS
# ==================================================

def airtable_table_url() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_CIS_TABLE_NAME}"


def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json",
    }


def airtable_get(params=None) -> dict:
    r = requests.get(airtable_table_url(), headers=airtable_headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def airtable_delete(record_ids: List[str]) -> None:
    # Airtable allows up to 10 record IDs per delete request
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i + 10]
        params = [("records[]", rid) for rid in batch]
        r = requests.delete(airtable_table_url(), headers=airtable_headers(), params=params, timeout=60)
        r.raise_for_status()
        time.sleep(REQUEST_SLEEP_SECONDS)


def airtable_create_batch(records_fields: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Create records and return mapping Code cis -> Airtable record_id for created rows.
    Assumes each record_fields has "Code cis".
    """
    payload = {"records": [{"fields": rf} for rf in records_fields]}
    r = requests.post(airtable_table_url(), headers=airtable_headers(), data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    data = r.json()

    created_map: Dict[str, str] = {}
    for rec in data.get("records", []):
        rid = rec.get("id")
        fields = rec.get("fields", {}) or {}
        code = str(fields.get("Code cis", "")).strip()
        if code and rid:
            created_map[code] = rid
    return created_map


def airtable_update_batch(updates: List[Tuple[str, Dict[str, str]]]) -> None:
    """
    updates: list of (record_id, fields_dict)
    """
    payload = {"records": [{"id": rid, "fields": fields} for rid, fields in updates]}
    r = requests.patch(airtable_table_url(), headers=airtable_headers(), data=json.dumps(payload), timeout=60)
    r.raise_for_status()


# ==================================================
# CLEAR TABLE
# ==================================================

def clear_airtable_table() -> None:
    print("🧹 Suppression complète de la table Airtable…")
    total_deleted = 0
    offset = None

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        data = airtable_get(params=params)
        recs = data.get("records", [])
        if not recs:
            break

        ids = [r["id"] for r in recs if "id" in r]
        if ids:
            airtable_delete(ids)
            total_deleted += len(ids)

        offset = data.get("offset")
        if not offset:
            break

    print(f"✅ {total_deleted} lignes supprimées")


# ==================================================
# DOWNLOAD + DECODE HELPERS (ACCENTS OK)
# ==================================================

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
    print(f"⬇️ Téléchargé: {dest_path} ({os.path.getsize(dest_path)} octets)")


def read_text_with_fallback(filepath: str) -> str:
    """
    BDPM files are often cp1252/latin-1.
    We'll decode with fallback to preserve accents.
    """
    with open(filepath, "rb") as f:
        raw = f.read()

    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            print(f"✅ Décodage {os.path.basename(filepath)} : {enc}")
            return text
        except UnicodeDecodeError:
            continue

    print(f"⚠️ Décodage forcé latin-1 pour {os.path.basename(filepath)}")
    return raw.decode("latin-1")


# ==================================================
# PARSING CIS_bdpm.txt
# ==================================================

def parse_cis_line(line: str) -> Optional[Dict[str, str]]:
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


def load_cis_records(filepath: str) -> List[Dict[str, str]]:
    text = read_text_with_fallback(filepath)
    records: List[Dict[str, str]] = []
    for line in text.splitlines():
        rec = parse_cis_line(line)
        if rec:
            records.append(rec)

    # De-dup by Code cis (keep last occurrence)
    dedup = {}
    for r in records:
        dedup[r["Code cis"]] = r
    return list(dedup.values())


# ==================================================
# PARSING CIS_CPD_bdpm.txt (enrichissement CPD)
# ==================================================

def parse_cpd_line(line: str) -> Optional[Tuple[str, str]]:
    """
    CIS_CPD_bdpm.txt:
      col 1 = Code CIS
      col 2 = Conditions de prescription et délivrance
    """
    if not line.strip():
        return None

    parts = line.split("\t")
    if len(parts) < 2:
        return None

    code_cis = parts[0].strip()
    cpd = parts[1].strip()
    if not code_cis:
        return None

    return code_cis, cpd


def load_cpd_map(filepath: str) -> Dict[str, str]:
    text = read_text_with_fallback(filepath)
    mapping: Dict[str, str] = {}
    for line in text.splitlines():
        parsed = parse_cpd_line(line)
        if parsed:
            code, cpd = parsed
            mapping[code] = cpd
    return mapping


# ==================================================
# RCP LINK BUILDER
# ==================================================

def build_rcp_link(code_cis: str) -> str:
    # Model requested by user:
    # https://base-donnees-publique.medicaments.gouv.fr/medicament/Code CIS/extrait#tab-rcp
    # We must insert the code cis as-is (string)
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{code_cis}/extrait#tab-rcp"


# ==================================================
# MAIN
# ==================================================

def main():
    require_env()

    # 1) Nettoyage total de la table
    clear_airtable_table()

    # 2) Téléchargements
    print("⬇️ Téléchargement CIS_bdpm.txt…")
    download_file(CIS_URL, DOWNLOAD_CIS_PATH)

    print("⬇️ Téléchargement CIS_CPD_bdpm.txt…")
    download_file(CIS_CPD_URL, DOWNLOAD_CPD_PATH)

    # 3) Charger CIS et réécrire table
    cis_records = load_cis_records(DOWNLOAD_CIS_PATH)
    print(f"📄 CIS: {len(cis_records)} lignes après dédoublonnage")

    print("✍️ Réécriture complète de la table (CIS)…")
    code_to_record_id: Dict[str, str] = {}

    for i in range(0, len(cis_records), BATCH_SIZE):
        batch = cis_records[i:i + BATCH_SIZE]
        created_map = airtable_create_batch(batch)
        code_to_record_id.update(created_map)

        print(f"➡️ Importées (CIS): {min(i + BATCH_SIZE, len(cis_records))}/{len(cis_records)}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"✅ Import CIS terminé. Records créés: {len(code_to_record_id)}")

    # 4) Enrichissement CPD par correspondance Code CIS
    cpd_map = load_cpd_map(DOWNLOAD_CPD_PATH)
    print(f"📌 CPD: {len(cpd_map)} codes CIS trouvés")

    updates: List[Tuple[str, Dict[str, str]]] = []
    matched_cpd = 0

    for code_cis, record_id in code_to_record_id.items():
        fields_to_update: Dict[str, str] = {}

        # CPD
        if code_cis in cpd_map:
            fields_to_update[FIELD_CPD] = cpd_map[code_cis]
            matched_cpd += 1

        # RCP link (always fill, because it's deterministic)
        fields_to_update[FIELD_RCP] = build_rcp_link(code_cis)

        if fields_to_update:
            updates.append((record_id, fields_to_update))

        if len(updates) >= BATCH_SIZE:
            airtable_update_batch(updates)
            updates.clear()
            time.sleep(REQUEST_SLEEP_SECONDS)

    # flush remaining
    if updates:
        airtable_update_batch(updates)
        updates.clear()

    print(f"🎉 Enrichissement terminé:")
    print(f"   - CPD mises à jour: {matched_cpd}")
    print(f"   - Lien RCP renseigné pour: {len(code_to_record_id)} lignes (champ '{FIELD_RCP}').")


if __name__ == "__main__":
    main()
