import os
import time
import json
import re
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup


# ==================================================
# CONFIGURATION
# ==================================================

CIS_URL = os.getenv(
    "CIS_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
).strip()

CIS_CPD_URL = os.getenv(
    "CIS_CPD_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"
).strip()

CIS_CIP_URL = os.getenv(
    "CIS_CIP_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
).strip()

ANSM_PAGE = os.getenv(
    "ANSM_RETRO_PAGE",
    "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"
).strip()

DOWNLOAD_CIS_PATH = os.getenv("DOWNLOAD_CIS_PATH", "data/CIS_bdpm.txt").strip()
DOWNLOAD_CPD_PATH = os.getenv("DOWNLOAD_CPD_PATH", "data/CIS_CPD_bdpm.txt").strip()
DOWNLOAD_CIS_CIP_PATH = os.getenv("DOWNLOAD_CIS_CIP_PATH", "data/CIS_CIP_bdpm.txt").strip()
DOWNLOAD_ANSM_RETRO_PATH = os.getenv("DOWNLOAD_ANSM_RETRO_PATH", "data/ANSM_retrocession.xls").strip()

AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

BATCH_SIZE = 10
REQUEST_SLEEP_SECONDS = 0.25

# Airtable field names (must match EXACTLY)
FIELD_CPD = "Conditions de prescription et délivrance"
FIELD_RCP = "Lien vers RCP"
FIELD_AGREMENT = "Agrément aux collectivités"
FIELD_CIP13 = "CIP 13"
FIELD_RETRO = "Rétrocession"

LABEL_RETRO = "Disponible en rétrocession hospitalière"
LABEL_RH = "Réservé à l'usage hospitalier"
LABEL_CITY = "Disponible en pharmacie de ville"


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
# UTILS
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


def read_text_with_fallback(filepath: str) -> str:
    raw = open(filepath, "rb").read()
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    return raw.decode("latin-1")


def build_rcp_link(code_cis: str) -> str:
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{code_cis}/extrait#tab-rcp"


# ==================================================
# ANSM
# ==================================================

def find_ansm_retro_excel_url() -> str:
    r = requests.get(ANSM_PAGE, timeout=60)
    r.raise_for_status()
    html = r.text

    soup = BeautifulSoup(html, "lxml")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if re.search(r"\.xls[x]?$", href, flags=re.IGNORECASE) and "retrocession" in href.lower():
            links.append(href)

    if not links:
        candidates = re.findall(r'href="([^"]+\.xls[x]?)"', html, flags=re.IGNORECASE)
        links = [c for c in candidates if "retrocession" in c.lower()]

    if not links:
        raise RuntimeError("❌ Impossible de trouver le fichier Excel de rétrocession sur la page ANSM.")

    return urljoin(ANSM_PAGE, links[0])


def download_ansm_retro_excel(dest_path: str) -> str:
    url = find_ansm_retro_excel_url()
    ensure_parent_dir(dest_path)
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(resp.content)
    return url


def load_ansm_retro_cis_set(xls_path: str) -> Set[str]:
    df = pd.read_excel(xls_path, sheet_name=0, header=0, dtype=str)
    if df.shape[1] < 3:
        raise RuntimeError("❌ Fichier ANSM rétrocession: moins de 3 colonnes.")
    cis = df.iloc[:, 2].dropna().astype(str).str.strip()
    cis = cis[cis.str.len() > 0]
    return set(cis.tolist())


# ==================================================
# PARSE BDPM
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
        "Laboratoire": parts[-2].strip(),
    }


def load_cis_records(filepath: str) -> List[Dict[str, str]]:
    text = read_text_with_fallback(filepath)
    records: List[Dict[str, str]] = []
    for line in text.splitlines():
        rec = parse_cis_line(line)
        if rec:
            records.append(rec)
    dedup = {r["Code cis"]: r for r in records}
    return list(dedup.values())


def load_cpd_map(filepath: str) -> Dict[str, str]:
    text = read_text_with_fallback(filepath)
    mapping: Dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        cis = parts[0].strip()
        cpd = parts[1].strip()
        if cis:
            mapping[cis] = cpd
    return mapping


def load_cis_cip_maps_and_city_status(filepath: str) -> Tuple[Dict[str, str], Dict[str, Set[str]], Set[str]]:
    """
    CIS_CIP_bdpm.txt:
      - CIS = col1 (idx 0)
      - CIP13 = col7 (idx 6)
      - Agrément = col8 (idx 7)

    Règle utilisateur:
      Si colonnes 8/9/10 (idx 7,8,9) sont TOUTES vides => RH (réservé)
      Sinon => "ville"
    Ici on renvoie la liste des CIS "VILLE" (qui ont au moins une valeur en 8/9/10).
    """
    text = read_text_with_fallback(filepath)

    agrement_map: Dict[str, str] = {}
    cip_map: Dict[str, Set[str]] = {}
    cis_ville: Set[str] = set()

    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 10:
            continue

        cis = parts[0].strip()
        if not cis:
            continue

        cip13 = parts[6].strip()
        agrement = parts[7].strip()

        col8 = parts[7].strip()
        col9 = parts[8].strip()
        col10 = parts[9].strip()

        if cip13:
            cip_map.setdefault(cis, set()).add(cip13)

        if cis not in agrement_map:
            agrement_map[cis] = agrement
        else:
            if agrement_map[cis].lower() != "oui" and agrement.lower() == "oui":
                agrement_map[cis] = "oui"

        # Si une des colonnes 8/9/10 a quelque chose => "ville"
        if (col8 != "") or (col9 != "") or (col10 != ""):
            cis_ville.add(cis)

    return agrement_map, cip_map, cis_ville


# ==================================================
# AIRTABLE API
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
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i + 10]
        params = [("records[]", rid) for rid in batch]
        r = requests.delete(airtable_table_url(), headers=airtable_headers(), params=params, timeout=60)
        r.raise_for_status()
        time.sleep(REQUEST_SLEEP_SECONDS)


def airtable_create_batch(records_fields: List[Dict[str, str]]) -> Dict[str, str]:
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
    payload = {"records": [{"id": rid, "fields": fields} for rid, fields in updates]}
    r = requests.patch(airtable_table_url(), headers=airtable_headers(), data=json.dumps(payload), timeout=60)
    if r.status_code >= 400:
        try:
            print("❌ Airtable error payload:", r.json())
        except Exception:
            print("❌ Airtable raw response:", r.text)
        r.raise_for_status()


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
# MAIN
# ==================================================

def main():
    require_env()

    print("🔎 Étape 1/2 — Télécharger TOUS les fichiers (AVANT tout effacement Airtable)…")

    try:
        download_file(CIS_URL, DOWNLOAD_CIS_PATH)
        download_file(CIS_CPD_URL, DOWNLOAD_CPD_PATH)
        download_file(CIS_CIP_URL, DOWNLOAD_CIS_CIP_PATH)

        ansm_url = download_ansm_retro_excel(DOWNLOAD_ANSM_RETRO_PATH)
        print(f"🔗 Lien ANSM détecté: {ansm_url}")

    except Exception as e:
        raise SystemExit(
            "❌ Échec téléchargement : arrêt immédiat, Airtable NON modifiée.\n"
            f"Détail: {e}"
        )

    print("✅ Tous les fichiers OK → on met à jour Airtable.")

    # Parse first (safety)
    cis_records = load_cis_records(DOWNLOAD_CIS_PATH)
    cpd_map = load_cpd_map(DOWNLOAD_CPD_PATH)
    agrement_map, cip_map, cis_ville = load_cis_cip_maps_and_city_status(DOWNLOAD_CIS_CIP_PATH)
    retro_cis_set = load_ansm_retro_cis_set(DOWNLOAD_ANSM_RETRO_PATH)

    print(f"📄 CIS records: {len(cis_records)}")
    print(f"🏥 Retro ANSM: {len(retro_cis_set)}")
    print(f"🏙️ Ville (col8/9/10 non vides): {len(cis_ville)}")

    # Now clear and rewrite
    clear_airtable_table()

    print("✍️ Import CIS…")
    code_to_record_id: Dict[str, str] = {}
    for i in range(0, len(cis_records), BATCH_SIZE):
        created_map = airtable_create_batch(cis_records[i:i + BATCH_SIZE])
        code_to_record_id.update(created_map)
        print(f"➡️ Importées (CIS): {min(i + BATCH_SIZE, len(cis_records))}/{len(cis_records)}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"✅ Import CIS terminé. Records créés: {len(code_to_record_id)}")

    # Enrich
    updates: List[Tuple[str, Dict[str, str]]] = []
    count_retro = 0
    count_rh = 0
    count_ville = 0

    for code_cis, rid in code_to_record_id.items():
        fields: Dict[str, str] = {}

        if code_cis in cpd_map:
            fields[FIELD_CPD] = cpd_map[code_cis]

        if code_cis in agrement_map and agrement_map[code_cis] != "":
            fields[FIELD_AGREMENT] = agrement_map[code_cis]

        if code_cis in cip_map and cip_map[code_cis]:
            fields[FIELD_CIP13] = ";".join(sorted(cip_map[code_cis]))

        fields[FIELD_RCP] = build_rcp_link(code_cis)

        # --- Availability stored INSIDE "Rétrocession"
        if code_cis in retro_cis_set:
            fields[FIELD_RETRO] = LABEL_RETRO
            count_retro += 1
        else:
            if code_cis in cis_ville:
                fields[FIELD_RETRO] = LABEL_CITY
                count_ville += 1
            else:
                fields[FIELD_RETRO] = LABEL_RH
                count_rh += 1

        updates.append((rid, fields))
        if len(updates) >= BATCH_SIZE:
            airtable_update_batch(updates)
            updates.clear()
            time.sleep(REQUEST_SLEEP_SECONDS)

    if updates:
        airtable_update_batch(updates)

    print("🎉 Terminé :")
    print(f" - {count_retro} : {LABEL_RETRO}")
    print(f" - {count_rh} : {LABEL_RH}")
    print(f" - {count_ville} : {LABEL_CITY}")


if __name__ == "__main__":
    main()
