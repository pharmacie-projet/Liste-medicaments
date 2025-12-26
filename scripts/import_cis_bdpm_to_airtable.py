import os
import time
import json
import re
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup


# ==================================================
# ENV / CONFIG
# ==================================================

AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

# BDPM sources
CIS_URL = os.getenv(
    "CIS_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
).strip()

CIS_CIP_URL = os.getenv(
    "CIS_CIP_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
).strip()

CIS_CPD_URL = os.getenv(
    "CIS_CPD_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"
).strip()

# ANSM page (dynamic Excel)
ANSM_RETRO_PAGE = os.getenv(
    "ANSM_RETRO_PAGE",
    "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"
).strip()

# Local paths
DOWNLOAD_CIS_PATH = os.getenv("DOWNLOAD_CIS_PATH", "data/CIS_bdpm.txt").strip()
DOWNLOAD_CIS_CIP_PATH = os.getenv("DOWNLOAD_CIS_CIP_PATH", "data/CIS_CIP_bdpm.txt").strip()
DOWNLOAD_CIS_CPD_PATH = os.getenv("DOWNLOAD_CIS_CPD_PATH", "data/CIS_CPD_bdpm.txt").strip()
DOWNLOAD_ANSM_RETRO_PATH = os.getenv("DOWNLOAD_ANSM_RETRO_PATH", "data/ANSM_retrocession.xlsx").strip()

# Airtable fields (names MUST match exactly)
FIELD_CODE_CIS = "Code cis"
FIELD_SPECIALITE = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE = "Voie d'administration"
FIELD_LABO = "Laboratoire"
FIELD_RCP_LINK = "Lien vers RCP"
FIELD_AGREMENT = "Agrément aux collectivités"
FIELD_CIP13 = "CIP 13"
FIELD_CPD = "Conditions de prescription et délivrance"
FIELD_RETRO = "Rétrocession"

# Values for "Rétrocession"
LABEL_RETRO = "Disponible en rétrocession hospitalière"
LABEL_RH = "Réservé à l'usage hospitalier"
LABEL_CITY = "Disponible en pharmacie de ville"

# Airtable limits / pacing
AIRTABLE_BATCH_SIZE = 10  # Airtable max 10 per create/update/delete
AIRTABLE_SLEEP = float(os.getenv("AIRTABLE_SLEEP", "0.25"))
AIRTABLE_MAX_RETRIES = int(os.getenv("AIRTABLE_MAX_RETRIES", "8"))

# RCP scraping pacing/retries
RCP_TIMEOUT = int(os.getenv("RCP_TIMEOUT", "45"))
RCP_SLEEP = float(os.getenv("RCP_SLEEP", "0.45"))
RCP_MAX_RETRIES = int(os.getenv("RCP_MAX_RETRIES", "4"))

# Detect hospital-only mention
HOSP_PATTERNS = [
    r"m[ée]dicament\s+r[ée]serv[ée]\s+[àa]\s+l[’']usage\s+hospitalier",
    r"r[ée]serv[ée]\s+[àa]\s+l[’']usage\s+hospitalier",
]


# ==================================================
# VALIDATION / UTILS
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


def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def download_file(url: str, dest_path: str, timeout: int = 180):
    ensure_parent_dir(dest_path)
    with requests.get(url, stream=True, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as r:
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


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def build_rcp_link(code_cis: str) -> str:
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{code_cis}/extrait#tab-rcp"


def clean_field_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        if pd.isna(v) or v == float("inf") or v == float("-inf"):
            return None
    s = str(v).strip()
    return s if s else None


def compact_fields(fields: Dict) -> Dict:
    out = {}
    for k, v in fields.items():
        v2 = clean_field_value(v)
        if v2 is not None:
            out[k] = v2
    return out


# ==================================================
# ANSM retro list
# ==================================================

def find_ansm_retro_excel_url() -> str:
    r = requests.get(ANSM_RETRO_PAGE, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "lxml")

    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if re.search(r"\.xls[x]?$", href, flags=re.IGNORECASE) and "retrocession" in href.lower():
            candidates.append(href)

    if not candidates:
        matches = re.findall(r'href="([^"]+\.xls[x]?)"', html, flags=re.IGNORECASE)
        candidates = [m for m in matches if "retrocession" in m.lower()]

    if not candidates:
        raise RuntimeError("❌ Impossible de trouver le fichier Excel de rétrocession sur la page ANSM.")

    return urljoin(ANSM_RETRO_PAGE, candidates[0])


def download_ansm_excel(dest_path: str) -> str:
    url = find_ansm_retro_excel_url()
    ensure_parent_dir(dest_path)
    resp = requests.get(url, timeout=180, headers={"User-Agent": "Mozilla/5.0"})
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
# BDPM parsers
# ==================================================

def parse_cis_records(filepath: str) -> Dict[str, Dict[str, str]]:
    """Return mapping: cis -> base fields"""
    text = read_text_with_fallback(filepath)
    out: Dict[str, Dict[str, str]] = {}

    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        cis = parts[0].strip()
        if not cis:
            continue

        out[cis] = {
            FIELD_CODE_CIS: cis,
            FIELD_SPECIALITE: parts[1].strip(),
            FIELD_FORME: parts[2].strip(),
            FIELD_VOIE: parts[3].strip(),
            FIELD_LABO: parts[-2].strip(),
            FIELD_RCP_LINK: build_rcp_link(cis),
        }

    return out


def parse_cpd_map(filepath: str) -> Dict[str, str]:
    text = read_text_with_fallback(filepath)
    m: Dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        cis = parts[0].strip()
        cpd = parts[1].strip()
        if cis:
            m[cis] = cpd
    return m


def parse_cis_cip_maps(filepath: str) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
    text = read_text_with_fallback(filepath)
    agrement_map: Dict[str, str] = {}
    cip_map: Dict[str, Set[str]] = {}

    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 8:
            continue
        cis = parts[0].strip()
        if not cis:
            continue
        cip13 = parts[6].strip()
        agrement = parts[7].strip()

        if cip13:
            cip_map.setdefault(cis, set()).add(cip13)

        if cis not in agrement_map:
            agrement_map[cis] = agrement
        else:
            if agrement_map[cis].lower() != "oui" and agrement.lower() == "oui":
                agrement_map[cis] = "oui"

    return agrement_map, cip_map


# ==================================================
# Airtable robust client
# ==================================================

class AirtableClient:
    def __init__(self, token: str, base_id: str, table_name: str):
        self.base_url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        })

    def _request(self, method: str, url: str, **kwargs):
        last_err = None
        for attempt in range(AIRTABLE_MAX_RETRIES):
            try:
                resp = self.session.request(method, url, timeout=60, **kwargs)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after else min(20.0, (2 ** attempt) * 0.7)
                    time.sleep(sleep_s)
                    last_err = RuntimeError(f"Airtable temporary error {resp.status_code}: {resp.text[:300]}")
                    continue

                if resp.status_code >= 400:
                    try:
                        payload = resp.json()
                    except Exception:
                        payload = resp.text
                    raise RuntimeError(f"Airtable error {resp.status_code}: {payload}")

                return resp
            except Exception as e:
                last_err = e
                time.sleep(min(10.0, (2 ** attempt) * 0.5))

        raise RuntimeError(f"❌ Airtable request failed after retries: {last_err}")

    def fetch_all_records_minimal(self) -> Dict[str, Dict]:
        """
        Return mapping cis -> {id, fields}
        Only needs Code cis and current Rétrocession/Link if present.
        """
        out: Dict[str, Dict] = {}
        offset = None

        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset

            r = self._request("GET", self.base_url, params=params)
            data = r.json()
            recs = data.get("records", [])
            for rec in recs:
                rid = rec.get("id")
                fields = rec.get("fields", {}) or {}
                cis = str(fields.get(FIELD_CODE_CIS, "")).strip()
                if cis and rid:
                    out[cis] = {"id": rid, "fields": fields}

            offset = data.get("offset")
            if not offset:
                break
            time.sleep(AIRTABLE_SLEEP)

        return out

    def delete_records_by_ids(self, record_ids: List[str]):
        for i in range(0, len(record_ids), 10):
            batch = record_ids[i:i + 10]
            params = [("records[]", rid) for rid in batch]
            self._request("DELETE", self.base_url, params=params)
            time.sleep(AIRTABLE_SLEEP)

    def create_records(self, records_fields: List[Dict]) -> Dict[str, str]:
        payload = {"records": [{"fields": compact_fields(f)} for f in records_fields]}
        r = self._request("POST", self.base_url, data=json.dumps(payload))
        data = r.json()
        mapping: Dict[str, str] = {}
        for rec in data.get("records", []):
            rid = rec.get("id")
            fields = rec.get("fields", {}) or {}
            cis = str(fields.get(FIELD_CODE_CIS, "")).strip()
            if cis and rid:
                mapping[cis] = rid
        time.sleep(AIRTABLE_SLEEP)
        return mapping

    def update_records(self, updates: List[Tuple[str, Dict]]):
        payload = {"records": [{"id": rid, "fields": compact_fields(fields)} for rid, fields in updates]}
        self._request("PATCH", self.base_url, data=json.dumps(payload))
        time.sleep(AIRTABLE_SLEEP)


# ==================================================
# RCP scraper (STOP if any inaccessible)
# ==================================================

class RCPScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        self.cache: Dict[str, bool] = {}

    def has_hospital_only_mention(self, rcp_url: str) -> bool:
        if not rcp_url:
            raise RuntimeError("RCP URL vide")

        base_url = rcp_url.split("#")[0]
        if base_url in self.cache:
            return self.cache[base_url]

        last_err = None
        for attempt in range(RCP_MAX_RETRIES):
            try:
                resp = self.session.get(base_url, timeout=RCP_TIMEOUT)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                txt = normalize_text(soup.get_text(" ", strip=True))
                found = any(re.search(p, txt) for p in HOSP_PATTERNS)
                self.cache[base_url] = found
                time.sleep(RCP_SLEEP)
                return found
            except Exception as e:
                last_err = e
                time.sleep(min(15.0, (2 ** attempt) * 0.8))

        raise RuntimeError(f"❌ RCP inaccessible après retries: {base_url} | {last_err}")


# ==================================================
# MAIN: INVENTORY SYNC
# ==================================================

def main():
    require_env()

    # 1) Download ALL files first - if any fails: STOP, no Airtable changes
    print("🔎 Étape 1/4 — Téléchargement de tous les fichiers (avant toute modification Airtable)…")
    try:
        download_file(CIS_URL, DOWNLOAD_CIS_PATH)
        print(f"✅ OK: {DOWNLOAD_CIS_PATH}")

        download_file(CIS_CIP_URL, DOWNLOAD_CIS_CIP_PATH)
        print(f"✅ OK: {DOWNLOAD_CIS_CIP_PATH}")

        download_file(CIS_CPD_URL, DOWNLOAD_CIS_CPD_PATH)
        print(f"✅ OK: {DOWNLOAD_CIS_CPD_PATH}")

        ansm_url = download_ansm_excel(DOWNLOAD_ANSM_RETRO_PATH)
        print(f"✅ OK: {DOWNLOAD_ANSM_RETRO_PATH}")
        print(f"🔗 Lien ANSM détecté: {ansm_url}")

    except Exception as e:
        raise SystemExit(
            "❌ ÉCHEC téléchargement. Mise à jour stoppée. Airtable NON modifiée.\n"
            f"Détail: {e}"
        )

    # 2) Parse
    print("🔎 Étape 2/4 — Parsing…")
    try:
        bdpm_cis_map = parse_cis_records(DOWNLOAD_CIS_PATH)            # cis -> base fields
        cpd_map = parse_cpd_map(DOWNLOAD_CIS_CPD_PATH)                 # cis -> CPD
        agrement_map, cip_map = parse_cis_cip_maps(DOWNLOAD_CIS_CIP_PATH)
        retro_set = load_ansm_retro_cis_set(DOWNLOAD_ANSM_RETRO_PATH)  # cis in retro list
    except Exception as e:
        raise SystemExit(
            "❌ ÉCHEC parsing. Mise à jour stoppée. Airtable NON modifiée.\n"
            f"Détail: {e}"
        )

    bdpm_set = set(bdpm_cis_map.keys())
    print(f"📄 BDPM CIS: {len(bdpm_set)}")
    print(f"🏥 ANSM rétro CIS: {len(retro_set)}")

    # 3) Fetch Airtable inventory
    print("🔎 Étape 3/4 — Inventaire Airtable…")
    airtable = AirtableClient(AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_CIS_TABLE_NAME)
    rcp = RCPScraper()

    airtable_map = airtable.fetch_all_records_minimal()  # cis -> {id, fields}
    airtable_set = set(airtable_map.keys())
    print(f"📌 Airtable CIS: {len(airtable_set)}")

    # Compute diffs
    to_delete_cis = sorted(list(airtable_set - bdpm_set))
    to_add_cis = sorted(list(bdpm_set - airtable_set))
    to_keep_cis = sorted(list(bdpm_set & airtable_set))

    print(f"🗑️ À supprimer (présents Airtable mais absents BDPM): {len(to_delete_cis)}")
    print(f"➕ À ajouter (présents BDPM mais absents Airtable): {len(to_add_cis)}")
    print(f"🔁 À conserver / maj: {len(to_keep_cis)}")

    # 4) Apply changes (delete/add/update)
    print("🔎 Étape 4/4 — Synchronisation + enrichissement (STOP si un RCP est inaccessible)…")

    # 4a) Delete records that are no longer in BDPM
    if to_delete_cis:
        ids_to_delete = [airtable_map[c]["id"] for c in to_delete_cis if c in airtable_map]
        print(f"🧹 Suppression Airtable: {len(ids_to_delete)} lignes…")
        airtable.delete_records_by_ids(ids_to_delete)
        print("✅ Suppressions terminées")

    # refresh local map after deletes
    for c in to_delete_cis:
        airtable_map.pop(c, None)

    # 4b) Create missing records from BDPM
    created_ids: Dict[str, str] = {}
    if to_add_cis:
        print("✍️ Création des lignes manquantes…")
        for i in range(0, len(to_add_cis), AIRTABLE_BATCH_SIZE):
            batch_cis = to_add_cis[i:i + AIRTABLE_BATCH_SIZE]
            batch_records = [bdpm_cis_map[c] for c in batch_cis]
            created = airtable.create_records(batch_records)
            created_ids.update(created)
            if (i + AIRTABLE_BATCH_SIZE) % 500 == 0:
                print(f"➡️ Créées: {min(i + AIRTABLE_BATCH_SIZE, len(to_add_cis))}/{len(to_add_cis)}")
        print(f"✅ Création terminée: {len(created_ids)} lignes")

        # inject created into airtable_map
        for cis, rid in created_ids.items():
            airtable_map[cis] = {"id": rid, "fields": {}}

    # 4c) Update/enrich ALL CIS in BDPM (existing + newly created)
    all_cis_for_update = to_keep_cis + to_add_cis
    total = len(all_cis_for_update)
    print(f"🔁 Mise à jour/enrichissement sur {total} CIS…")

    updates: List[Tuple[str, Dict]] = []
    done = 0

    for cis in all_cis_for_update:
        rec = airtable_map.get(cis)
        if not rec:
            continue
        rid = rec["id"]

        fields: Dict = {}

        # Always sync base descriptive fields from BDPM (so Airtable stays aligned)
        base_fields = bdpm_cis_map.get(cis, {})
        # include only known fields
        for k in (FIELD_CODE_CIS, FIELD_SPECIALITE, FIELD_FORME, FIELD_VOIE, FIELD_LABO, FIELD_RCP_LINK):
            if k in base_fields:
                fields[k] = base_fields[k]

        # Enrichment: CPD, agrement, CIP13
        if cis in cpd_map:
            fields[FIELD_CPD] = cpd_map[cis]

        if cis in agrement_map and agrement_map[cis]:
            fields[FIELD_AGREMENT] = agrement_map[cis]

        if cis in cip_map and cip_map[cis]:
            fields[FIELD_CIP13] = ";".join(sorted(cip_map[cis]))

        # Decision: ANSM retro > else RCP mention
        if cis in retro_set:
            fields[FIELD_RETRO] = LABEL_RETRO
        else:
            rcp_url = base_fields.get(FIELD_RCP_LINK) or build_rcp_link(cis)
            hosp_only = rcp.has_hospital_only_mention(rcp_url)  # STOP if any inaccessible
            fields[FIELD_RETRO] = LABEL_RH if hosp_only else LABEL_CITY

        updates.append((rid, fields))
        done += 1

        if len(updates) >= AIRTABLE_BATCH_SIZE:
            airtable.update_records(updates)
            updates.clear()

        if done % 250 == 0:
            print(f"✅ Maj/enrichies: {done}/{total}")

    if updates:
        airtable.update_records(updates)

    print("🎉 Synchronisation terminée avec succès (inventaire + enrichissement complet).")


if __name__ == "__main__":
    main()
