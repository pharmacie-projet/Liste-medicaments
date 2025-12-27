import os
import re
import time
import json
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup


# ==================================================
# ENV
# ==================================================
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

# Sources BDPM
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

# Local data
DATA_DIR = os.getenv("DATA_DIR", "data").strip()
CIS_PATH = os.path.join(DATA_DIR, "CIS_bdpm.txt")
CIS_CIP_PATH = os.path.join(DATA_DIR, "CIS_CIP_bdpm.txt")
CIS_CPD_PATH = os.path.join(DATA_DIR, "CIS_CPD_bdpm.txt")
ANSM_XLS_PATH = os.path.join(DATA_DIR, "ANSM_retrocession.xlsx")

# Airtable fields (must match your columns exactly)
FIELD_CODE_CIS = "Code cis"
FIELD_SPECIALITE = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE = "Voie d'administration"
FIELD_LABO = "Laboratoire"
FIELD_RCP_LINK = "Lien vers RCP"
FIELD_RETRO = "Rétrocession"

# Labels for FIELD_RETRO
LABEL_CITY = "Disponible en pharmacie de ville"
LABEL_RETRO = "Disponible en rétrocession hospitalière"
LABEL_HOSP = "Réservé à l'usage hospitalier"
LABEL_UNKNOWN = "Pas d'informations mentionnées"

# Airtable pacing / retries
AIRTABLE_BATCH_SIZE = 10
AIRTABLE_SLEEP = float(os.getenv("AIRTABLE_SLEEP", "0.25"))
AIRTABLE_MAX_RETRIES = int(os.getenv("AIRTABLE_MAX_RETRIES", "8"))

# RCP scraping
STOP_ON_RCP_ERROR = os.getenv("STOP_ON_RCP_ERROR", "true").lower() in ("1", "true", "yes", "y")
RCP_TIMEOUT = int(os.getenv("RCP_TIMEOUT", "45"))
RCP_MAX_RETRIES = int(os.getenv("RCP_MAX_RETRIES", "4"))
RCP_SLEEP = float(os.getenv("RCP_SLEEP", "0.40"))

# Patterns
HOSP_PATTERNS = [
    r"usage\s+hospitalier",
    r"r[ée]serv[ée]\s+[àa]\s+l[’']usage\s+hospitalier",
    r"m[ée]dicament\s+r[ée]serv[ée]\s+[àa]\s+l[’']usage\s+hospitalier",
]
HOMEOPATHY_PATTERN = r"hom[éee]opathi"


# ==================================================
# UTILS
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
        raise SystemExit("❌ Variables manquantes : " + ", ".join(missing))


def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)


def download_file(url: str, dest_path: str, timeout: int = 180):
    ensure_dir(dest_path)
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


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def compact_fields(fields: Dict) -> Dict:
    out = {}
    for k, v in fields.items():
        v2 = clean(v)
        if v2 is not None:
            out[k] = v2
    return out


def build_rcp_link(code_cis: str) -> str:
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{code_cis}/extrait#tab-rcp"


# ==================================================
# ANSM retro list (dynamic excel link)
# ==================================================
def find_ansm_excel_url() -> str:
    r = requests.get(ANSM_RETRO_PAGE, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if re.search(r"\.xls[x]?$", href, flags=re.IGNORECASE):
            candidates.append(href)

    if not candidates:
        # fallback raw scan
        matches = re.findall(r'href="([^"]+\.xls[x]?)"', r.text, flags=re.IGNORECASE)
        candidates = matches

    if not candidates:
        raise RuntimeError("❌ Impossible de trouver le fichier Excel de rétrocession sur la page ANSM.")

    return urljoin(ANSM_RETRO_PAGE, candidates[0])


def download_ansm_excel(dest_path: str) -> str:
    url = find_ansm_excel_url()
    ensure_dir(dest_path)
    resp = requests.get(url, timeout=180, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(resp.content)
    return url


def load_retro_cis_set(xls_path: str) -> Set[str]:
    df = pd.read_excel(xls_path, sheet_name=0, header=0, dtype=str)
    if df.shape[1] < 3:
        raise RuntimeError("❌ Fichier ANSM: moins de 3 colonnes (la 3e doit contenir le CIS).")
    cis = df.iloc[:, 2].dropna().astype(str).str.strip()
    cis = cis[cis.str.len() > 0]
    return set(cis.tolist())


# ==================================================
# BDPM parsers
# ==================================================
def parse_cis_base(filepath: str) -> Dict[str, Dict]:
    """
    CIS_bdpm.txt columns: CIS, Specialité, Forme, Voie, ... , Laboratoire, ...
    User mapping: 1=Code CIS, 2=Spécialité, 3=Forme, 4=Voie, avant-dernière=Laboratoire
    """
    txt = read_text_with_fallback(filepath)
    out: Dict[str, Dict] = {}
    for line in txt.splitlines():
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


def build_reimbursed_cis_set_from_cis_cip(filepath: str) -> Set[str]:
    """
    Detect reimbursement presence by percent token in CIS_CIP lines.
    """
    txt = read_text_with_fallback(filepath)
    reimbursed: Set[str] = set()
    pct_re = re.compile(r"\b\d{1,3}\s*%")
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        cis = parts[0].strip()
        if cis and pct_re.search(line):
            reimbursed.add(cis)
    return reimbursed


def build_hospital_cis_set_from_cpd(filepath: str) -> Set[str]:
    txt = read_text_with_fallback(filepath)
    hosp: Set[str] = set()
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        cis = parts[0].strip()
        cond = norm(parts[1])
        if cis and any(re.search(p, cond) for p in HOSP_PATTERNS):
            hosp.add(cis)
    return hosp


# ==================================================
# Airtable client (robust)
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
                    last_err = RuntimeError(f"Airtable temp {resp.status_code}: {resp.text[:250]}")
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
        raise RuntimeError(f"❌ Airtable request failed: {last_err}")

    def fetch_all_records_minimal(self) -> Dict[str, Dict]:
        """
        Return mapping cis -> {id, fields}
        """
        out: Dict[str, Dict] = {}
        offset = None
        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset
            r = self._request("GET", self.base_url, params=params)
            data = r.json()
            for rec in data.get("records", []):
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

    def delete_records_by_ids(self, ids: List[str]):
        for i in range(0, len(ids), 10):
            batch = ids[i:i+10]
            params = [("records[]", rid) for rid in batch]
            self._request("DELETE", self.base_url, params=params)
            time.sleep(AIRTABLE_SLEEP)

    def create_records(self, fields_list: List[Dict]):
        payload = {"records": [{"fields": compact_fields(f)} for f in fields_list]}
        self._request("POST", self.base_url, data=json.dumps(payload))
        time.sleep(AIRTABLE_SLEEP)

    def update_records(self, updates: List[Tuple[str, Dict]]):
        payload = {"records": [{"id": rid, "fields": compact_fields(fields)} for rid, fields in updates]}
        self._request("PATCH", self.base_url, data=json.dumps(payload))
        time.sleep(AIRTABLE_SLEEP)


# ==================================================
# RCP checker (homeopathy + hospital flags)
# ==================================================
class RCPChecker:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        self.cache: Dict[str, Tuple[bool, bool]] = {}

    def flags(self, url: str) -> Tuple[bool, bool]:
        base_url = (url or "").split("#")[0].strip()
        if not base_url:
            raise RuntimeError("Lien RCP vide")

        if base_url in self.cache:
            return self.cache[base_url]

        last_err = None
        for attempt in range(RCP_MAX_RETRIES):
            try:
                resp = self.session.get(base_url, timeout=RCP_TIMEOUT)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                text = norm(soup.get_text(" ", strip=True))

                has_homeo = bool(re.search(HOMEOPATHY_PATTERN, text))
                has_hosp = any(re.search(p, text) for p in HOSP_PATTERNS)

                self.cache[base_url] = (has_homeo, has_hosp)
                time.sleep(RCP_SLEEP)
                return has_homeo, has_hosp
            except Exception as e:
                last_err = e
                time.sleep(min(15.0, (2 ** attempt) * 0.8))

        raise RuntimeError(f"RCP inaccessible: {base_url} | {last_err}")


# ==================================================
# Decision rules (exact order)
# ==================================================
def decide_status(
    cis: str,
    rcp_link: str,
    reimbursed_set: Set[str],
    retro_set: Set[str],
    hosp_cpd_set: Set[str],
    rcp_checker: RCPChecker,
) -> str:
    # 2) ANSM retrocession has priority
    if cis in retro_set:
        return LABEL_RETRO

    # RCP flags (we may need them for homeopathy / hospital)
    has_homeo = False
    has_hosp = False
    try:
        has_homeo, has_hosp = rcp_checker.flags(rcp_link)
    except Exception:
        if STOP_ON_RCP_ERROR:
            raise

    # Homeopathy forces "ville"
    if has_homeo:
        return LABEL_CITY

    # 1) Reimbursement => city
    if cis in reimbursed_set:
        return LABEL_CITY

    # 3) hospital (explicit CPD or RCP)
    if cis in hosp_cpd_set or has_hosp:
        return LABEL_HOSP

    # 4) otherwise unknown
    return LABEL_UNKNOWN


# ==================================================
# MAIN
# ==================================================
def main():
    require_env()
    os.makedirs(DATA_DIR, exist_ok=True)

    # 0) Download ALL first (so we don't touch Airtable if a file is missing)
    print("1) Téléchargements…")
    try:
        download_file(CIS_URL, CIS_PATH)
        download_file(CIS_CIP_URL, CIS_CIP_PATH)
        download_file(CIS_CPD_URL, CIS_CPD_PATH)
        ansm_url = download_ansm_excel(ANSM_XLS_PATH)
        print(f"   ✅ ANSM Excel: {ansm_url}")
    except Exception as e:
        raise SystemExit(f"❌ Téléchargement échoué -> STOP sans modifier Airtable: {e}")

    # 1) Parse BDPM/ANSM
    print("2) Parsing…")
    cis_base = parse_cis_base(CIS_PATH)
    bdpm_set = set(cis_base.keys())

    reimbursed_set = build_reimbursed_cis_set_from_cis_cip(CIS_CIP_PATH)
    hosp_cpd_set = build_hospital_cis_set_from_cpd(CIS_CPD_PATH)
    retro_set = load_retro_cis_set(ANSM_XLS_PATH)

    print(f"   📄 BDPM CIS: {len(bdpm_set)}")
    print(f"   💶 CIS avec taux remboursement (ville): {len(reimbursed_set)}")
    print(f"   🏥 CIS usage hospitalier via CPD: {len(hosp_cpd_set)}")
    print(f"   🔁 CIS ANSM rétrocession: {len(retro_set)}")

    # 2) Airtable inventory
    print("3) Inventaire Airtable…")
    at = AirtableClient(AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_CIS_TABLE_NAME)
    rcp = RCPChecker()

    airtable_map = at.fetch_all_records_minimal()  # cis -> {id, fields}
    airtable_set = set(airtable_map.keys())
    print(f"   📌 Airtable CIS: {len(airtable_set)}")

    to_delete = sorted(list(airtable_set - bdpm_set))
    to_add = sorted(list(bdpm_set - airtable_set))
    to_keep = sorted(list(bdpm_set & airtable_set))

    print(f"   🗑️ À supprimer: {len(to_delete)}")
    print(f"   ➕ À créer: {len(to_add)}")
    print(f"   🔁 À conserver: {len(to_keep)}")

    # 3) Apply deletes (only if needed)
    if to_delete:
        ids = [airtable_map[c]["id"] for c in to_delete if c in airtable_map]
        print(f"   🧹 Suppression {len(ids)} lignes…")
        at.delete_records_by_ids(ids)

    # 4) Apply creates (THIS is what you were missing)
    if to_add:
        print(f"   ✍️ Création {len(to_add)} lignes…")
        for i in range(0, len(to_add), AIRTABLE_BATCH_SIZE):
            batch = to_add[i:i+AIRTABLE_BATCH_SIZE]
            records = [cis_base[c] for c in batch]
            at.create_records(records)
            if (i + AIRTABLE_BATCH_SIZE) % 500 == 0:
                print(f"   …créées: {min(i + AIRTABLE_BATCH_SIZE, len(to_add))}/{len(to_add)}")

    # refresh inventory map (simpler: re-fetch after create/delete)
    airtable_map = at.fetch_all_records_minimal()

    # 5) Enrich / update retro for all current BDPM cis
    print("4) Enrichissement (Rétrocession)…")
    updates: List[Tuple[str, Dict]] = []
    done = 0
    total = len(bdpm_set)

    for cis in sorted(list(bdpm_set)):
        rec = airtable_map.get(cis)
        if not rec:
            continue

        rid = rec["id"]
        base_fields = cis_base[cis]
        rcp_link = base_fields.get(FIELD_RCP_LINK) or build_rcp_link(cis)

        status = decide_status(cis, rcp_link, reimbursed_set, retro_set, hosp_cpd_set, rcp)

        updates.append((rid, {FIELD_RETRO: status}))

        if len(updates) >= AIRTABLE_BATCH_SIZE:
            at.update_records(updates)
            updates.clear()

        done += 1
        if done % 500 == 0:
            print(f"   …{done}/{total}")

    if updates:
        at.update_records(updates)

    print(f"✅ Terminé. CIS traités: {done}/{total}")


if __name__ == "__main__":
    main()
