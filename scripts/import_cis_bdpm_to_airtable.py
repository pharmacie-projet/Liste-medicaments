import os
import re
import time
import json
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup


# =========================================================
# ENV
# =========================================================
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

# Airtable field names (must match your columns)
FIELD_CODE_CIS = "Code cis"
FIELD_RCP_LINK = "Lien vers RCP"
FIELD_RETRO = "Rétrocession"

# Sources
CIS_CIP_URL = os.getenv(
    "CIS_CIP_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
).strip()

CIS_CPD_URL = os.getenv(
    "CIS_CPD_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"
).strip()

ANSM_RETRO_PAGE = os.getenv(
    "ANSM_RETRO_PAGE",
    "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"
).strip()

# Local paths
DATA_DIR = os.getenv("DATA_DIR", "data").strip()
CIS_CIP_PATH = os.path.join(DATA_DIR, "CIS_CIP_bdpm.txt")
CIS_CPD_PATH = os.path.join(DATA_DIR, "CIS_CPD_bdpm.txt")
ANSM_XLS_PATH = os.path.join(DATA_DIR, "ANSM_retrocession.xlsx")

# Labels
LABEL_CITY = "Disponible en pharmacie de ville"
LABEL_RETRO = "Disponible en rétrocession hospitalière"
LABEL_HOSP = "Réservé à l'usage hospitalier"
LABEL_UNKNOWN = "Pas d'informations mentionnées"

# Airtable pacing
AIRTABLE_BATCH_SIZE = 10
AIRTABLE_SLEEP = float(os.getenv("AIRTABLE_SLEEP", "0.25"))
AIRTABLE_MAX_RETRIES = int(os.getenv("AIRTABLE_MAX_RETRIES", "8"))

# RCP scraping behavior
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

# New: homeopathy detection (homéopathi / homeopathi)
HOMEOPATHY_PATTERN = r"hom[éee]opathi"


# =========================================================
# UTILS
# =========================================================
def require_env():
    missing = []
    if not AIRTABLE_API_TOKEN:
        missing.append("AIRTABLE_API_TOKEN")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if not AIRTABLE_CIS_TABLE_NAME:
        missing.append("AIRTABLE_CIS_TABLE_NAME")
    if missing:
        raise SystemExit("❌ Variables manquantes: " + ", ".join(missing))


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


# =========================================================
# ANSM retro (dynamic excel link)
# =========================================================
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
        raise RuntimeError("❌ Impossible de trouver un lien .xls/.xlsx sur la page ANSM.")

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


# =========================================================
# BDPM parsing
# =========================================================
def build_reimbursed_cis_set_from_cis_cip(filepath: str) -> Set[str]:
    """
    Mark CIS as "ville" if we detect any percent token in its CIS_CIP lines.
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


# =========================================================
# Airtable client (robust)
# =========================================================
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

    def fetch_all_records(self) -> List[Dict]:
        out = []
        offset = None
        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset
            r = self._request("GET", self.base_url, params=params)
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(AIRTABLE_SLEEP)
        return out

    def patch_records(self, updates: List[Tuple[str, Dict]]):
        if not updates:
            return
        payload = {"records": [{"id": rid, "fields": compact_fields(fields)} for rid, fields in updates]}
        self._request("PATCH", self.base_url, data=json.dumps(payload))
        time.sleep(AIRTABLE_SLEEP)


# =========================================================
# RCP checker (returns BOTH flags: homeopathy & hospital)
# =========================================================
class RCPChecker:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        # cache url -> (has_homeopathy, has_hospital)
        self.cache: Dict[str, Tuple[bool, bool]] = {}

    def rcp_flags(self, url: str) -> Tuple[bool, bool]:
        if not url:
            raise RuntimeError("Lien RCP vide")
        base_url = url.split("#")[0]
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


# =========================================================
# DECISION LOGIC
# =========================================================
def decide_status(
    cis: str,
    rcp_link: str,
    reimbursed_set: Set[str],
    retro_set: Set[str],
    hosp_cpd_set: Set[str],
    rcp_checker: RCPChecker,
) -> str:
    # 1) taux de remboursement => ville
    base_status = LABEL_CITY if cis in reimbursed_set else LABEL_UNKNOWN

    # 2) liste ANSM rétrocession => override
    if cis in retro_set:
        return LABEL_RETRO

    # 3) autres: vérifier homéopathie / usage hospitalier (CPD ou RCP)
    # CPD (explicite)
    if cis in hosp_cpd_set:
        return LABEL_HOSP

    # RCP (si nécessaire)
    try:
        has_homeo, has_hosp = rcp_checker.rcp_flags(rcp_link)
    except Exception:
        if STOP_ON_RCP_ERROR:
            raise
        return base_status if base_status != LABEL_UNKNOWN else LABEL_UNKNOWN

    # NEW RULE: homeopathy forces "ville"
    if has_homeo:
        return LABEL_CITY

    if has_hosp:
        return LABEL_HOSP

    return base_status


# =========================================================
# MAIN
# =========================================================
def main():
    require_env()
    os.makedirs(DATA_DIR, exist_ok=True)

    print("1) Téléchargements…")
    download_file(CIS_CIP_URL, CIS_CIP_PATH)
    download_file(CIS_CPD_URL, CIS_CPD_PATH)
    ansm_url = download_ansm_excel(ANSM_XLS_PATH)
    print(f"   ✅ ANSM Excel: {ansm_url}")

    print("2) Parsing…")
    reimbursed_set = build_reimbursed_cis_set_from_cis_cip(CIS_CIP_PATH)  # :contentReference[oaicite:0]{index=0}
    hosp_cpd_set = build_hospital_cis_set_from_cpd(CIS_CPD_PATH)
    retro_set = load_retro_cis_set(ANSM_XLS_PATH)

    print(f"   📌 CIS avec taux remboursement (ville): {len(reimbursed_set)}")
    print(f"   🏥 CIS marqués usage hospitalier via CPD: {len(hosp_cpd_set)}")
    print(f"   🔁 CIS ANSM rétrocession: {len(retro_set)}")

    print("3) Lecture Airtable + MAJ…")
    at = AirtableClient(AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_CIS_TABLE_NAME)
    rcp = RCPChecker()

    records = at.fetch_all_records()
    updates: List[Tuple[str, Dict]] = []
    total = 0

    for rec in records:
        rid = rec.get("id")
        fields = rec.get("fields", {}) or {}
        cis = str(fields.get(FIELD_CODE_CIS, "")).strip()
        if not rid or not cis:
            continue

        rcp_link = str(fields.get(FIELD_RCP_LINK, "")).strip()
        if not rcp_link:
            rcp_link = f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"

        status = decide_status(cis, rcp_link, reimbursed_set, retro_set, hosp_cpd_set, rcp)

        updates.append((rid, {FIELD_RETRO: status}))
        total += 1

        if len(updates) >= AIRTABLE_BATCH_SIZE:
            at.patch_records(updates)
            updates.clear()

        if total % 500 == 0:
            print(f"   …{total} lignes traitées")

    if updates:
        at.patch_records(updates)

    print(f"✅ Terminé. Lignes mises à jour: {total}")


if __name__ == "__main__":
    main()
