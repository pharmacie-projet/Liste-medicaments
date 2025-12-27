#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import random
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIG
# ============================================================

BDPM_CIS_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
BDPM_CIS_CIP_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"

ANSM_RETRO_PAGE = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

AIRTABLE_MIN_DELAY_S = 0.25
AIRTABLE_BATCH_SIZE = 10

REQUEST_TIMEOUT = 30
MAX_RETRIES = 4

# ============================================================
# LOGGING
# ============================================================

def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def die(msg: str, code: int = 1):
    print(f"[{_ts()}] ❌ {msg}", flush=True)
    raise SystemExit(code)

def info(msg: str):
    print(f"[{_ts()}] ℹ️ {msg}", flush=True)

def ok(msg: str):
    print(f"[{_ts()}] ✅ {msg}", flush=True)

def warn(msg: str):
    print(f"[{_ts()}] ⚠️ {msg}", flush=True)

def sleep_throttle():
    time.sleep(AIRTABLE_MIN_DELAY_S)

def retry_sleep(attempt: int):
    time.sleep(min(8, 0.6 * (2 ** (attempt - 1))) + random.random() * 0.2)

# ============================================================
# STRING UTIL
# ============================================================

def safe_text(s: str) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = s.replace("\uFFFD", "")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s.strip()

def normalize_ws_keep_lines(s: str) -> str:
    s = safe_text(s)
    lines = []
    for line in s.split("\n"):
        line = re.sub(r"[ \t]{2,}", " ", line).strip()
        lines.append(line)
    out = []
    empty = 0
    for line in lines:
        if line == "":
            empty += 1
            if empty <= 1:
                out.append("")
        else:
            empty = 0
            out.append(line)
    return "\n".join(out).strip()

def chunked(lst: List, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def _bs_parser():
    try:
        import lxml  # noqa: F401
        return "lxml"
    except Exception:
        return "html.parser"

# ============================================================
# DOWNLOAD
# ============================================================

def http_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=timeout)
            return r
        except Exception as e:
            last_err = e
            retry_sleep(attempt)
    raise RuntimeError(f"GET failed: {url} / {last_err}")

def download_text(url: str, encoding: str = "latin-1") -> str:
    r = http_get(url)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    r.encoding = encoding
    return r.text

def download_bytes(url: str) -> bytes:
    r = http_get(url)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.content

# ============================================================
# ANSM retrocession link discovery + parse
# ============================================================

def find_ansm_retro_excel_link() -> str:
    r = http_get(ANSM_RETRO_PAGE)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} {ANSM_RETRO_PAGE}")
    soup = BeautifulSoup(r.text, _bs_parser())

    links = []
    for a in soup.find_all("a", href=True):
        href = (a["href"] or "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = "https://ansm.sante.fr" + href
        low = href.lower()
        if ("ansm.sante.fr/uploads/" in low) and ("retrocession" in low) and re.search(r"\.xlsx?$|\.xls$", low):
            links.append(href)

    if not links:
        raise RuntimeError("Lien Excel ANSM (rétrocession) introuvable sur la page")

    def score(u: str) -> Tuple[int, str]:
        m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", u)
        if m:
            return (1, f"{m.group(1)}{m.group(2)}{m.group(3)}")
        return (0, u)

    links.sort(key=score, reverse=True)
    return links[0]

def parse_ansm_retrocession_cis(excel_bytes: bytes, url_hint: str = "") -> Set[str]:
    cis_set: Set[str] = set()

    ext = ""
    if url_hint:
        ext = url_hint.lower().split("?")[0].split("#")[0]
        ext = os.path.splitext(ext)[1].lower()

    import io
    if ext == ".xlsx":
        from openpyxl import load_workbook
        wb = load_workbook(io.BytesIO(excel_bytes), read_only=True, data_only=True)
        ws = wb.worksheets[0]
        for row in ws.iter_rows(values_only=True):
            if not row or len(row) < 3:
                continue
            v = row[2]
            if v is None:
                continue
            v = re.sub(r"\D", "", str(v))
            if len(v) == 8:
                cis_set.add(v)
        return cis_set

    try:
        import xlrd  # type: ignore
    except Exception:
        raise RuntimeError("Le fichier ANSM est en .xls mais 'xlrd' n'est pas installé. pip install xlrd==1.2.0")

    book = xlrd.open_workbook(file_contents=excel_bytes)
    sheet = book.sheet_by_index(0)
    for rx in range(sheet.nrows):
        row = sheet.row_values(rx)
        if len(row) < 3:
            continue
        v = row[2]
        if v is None:
            continue
        v = re.sub(r"\D", "", str(v))
        if len(v) == 8:
            cis_set.add(v)

    return cis_set

# ============================================================
# BDPM PARSE
# ============================================================

@dataclass
class CisRow:
    cis: str
    specialite: str
    forme: str
    voie_admin: str
    titulaire: str

def parse_bdpm_cis(txt: str) -> Dict[str, CisRow]:
    out: Dict[str, CisRow] = {}
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        cis = re.sub(r"\D", "", parts[0].strip())
        if len(cis) != 8:
            continue
        denom = safe_text(parts[1]) if len(parts) > 1 else ""
        forme = safe_text(parts[2]) if len(parts) > 2 else ""
        voie = safe_text(parts[3]) if len(parts) > 3 else ""
        titulaire = safe_text(parts[10]) if len(parts) > 10 else ""
        out[cis] = CisRow(cis=cis, specialite=denom, forme=forme, voie_admin=voie, titulaire=titulaire)
    return out

@dataclass
class CipInfo:
    cip13: str
    has_taux: bool
    agrement_collectivites: str

def looks_like_taux(val: str) -> bool:
    v = (val or "").strip()
    if not v:
        return False
    v2 = v.replace(",", ".").replace("%", "").strip()
    if not re.fullmatch(r"\d{1,3}(\.\d+)?", v2):
        return False
    try:
        x = float(v2)
    except Exception:
        return False
    return x in {0, 15, 30, 35, 65, 100}

def parse_bdpm_cis_cip(txt: str) -> Dict[str, CipInfo]:
    out: Dict[str, CipInfo] = {}
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        cis = re.sub(r"\D", "", parts[0].strip())
        if len(cis) != 8:
            continue

        cip13 = ""
        for p in parts:
            d = re.sub(r"\D", "", p)
            if len(d) == 13:
                cip13 = d
                break

        has_taux = any(looks_like_taux(p) for p in parts)

        agrement = ""
        joined = " ".join(parts).lower()
        if "collectiv" in joined or "agrément" in joined or "agrement" in joined:
            agrement = "Oui"

        if cis not in out:
            out[cis] = CipInfo(cip13=cip13, has_taux=has_taux, agrement_collectivites=agrement)
        else:
            if not out[cis].cip13 and cip13:
                out[cis].cip13 = cip13
            out[cis].has_taux = out[cis].has_taux or has_taux
            if not out[cis].agrement_collectivites and agrement:
                out[cis].agrement_collectivites = agrement

    return out

def normalize_lab_name(titulaire: str) -> str:
    t = titulaire or ""
    t = t.replace(",", " ").replace(";", " ")
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return ""
    t = re.sub(r"\b(SAS|SA|SARL|S\.A\.|S\.A\.S\.|GMBH|LTD|INC|BV|AG|SPA|S\.P\.A\.)\b", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s+", " ", t).strip()
    first = t.split(" ")[0].strip()
    if first.isupper() and len(first) > 2:
        first = first.capitalize()
    if first.lower() in {"laboratoires", "laboratoire"} and len(t.split(" ")) > 1:
        nxt = t.split(" ")[1]
        if nxt.isupper() and len(nxt) > 2:
            nxt = nxt.capitalize()
        return nxt
    return first

# ============================================================
# RULES / PATTERNS
# ============================================================

HOMEOPATHY_PAT = re.compile(r"hom[ée]opath(?:ie|ique)", flags=re.IGNORECASE)

# badge "usage hospitalier" (en haut)
BADGE_USAGE_HOSP_PAT = re.compile(r"\busage\s+hospitalier\b", flags=re.IGNORECASE)

# uniquement dans le sous-bloc "Conditions de prescription et de délivrance" (Autres informations)
RESERVED_HOSP_PAT = re.compile(r"réserv[ée]?\s+à\s+l['’]usage\s+hospitalier", flags=re.IGNORECASE)
USAGE_HOSP_PAT = re.compile(r"\busage\s+hospitalier\b", flags=re.IGNORECASE)

NEGATION_PAT = re.compile(
    r"(?:\bnon\b|\bpas\b|\bjamais\b)\s+(?:réserv[ée]?\s+à\s+l['’]usage\s+hospitalier|\busage\s+hospitalier\b)",
    flags=re.IGNORECASE
)

# Lignes à ignorer dans CPD
GLOSSARY_PAT = re.compile(r"\baller\s+au\s+glossaire\b", flags=re.IGNORECASE)

# ATC
ATC_PAT = re.compile(r"\b[A-Z]\d{2}[A-Z]{2}\d{2}\b")

def clean_cpd_text_keep_useful(text: str) -> str:
    if not text:
        return ""
    lines = [ln.strip() for ln in text.split("\n")]
    kept = []
    for ln in lines:
        if not ln:
            kept.append("")
            continue
        if GLOSSARY_PAT.search(ln):
            continue
        kept.append(ln)
    out = "\n".join(kept)
    out = normalize_ws_keep_lines(out)
    return out

def compute_disponibilite(has_taux_ville: bool, is_ansm_retro: bool, is_homeopathy: bool,
                          reserved_hospital: bool, usage_hospital: bool) -> str:
    ville = bool(has_taux_ville or is_homeopathy)

    if is_ansm_retro and ville:
        return "Disponible en ville et en rétrocession hospitalière"
    if is_ansm_retro and not ville:
        return "Disponible en rétrocession hospitalière"

    if reserved_hospital:
        return "Réservé à l'usage hospitalier"
    if usage_hospital and not ville:
        return "Réservé à l'usage hospitalier"

    if ville:
        return "Disponible en pharmacie de ville"

    return "Pas d'informations mentionnées"

# ============================================================
# RCP SCRAPING: ONLY TARGET ZONES + ATC
# ============================================================

def fetch_html(url: str, timeout: int = 20, max_retries: int = 3) -> str:
    if not url or not url.startswith("http"):
        raise RuntimeError(f"URL invalide: {url}")

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=timeout)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}")
            if not r.text or len(r.text) < 200:
                raise RuntimeError("HTML vide/trop court")
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.0 * attempt)

    raise RuntimeError(f"Page inaccessible: {url}. Détail: {last_err}")

def extract_badge_usage_hospitalier(soup: BeautifulSoup) -> bool:
    for el in soup.find_all(["span", "div", "a", "p", "li"]):
        t = (el.get_text(" ", strip=True) or "")
        if len(t) > 40:
            continue
        if BADGE_USAGE_HOSP_PAT.search(t):
            return True
    return False

def extract_autres_infos_cpd_block(soup: BeautifulSoup) -> str:
    page_text_lines = [ln.strip() for ln in soup.get_text("\n", strip=True).split("\n")]

    autres_pat = re.compile(r"^Autres\s+informations$", re.IGNORECASE)
    cpd_pat = re.compile(r"^Conditions\s+de\s+prescription\s+et\s+de\s+d[ée]livrance\s*:?$", re.IGNORECASE)
    stop_pat = re.compile(
        r"^(Statut\s+de\s+l['’]autorisation|Type\s+de\s+proc[ée]dure|Code\s+CIS|Titulaire\s+de\s+l['’]autorisation)\s*:",
        re.IGNORECASE
    )

    start_autres = None
    for i, ln in enumerate(page_text_lines):
        if autres_pat.match(ln):
            start_autres = i
            break
    if start_autres is None:
        return ""

    start_cpd = None
    for i in range(start_autres, len(page_text_lines)):
        if cpd_pat.match(page_text_lines[i]):
            start_cpd = i
            break
    if start_cpd is None:
        return ""

    collected = []
    for ln in page_text_lines[start_cpd + 1:]:
        if stop_pat.search(ln):
            break
        collected.append(ln)

    bloc = "\n".join(collected).strip()
    bloc = normalize_ws_keep_lines(bloc)
    bloc = clean_cpd_text_keep_useful(bloc)
    return bloc

def extract_atc_code_from_page(soup: BeautifulSoup) -> str:
    """
    Extrait le Code ATC si présent (ex: "code ATC : N02AX02").
    On prend le 1er match.
    """
    text = soup.get_text("\n", strip=True)
    m = ATC_PAT.search(text or "")
    if not m:
        return ""
    return m.group(0).strip()

def analyze_rcp_page(rcp_url: str) -> Tuple[str, bool, bool, bool, str]:
    """
    Retourne:
    - cpd_text (nettoyé, sans "Aller au glossaire")
    - is_homeopathy
    - reserved_hospital (strict, seulement dans CPD)
    - usage_hospital (badge ou CPD)
    - atc_code
    """
    html = fetch_html(rcp_url)
    soup = BeautifulSoup(html, _bs_parser())

    full_text = soup.get_text("\n", strip=True)
    is_homeo = bool(HOMEOPATHY_PAT.search(full_text or ""))

    badge_usage = extract_badge_usage_hospitalier(soup)
    cpd_text = extract_autres_infos_cpd_block(soup)

    zone_text = (cpd_text or "")
    if NEGATION_PAT.search(zone_text):
        reserved = False
        usage = False
    else:
        reserved = bool(RESERVED_HOSP_PAT.search(zone_text))
        usage = bool(USAGE_HOSP_PAT.search(zone_text)) or badge_usage

    atc_code = extract_atc_code_from_page(soup)

    return cpd_text, is_homeo, reserved, usage, atc_code

# ============================================================
# AIRTABLE CLIENT
# ============================================================

class AirtableClient:
    def __init__(self, api_token: str, base_id: str, table_name: str):
        self.api_token = api_token
        self.base_id = base_id
        self.table_name = table_name
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        })

    @property
    def table_url(self) -> str:
        t = urllib.parse.quote(self.table_name, safe="")
        return f"{AIRTABLE_API_BASE}/{self.base_id}/{t}"

    def _request(self, method: str, url: str, **kwargs):
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sleep_throttle()
                r = self.session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                if r.status_code >= 400:
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
                return r
            except Exception as e:
                last_err = e
                retry_sleep(attempt)
        raise RuntimeError(f"Airtable request failed: {method} {url} / {last_err}")

    def list_all_records(self, fields: Optional[List[str]] = None) -> List[dict]:
        out = []
        params = {}
        if fields:
            params["fields[]"] = fields

        offset = None
        while True:
            if offset:
                params["offset"] = offset
            r = self._request("GET", self.table_url, params=params)
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return out

    def create_records(self, records: List[dict]) -> None:
        for batch in chunked(records, AIRTABLE_BATCH_SIZE):
            payload = {"records": batch, "typecast": True}
            self._request("POST", self.table_url, data=json.dumps(payload))

    def update_records(self, records: List[dict]) -> None:
        for batch in chunked(records, AIRTABLE_BATCH_SIZE):
            payload = {"records": batch, "typecast": True}
            self._request("PATCH", self.table_url, data=json.dumps(payload))

    def delete_records(self, record_ids: List[str]) -> None:
        for batch in chunked(record_ids, AIRTABLE_BATCH_SIZE):
            params = [("records[]", rid) for rid in batch]
            self._request("DELETE", self.table_url, params=params)

# ============================================================
# MAIN
# ============================================================

def main():
    api_token = os.getenv("AIRTABLE_API_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table_name = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

    force_refresh = os.getenv("FORCE_REFRESH", "0").strip() == "1"
    max_cis = os.getenv("MAX_CIS_TO_PROCESS", "").strip()
    max_cis = int(max_cis) if max_cis.isdigit() else 0

    if not api_token or not base_id or not table_name:
        die("Variables manquantes: AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME")

    info("Téléchargement BDPM CIS ...")
    cis_txt = download_text(BDPM_CIS_URL, encoding="latin-1")
    ok(f"BDPM CIS OK ({len(cis_txt)} chars)")

    info("Téléchargement BDPM CIS_CIP ...")
    cis_cip_txt = download_text(BDPM_CIS_CIP_URL, encoding="latin-1")
    ok(f"BDPM CIS_CIP OK ({len(cis_cip_txt)} chars)")

    info("Recherche lien Excel ANSM (rétrocession) ...")
    ansm_link = find_ansm_retro_excel_link()
    ok(f"Lien ANSM trouvé: {ansm_link}")

    info("Téléchargement Excel ANSM ...")
    ansm_bytes = download_bytes(ansm_link)
    ok(f"ANSM Excel OK ({len(ansm_bytes)} bytes)")

    cis_map = parse_bdpm_cis(cis_txt)
    cip_map = parse_bdpm_cis_cip(cis_cip_txt)
    ansm_retro_cis = parse_ansm_retrocession_cis(ansm_bytes, url_hint=ansm_link)

    at = AirtableClient(api_token, base_id, table_name)

    needed_fields = [
        "Code cis",
        "Lien vers RCP",
        "CIP 13",
        "Agrément aux collectivités",
        "Disponibilité du traitement",
        "Conditions de prescription et délivrance",
        "Laboratoire",
        "Spécialité",
        "Forme",
        "Voie d'administration",
        "Code ATC",
    ]

    info("Inventaire Airtable ...")
    records = at.list_all_records(fields=needed_fields)
    ok(f"Enregistrements Airtable: {len(records)}")

    airtable_by_cis: Dict[str, dict] = {}
    for rec in records:
        cis = str(rec.get("fields", {}).get("Code cis", "")).strip()
        cis = re.sub(r"\D", "", cis)
        if len(cis) == 8:
            airtable_by_cis[cis] = rec

    bdpm_cis_set = set(cis_map.keys())
    airtable_cis_set = set(airtable_by_cis.keys())

    to_create = sorted(list(bdpm_cis_set - airtable_cis_set))
    to_delete = sorted(list(airtable_cis_set - bdpm_cis_set))

    info(f"À créer: {len(to_create)} | À supprimer: {len(to_delete)} | Dans les 2: {len(bdpm_cis_set & airtable_cis_set)}")

    # CREATE missing
    if to_create:
        info("Création des enregistrements manquants ...")
        new_recs = []
        for cis in to_create:
            row = cis_map[cis]
            cip = cip_map.get(cis)
            labo = normalize_lab_name(row.titulaire)

            fields = {
                "Code cis": cis,
                "Spécialité": safe_text(row.specialite),
                "Forme": safe_text(row.forme),
                "Voie d'administration": safe_text(row.voie_admin),
                "Laboratoire": labo,
                "Lien vers RCP": f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp",
            }
            if cip and cip.cip13:
                fields["CIP 13"] = cip.cip13
            if cip and cip.agrement_collectivites:
                fields["Agrément aux collectivités"] = cip.agrement_collectivites
            new_recs.append({"fields": fields})

        at.create_records(new_recs)
        ok(f"Créés: {len(new_recs)}")

        # refresh
        records = at.list_all_records(fields=needed_fields)
        airtable_by_cis = {}
        for rec in records:
            cis = str(rec.get("fields", {}).get("Code cis", "")).strip()
            cis = re.sub(r"\D", "", cis)
            if len(cis) == 8:
                airtable_by_cis[cis] = rec

    # DELETE
    if to_delete:
        info("Suppression des enregistrements Airtable absents de BDPM ...")
        ids = [airtable_by_cis[c]["id"] for c in to_delete if c in airtable_by_cis]
        if ids:
            at.delete_records(ids)
            ok(f"Supprimés: {len(ids)}")
        # refresh
        records = at.list_all_records(fields=needed_fields)
        airtable_by_cis = {}
        for rec in records:
            cis = str(rec.get("fields", {}).get("Code cis", "")).strip()
            cis = re.sub(r"\D", "", cis)
            if len(cis) == 8:
                airtable_by_cis[cis] = rec

    # ENRICH
    all_cis = sorted(list(set(cis_map.keys()) & set(airtable_by_cis.keys())))
    if max_cis > 0:
        all_cis = all_cis[:max_cis]
        warn(f"MAX_CIS_TO_PROCESS={max_cis} -> {len(all_cis)} CIS traités")

    info("Enrichissement (CPD sans glossaire + ATC + dispo + CIP) ...")

    updates = []
    failures_rcp = 0
    start = time.time()

    for idx, cis in enumerate(all_cis, start=1):
        rec = airtable_by_cis[cis]
        fields_cur = rec.get("fields", {}) or {}
        upd_fields = {}

        row = cis_map.get(cis)
        if row:
            labo = normalize_lab_name(row.titulaire)
            if labo and str(fields_cur.get("Laboratoire", "")).strip() != labo:
                upd_fields["Laboratoire"] = labo
            if safe_text(row.specialite) and str(fields_cur.get("Spécialité", "")).strip() != safe_text(row.specialite):
                upd_fields["Spécialité"] = safe_text(row.specialite)
            if safe_text(row.forme) and str(fields_cur.get("Forme", "")).strip() != safe_text(row.forme):
                upd_fields["Forme"] = safe_text(row.forme)
            if safe_text(row.voie_admin) and str(fields_cur.get("Voie d'administration", "")).strip() != safe_text(row.voie_admin):
                upd_fields["Voie d'administration"] = safe_text(row.voie_admin)

        cip = cip_map.get(cis)
        if cip:
            if cip.cip13 and str(fields_cur.get("CIP 13", "")).strip() != cip.cip13:
                upd_fields["CIP 13"] = cip.cip13
            if cip.agrement_collectivites and str(fields_cur.get("Agrément aux collectivités", "")).strip() != cip.agrement_collectivites:
                upd_fields["Agrément aux collectivités"] = cip.agrement_collectivites

        rcp_url = str(fields_cur.get("Lien vers RCP", "")).strip()
        if not rcp_url:
            rcp_url = f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"
            upd_fields["Lien vers RCP"] = rcp_url

        cur_cpd = str(fields_cur.get("Conditions de prescription et délivrance", "")).strip()
        cur_dispo = str(fields_cur.get("Disponibilité du traitement", "")).strip()
        cur_atc = str(fields_cur.get("Code ATC", "")).strip()

        need_rcp = force_refresh or (not cur_cpd) or (not cur_dispo) or (not cur_atc)

        is_retro = cis in ansm_retro_cis

        if need_rcp:
            try:
                cpd_text, is_homeo, reserved_hosp, usage_hosp, atc_code = analyze_rcp_page(rcp_url)

                if cpd_text and cpd_text != cur_cpd:
                    upd_fields["Conditions de prescription et délivrance"] = cpd_text

                if atc_code and atc_code != cur_atc:
                    upd_fields["Code ATC"] = atc_code

                has_taux = cip.has_taux if cip else False
                dispo = compute_disponibilite(
                    has_taux_ville=has_taux,
                    is_ansm_retro=is_retro,
                    is_homeopathy=is_homeo,
                    reserved_hospital=reserved_hosp,
                    usage_hospital=usage_hosp,
                )

                if dispo != cur_dispo:
                    upd_fields["Disponibilité du traitement"] = dispo

            except Exception as e:
                failures_rcp += 1
                warn(f"RCP KO CIS={cis}: {e} (on continue)")
        else:
            has_taux = cip.has_taux if cip else False
            # sans fetch, on reste conservateur (on ne déduit pas réservé hosp)
            dispo = compute_disponibilite(
                has_taux_ville=has_taux,
                is_ansm_retro=is_retro,
                is_homeopathy=False,
                reserved_hospital=False,
                usage_hospital=False,
            )
            if dispo != cur_dispo:
                upd_fields["Disponibilité du traitement"] = dispo

        if upd_fields:
            updates.append({"id": rec["id"], "fields": upd_fields})

        if len(updates) >= 200:
            at.update_records(updates)
            ok(f"Batch updates: {len(updates)}")
            updates = []

        if idx % 200 == 0:
            elapsed = time.time() - start
            rate = idx / elapsed if elapsed > 0 else 0
            remaining = (len(all_cis) - idx) / rate if rate > 0 else 0
            info(f"Progress {idx}/{len(all_cis)} | {rate:.2f} CIS/s | RCP KO: {failures_rcp} | reste ~{int(remaining)}s")

    if updates:
        at.update_records(updates)
        ok(f"Updates finaux: {len(updates)}")

    ok(f"Terminé. RCP KO: {failures_rcp}")

if __name__ == "__main__":
    main()
