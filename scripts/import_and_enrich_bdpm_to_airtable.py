#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import random
import urllib.parse
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup

# ✅ NEW: lecture Excel pour l'équivalence ATC
import pandas as pd

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

# Airtable
AIRTABLE_MIN_DELAY_S = float(os.getenv("AIRTABLE_MIN_DELAY_S", "0.25"))
AIRTABLE_BATCH_SIZE = 10

UPDATE_FLUSH_THRESHOLD = int(os.getenv("UPDATE_FLUSH_THRESHOLD", "200"))

HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "20"))

REQUEST_TIMEOUT = 30
MAX_RETRIES = 4

REPORT_DIR = os.getenv("REPORT_DIR", "reports")
REPORT_COMMIT = os.getenv("GITHUB_COMMIT_REPORT", "0").strip() == "1"

HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "50"))

# ✅ NEW: on pointe vers ton fichier Excel "equivalence atc" dans data
# (tu peux aussi surcharger via variable d'env ATC_LABELS_FILE)
ATC_LABELS_FILE = os.getenv("ATC_LABELS_FILE", "data/equivalence atc.xlsx")

# ============================================================
# LOG
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
# REPORTING (GitHub workspace)
# ============================================================

def report_path_today() -> str:
    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = f"deleted_records_{time.strftime('%Y-%m-%d')}.txt"
    return os.path.join(REPORT_DIR, fname)

def append_deleted_report(cis: str, reason: str, url: str):
    p = report_path_today()
    line = f"{_ts()}\tCIS={cis}\tSUPPRIME\treason={reason}\turl={url}\n"
    with open(p, "a", encoding="utf-8") as f:
        f.write(line)

def try_git_commit_report():
    if not REPORT_COMMIT:
        return
    try:
        p = report_path_today()
        if not os.path.exists(p):
            return
        subprocess.run(["git", "status"], check=False)
        subprocess.run(["git", "add", p], check=True)
        subprocess.run(["git", "commit", "-m", f"Report: deleted Airtable records ({time.strftime('%Y-%m-%d')})"], check=True)
        subprocess.run(["git", "push"], check=True)
        ok("Rapport commit/push sur GitHub effectué.")
    except Exception as e:
        warn(f"Commit/push du rapport impossible (on continue): {e}")

# ============================================================
# TEXT UTIL
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

def capitalize_each_line(text: str) -> str:
    if not text:
        return text
    out_lines: List[str] = []
    for line in text.split("\n"):
        stripped = line.lstrip()
        if stripped:
            prefix_len = len(line) - len(stripped)
            prefix = line[:prefix_len]
            stripped = stripped[0].upper() + stripped[1:]
            out_lines.append(prefix + stripped)
        else:
            out_lines.append(line)
    return "\n".join(out_lines)

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
# ATC LABELS (niveau 4)
# ============================================================

ATC7_PAT = re.compile(r"^[A-Z]\d{2}[A-Z]{2}\d{2}$")  # ex A11CA01
ATC5_PAT = re.compile(r"^[A-Z]\d{2}[A-Z]{2}$")       # ex A11CA

def load_atc_labels(path: str) -> Dict[str, str]:
    """
    ✅ Charge l'équivalence ATC depuis un fichier Excel
    attendu: colonnes "Code ATC (niveau 4)" et "Libellé ATC"
    """
    if not os.path.exists(path):
        warn(f"Fichier d'équivalence ATC introuvable: {path} (Libellé ATC restera vide)")
        return {}

    try:
        df = pd.read_excel(path)
    except Exception as e:
        warn(f"Impossible de lire l'Excel {path}: {e} (Libellé ATC restera vide)")
        return {}

    col_code = "Code ATC (niveau 4)"
    col_label = "Libellé ATC"

    if col_code not in df.columns or col_label not in df.columns:
        warn(
            f"Colonnes attendues absentes dans {path}. "
            f"Trouvé: {list(df.columns)} | Attendu: ['{col_code}', '{col_label}']"
        )
        return {}

    mapping: Dict[str, str] = {}

    for _, row in df.iterrows():
        code = safe_text(row.get(col_code, "")).upper()
        label = safe_text(row.get(col_label, ""))
        if not code or not label:
            continue
        # sécurité: on ne garde que les codes de niveau 4 (5 chars) si possible
        code = atc_level4_from_any(code) or code
        if code and code not in mapping:
            mapping[code] = label

    ok(f"ATC labels chargés depuis Excel: {len(mapping)} entrées")
    return mapping

def atc_level4_from_any(atc: str) -> str:
    """
    Retourne le code ATC niveau 4 (5 caractères) même si ATC est niveau 5 (7 caractères).
    - A11CA01 -> A11CA
    - A11CA   -> A11CA
    Sinon -> ""
    """
    a = (atc or "").strip().upper()
    if ATC7_PAT.fullmatch(a):
        return a[:5]
    if ATC5_PAT.fullmatch(a):
        return a
    m7 = re.search(r"\b([A-Z]\d{2}[A-Z]{2}\d{2})\b", a)
    if m7:
        return m7.group(1)[:5]
    m5 = re.search(r"\b([A-Z]\d{2}[A-Z]{2})\b", a)
    if m5:
        return m5.group(1)
    return ""

# ============================================================
# URL HELPERS
# ============================================================

def base_extrait_url_from_cis(cis: str) -> str:
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait"

def normalize_to_fiche_info(url: str, cis_fallback: str) -> str:
    if not url or not url.startswith("http"):
        return f"{base_extrait_url_from_cis(cis_fallback)}?tab=fiche-info"
    parts = urllib.parse.urlsplit(url)
    qs = urllib.parse.parse_qs(parts.query, keep_blank_values=True)
    qs["tab"] = ["fiche-info"]
    new_query = urllib.parse.urlencode(qs, doseq=True)
    cleaned = parts._replace(query=new_query, fragment="")
    return urllib.parse.urlunsplit(cleaned)

def rcp_link_default(cis: str) -> str:
    return f"{base_extrait_url_from_cis(cis)}?tab=rcp#tab-rcp"

# ============================================================
# DOWNLOAD
# ============================================================

def http_get(url: str, timeout: Tuple[float, float] = (HTTP_CONNECT_TIMEOUT, 30.0)) -> requests.Response:
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
    r = http_get(url, timeout=(HTTP_CONNECT_TIMEOUT, 30.0))
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    r.encoding = encoding
    return r.text

def download_bytes(url: str) -> bytes:
    r = http_get(url, timeout=(HTTP_CONNECT_TIMEOUT, 60.0))
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.content

# ============================================================
# ANSM retrocession
# ============================================================

def find_ansm_retro_excel_link() -> str:
    r = http_get(ANSM_RETRO_PAGE, timeout=(HTTP_CONNECT_TIMEOUT, 30.0))
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

        if cis not in out:
            out[cis] = CipInfo(cip13=cip13, has_taux=has_taux)
        else:
            if not out[cis].cip13 and cip13:
                out[cis].cip13 = cip13
            out[cis].has_taux = out[cis].has_taux or has_taux

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
# FICHE-INFO SCRAPING
# ============================================================

HOMEOPATHY_PAT = re.compile(r"hom[ée]opath(?:ie|ique)", flags=re.IGNORECASE)
HOMEOPATHY_CLASS_PAT = re.compile(r"m[ée]dicament\s+hom[ée]opathique", flags=re.IGNORECASE)

RESERVED_HOSP_PAT = re.compile(r"réserv[ée]?\s+à\s+l['’]usage\s+hospitalier", flags=re.IGNORECASE)
USAGE_HOSP_PAT = re.compile(r"\busage\s+hospitalier\b", flags=re.IGNORECASE)
NEGATION_PAT = re.compile(
    r"(?:\bnon\b|\bpas\b|\bjamais\b)\s+(?:réserv[ée]?\s+à\s+l['’]usage\s+hospitalier|\busage\s+hospitalier\b)",
    flags=re.IGNORECASE
)

GLOSSARY_PAT = re.compile(r"\baller\s+au\s+glossaire\b", flags=re.IGNORECASE)
ATC_PAT = re.compile(r"\b[A-Z]\d{2}[A-Z]{2}\d{2}\b")

PHARM_CLASS_LINE_PAT = re.compile(r"^Classe\s+pharmacoth[ée]rapeutique\b", re.IGNORECASE)
CODE_ATC_INLINE_PAT = re.compile(r"code\s+ATC\s*[:\-]\s*([A-Z]\d{2}[A-Z]{2}\d{2})", re.IGNORECASE)

class PageUnavailable(Exception):
    def __init__(self, url: str, status: Optional[int], detail: str):
        super().__init__(detail)
        self.url = url
        self.status = status
        self.detail = detail

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
    return normalize_ws_keep_lines("\n".join(kept))

def fetch_html_checked(url: str, timeout: Tuple[float, float] = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT), max_retries: int = 3) -> str:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=timeout)
            if r.status_code == 404:
                raise PageUnavailable(url, 404, "HTTP 404")
            if r.status_code >= 400:
                raise PageUnavailable(url, r.status_code, f"HTTP {r.status_code}")
            if not r.text or len(r.text) < 200:
                raise PageUnavailable(url, r.status_code, "HTML vide/trop court")
            return r.text
        except PageUnavailable:
            raise
        except Exception as e:
            last_err = e
            time.sleep(1.0 * attempt)
    raise PageUnavailable(url, None, f"Erreur réseau: {last_err}")

def extract_badge_usage_hospitalier_only(soup: BeautifulSoup) -> bool:
    for el in soup.find_all(["span", "div", "a", "p", "li"]):
        t = (el.get_text(" ", strip=True) or "")
        if not t:
            continue
        if len(t) > 60:
            continue
        if "cela signifie" in t.lower():
            continue
        if t.strip().lower() == "usage hospitalier":
            return True
    return False

def extract_cpd_from_fiche_info(soup: BeautifulSoup) -> str:
    lines = [ln.strip() for ln in soup.get_text("\n", strip=True).split("\n")]

    autres_pat = re.compile(r"^Autres\s+informations$", re.IGNORECASE)
    cpd_pat = re.compile(r"^Conditions\s+de\s+prescription\s+et\s+de\s+d[ée]livrance\b", re.IGNORECASE)
    stop_pat = re.compile(
        r"^(Statut\s+de\s+l['’]autorisation|Type\s+de\s+proc[ée]dure|Code\s+CIS|Titulaire\s+de\s+l['’]autorisation)\s*:",
        re.IGNORECASE
    )

    start_autres = None
    for i, ln in enumerate(lines):
        if autres_pat.match(ln):
            start_autres = i
            break
    if start_autres is None:
        return ""

    start_cpd = None
    inline_value = ""
    for i in range(start_autres, len(lines)):
        ln = lines[i]
        if cpd_pat.match(ln):
            start_cpd = i
            if ":" in ln:
                inline_value = ln.split(":", 1)[1].strip()
            break
    if start_cpd is None:
        return ""

    collected: List[str] = []
    if inline_value:
        collected.append(inline_value)

    for ln in lines[start_cpd + 1:]:
        if stop_pat.search(ln):
            break
        collected.append(ln)

    return clean_cpd_text_keep_useful(normalize_ws_keep_lines("\n".join(collected)))

def extract_atc_from_fiche_info(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n", strip=True)
    m = re.search(r"code\s+ATC\s*[:\-]\s*([A-Z]\d{2}[A-Z]{2}\d{2})", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m2 = ATC_PAT.search(text or "")
    return m2.group(0).strip() if m2 else ""

def extract_pharm_class_and_atc_from_fiche_info(soup: BeautifulSoup) -> Tuple[str, str]:
    lines = [ln.strip() for ln in soup.get_text("\n", strip=True).split("\n") if ln.strip()]
    for ln in lines:
        if not PHARM_CLASS_LINE_PAT.search(ln):
            continue

        atc = ""
        m_atc = CODE_ATC_INLINE_PAT.search(ln)
        if m_atc:
            atc = m_atc.group(1).strip()

        s = re.sub(PHARM_CLASS_LINE_PAT, "", ln).strip()
        s = s.lstrip(":").strip()
        s = re.sub(r"[–\-]\s*code\s+ATC\s*[:\-]\s*[A-Z]\d{2}[A-Z]{2}\d{2}\.?\s*$", "", s, flags=re.IGNORECASE).strip()

        if re.fullmatch(r"code\s+ATC\s*[:\-]\s*[A-Z]\d{2}[A-Z]{2}\d{2}\.?", s, flags=re.IGNORECASE):
            s = ""

        return s, atc

    return "", ""

def detect_homeopathy_from_fiche_info(soup: BeautifulSoup) -> bool:
    text = soup.get_text("\n", strip=True)
    return bool(HOMEOPATHY_PAT.search(text) or HOMEOPATHY_CLASS_PAT.search(text))

def analyze_fiche_info(fiche_url: str) -> Tuple[str, bool, bool, bool, str, str]:
    html = fetch_html_checked(fiche_url)
    soup = BeautifulSoup(html, _bs_parser())

    is_homeo = detect_homeopathy_from_fiche_info(soup)

    cpd_text = extract_cpd_from_fiche_info(soup)
    cpd_text = capitalize_each_line(cpd_text)

    pharm_class, atc_from_class_line = extract_pharm_class_and_atc_from_fiche_info(soup)
    atc_code = (atc_from_class_line.strip() or extract_atc_from_fiche_info(soup)).strip()

    badge_usage = extract_badge_usage_hospitalier_only(soup)

    zone_text = cpd_text or ""
    if NEGATION_PAT.search(zone_text):
        reserved = False
        usage = False
    else:
        reserved = bool(RESERVED_HOSP_PAT.search(zone_text))
        usage = bool(USAGE_HOSP_PAT.search(zone_text)) or badge_usage

    return cpd_text, is_homeo, reserved, usage, atc_code, pharm_class

def compute_disponibilite(
    has_taux_ville: bool,
    is_ansm_retro: bool,
    is_homeo: bool,
    reserved_hospital: bool,
    usage_hospital: bool
) -> str:
    ville = bool(has_taux_ville or is_homeo)

    if is_ansm_retro and ville:
        return "Disponible en pharmacie de ville et en rétrocession hospitalière"
    if is_ansm_retro and not ville:
        return "Disponible en rétrocession hospitalière"

    if reserved_hospital:
        return "Réservé à l'usage hospitalier"
    if usage_hospital and not ville:
        return "Réservé à l'usage hospitalier"

    if ville:
        return "Disponible en pharmacie de ville"

    return "Pas d'information sur la disponibilité mentionnée"

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

    # ✅ NEW: charge les libellés depuis ton Excel d'équivalence ATC
    atc_labels = load_atc_labels(ATC_LABELS_FILE)

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
        "Disponibilité du traitement",
        "Conditions de prescription et délivrance",
        "Laboratoire",
        "Spécialité",
        "Forme",
        "Voie d'administration",
        "Code ATC",
        "Code ATC (niveau 4)",
        "Libellé ATC",
        "Classe pharmacothérapeutique",
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

    # CREATE
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
                "Lien vers RCP": rcp_link_default(cis),
            }
            if cip and cip.cip13:
                fields["CIP 13"] = cip.cip13
            new_recs.append({"fields": fields})
        at.create_records(new_recs)
        ok(f"Créés: {len(new_recs)}")

        records = at.list_all_records(fields=needed_fields)
        airtable_by_cis = {}
        for rec in records:
            cis = str(rec.get("fields", {}).get("Code cis", "")).strip()
            cis = re.sub(r"\D", "", cis)
            if len(cis) == 8:
                airtable_by_cis[cis] = rec

    # DELETE init
    if to_delete:
        info("Suppression des enregistrements Airtable absents de BDPM ...")
        ids = [airtable_by_cis[c]["id"] for c in to_delete if c in airtable_by_cis]
        if ids:
            at.delete_records(ids)
            ok(f"Supprimés: {len(ids)}")

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

    info("Enrichissement (fiche-info) ...")

    updates = []
    failures = 0
    deleted_count = 0
    start = time.time()

    for idx, cis in enumerate(all_cis, start=1):
        if HEARTBEAT_EVERY > 0 and idx % HEARTBEAT_EVERY == 0:
            info(f"Heartbeat: {idx}/{len(all_cis)} (CIS={cis})")

        rec = airtable_by_cis.get(cis)
        if not rec:
            continue

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

        link_rcp = str(fields_cur.get("Lien vers RCP", "")).strip()
        if not link_rcp:
            link_rcp = rcp_link_default(cis)
            upd_fields["Lien vers RCP"] = link_rcp

        fiche_url = normalize_to_fiche_info(link_rcp, cis)

        cur_cpd = str(fields_cur.get("Conditions de prescription et délivrance", "")).strip()
        cur_dispo = str(fields_cur.get("Disponibilité du traitement", "")).strip()
        cur_atc = str(fields_cur.get("Code ATC", "")).strip()
        cur_atc4 = str(fields_cur.get("Code ATC (niveau 4)", "")).strip()
        cur_label = str(fields_cur.get("Libellé ATC", "")).strip()
        cur_class = str(fields_cur.get("Classe pharmacothérapeutique", "")).strip()

        # ✅ IMPORTANT: on ne refetch pas la page juste pour le libellé
        need_fetch = force_refresh or (not cur_cpd) or (not cur_dispo) or (not cur_atc) or (not cur_class)
        is_retro = cis in ansm_retro_cis

        # ✅ Si on a déjà atc4 mais pas le libellé -> on remplit SANS fetch
        if (not cur_label) and cur_atc4:
            atc4_norm = atc_level4_from_any(cur_atc4) or cur_atc4.strip().upper()
            if atc4_norm in atc_labels:
                label = atc_labels[atc4_norm]
                if label:
                    upd_fields["Libellé ATC"] = label

        if need_fetch:
            try:
                cpd_text, is_homeo, reserved_hosp, usage_hosp, atc_code, pharm_class = analyze_fiche_info(fiche_url)

                if cpd_text and cpd_text != cur_cpd:
                    upd_fields["Conditions de prescription et délivrance"] = cpd_text

                if atc_code and atc_code != cur_atc:
                    upd_fields["Code ATC"] = atc_code

                # ✅ calcule niveau 4 à partir de l'ATC (niveau 5 en général)
                atc4 = atc_level4_from_any(atc_code)
                if atc4 and atc4 != cur_atc4:
                    upd_fields["Code ATC (niveau 4)"] = atc4

                # ✅ libellé basé UNIQUEMENT sur le niveau 4
                # (on privilégie atc4 calculé, sinon celui déjà en base)
                atc4_for_label = (atc4 or cur_atc4 or "").strip().upper()
                atc4_for_label = atc_level4_from_any(atc4_for_label) or atc4_for_label

                if atc4_for_label and atc4_for_label in atc_labels:
                    label = atc_labels[atc4_for_label]
                    if label and label != cur_label:
                        upd_fields["Libellé ATC"] = label

                if pharm_class:
                    pc = safe_text(pharm_class)
                    if pc and pc != cur_class:
                        upd_fields["Classe pharmacothérapeutique"] = pc

                has_taux = cip.has_taux if cip else False
                dispo = compute_disponibilite(
                    has_taux_ville=has_taux,
                    is_ansm_retro=is_retro,
                    is_homeo=is_homeo,
                    reserved_hospital=reserved_hosp,
                    usage_hospital=usage_hosp,
                )
                if dispo != cur_dispo:
                    upd_fields["Disponibilité du traitement"] = dispo

            except PageUnavailable as e:
                warn(f"Fiche-info KO CIS={cis}: {e.detail} ({e.url}) -> suppression Airtable")

                if updates:
                    at.update_records(updates)
                    ok(f"Batch updates (flush before delete): {len(updates)}")
                    updates = []

                try:
                    at.delete_records([rec["id"]])
                    deleted_count += 1
                    append_deleted_report(cis=cis, reason=f"Page indisponible ({e.status})", url=e.url)
                except Exception as de:
                    failures += 1
                    warn(f"Suppression Airtable impossible CIS={cis}: {de} (on continue)")
                continue

            except Exception as e:
                failures += 1
                warn(f"Fiche-info KO CIS={cis}: {e} (on continue)")

        if upd_fields:
            updates.append({"id": rec["id"], "fields": upd_fields})

        if len(updates) >= UPDATE_FLUSH_THRESHOLD:
            at.update_records(updates)
            ok(f"Batch updates: {len(updates)}")
            updates = []

        if idx % 1000 == 0:
            elapsed = time.time() - start
            rate = idx / elapsed if elapsed > 0 else 0
            remaining = (len(all_cis) - idx) / rate if rate > 0 else 0
            info(f"Progress {idx}/{len(all_cis)} | {rate:.2f} CIS/s | échecs: {failures} | supprimés: {deleted_count} | reste ~{int(remaining)}s")

    if updates:
        at.update_records(updates)
        ok(f"Updates finaux: {len(updates)}")

    try_git_commit_report()
    ok(f"Terminé. échecs: {failures} | supprimés: {deleted_count} | rapport: {report_path_today()}")

if __name__ == "__main__":
    main()
