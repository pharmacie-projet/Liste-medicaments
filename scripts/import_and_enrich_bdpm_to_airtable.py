#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import random
import urllib.parse
import subprocess
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# Excel equivalence ATC (optionnel si tu veux remplir "Libellé ATC")
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # noqa

# ============================================================
# CONFIG
# ============================================================

# BDPM (TXT)
BDPM_CIS_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
BDPM_CIS_CIP_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
BDPM_COMPO_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_COMPO_bdpm.txt"

# BDPM "Médicaments d'intérêt thérapeutique majeur" (CIS -> ATC)
BDPM_MITM_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_MITM.txt"

# BDPM "Informations importantes" (génération en direct) (CIS -> URL info importante)
BDPM_INFO_IMPORTANTES_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/CIS_InfoImportantes.txt"

# Page BDPM pour fiche-info (HTML)
BDPM_DOC_EXTRACT_URL = "https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait"

# ANSM rétrocession
ANSM_RETRO_PAGE = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

# Airtable
AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_MIN_DELAY_S = float(os.getenv("AIRTABLE_MIN_DELAY_S", "0.25"))
AIRTABLE_BATCH_SIZE = 10
UPDATE_FLUSH_THRESHOLD = int(os.getenv("UPDATE_FLUSH_THRESHOLD", "200"))

HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "25"))
REQUEST_TIMEOUT = 35
MAX_RETRIES = 4

REPORT_DIR = os.getenv("REPORT_DIR", "reports")
REPORT_COMMIT = os.getenv("GITHUB_COMMIT_REPORT", "0").strip() == "1"

HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "50"))

# Fichier Excel d'équivalence ATC (niveau 4 -> libellé)
ATC_EQUIVALENCE_FILE = os.getenv("ATC_EQUIVALENCE_FILE", "data/equivalence atc.xlsx")

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
}

# ✅ Session HTTP réutilisable (gros gain perf sur GitHub Actions)
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update(HEADERS_WEB)

# ============================================================
# AIRTABLE FIELDS (adapte ici si besoin)
# ============================================================

FIELD_CIS = "Code cis"
FIELD_RCP = "Lien vers RCP"
FIELD_CIP13 = "CIP 13"
FIELD_DISPO = "Disponibilité du traitement"
FIELD_CPD = "Conditions de prescription et délivrance"
FIELD_LABO = "Laboratoire"
FIELD_SPEC = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE = "Voie d'administration"
FIELD_ATC = "Code ATC"
FIELD_ATC4 = "Code ATC (niveau 4)"      # computed -> lecture seulement
FIELD_ATC_LABEL = "Libellé ATC"          # champ à écrire
FIELD_COMPOSITION = "Composition"        # champ à écrire
FIELD_COMPOSITION_DETAILS = "Composition détails"  # champ à écrire
FIELD_LIEN_INFO_IMPORTANTE = "Lien vers information importante"  # champ à écrire (URL)

# ✅ RCP content fields
FIELD_INTERACTIONS_RCP = "Interactions RCP"
FIELD_INDICATIONS_RCP = "Indications RCP"
FIELD_POSOLOGIE_RCP = "Posologie RCP"

# ✅ timestamp de revue
FIELD_DATE_REVUE = "Date revue ligne"    # champ à écrire

# règle absolue : ne jamais écrire ces champs (computed)
DO_NOT_WRITE_FIELDS: Set[str] = {
    FIELD_ATC4,
}

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
    time.sleep(min(10, 0.6 * (2 ** (attempt - 1))) + random.random() * 0.25)

# ============================================================
# REVIEW TIMESTAMP
# ============================================================

def now_paris_iso_seconds() -> str:
    return datetime.now(ZoneInfo("Europe/Paris")).isoformat(timespec="seconds")

# ============================================================
# REPORTING
# ============================================================

def report_path_deleted_today() -> str:
    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = f"deleted_records_{time.strftime('%Y-%m-%d')}.txt"
    return os.path.join(REPORT_DIR, fname)

def append_deleted_report(cis: str, reason: str, url: str):
    p = report_path_deleted_today()
    line = f"{_ts()}\tCIS={cis}\tSUPPRIME\treason={reason}\turl={url}\n"
    with open(p, "a", encoding="utf-8") as f:
        f.write(line)

def try_git_commit_report():
    if not REPORT_COMMIT:
        return
    try:
        p = report_path_deleted_today()
        if not os.path.exists(p):
            return
        subprocess.run(["git", "status"], check=False)
        subprocess.run(["git", "add", p], check=True)
        subprocess.run(["git", "commit", "-m", f"Report: deleted records ({time.strftime('%Y-%m-%d')})"], check=True)
        subprocess.run(["git", "push"], check=True)
        ok("Rapport (suppressions) commit/push sur GitHub effectué.")
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

def strip_accents(s: str) -> str:
    s = safe_text(s)
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

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
# EXTRACTION SECTIONS RCP (CONTENU ROBUSTE)
# - Ne détecte les rubriques que si elles sont en début de ligne
#   => évite les faux positifs "voir rubrique 4.2"
# ============================================================

_TITLE_ONLY_MIN_WORDS = 10
_TITLE_ONLY_MAX_CHARS = 240

def _clean_section_text(s: str, max_chars: int = 20000) -> str:
    s = normalize_ws_keep_lines(safe_text(s))
    if not s:
        return ""
    lines = [ln.strip() for ln in s.split("\n")]
    out: List[str] = []
    last = None
    for ln in lines:
        if not ln:
            if out and out[-1] != "":
                out.append("")
            continue
        if ln == last:
            continue
        out.append(ln)
        last = ln
    s2 = "\n".join(out).strip()
    if len(s2) > max_chars:
        s2 = s2[:max_chars].rstrip() + "\n\n[Texte tronqué]"
    return s2

def _looks_like_title_only(text: str) -> bool:
    t = safe_text(text)
    if not t:
        return True
    if len(t) <= _TITLE_ONLY_MAX_CHARS:
        words = re.findall(r"\w+", t, flags=re.UNICODE)
        if len(words) <= _TITLE_ONLY_MIN_WORDS:
            return True
    punct = sum(t.count(x) for x in [".", ";", ":", "!", "?", "—"])
    if punct <= 2 and len(t) < 400:
        return True
    return False

def _strip_leading_heading_lines(text: str, major: int, minor: int) -> str:
    t = _clean_section_text(text)
    if not t:
        return ""
    lines = t.split("\n")
    cleaned: List[str] = []
    head_pat = re.compile(rf"^\s*{major}\s*\.\s*{minor}\s*(?:\.)?\s*", re.IGNORECASE)

    for i, ln in enumerate(lines):
        ln_stripped = ln.strip()
        if i < 3:
            if head_pat.search(ln_stripped):
                continue
            if len(ln_stripped) <= 70 and not re.search(
                r"\b(est|sont|doit|doivent|administr|prendre|utilis|trait|surveillance|risque|patients|posologie|dose)\b",
                ln_stripped,
                re.IGNORECASE
            ):
                if re.fullmatch(r"[\d\.\sA-Za-zÀ-ÿ'’\-()]+", ln_stripped):
                    continue
        cleaned.append(ln)

    out = "\n".join(cleaned).strip()
    return out if out else t

def _extract_section_best(raw: str, major: int, minor: int, end_markers: List[Tuple[int, int]]) -> str:
    """
    Extrait une rubrique RCP (ex 4.2) en ne reconnaissant les numéros
    QUE s'ils apparaissent comme titres en début de ligne.
    Évite les faux positifs du type: "voir rubrique 4.2".
    """
    if not raw:
        return ""

    t = raw.replace("\xa0", " ")
    t = t.replace("\r", "\n")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    if not t:
        return ""

    # Titre en début de ligne (multiline)
    start_re = re.compile(rf"(?m)^\s*{major}\s*\.\s*{minor}\s*(?:\.)?\s*(.*)$")

    # Fin = prochaine rubrique (titre en début de ligne)
    end_nums = list(end_markers) + [(5, i) for i in range(1, 11)]
    end_alt = "|".join([rf"{mj}\s*\.\s*{mn}\s*(?:\.)?\s*" for mj, mn in end_nums])
    end_re = re.compile(rf"(?m)^\s*(?:{end_alt}).*$")

    starts = list(start_re.finditer(t))
    if not starts:
        return ""

    best = ""
    best_len = 0

    for m in starts:
        line_end = t.find("\n", m.end())
        content_start = (line_end + 1) if line_end != -1 else m.end()

        tail = t[content_start:]

        m_end = end_re.search(tail)
        block = tail[:m_end.start()] if m_end else tail
        block = block.strip()
        if not block:
            continue

        cleaned = _strip_leading_heading_lines(block, major, minor)
        cleaned = _clean_section_text(cleaned)

        if _looks_like_title_only(cleaned):
            continue

        if len(cleaned) > best_len:
            best = cleaned
            best_len = len(cleaned)

    return best.strip()

def extract_rcp_sections_from_rcp_html(html: str) -> Dict[str, str]:
    if not html:
        return {}

    soup = BeautifulSoup(html, _bs_parser())
    raw = soup.get_text("\n")

    raw = raw.replace("\r", "\n").replace("\xa0", " ")
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw).strip()

    if not raw or len(raw) < 200:
        return {}

    out: Dict[str, str] = {}

    ind = _extract_section_best(raw, 4, 1, end_markers=[(4, 2), (4, 3)])
    poso = _extract_section_best(raw, 4, 2, end_markers=[(4, 3), (4, 4)])
    sec44 = _extract_section_best(raw, 4, 4, end_markers=[(4, 5), (4, 6)])
    sec45 = _extract_section_best(raw, 4, 5, end_markers=[(4, 6), (4, 7)])

    if ind:
        out["indications_4_1"] = ind
    if poso:
        out["posologie_4_2"] = poso
    if sec44:
        out["mises_en_garde_4_4"] = sec44
    if sec45:
        out["interactions_4_5"] = sec45

    return out

def format_interactions_field(sec44: str, sec45: str) -> str:
    sec44 = safe_text(sec44)
    sec45 = safe_text(sec45)

    blocks: List[str] = []
    if sec44 and not _looks_like_title_only(sec44):
        blocks.append("4.4. Mises en garde spéciales et précautions d'emploi\n" + sec44)
    if sec45 and not _looks_like_title_only(sec45):
        blocks.append("4.5. Interactions avec d'autres médicaments et autres formes d'interactions\n" + sec45)

    return _clean_section_text("\n\n".join(blocks)).strip()

# ============================================================
# ATC HELPERS
# ============================================================

ATC7_PAT = re.compile(r"^[A-Z]\d{2}[A-Z]{2}\d{2}$")  # ex A11CA01
ATC5_PAT = re.compile(r"^[A-Z]\d{2}[A-Z]{2}$")       # ex A11CA

def canonical_atc7(raw: str) -> str:
    if not raw:
        return ""
    s = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return s if ATC7_PAT.fullmatch(s) else ""

def atc_level4_from_any(atc: str) -> str:
    a = (atc or "").strip().upper().replace(" ", "")
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

def load_atc_equivalence_excel(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        warn(f"Fichier d'équivalence ATC introuvable: {path} (Libellé ATC restera vide)")
        return {}
    if pd is None:
        warn("pandas non disponible -> impossible de lire l'Excel d'équivalence ATC (Libellé ATC restera vide)")
        return {}
    try:
        df = pd.read_excel(path)
    except Exception as e:
        warn(f"Impossible de lire l'Excel {path}: {e} (Libellé ATC restera vide)")
        return {}

    if FIELD_ATC4 not in df.columns or FIELD_ATC_LABEL not in df.columns:
        warn(
            f"Colonnes attendues absentes dans {path}. "
            f"Trouvé: {list(df.columns)} | Attendu: ['{FIELD_ATC4}', '{FIELD_ATC_LABEL}']"
        )
        return {}

    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        code = safe_text(row.get(FIELD_ATC4, "")).upper()
        label = safe_text(row.get(FIELD_ATC_LABEL, ""))
        if not code or not label:
            continue
        code = atc_level4_from_any(code) or code
        mapping[code] = label

    ok(f"Équivalence ATC chargée: {len(mapping)} entrées")
    return mapping

# ============================================================
# DOWNLOAD
# ============================================================

def http_get(url: str, timeout: Tuple[float, float] = (HTTP_CONNECT_TIMEOUT, 60.0)) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = HTTP_SESSION.get(url, timeout=timeout, allow_redirects=True)
            return r
        except Exception as e:
            last_err = e
            retry_sleep(attempt)
    raise RuntimeError(f"GET failed: {url} / {last_err}")

def download_text(url: str, encoding: str = "latin-1") -> str:
    r = http_get(url, timeout=(HTTP_CONNECT_TIMEOUT, 120.0))
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    r.encoding = encoding
    return r.text

def download_bytes(url: str, timeout_s: float = 140.0) -> bytes:
    r = http_get(url, timeout=(HTTP_CONNECT_TIMEOUT, timeout_s))
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.content

# ============================================================
# ANSM retrocession
# ============================================================

def find_ansm_retro_excel_link() -> str:
    r = http_get(ANSM_RETRO_PAGE, timeout=(HTTP_CONNECT_TIMEOUT, 60.0))
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
        raise RuntimeError("Le fichier ANSM est en .xls mais 'xlrd' n'est pas installé. pip install xlrd==2.0.1")

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
# BDPM PARSE (CIS, CIP)
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
# COMPOSITION BDPM -> DCI principales
# ============================================================

_NOISE_RE = re.compile(r"\b(pour\s+pr[ée]parations?\s+hom[ée]opathiques)\b", flags=re.IGNORECASE)
_COMPLEX_PREFIX_RE = re.compile(r"^\s*complexe\s+d['’]\s*", flags=re.IGNORECASE)

_SALT_WORDS = (
    r"chlorhydrate|dichlorhydrate|bromhydrate|chlorure|bromure|iodure|fluorure|"
    r"citrate|fumarate|succinate|tartrate|mal[eé]ate|m[eé]silate|mesilate|"
    r"tosylate|benzoate|gluconate|lactate|carbonate|phosphate|sulfate|sulphate|"
    r"ac[eé]tate|oxalate|nitrate|nitrite|hydrog[eé]nosuccinate|"
    r"b[eé]silate|besilate|besylate"
)

_SALT_PREFIX_RE = re.compile(rf"\b({_SALT_WORDS})\s+(?:de|d['’])\s*", flags=re.IGNORECASE)
_SALT_LEADING_RE = re.compile(rf"^\s*({_SALT_WORDS})\s+", flags=re.IGNORECASE)
_SALT_GLUE_RE = re.compile(rf"^({_SALT_WORDS})([a-zà-ÿ])", flags=re.IGNORECASE)

_PREFIX_GLUE_RES = [
    re.compile(r"^(dichlorhydrate)([a-zà-ÿ])", re.IGNORECASE),
    re.compile(r"^(chlorhydrate)([a-zà-ÿ])", re.IGNORECASE),
    re.compile(r"^(dioxyd(e|e)\s*|dioxyde)([a-zà-ÿ])", re.IGNORECASE),
    re.compile(r"^(dioxyde)([a-zà-ÿ])", re.IGNORECASE),
    re.compile(r"^(peroxyde)([a-zà-ÿ])", re.IGNORECASE),
    re.compile(r"^(oxyde)([a-zà-ÿ])", re.IGNORECASE),
]

_HYDRATE_RE = re.compile(
    r"\b("
    r"anhydre|base|"
    r"sesquihydrat[ée]?|"
    r"monohydrat[ée]?|dihydrat[ée]?|trihydrat[ée]?|t[ée]trahydrat[ée]?|"
    r"pentahydrat[ée]?|h[ée]mihydrat[ée]?|hydrat[ée]?"
    r")\b",
    flags=re.IGNORECASE
)

_COUNTERION_TAIL_RE = re.compile(
    r"\b("
    r"arginine|sodique|sodium|potassique|potassium|calcique|calcium|magnesium|magn[eé]sium|lithium|ammonium|"
    r"zinc|cuivre|aluminium|manganese|manganèse"
    r")\b",
    flags=re.IGNORECASE
)

_DESC_TAIL_RE = re.compile(
    r"\b("
    r"humain|humaine|biog[eé]n[eé]tique|recombinant|recombinante|"
    r"biosynth[eé]tique|biotechnologique|analogue|synthetique|synth[eé]tique"
    r")\b",
    flags=re.IGNORECASE
)

def _pretty_segment(s: str) -> str:
    s = safe_text(s)
    if not s:
        return ""
    s = s.lower()
    return s[0].upper() + s[1:] if s else ""

def clean_to_main_dci(raw: str) -> str:
    s = safe_text(raw)
    if not s:
        return ""

    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"\([^)]*\)", " ", s)

    s = s.replace("\\", " ")
    s = s.replace("/", " / ")
    s = re.sub(r"\s+", " ", s).strip()

    s = _COMPLEX_PREFIX_RE.sub("", s)
    s = _NOISE_RE.sub(" ", s)

    for rgx in _PREFIX_GLUE_RES:
        s = rgx.sub(r"\1 \2", s)

    s = _SALT_GLUE_RE.sub(r"\1 \2", s)

    if re.match(r"^\s*(dioxyde|oxyde|peroxyde)\b", s, flags=re.IGNORECASE):
        return ""

    s = _SALT_PREFIX_RE.sub("", s)
    s = _SALT_LEADING_RE.sub("", s)
    s = _HYDRATE_RE.sub(" ", s)
    s = _DESC_TAIL_RE.sub(" ", s)

    s = re.sub(r"[;,:]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""

    tokens = s.split()
    while tokens and _COUNTERION_TAIL_RE.fullmatch(tokens[-1]):
        tokens.pop()
    s = " ".join(tokens).strip()
    if not s:
        return ""

    return _pretty_segment(s)

def parse_bdpm_compositions(txt: str) -> Dict[str, str]:
    cis_to_set: Dict[str, Dict[str, str]] = {}

    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue

        cis = re.sub(r"\D", "", parts[0].strip())
        if len(cis) != 8:
            continue

        denom = safe_text(parts[3])
        if not denom:
            continue

        denom_norm = denom.replace("\\|", "|").replace("|", "|")
        denom_norm = denom_norm.replace("/", "|")
        pieces = [p.strip() for p in denom_norm.split("|") if p.strip()]

        for piece in pieces:
            dci = clean_to_main_dci(piece)
            if not dci:
                continue
            key = dci.lower().strip()
            cis_to_set.setdefault(cis, {})
            cis_to_set[cis][key] = dci

    out: Dict[str, str] = {}
    for cis, kv in cis_to_set.items():
        values = list(kv.values())
        values.sort(key=lambda x: x.lower())
        out[cis] = " - ".join(values)

    ok(f"Compositions (DCI principales) chargées: {len(out)} CIS")
    return out

# ============================================================
# BDPM MITM (CIS -> ATC)
# ============================================================

def parse_mitm_cis_to_atc(txt: str) -> Dict[str, str]:
    cis_to_atc: Dict[str, str] = {}
    for line in (txt or "").splitlines():
        if not line.strip():
            continue
        cis_m = re.search(r"\b(\d{8})\b", line)
        if not cis_m:
            continue
        cis = cis_m.group(1)
        atc_m = re.search(r"\b([A-Z]\d{2}[A-Z]{2}\d{2})\b", line.upper())
        if not atc_m:
            continue
        atc = canonical_atc7(atc_m.group(1))
        if atc:
            cis_to_atc[cis] = atc
    ok(f"MITM (ATC) chargé: {len(cis_to_atc)} correspondances CIS->ATC")
    return cis_to_atc

# ============================================================
# BDPM Informations importantes (CIS -> URL)
# ============================================================

_URL_PAT = re.compile(r"(https?://[^\s\"'<>]+)", re.IGNORECASE)

def parse_info_importantes_cis_to_url(txt: str) -> Dict[str, str]:
    cis_to_url: Dict[str, str] = {}
    for line in (txt or "").splitlines():
        if not line.strip():
            continue
        cis_m = re.search(r"\b(\d{8})\b", line)
        if not cis_m:
            continue
        cis = cis_m.group(1)
        um = _URL_PAT.search(line)
        if not um:
            continue
        url = um.group(1).strip().rstrip(").,;")
        if url.startswith("http"):
            cis_to_url[cis] = url
    ok(f"Infos importantes chargées: {len(cis_to_url)} correspondances CIS->URL")
    return cis_to_url

# ============================================================
# URL HELPERS
# ============================================================

def base_extrait_url_from_cis(cis: str) -> str:
    return BDPM_DOC_EXTRACT_URL.format(cis=cis)

def set_tab(url: str, cis_fallback: str, tab: str) -> str:
    if not url or not url.startswith("http"):
        url = base_extrait_url_from_cis(cis_fallback)
    parts = urllib.parse.urlsplit(url)
    qs = urllib.parse.parse_qs(parts.query, keep_blank_values=True)
    qs["tab"] = [tab]
    new_query = urllib.parse.urlencode(qs, doseq=True)

    frag = parts.fragment or ""
    if re.search(r"\btab=", frag):
        frag = re.sub(r"tab=[^&]+", f"tab={tab}", frag)
    else:
        frag = f"tab={tab}"

    cleaned = parts._replace(query=new_query, fragment=frag)
    return urllib.parse.urlunsplit(cleaned)

def rcp_link_default(cis: str) -> str:
    return f"{base_extrait_url_from_cis(cis)}#tab=rcp"

# ============================================================
# FICHE-INFO SCRAPING (CPD/dispo)
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
            r = HTTP_SESSION.get(url, timeout=timeout, allow_redirects=True)
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

def detect_homeopathy_from_fiche_info(soup: BeautifulSoup) -> bool:
    text = soup.get_text("\n", strip=True)
    return bool(HOMEOPATHY_PAT.search(text) or HOMEOPATHY_CLASS_PAT.search(text))

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

def analyze_fiche_info(fiche_url: str) -> Tuple[str, bool, bool, bool]:
    html = fetch_html_checked(fiche_url)
    soup = BeautifulSoup(html, _bs_parser())

    is_homeo = detect_homeopathy_from_fiche_info(soup)

    cpd_text = extract_cpd_from_fiche_info(soup)
    cpd_text = capitalize_each_line(cpd_text)

    badge_usage = extract_badge_usage_hospitalier_only(soup)

    zone_text = cpd_text or ""
    if NEGATION_PAT.search(zone_text):
        reserved = False
        usage = False
    else:
        reserved = bool(RESERVED_HOSP_PAT.search(zone_text))
        usage = bool(USAGE_HOSP_PAT.search(zone_text)) or badge_usage

    return cpd_text, is_homeo, reserved, usage

# ============================================================
# DISPONIBILITE
# ============================================================

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
        requested_fields = list(fields) if fields else None

        while True:
            out: List[dict] = []
            params = {}
            if requested_fields:
                params["fields[]"] = requested_fields

            offset = None
            try:
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
            except Exception as e:
                msg = str(e)
                m = re.search(r'UNKNOWN_FIELD_NAME.*Unknown field name:\s*\\"([^\\"]+)\\"', msg)
                if not m:
                    m = re.search(r'UNKNOWN_FIELD_NAME.*Unknown field name:\s*"([^"]+)"', msg)
                if requested_fields and m:
                    bad = m.group(1)
                    warn(f"Airtable: champ inconnu '{bad}' -> retrait du filtre fields[] et retry")
                    requested_fields = [f for f in requested_fields if f != bad]
                    continue
                raise

    # ✅ NOUVEAU: inventaire filtré (évite de traiter 15 000 lignes à chaque run)
    def list_records_filtered(self, fields: Optional[List[str]] = None, filter_by_formula: str = "") -> List[dict]:
        requested_fields = list(fields) if fields else None

        while True:
            out: List[dict] = []
            params = {}
            if requested_fields:
                params["fields[]"] = requested_fields
            if filter_by_formula:
                params["filterByFormula"] = filter_by_formula

            offset = None
            try:
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
            except Exception as e:
                msg = str(e)
                m = re.search(r'UNKNOWN_FIELD_NAME.*Unknown field name:\s*\\"([^\\"]+)\\"', msg)
                if not m:
                    m = re.search(r'UNKNOWN_FIELD_NAME.*Unknown field name:\s*"([^"]+)"', msg)
                if requested_fields and m:
                    bad = m.group(1)
                    warn(f"Airtable: champ inconnu '{bad}' -> retrait du filtre fields[] et retry")
                    requested_fields = [f for f in requested_fields if f != bad]
                    continue
                raise

    def _strip_forbidden_fields(self, recs: List[dict]) -> None:
        for rec in recs:
            fields = rec.get("fields")
            if isinstance(fields, dict):
                for f in DO_NOT_WRITE_FIELDS:
                    fields.pop(f, None)

    def create_records(self, records: List[dict]) -> None:
        self._strip_forbidden_fields(records)
        for batch in chunked(records, AIRTABLE_BATCH_SIZE):
            payload = {"records": batch, "typecast": True}
            self._request("POST", self.table_url, data=json.dumps(payload))

    def update_records(self, records: List[dict]) -> None:
        self._strip_forbidden_fields(records)
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

    atc_labels = load_atc_equivalence_excel(ATC_EQUIVALENCE_FILE)

    info("Téléchargement BDPM CIS ...")
    cis_txt = download_text(BDPM_CIS_URL, encoding="latin-1")
    ok(f"BDPM CIS OK ({len(cis_txt)} chars)")

    info("Téléchargement BDPM CIS_CIP ...")
    cis_cip_txt = download_text(BDPM_CIS_CIP_URL, encoding="latin-1")
    ok(f"BDPM CIS_CIP OK ({len(cis_cip_txt)} chars)")

    info("Téléchargement BDPM COMPO ...")
    compo_txt = download_text(BDPM_COMPO_URL, encoding="latin-1")
    ok(f"BDPM COMPO OK ({len(compo_txt)} chars)")
    compo_map = parse_bdpm_compositions(compo_txt)

    info("Téléchargement BDPM MITM (ATC) ...")
    mitm_txt = download_text(BDPM_MITM_URL, encoding="latin-1")
    ok(f"BDPM MITM OK ({len(mitm_txt)} chars)")
    cis_to_atc = parse_mitm_cis_to_atc(mitm_txt)

    info("Téléchargement BDPM Informations importantes (génération en direct) ...")
    info_imp_txt = download_text(BDPM_INFO_IMPORTANTES_URL, encoding="latin-1")
    ok(f"BDPM Infos importantes OK ({len(info_imp_txt)} chars)")
    cis_to_info_url = parse_info_importantes_cis_to_url(info_imp_txt)

    info("Recherche lien Excel ANSM (rétrocession) ...")
    ansm_link = find_ansm_retro_excel_link()
    ok(f"Lien ANSM trouvé: {ansm_link}")

    info("Téléchargement Excel ANSM ...")
    ansm_bytes = download_bytes(ansm_link, timeout_s=140.0)
    ok(f"ANSM Excel OK ({len(ansm_bytes)} bytes)")

    cis_map = parse_bdpm_cis(cis_txt)
    cip_map = parse_bdpm_cis_cip(cis_cip_txt)
    ansm_retro_cis = parse_ansm_retrocession_cis(ansm_bytes, url_hint=ansm_link)

    at = AirtableClient(api_token, base_id, table_name)

    needed_fields = [
        FIELD_CIS,
        FIELD_RCP,
        FIELD_CIP13,
        FIELD_DISPO,
        FIELD_CPD,
        FIELD_LABO,
        FIELD_SPEC,
        FIELD_FORME,
        FIELD_VOIE,
        FIELD_ATC,
        FIELD_ATC4,
        FIELD_ATC_LABEL,
        FIELD_COMPOSITION,
        FIELD_COMPOSITION_DETAILS,
        FIELD_LIEN_INFO_IMPORTANTE,
        FIELD_INTERACTIONS_RCP,
        FIELD_INDICATIONS_RCP,
        FIELD_POSOLOGIE_RCP,
        FIELD_DATE_REVUE,
    ]

    # ✅ Inventaire filtré: on ne récupère que les lignes à compléter (sauf FORCE_REFRESH)
    if force_refresh:
        info("Inventaire Airtable (FORCE_REFRESH=1 => tout) ...")
        records = at.list_all_records(fields=needed_fields)
    else:
        info("Inventaire Airtable (filtré sur champs RCP manquants) ...")
        f = (
            f"OR("
            f"{{{FIELD_INDICATIONS_RCP}}}=BLANK(), {{{FIELD_INDICATIONS_RCP}}}='',"
            f"{{{FIELD_POSOLOGIE_RCP}}}=BLANK(), {{{FIELD_POSOLOGIE_RCP}}}='',"
            f"{{{FIELD_INTERACTIONS_RCP}}}=BLANK(), {{{FIELD_INTERACTIONS_RCP}}}=''"
            f")"
        )
        records = at.list_records_filtered(fields=needed_fields, filter_by_formula=f)

    ok(f"Enregistrements Airtable (ciblés): {len(records)}")

    airtable_by_cis: Dict[str, dict] = {}
    for rec in records:
        cis = str(rec.get("fields", {}).get(FIELD_CIS, "")).strip()
        cis = re.sub(r"\D", "", cis)
        if len(cis) == 8:
            airtable_by_cis[cis] = rec

    all_cis = sorted(list(airtable_by_cis.keys()))
    if max_cis > 0:
        all_cis = all_cis[:max_cis]
        warn(f"MAX_CIS_TO_PROCESS={max_cis} -> {len(all_cis)} CIS traités")

    review_ts = now_paris_iso_seconds()

    info("Enrichissement: contenu RCP + CPD/dispo + ATC + composition + lien info importante ...")
    info(f"Revue du jour (timestamp): {review_ts}")

    updates: List[dict] = []
    failures = 0
    deleted_count = 0
    start = time.time()

    fiche_checks = 0
    info_added = 0
    atc_added = 0
    rcp_checks = 0
    rcp_added = 0

    for idx, cis in enumerate(all_cis, start=1):
        if HEARTBEAT_EVERY > 0 and idx % HEARTBEAT_EVERY == 0:
            info(
                f"Heartbeat: {idx}/{len(all_cis)} (CIS={cis}) | fiche checks={fiche_checks} | "
                f"rcp checks={rcp_checks} | rcp added={rcp_added} | ATC added={atc_added} | info importante added={info_added}"
            )

        rec = airtable_by_cis.get(cis)
        if not rec:
            continue

        fields_cur = rec.get("fields", {}) or {}
        upd_fields: Dict[str, object] = {FIELD_DATE_REVUE: review_ts}

        link_rcp = str(fields_cur.get(FIELD_RCP, "")).strip()
        if not link_rcp:
            link_rcp = rcp_link_default(cis)
            upd_fields[FIELD_RCP] = link_rcp

        # --- CONTENU RCP
        cur_ind = str(fields_cur.get(FIELD_INDICATIONS_RCP, "")).strip()
        cur_poso = str(fields_cur.get(FIELD_POSOLOGIE_RCP, "")).strip()
        cur_inter = str(fields_cur.get(FIELD_INTERACTIONS_RCP, "")).strip()

        need_fetch_rcp = force_refresh or (not cur_ind) or (not cur_poso) or (not cur_inter)
        if need_fetch_rcp and link_rcp:
            try:
                rcp_checks += 1
                rcp_url = set_tab(link_rcp, cis, "rcp")
                html_rcp = fetch_html_checked(rcp_url)

                secs = extract_rcp_sections_from_rcp_html(html_rcp)
                ind = secs.get("indications_4_1", "").strip()
                poso = secs.get("posologie_4_2", "").strip()
                inter = format_interactions_field(
                    secs.get("mises_en_garde_4_4", ""),
                    secs.get("interactions_4_5", ""),
                )

                if ind and ind != cur_ind:
                    upd_fields[FIELD_INDICATIONS_RCP] = ind
                    rcp_added += 1
                if poso and poso != cur_poso:
                    upd_fields[FIELD_POSOLOGIE_RCP] = poso
                    rcp_added += 1
                if inter and inter != cur_inter:
                    upd_fields[FIELD_INTERACTIONS_RCP] = inter
                    rcp_added += 1

            except PageUnavailable as e:
                warn(f"RCP KO CIS={cis}: {e.detail} ({e.url}) (on continue)")
            except Exception as e:
                warn(f"RCP parse KO CIS={cis}: {e} (on continue)")

        updates.append({"id": rec["id"], "fields": upd_fields})

        if len(updates) >= UPDATE_FLUSH_THRESHOLD:
            at.update_records(updates)
            ok(f"Batch updates: {len(updates)}")
            updates = []

    if updates:
        at.update_records(updates)
        ok(f"Updates finaux: {len(updates)}")

    try_git_commit_report()
    ok(
        f"Terminé. échecs={failures} | supprimés={deleted_count} | "
        f"rcp checks={rcp_checks} | rcp added={rcp_added} | "
        f"ATC added={atc_added} | info importante added={info_added}"
    )

if __name__ == "__main__":
    main()
