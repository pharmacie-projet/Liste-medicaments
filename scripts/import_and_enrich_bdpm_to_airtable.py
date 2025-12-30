#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import io
import sys
import json
import time
import math
import random
import shutil
import hashlib
import logging
import zipfile
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Excel read (xlrd) + parsing logic
import xlrd  # type: ignore

# PDF extraction: PyMuPDF (recommandé) + pypdf fallback
try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None  # type: ignore

try:
    from pypdf import PdfReader  # type: ignore
except Exception:  # pragma: no cover
    PdfReader = None  # type: ignore

# OCR fallback
try:
    from PIL import Image  # type: ignore
except Exception:  # pragma: no cover
    Image = None  # type: ignore

try:
    import pytesseract  # type: ignore
except Exception:  # pragma: no cover
    pytesseract = None  # type: ignore


# ============================================================
# ENV / CONFIG
# ============================================================

load_dotenv()

AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip() or os.getenv("AIRTABLE_TABLE_NAME", "").strip()

# Source file (BDPM)
INPUT_FILE = os.getenv("INPUT_FILE", "CIS_bdpm.txt").strip()

# OCR controls
OCR_ENABLE = os.getenv("OCR_ENABLE", "0").strip() in ("1", "true", "True", "YES", "yes")
OCR_MAX_PAGES = int(os.getenv("OCR_MAX_PAGES", "2").strip() or "2")
OCR_DPI = int(os.getenv("OCR_DPI", "200").strip() or "200")
OCR_PSM = int(os.getenv("OCR_PSM", "6").strip() or "6")

# Reports
REPORT_DIR = os.getenv("REPORT_DIR", "reports").strip() or "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# Airtable rate limits
AIRTABLE_MAX_RETRY = int(os.getenv("AIRTABLE_MAX_RETRY", "8"))
AIRTABLE_SLEEP_BASE = float(os.getenv("AIRTABLE_SLEEP_BASE", "1.2"))

# Network
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "45.0"))
HTTP_RETRY = int(os.getenv("HTTP_RETRY", "5"))
HTTP_SLEEP_BASE = float(os.getenv("HTTP_SLEEP_BASE", "0.9"))

# Behavior
FORCE_REFRESH = os.getenv("FORCE_REFRESH", "0").strip() in ("1", "true", "True", "YES", "yes")

# Airtable fields (CIS table)
FIELD_CIS = os.getenv("FIELD_CIS", "Code cis")
FIELD_SPEC = os.getenv("FIELD_SPEC", "Spécialité")
FIELD_FORME = os.getenv("FIELD_FORME", "Forme")
FIELD_VOIE = os.getenv("FIELD_VOIE", "Voie d'administration")
FIELD_LABO = os.getenv("FIELD_LABO", "Laboratoire")
FIELD_COMPO = os.getenv("FIELD_COMPO", "Composition")  # champ texte (optionnel)
FIELD_DISPO = os.getenv("FIELD_DISPO", "Disponibilité")
FIELD_ATC = os.getenv("FIELD_ATC", "Code ATC")

# Base URLs
BDPM_BASE = "https://base-donnees-publique.medicaments.gouv.fr"
BDPM_SEARCH = f"{BDPM_BASE}/affichageDoc.php?specid={{cis}}&typedoc=R"
BDPM_RCP_NOTICE = f"{BDPM_BASE}/extrait.php?specid={{cis}}"

# Headers
UA = os.getenv("HTTP_USER_AGENT", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Safari/537.36").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("import_bdpm")


# ============================================================
# UTIL / LOG
# ============================================================

def info(msg: str):
    log.info(msg)

def ok(msg: str):
    log.info("✅ " + msg)

def warn(msg: str):
    log.warning("⚠️ " + msg)

def err(msg: str):
    log.error("❌ " + msg)

def sha1_bytes(b: bytes) -> str:
    h = hashlib.sha1()
    h.update(b)
    return h.hexdigest()

def try_git_commit_report():
    """
    Si le repo est un checkout git et que reports/ a changé, commit + push (optionnel).
    Ne plante jamais si git absent.
    """
    try:
        if not os.path.isdir(".git"):
            return
        subprocess.run(["git", "status"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "add", REPORT_DIR], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        r = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False)
        if r.returncode == 0:
            return
        subprocess.run(["git", "commit", "-m", f"update reports {time.strftime('%Y-%m-%d %H:%M:%S')}"], check=False)
        subprocess.run(["git", "push"], check=False)
    except Exception:
        return


# ============================================================
# ATC HELPERS
# ============================================================

ATC7_PAT = re.compile(r"^[A-Z]\d{2}[A-Z]{2}\d{2}$")  # ex A11CA01
ATC5_PAT = re.compile(r"^[A-Z]\d{2}[A-Z]{2}$")       # ex A11CA

# pattern tolérant (espaces entre chaque caractère)
ATC7_ANY_FLEX_PAT = re.compile(r"\b([A-Z]\s*\d\s*\d\s*[A-Z]\s*[A-Z]\s*\d\s*\d)\b", re.IGNORECASE)
ATC7_NEAR_ATC_WORD = re.compile(
    r"(?:\bATC\b|code\s*ATC|ATC\s*code)\s*[:\-]?\s*([A-Z]\s*\d\s*\d\s*[A-Z]\s*[A-Z]\s*\d\s*\d)",
    re.IGNORECASE
)

def canonical_atc7(raw: str) -> str:
    """Retourne un ATC7 canonique (sans espaces/ponctuation) si valide, sinon ''."""
    if not raw:
        return ""
    s = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return s if ATC7_PAT.fullmatch(s) else ""

def format_atc_like_found(found: str) -> str:
    """
    Si le texte trouvé contient au moins un espace/retour/ponctuation -> renvoie 'XXXX XXX' (format espace).
    Sinon -> renvoie 'XXXXXXX'.
    """
    canon = canonical_atc7(found)
    if not canon:
        return ""
    has_separator = bool(re.search(r"\s|\.", found))
    return f"{canon[:4]} {canon[4:]}" if has_separator else canon

def extract_atc_from_text_blob(text: str) -> str:
    """
    ATC depuis un texte (HTML brut, HTML->text, PDF->text, OCR):
    - match avec ou sans espace/ponctuation
    - retourne 'C10A A07' si écrit avec séparateur, sinon 'C10AA07'
    """
    if not text:
        return ""
    t = (text or "").replace("\u00a0", " ")

    m = ATC7_NEAR_ATC_WORD.search(t)
    if m:
        out = format_atc_like_found(m.group(1))
        if out:
            return out

    m2 = ATC7_ANY_FLEX_PAT.search(t)
    if m2:
        out = format_atc_like_found(m2.group(1))
        if out:
            return out

    return ""


# ============================================================
# REPORTS
# ============================================================

def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def report_path_pdf_atc_today() -> str:
    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = f"atc_from_pdf_{time.strftime('%Y-%m-%d')}.tsv"
    return os.path.join(REPORT_DIR, fname)

def report_path_pdf_atc_ocr_today() -> str:
    """Backup des ATC trouvés via OCR uniquement (inclut le nom de spécialité)."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = f"atc_from_pdf_ocr_{time.strftime('%Y-%m-%d')}.tsv"
    return os.path.join(REPORT_DIR, fname)

def append_pdf_atc_report(cis: str, atc: str, pdf_url: str, origin_url: str):
    p = report_path_pdf_atc_today()
    header = "timestamp\tcis\tatc\tpdf_url\torigin_url\n"
    if not os.path.exists(p):
        with open(p, "w", encoding="utf-8") as f:
            f.write(header)
    line = f"{_ts()}\t{cis}\t{atc}\t{pdf_url}\t{origin_url}\n"
    with open(p, "a", encoding="utf-8") as f:
        f.write(line)

def append_pdf_atc_ocr_report(cis: str, speciality: str, atc: str, pdf_url: str, origin_url: str):
    p = report_path_pdf_atc_ocr_today()
    header = "timestamp\tcis\tspecialite\tatc\tpdf_url\torigin_url\n"
    if not os.path.exists(p):
        with open(p, "w", encoding="utf-8") as f:
            f.write(header)
    line = f"{_ts()}\t{cis}\t{speciality}\t{atc}\t{pdf_url}\t{origin_url}\n"
    with open(p, "a", encoding="utf-8") as f:
        f.write(line)


# ============================================================
# HTTP / DOWNLOAD
# ============================================================

def http_get(url: str, timeout_s: float = HTTP_TIMEOUT) -> requests.Response:
    headers = {"User-Agent": UA}
    last = None
    for i in range(HTTP_RETRY):
        try:
            r = requests.get(url, headers=headers, timeout=timeout_s)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
        except Exception as e:
            last = e
            sleep = HTTP_SLEEP_BASE * (2 ** i) + random.random() * 0.25
            time.sleep(sleep)
    raise RuntimeError(f"GET failed: {url} ({last})")

def download_bytes(url: str, timeout_s: float = 160.0) -> bytes:
    r = http_get(url, timeout_s=timeout_s)
    r.raise_for_status()
    return r.content


# ============================================================
# BDPM SCRAPING
# ============================================================

def safe_text(el: Any) -> str:
    if el is None:
        return ""
    return " ".join(el.get_text(" ", strip=True).split())

def analyze_rcp_html_for_atc(rcp_html_url: str) -> str:
    """
    Fallback: cherche un code ATC dans la page HTML (RCP/Notice) si présent.
    """
    if not rcp_html_url:
        return ""
    try:
        r = http_get(rcp_html_url)
        r.raise_for_status()
        html = r.text
        soup = BeautifulSoup(html, "lxml")
        txt = soup.get_text("\n", strip=True)
        return extract_atc_from_text_blob(txt)
    except Exception:
        return ""

def find_pdf_url_from_rcp_notice_page(rcp_notice_url: str) -> str:
    """
    Sur base-donnees-publique.medicaments.gouv.fr, un lien PDF peut être présent.
    Cette fonction tente de trouver un href .pdf.
    """
    if not rcp_notice_url:
        return ""
    try:
        r = http_get(rcp_notice_url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a"):
            href = a.get("href") or ""
            if ".pdf" in href.lower():
                if href.startswith("http"):
                    return href
                if href.startswith("/"):
                    return BDPM_BASE + href
                return BDPM_BASE + "/" + href
        return ""
    except Exception:
        return ""

def fetch_bdpm_doc_page(cis: str) -> str:
    url = BDPM_SEARCH.format(cis=cis)
    try:
        r = http_get(url)
        r.raise_for_status()
        return r.text
    except Exception:
        return ""

def parse_bdpm_compo_dispo_atc_from_doc_page(html: str) -> Tuple[str, str, str]:
    """
    Extraction légère: composition + disponibilité + atc si visible.
    """
    if not html:
        return "", "", ""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)
    atc = extract_atc_from_text_blob(text)

    compo = ""
    dispo = ""

    if "Composition" in text:
        compo = ""

    for key in ("Commercialisation", "Statut", "Rupture", "Disponibilité"):
        if key.lower() in text.lower():
            dispo = key
            break

    return compo, dispo, atc


# ============================================================
# PDF ATC EXTRACTION (TEXT THEN OCR)
# ============================================================

def _ocr_available() -> bool:
    return bool(OCR_ENABLE and (fitz is not None) and (Image is not None) and (pytesseract is not None))

def ocr_text_from_pdf_bytes(pdf_bytes: bytes, max_pages: int = 2, dpi: int = 200) -> str:
    """OCR le PDF (pages rendues via PyMuPDF) et renvoie un blob texte."""
    if not pdf_bytes or fitz is None or Image is None or pytesseract is None:
        return ""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        n = len(doc)
        limit = n
        if max_pages and max_pages > 0:
            limit = min(n, max_pages)
        zoom = max(1.0, float(dpi) / 72.0)
        mat = fitz.Matrix(zoom, zoom)
        parts: List[str] = []
        for i in range(limit):
            try:
                page = doc.load_page(i)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                txt = pytesseract.image_to_string(img, config=f"--psm {OCR_PSM}") or ""
                parts.append(txt)
            except Exception:
                continue
        return "\n\n".join(parts)
    except Exception:
        return ""

def extract_atc_from_pdf_bytes_with_method(pdf_bytes: bytes, max_pages_text: int = 30) -> Tuple[str, str]:
    """Retourne (atc, method) où method ∈ {'TEXT','OCR',''}"""
    if not pdf_bytes:
        return "", ""

    # 1) Extraction texte (rapide)
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            n = len(doc)
            limit = min(n, max_pages_text if max_pages_text and max_pages_text > 0 else n)
            for i in range(limit):
                txt = doc.load_page(i).get_text("text") or ""
                atc = extract_atc_from_text_blob(txt)
                if atc:
                    return atc, "TEXT"
        except Exception:
            pass

    # fallback pypdf (texte)
    if PdfReader is not None:
        try:
            from io import BytesIO
            reader = PdfReader(BytesIO(pdf_bytes))
            limit = min(len(reader.pages), max_pages_text if max_pages_text and max_pages_text > 0 else len(reader.pages))
            for i in range(limit):
                txt = reader.pages[i].extract_text() or ""
                atc = extract_atc_from_text_blob(txt)
                if atc:
                    return atc, "TEXT"
        except Exception:
            pass

    # 2) OCR (si activé)
    if _ocr_available():
        ocr_txt = ocr_text_from_pdf_bytes(pdf_bytes, max_pages=OCR_MAX_PAGES, dpi=OCR_DPI)
        if ocr_txt:
            atc = extract_atc_from_text_blob(ocr_txt)
            if atc:
                return atc, "OCR"

    return "", ""

def extract_atc_from_pdf_bytes(pdf_bytes: bytes, max_pages: int = 30) -> str:
    """Compat: renvoie seulement le code ATC."""
    atc, _method = extract_atc_from_pdf_bytes_with_method(pdf_bytes, max_pages_text=max_pages)
    return atc

def get_atc_from_pdf_url(pdf_url: str) -> Tuple[str, str]:
    """Télécharge le PDF et retourne (atc, method)."""
    if not pdf_url:
        return "", ""
    try:
        b = download_bytes(pdf_url, timeout_s=160.0)
    except Exception:
        return "", ""
    return extract_atc_from_pdf_bytes_with_method(b)

# ============================================================
# AIRTABLE API (version complète utilisée par le projet)
# ============================================================

class AirtableClient:
    def __init__(self, token: str, base_id: str, table_name: str):
        self.token = token
        self.base_id = base_id
        self.table_name = table_name

    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def table_url(self) -> str:
        return f"https://api.airtable.com/v0/{self.base_id}/{requests.utils.quote(self.table_name)}"

    def list_records(self, filter_formula: Optional[str] = None, fields: Optional[List[str]] = None, page_size: int = 100) -> List[Dict[str, Any]]:
        url = self.table_url()
        params: Dict[str, Any] = {"pageSize": min(page_size, 100)}
        if filter_formula:
            params["filterByFormula"] = filter_formula
        if fields:
            for f in fields:
                params.setdefault("fields[]", [])
                params["fields[]"].append(f)

        out: List[Dict[str, Any]] = []
        offset = None
        for _ in range(200):
            if offset:
                params["offset"] = offset
            r = requests.get(url, headers=self.headers(), params=params, timeout=HTTP_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.2)
                continue
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return out

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> bool:
        url = f"{self.table_url()}/{record_id}"
        payload = {"fields": fields}
        last = None
        for i in range(AIRTABLE_MAX_RETRY):
            try:
                r = requests.patch(url, headers=self.headers(), data=json.dumps(payload), timeout=HTTP_TIMEOUT)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:180]}")
                r.raise_for_status()
                return True
            except Exception as e:
                last = e
                sleep = AIRTABLE_SLEEP_BASE * (2 ** i) + random.random() * 0.25
                time.sleep(sleep)
        err(f"Airtable update failed: {record_id} ({last})")
        return False

    def update_records(self, updates: List[Tuple[str, Dict[str, Any]]], chunk: int = 10):
        """
        Batch PATCH (Airtable accepte 10 max par requête)
        """
        if not updates:
            return
        url = self.table_url()
        for i in range(0, len(updates), chunk):
            batch = updates[i:i+chunk]
            payload = {"records": [{"id": rid, "fields": fields} for rid, fields in batch]}
            last = None
            for attempt in range(AIRTABLE_MAX_RETRY):
                try:
                    r = requests.patch(url, headers=self.headers(), data=json.dumps(payload), timeout=HTTP_TIMEOUT)
                    if r.status_code in (429, 500, 502, 503, 504):
                        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:180]}")
                    r.raise_for_status()
                    break
                except Exception as e:
                    last = e
                    sleep = AIRTABLE_SLEEP_BASE * (2 ** attempt) + random.random() * 0.25
                    time.sleep(sleep)
            else:
                err(f"Batch update failed ({i}-{i+len(batch)}): {last}")


# ============================================================
# PARSING CIS_bdpm.txt
# ============================================================

@dataclass
class CisRow:
    cis: str
    speciality: str
    forme: str
    voie: str
    labo: str

def read_cis_bdpm_txt(path: str) -> List[CisRow]:
    rows: List[CisRow] = []
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            cis = (parts[0] if len(parts) > 0 else "").strip()
            denom = (parts[1] if len(parts) > 1 else "").strip()
            forme = (parts[2] if len(parts) > 2 else "").strip()
            voie = (parts[3] if len(parts) > 3 else "").strip()
            labo = (parts[7] if len(parts) > 7 else (parts[4] if len(parts) > 4 else "")).strip()
            if cis:
                rows.append(CisRow(cis=cis, speciality=denom, forme=forme, voie=voie, labo=labo))
    return rows


# ============================================================
# EQUIVALENCE ATC (xlsx) - utilitaire (si présent)
# ============================================================

def load_atc_equivalence_xlsx(path: str) -> Dict[str, str]:
    """
    Charge le fichier Excel d'équivalence ATC (si présent) pour mapping.
    Retourne {CIS -> ATC}
    """
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_excel(path)
        # colonnes attendues possibles
        # CIS / Code cis / cis ; ATC / Code ATC
        cis_col = None
        atc_col = None
        for c in df.columns:
            lc = str(c).strip().lower()
            if lc in ("cis", "code cis", "codecis"):
                cis_col = c
            if lc in ("atc", "code atc", "codeatc"):
                atc_col = c
        if cis_col is None or atc_col is None:
            return {}
        out: Dict[str, str] = {}
        for _, row in df.iterrows():
            cis = str(row.get(cis_col, "")).strip()
            atc = str(row.get(atc_col, "")).strip()
            if cis and canonical_atc7(atc):
                out[cis] = atc
        return out
    except Exception:
        return {}


# ============================================================
# MAIN ENRICHMENT (version projet)
# ============================================================

def enrich_one_record(
    at: AirtableClient,
    record: Dict[str, Any],
    force_refresh: bool = False,
) -> Tuple[bool, int, int, int, int]:
    """
    Enrichit un record Airtable CIS:
    - composition / disponibilité / atc depuis BDPM (HTML)
    - fallback pdf text -> OCR si ATC vide
    Retourne: (updated, rcp_checks, pdf_checks, pdf_hits, pdf_atc_added)
    """
    record_id = record.get("id", "")
    fields_cur = record.get("fields", {}) or {}

    cis = str(fields_cur.get(FIELD_CIS, "")).strip()
    if not cis:
        return False, 0, 0, 0, 0

    cur_spec = str(fields_cur.get(FIELD_SPEC, "")).strip()
    cur_cpd = str(fields_cur.get(FIELD_COMPO, "")).strip()
    cur_dispo = str(fields_cur.get(FIELD_DISPO, "")).strip()
    cur_atc_raw = str(fields_cur.get(FIELD_ATC, "")).strip()
    atc_is_blank = (cur_atc_raw == "")

    need_fetch_fiche = force_refresh or (not cur_cpd) or (not cur_dispo) or atc_is_blank

    upd_fields: Dict[str, Any] = {}
    rcp_checks = 0
    pdf_checks = 0
    pdf_hits = 0
    pdf_atc_added = 0

    if need_fetch_fiche:
        html = fetch_bdpm_doc_page(cis)
        compo, dispo, atc_code = parse_bdpm_compo_dispo_atc_from_doc_page(html)
        rcp_checks += 1

        if compo and (force_refresh or not cur_cpd):
            upd_fields[FIELD_COMPO] = compo

        if dispo and (force_refresh or not cur_dispo):
            upd_fields[FIELD_DISPO] = dispo

        # ATC: n'écrase que si vide
        atc_found = ""
        if atc_is_blank:
            atc_found = (atc_code or "").strip()

            # fallback RCP HTML
            if not canonical_atc7(atc_found):
                rcp_html_url = BDPM_SEARCH.format(cis=cis)
                atc_found = analyze_rcp_html_for_atc(rcp_html_url).strip()

            # fallback PDF (TEXT puis OCR)
            pdf_url_used = ""
            pdf_method = ""
            if not canonical_atc7(atc_found):
                rcp_notice_url = BDPM_RCP_NOTICE.format(cis=cis)
                pdf_url = find_pdf_url_from_rcp_notice_page(rcp_notice_url)
                pdf_checks += 1
                if pdf_url:
                    pdf_hits += 1
                    pdf_url_used = pdf_url
                    atc_from_pdf, pdf_method = get_atc_from_pdf_url(pdf_url)
                    atc_found = (atc_from_pdf or "").strip()

            if canonical_atc7(atc_found):
                upd_fields[FIELD_ATC] = atc_found
                pdf_atc_added += 1
                if pdf_url_used:
                    append_pdf_atc_report(cis=cis, atc=atc_found, pdf_url=pdf_url_used, origin_url=BDPM_RCP_NOTICE.format(cis=cis))
                    if (pdf_method or "").upper() == "OCR":
                        append_pdf_atc_ocr_report(
                            cis=cis,
                            speciality=cur_spec,
                            atc=atc_found,
                            pdf_url=pdf_url_used,
                            origin_url=BDPM_RCP_NOTICE.format(cis=cis)
                        )

    if not upd_fields:
        return False, rcp_checks, pdf_checks, pdf_hits, pdf_atc_added

    ok_upd = at.update_record(record_id, upd_fields)
    return ok_upd, rcp_checks, pdf_checks, pdf_hits, pdf_atc_added


def main():
    if not AIRTABLE_API_TOKEN or not AIRTABLE_BASE_ID or not AIRTABLE_CIS_TABLE_NAME:
        raise RuntimeError("AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME manquants")

    at = AirtableClient(AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_CIS_TABLE_NAME)

    # Lecture BDPM
    rows = read_cis_bdpm_txt(INPUT_FILE)
    info(f"CIS rows read: {len(rows)}")

    # Index CIS -> row
    all_cis = [r.cis for r in rows]
    all_cis_set = set(all_cis)

    # Optionnel: mapping equivalence atc.xlsx (si dispo)
    equiv_path = os.path.join("data", "equivalence atc.xlsx")
    eq_map = load_atc_equivalence_xlsx(equiv_path)
    if eq_map:
        info(f"Equivalence ATC chargée: {len(eq_map)} lignes")

    # Récup records Airtable (en une fois) pour accélérer
    fields = [FIELD_CIS, FIELD_SPEC, FIELD_COMPO, FIELD_DISPO, FIELD_ATC]
    records = at.list_records(fields=fields, page_size=100)
    info(f"Airtable records fetched: {len(records)}")

    # Index records par CIS
    rec_by_cis: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        cis = str((rec.get("fields") or {}).get(FIELD_CIS, "")).strip()
        if not cis:
            continue
        rec_by_cis.setdefault(cis, []).append(rec)

    updates: List[Tuple[str, Dict[str, Any]]] = []
    failures = 0
    deleted_count = 0

    rcp_checks = 0
    pdf_checks = 0
    pdf_hits = 0
    pdf_atc_added = 0

    t0 = time.time()

    for idx, cis in enumerate(all_cis, start=1):
        recs = rec_by_cis.get(cis, [])
        if not recs:
            continue

        # Si on a un mapping d'ATC et que le champ est vide, on peut remplir direct (avant OCR)
        for rec in recs:
            fields_cur = rec.get("fields", {}) or {}
            cur_atc = str(fields_cur.get(FIELD_ATC, "")).strip()
            if cur_atc == "" and cis in eq_map and canonical_atc7(eq_map[cis]):
                updates.append((rec["id"], {FIELD_ATC: eq_map[cis]}))

        # Enrich BDPM/OCR
        for rec in recs:
            try:
                updated, rcp_c, pdf_c, pdf_h, pdf_a = enrich_one_record(at, rec, force_refresh=FORCE_REFRESH)
                rcp_checks += rcp_c
                pdf_checks += pdf_c
                pdf_hits += pdf_h
                pdf_atc_added += pdf_a
                if not updated:
                    continue
            except Exception:
                failures += 1

        # flush updates en batch
        if len(updates) >= 50:
            at.update_records(updates)
            ok(f"Batch updates: {len(updates)}")
            updates = []

        if idx % 100 == 0:
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 0
            remaining = (len(all_cis) - idx) / rate if rate > 0 else 0
            info(
                f"Progress {idx}/{len(all_cis)} | {rate:.2f} CIS/s | échecs: {failures} | supprimés: {deleted_count} "
                f"| RCP checks: {rcp_checks} | PDF checks: {pdf_checks} | PDF hits: {pdf_hits} | PDF ATC added: {pdf_atc_added} | reste ~{int(remaining)}s"
            )

    if updates:
        at.update_records(updates)
        ok(f"Updates finaux: {len(updates)}")

    try_git_commit_report()
    ok(
        f"Terminé. échecs: {failures} | supprimés: {deleted_count} | "
        f"RCP checks: {rcp_checks} | PDF checks: {pdf_checks} | PDF hits: {pdf_hits} | PDF ATC added: {pdf_atc_added} | "
        f"rapport PDF: {report_path_pdf_atc_today()} | rapport OCR: {report_path_pdf_atc_ocr_today()}"
    )

if __name__ == "__main__":
    main()
