#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import math
import random
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Excel read (xlrd) + parsing logic (pas via pandas pour éviter deps lourdes)
import xlrd  # type: ignore

# PDF text extraction fallback (optionnel)
try:
    from pypdf import PdfReader  # type: ignore
except Exception:  # pragma: no cover
    PdfReader = None  # type: ignore

# PDF extraction (recommandé)
try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None  # type: ignore

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
        # Text blob
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

    # heuristiques
    # Composition : section "Composition qualitative et quantitative"
    if "Composition" in text:
        compo = ""
    # Disponibilité : on tente de récupérer un texte autour de "Statut" / "Commercialisation"
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
# AIRTABLE API
# ============================================================

def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json"
    }

def airtable_url_table() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_CIS_TABLE_NAME)}"

def airtable_get_records_by_formula(formula: str, max_records: int = 100) -> List[Dict[str, Any]]:
    """
    Récupère des records Airtable via filterByFormula, gère pagination.
    """
    url = airtable_url_table()
    params = {
        "filterByFormula": formula,
        "pageSize": min(max_records, 100),
    }
    out: List[Dict[str, Any]] = []
    offset = None
    for _ in range(50):
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=airtable_headers(), params=params, timeout=HTTP_TIMEOUT)
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

def airtable_update_record(record_id: str, fields: Dict[str, Any]) -> bool:
    """
    Update partiel.
    """
    url = f"{airtable_url_table()}/{record_id}"
    payload = {"fields": fields}
    last = None
    for i in range(AIRTABLE_MAX_RETRY):
        try:
            r = requests.patch(url, headers=airtable_headers(), data=json.dumps(payload), timeout=HTTP_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:120]}")
            r.raise_for_status()
            return True
        except Exception as e:
            last = e
            sleep = AIRTABLE_SLEEP_BASE * (2 ** i) + random.random() * 0.25
            time.sleep(sleep)
    log.error("Airtable update failed: %s (%s)", record_id, last)
    return False


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
            # structure habituelle BDPM:
            # 0 CIS, 1 denom, 2 forme, 3 voie, 4 statut, 5 type, 6 state, 7 labo
            cis = (parts[0] if len(parts) > 0 else "").strip()
            denom = (parts[1] if len(parts) > 1 else "").strip()
            forme = (parts[2] if len(parts) > 2 else "").strip()
            voie = (parts[3] if len(parts) > 3 else "").strip()
            labo = (parts[7] if len(parts) > 7 else (parts[4] if len(parts) > 4 else "")).strip()
            if cis:
                rows.append(CisRow(cis=cis, speciality=denom, forme=forme, voie=voie, labo=labo))
    return rows


# ============================================================
# MAIN ENRICHMENT
# ============================================================

def enrich_one_record(record: Dict[str, Any], force_refresh: bool = False) -> bool:
    """
    Enrichit un record Airtable CIS:
    - composition / disponibilité / atc depuis BDPM (HTML)
    - fallback pdf text -> OCR si ATC vide
    """
    record_id = record.get("id", "")
    fields_cur = record.get("fields", {}) or {}

    cis = str(fields_cur.get(FIELD_CIS, "")).strip()
    if not cis:
        return False

    cur_spec = str(fields_cur.get(FIELD_SPEC, "")).strip()
    cur_forme = str(fields_cur.get(FIELD_FORME, "")).strip()
    cur_voie = str(fields_cur.get(FIELD_VOIE, "")).strip()
    cur_labo = str(fields_cur.get(FIELD_LABO, "")).strip()

    cur_cpd = str(fields_cur.get(FIELD_COMPO, "")).strip()
    cur_dispo = str(fields_cur.get(FIELD_DISPO, "")).strip()
    cur_atc_raw = str(fields_cur.get(FIELD_ATC, "")).strip()
    atc_is_blank = (cur_atc_raw == "")

    need_fetch_fiche = force_refresh or (not cur_cpd) or (not cur_dispo) or atc_is_blank

    upd_fields: Dict[str, Any] = {}

    if need_fetch_fiche:
        html = fetch_bdpm_doc_page(cis)
        compo, dispo, atc_code = parse_bdpm_compo_dispo_atc_from_doc_page(html)

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
                if pdf_url:
                    pdf_url_used = pdf_url
                    atc_from_pdf, pdf_method = get_atc_from_pdf_url(pdf_url)
                    atc_found = (atc_from_pdf or "").strip()

            if canonical_atc7(atc_found):
                upd_fields[FIELD_ATC] = atc_found
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
        return False

    return airtable_update_record(record_id, upd_fields)


def main():
    if not AIRTABLE_API_TOKEN or not AIRTABLE_BASE_ID or not AIRTABLE_CIS_TABLE_NAME:
        raise RuntimeError("AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME manquants")

    # Lecture du fichier BDPM CIS
    rows = read_cis_bdpm_txt(INPUT_FILE)
    log.info("CIS rows read: %s", len(rows))

    # Pour chaque CIS, on cherche le record et on enrichit
    # (NB: on suppose que les records existent déjà; ce script est un enrichissement)
    updates = 0
    total = 0

    for row in rows:
        total += 1
        formula = f"{{{FIELD_CIS}}}='{row.cis}'"
        recs = airtable_get_records_by_formula(formula, max_records=5)
        if not recs:
            continue
        ok_any = False
        for rec in recs:
            ok = enrich_one_record(rec, force_refresh=FORCE_REFRESH)
            if ok:
                ok_any = True
        if ok_any:
            updates += 1

        if total % 100 == 0:
            log.info("Progress: %s/%s, updates=%s", total, len(rows), updates)

    log.info(
        "Done. Records updated=%s | Reports: %s | OCR reports: %s",
        updates,
        report_path_pdf_atc_today(),
        report_path_pdf_atc_ocr_today(),
    )

if __name__ == "__main__":
    main()
