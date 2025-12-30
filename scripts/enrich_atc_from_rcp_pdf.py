import os
import re
import csv
import time
from typing import Optional, Tuple, Dict, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "")

FIELD_CIS = os.getenv("AIRTABLE_FIELD_CIS", "Code cis")
FIELD_ATC = os.getenv("AIRTABLE_FIELD_ATC", "Code ATC")
FIELD_RCP_LINK = os.getenv("AIRTABLE_FIELD_RCP_LINK", "Lien vers RCP")
FIELD_SPECIALITE = os.getenv("AIRTABLE_FIELD_SPECIALITE", "Spécialité")  # optionnel
FIELD_ATC_LABEL = os.getenv("AIRTABLE_FIELD_ATC_LABEL", "Libellé ATC")  # optionnel

ATC_MAPPING_CSV = os.getenv("ATC_MAPPING_CSV", "")  # optionnel (csv atc_code,label)
REQUEST_SLEEP_SEC = float(os.getenv("REQUEST_SLEEP_SEC", "0.15"))

USER_AGENT = os.getenv(
    "HTTP_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

# ---------- PDF engine ----------
PDF_ENGINE = None
try:
    import fitz  # PyMuPDF
    PDF_ENGINE = "pymupdf"
except Exception:
    PDF_ENGINE = None

# ---------- Regex ATC (accepte espaces / ponctuation) ----------
# ATC complet = 7 caractères : LDDLLDD, ex: C10AA07
ATC_FLEX_RE = re.compile(
    r"(?:\bATC\b|code\s*ATC|ATC\s*code)\s*[:\-]?\s*"
    r"([A-Z]\s*\d\s*\d\s*[A-Z]\s*[A-Z]\s*\d\s*\d)",
    re.IGNORECASE
)

# Fallback: trouve un motif ATC 7 chars même si "ATC" n'est pas juste à côté
ATC_7_ANYWHERE_FLEX_RE = re.compile(
    r"\b([A-Z]\s*\d\s*\d\s*[A-Z]\s*[A-Z]\s*\d\s*\d)\b",
    re.IGNORECASE
)

VALID_ATC7_RE = re.compile(r"^[A-Z]\d{2}[A-Z]{2}\d{2}$")


def log(msg: str) -> None:
    print(msg, flush=True)


def normalize_atc(candidate: str) -> Optional[str]:
    """
    Normalise un ATC potentiellement écrit avec espaces/points.
    Ex: 'C10A A07' -> 'C10AA07', 'N05A H03.' -> 'N05AH03'
    """
    if not candidate:
        return None
    s = re.sub(r"[^A-Za-z0-9]", "", candidate).upper()
    if VALID_ATC7_RE.match(s):
        return s
    return None


def is_http_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https")
    except Exception:
        return False


def safe_urljoin(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("javascript:") or href.startswith("mailto:"):
        return None
    # liens du type //ema.europa.eu/...
    if href.startswith("//"):
        href = "https:" + href
    try:
        u = urljoin(base, href)
        if is_http_url(u):
            return u
        return None
    except Exception:
        return None


def requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    })
    return s


def extract_atc_from_text(text: str) -> Optional[str]:
    if not text:
        return None

    # 1) Priorité: motif près de "ATC"
    m = ATC_FLEX_RE.search(text)
    if m:
        atc = normalize_atc(m.group(1))
        if atc:
            return atc

    # 2) Sinon: un motif ATC 7 chars n'importe où
    m2 = ATC_7_ANYWHERE_FLEX_RE.search(text)
    if m2:
        atc = normalize_atc(m2.group(1))
        if atc:
            return atc

    return None


def fetch_html_text(sess: requests.Session, url: str, timeout: int = 25) -> Optional[str]:
    time.sleep(REQUEST_SLEEP_SEC)
    r = sess.get(url, timeout=timeout, allow_redirects=True)
    if r.status_code >= 400:
        return None
    return r.text


def extract_atc_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    return extract_atc_from_text(text)


def find_pdf_url_in_html(sess: requests.Session, base_url: str, html: str) -> Optional[str]:
    """
    Cherche un lien PDF direct ou un lien EMA/ANSM menant à un PDF.
    (corrigé: sess passé en paramètre)
    """
    soup = BeautifulSoup(html, "lxml")
    links = []

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        u = safe_urljoin(base_url, href)
        if not u:
            continue
        links.append((a.get_text(" ", strip=True).lower(), u))

    # 1) PDF direct prioritaire
    for label, u in links:
        if u.lower().endswith(".pdf"):
            return u

    # 2) Liens “Vers le RCP” / “RCP et notice” / EMA
    priority = []
    for label, u in links:
        if ("rcp" in label) or ("notice" in label) or ("product-information" in u) or ("ema.europa.eu" in u):
            priority.append(u)

    # 3) Suivre quelques liens prioritaires et trouver un PDF dedans
    for u in priority[:12]:
        if u.lower().endswith(".pdf"):
            return u
        try:
            html2 = fetch_html_text(sess, u)
        except Exception:
            continue
        if not html2:
            continue
        pdf = find_pdf_url_in_html(sess, u, html2)
        if pdf:
            return pdf

    return None


def extract_atc_from_pdf_bytes(pdf_bytes: bytes) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Retourne (atc, page_index_1based, contexte)
    """
    if not pdf_bytes:
        return None, None, None
    if PDF_ENGINE != "pymupdf":
        return None, None, None

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    for i in range(len(doc)):
        page = doc.load_page(i)
        text = page.get_text("text") or ""
        atc = extract_atc_from_text(text)
        if atc:
            idx = text.lower().find("atc")
            snippet = None
            if idx != -1:
                start = max(0, idx - 90)
                end = min(len(text), idx + 160)
                snippet = re.sub(r"\s+", " ", text[start:end]).strip()
            return atc, i + 1, snippet
    return None, None, None


def airtable_list_records(sess: requests.Session) -> List[dict]:
    if not (AIRTABLE_API_TOKEN and AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME):
        raise RuntimeError("Variables Airtable manquantes (AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME)")

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_TOKEN}"}

    # Filtre: seulement les lignes où ATC est vide (=> beaucoup moins d'appels)
    filter_formula = f"AND({{{FIELD_CIS}}}!='', OR({{{FIELD_ATC}}}='', {{"+FIELD_ATC+"}}=BLANK()))"

    params = {
        "pageSize": 100,
        "filterByFormula": filter_formula,
        "fields[]": [FIELD_CIS, FIELD_ATC, FIELD_RCP_LINK, FIELD_SPECIALITE],
    }

    records = []
    offset = None
    while True:
        if offset:
            params["offset"] = offset
        r = sess.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def airtable_batch_update(sess: requests.Session, updates: List[dict]) -> None:
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_TOKEN}", "Content-Type": "application/json"}

    for i in range(0, len(updates), 10):
        chunk = updates[i:i+10]
        payload = {"records": chunk}
        r = sess.patch(url, headers=headers, json=payload, timeout=45)
        r.raise_for_status()


def load_atc_mapping() -> Dict[str, str]:
    if not ATC_MAPPING_CSV:
        return {}
    if not os.path.exists(ATC_MAPPING_CSV):
        log(f"[WARN] ATC_MAPPING_CSV introuvable: {ATC_MAPPING_CSV} (libellés ATC ignorés)")
        return {}

    mapping = {}
    with open(ATC_MAPPING_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = normalize_atc(row.get("atc_code", "") or row.get("ATC", "") or row.get("code", ""))
            label = (row.get("label", "") or row.get("Libellé", "") or row.get("libelle", "")).strip()
            if code and label:
                mapping[code] = label
    log(f"[OK] Mapping ATC chargé: {len(mapping)} entrées")
    return mapping


def bdm_urls(cis: str) -> Dict[str, str]:
    base = f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/"
    fiche_info = base
    rcp_html = urljoin(base, "extrait?tab=rcp")
    rcp_notice = urljoin(base, "extrait?tab=rcp-et-notice")
    return {"fiche_info": fiche_info, "rcp_html": rcp_html, "rcp_notice": rcp_notice}


def main():
    if PDF_ENGINE != "pymupdf":
        log("[ERREUR] PyMuPDF non dispo => impossible de lire les PDF. Ajoute PyMuPDF==1.24.14 dans requirements.txt")
        raise SystemExit(1)

    sess = requests_session()
    atc_map = load_atc_mapping()

    log("📥 Lecture Airtable (lignes sans ATC) ...")
    missing = airtable_list_records(sess)
    log(f"🔎 Lignes sans ATC: {len(missing)}")

    updates = []
    pdf_report_rows = []

    rcp_checks = 0
    pdf_checks = 0
    pdf_hits = 0
    atc_added = 0

    for idx, rec in enumerate(missing, start=1):
        rid = rec["id"]
        fields = rec.get("fields", {})
        cis = str(fields.get(FIELD_CIS) or "").strip()
        spec = str(fields.get(FIELD_SPECIALITE) or "").strip()

        if not cis:
            continue

        urls = bdm_urls(cis)

        found_atc = None
        source = None
        source_url = None
        page_no = None
        snippet = None

        # 1) RCP HTML
        try:
            html = fetch_html_text(sess, urls["rcp_html"])
            rcp_checks += 1
            if html:
                found_atc = extract_atc_from_html(html)
                if found_atc:
                    source = "RCP_HTML"
                    source_url = urls["rcp_html"]
        except Exception:
            pass

        # 2) Sinon: PDF (fiche-info / rcp-et-notice / lien Airtable)
        if not found_atc:
            pdf_url = None

            link_rcp = (fields.get(FIELD_RCP_LINK) or "").strip()
            if link_rcp.startswith("//"):
                link_rcp = "https:" + link_rcp

            try:
                html_fiche = fetch_html_text(sess, urls["fiche_info"])
                if html_fiche:
                    pdf_url = find_pdf_url_in_html(sess, urls["fiche_info"], html_fiche)
            except Exception:
                pass

            if not pdf_url:
                try:
                    html_notice = fetch_html_text(sess, urls["rcp_notice"])
                    if html_notice:
                        pdf_url = find_pdf_url_in_html(sess, urls["rcp_notice"], html_notice)
                except Exception:
                    pass

            if not pdf_url and link_rcp and is_http_url(link_rcp):
                try:
                    html_lr = fetch_html_text(sess, link_rcp)
                    if html_lr:
                        pdf_url = find_pdf_url_in_html(sess, link_rcp, html_lr)
                    if not pdf_url and link_rcp.lower().endswith(".pdf"):
                        pdf_url = link_rcp
                except Exception:
                    pass

            # Téléchargement + extraction
            if pdf_url:
                try:
                    time.sleep(REQUEST_SLEEP_SEC)
                    r = sess.get(pdf_url, timeout=60, allow_redirects=True)
                    ctype = (r.headers.get("content-type", "") or "").lower()
                    if r.status_code < 400 and (("pdf" in ctype) or pdf_url.lower().endswith(".pdf")):
                        pdf_checks += 1
                        found_atc, page_no, snippet = extract_atc_from_pdf_bytes(r.content)
                        if found_atc:
                            pdf_hits += 1
                            source = "PDF"
                            source_url = pdf_url
                except Exception:
                    pass

        if found_atc:
            payload_fields = {FIELD_ATC: found_atc}

            if atc_map.get(found_atc) and FIELD_ATC_LABEL:
                payload_fields[FIELD_ATC_LABEL] = atc_map[found_atc]

            updates.append({"id": rid, "fields": payload_fields})
            atc_added += 1

            if source == "PDF":
                pdf_report_rows.append({
                    "cis": cis,
                    "specialite": spec,
                    "atc": found_atc,
                    "pdf_url": source_url or "",
                    "page": str(page_no or ""),
                    "snippet": snippet or ""
                })
                log(f"[PDF ✅] CIS={cis} ATC={found_atc} page={page_no}")
            else:
                log(f"[HTML ✅] CIS={cis} ATC={found_atc}")

        # Push Airtable par paquets
        if len(updates) >= 50:
            airtable_batch_update(sess, updates)
            updates = []

        if idx % 100 == 0:
            log(f"Heartbeat: {idx}/{len(missing)} | RCP checks: {rcp_checks} | PDF checks: {pdf_checks} | PDF hits: {pdf_hits} | ATC added: {atc_added}")

    if updates:
        airtable_batch_update(sess, updates)

    os.makedirs("reports", exist_ok=True)
    report_path = "reports/atc_added_from_pdf.csv"
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["cis", "specialite", "atc", "pdf_url", "page", "snippet"])
        w.writeheader()
        for row in pdf_report_rows:
            w.writerow(row)

    log("-----")
    log(f"✅ Terminé. ATC ajoutés: {atc_added}")
    log(f"📄 PDF checks: {pdf_checks} | PDF hits: {pdf_hits}")
    log(f"🧾 Rapport PDF: {report_path}")


if __name__ == "__main__":
    main()
