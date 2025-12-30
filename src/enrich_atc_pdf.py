#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
import time
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ----------------------------
# Config / ENV
# ----------------------------

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY") or os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Liste médicaments")

FIELD_CIS = os.getenv("AIRTABLE_FIELD_CIS", "Code cis")
FIELD_ATC = os.getenv("AIRTABLE_FIELD_ATC", "Code ATC")
FIELD_ATC_L4 = os.getenv("AIRTABLE_FIELD_ATC_L4", "Code ATC (niveau 4)")
FIELD_RCP_LINK = os.getenv("AIRTABLE_FIELD_RCP_LINK", "Lien vers RCP")  # optionnel

# Si tu as aussi un champ “Libellé ATC” et que tu veux y mettre une info récupérée du texte :
FIELD_ATC_LABEL = os.getenv("AIRTABLE_FIELD_ATC_LABEL", "")  # ex: "Libellé ATC" (laisser vide si tu ne veux pas le toucher)

# Airtable API URL
AIRTABLE_API_URL = "https://api.airtable.com/v0"

if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_NAME:
    raise SystemExit("ERROR: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME doivent être définis en secrets/env.")

HEADERS_AIRTABLE = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

# Requests session (perf + retries simples)
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "medicaments-atc-enricher/1.0 (+github-actions)"
})

HTTP_TIMEOUT = 25

# ----------------------------
# Logging helpers
# ----------------------------

def info(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] i {msg}", flush=True)

def warn(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ! {msg}", flush=True)

def err(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] x {msg}", flush=True)

# ----------------------------
# Text utils
# ----------------------------

def normalize_ws_keep_lines(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ")  # NBSP
    # normalise espaces (sans tuer les retours à la ligne)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def normalize_url(u: str) -> str:
    if not u:
        return u
    u = u.strip()
    # Cas fréquent dans tes logs: URL "protocol-relative" //...
    if u.startswith("//"):
        return "https:" + u
    return u

# ----------------------------
# ATC extraction / normalization
# ----------------------------

# Format ATC (WHO): 7 caractères: LDDLLDD
# Exemples possibles dans docs: "C10A A07", "N05A H03.", "J02A X06", "N02BF02"
ATC_SPACED_RE = re.compile(
    r"\b([A-Z])\s*([0-9])\s*([0-9])\s*([A-Z])\s*([A-Z])\s*([0-9])\s*([0-9])\b"
)

ATC_COMPACT_RE = re.compile(r"\b([A-Z][0-9]{2}[A-Z]{2}[0-9]{2})\b")

def normalize_atc_code(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip().upper()
    raw = raw.replace("\u00a0", " ")
    # retire ponctuation autour
    raw = re.sub(r"[,:;()]", " ", raw)
    raw = raw.replace(".", " ")
    raw = raw.strip()

    # si déjà compact
    m = ATC_COMPACT_RE.search(raw.replace(" ", ""))
    if m:
        return m.group(1)

    # si format avec espaces
    m2 = ATC_SPACED_RE.search(raw)
    if m2:
        return "".join(m2.groups())

    # fallback: enlever espaces et retester
    compact = re.sub(r"\s+", "", raw)
    m3 = ATC_COMPACT_RE.search(compact)
    if m3:
        return m3.group(1)

    return None

def extract_atc_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Retourne (ATC_7, label_texte_optionnel)
    label_texte_optionnel = ce qu'il y a souvent avant "code ATC" (pas le libellé WHO officiel)
    """
    if not text:
        return None, None

    t = normalize_ws_keep_lines(text)

    # 1) cas "..., code ATC : C10A A07"
    # On capture un "label" local (avant code ATC) si présent.
    # Exemple PDF EMA: "Classe pharmacothérapeutique : ... , Code ATC : N05A H03."
    label = None
    m = re.search(r"(?:Classe\s+pharmacoth[ée]rapeutique\s*:\s*)(.{0,180}?)(?:,|\s)\s*Code\s*ATC\s*[:\-]\s*([A-Z0-9 \.\u00a0]{6,20})", t, flags=re.IGNORECASE)
    if m:
        label = m.group(1).strip(" -–—:,;")
        atc = normalize_atc_code(m.group(2))
        if atc:
            return atc, label

    # 2) autres formes "code ATC : ..."
    m2 = re.search(r"Code\s*ATC\s*[:\-]\s*([A-Z0-9 \.\u00a0]{6,20})", t, flags=re.IGNORECASE)
    if m2:
        atc = normalize_atc_code(m2.group(1))
        if atc:
            return atc, None

    # 3) scanner tout le texte
    # a) compact
    m3 = ATC_COMPACT_RE.search(t.replace("\u00a0", " "))
    if m3:
        return m3.group(1), None

    # b) espacés
    m4 = ATC_SPACED_RE.search(t)
    if m4:
        return "".join(m4.groups()), None

    return None, None

def atc_level4(atc7: str) -> Optional[str]:
    if not atc7 or len(atc7) < 5:
        return None
    return atc7[:5]

# ----------------------------
# HTML/PDF fetch + parse
# ----------------------------

def fetch_html(url: str) -> Optional[str]:
    url = normalize_url(url)
    try:
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        r.encoding = r.apparent_encoding or r.encoding
        return r.text
    except Exception as e:
        warn(f"HTML fetch failed: {url} ({e})")
        return None

def soup_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    # enlever scripts/styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n")

def find_pdf_urls_in_html(html: str, base_url: str) -> List[str]:
    """
    Cherche toutes les URLs PDF plausibles (EMA, etc.)
    """
    soup = BeautifulSoup(html, "lxml")
    urls = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(" ", strip=True) or "").lower()

        # PDF direct
        if ".pdf" in href.lower():
            urls.append(href)
            continue

        # Lien EMA product-information
        if "ema.europa.eu" in href.lower() and "product-information" in href.lower():
            urls.append(href)
            continue

        # Le lien "Vers le RCP et la notice" (souvent EMA)
        if "rcp" in text and "notice" in text:
            urls.append(href)
            continue

    # normaliser + dédoublonner
    out = []
    seen = set()
    for u in urls:
        u = normalize_url(u)
        if u.startswith("/"):
            # base_url e.g. https://base-donnees.../medicament/.../extrait
            from urllib.parse import urljoin
            u = urljoin(base_url, u)
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def download_pdf(url: str) -> Optional[bytes]:
    url = normalize_url(url)
    try:
        r = SESSION.get(url, timeout=HTTP_TIMEOUT, stream=True)
        if r.status_code != 200:
            return None
        ctype = (r.headers.get("Content-Type") or "").lower()
        # parfois serveurs renvoient octet-stream
        content = r.content
        if not content or len(content) < 5000:
            return None
        return content
    except Exception as e:
        warn(f"PDF download failed: {url} ({e})")
        return None

def extract_text_from_pdf_bytes(pdf_bytes: bytes, max_pages: int = 0) -> str:
    """
    Extrait le texte d'un PDF (bytes) avec pdfminer.six.
    max_pages=0 => toutes les pages
    """
    if not pdf_bytes:
        return ""
    try:
        from io import BytesIO
        from pdfminer.high_level import extract_text as _extract_text

        bio = BytesIO(pdf_bytes)
        text = _extract_text(bio, maxpages=max_pages if max_pages and max_pages > 0 else 0) or ""
        return normalize_ws_keep_lines(text)
    except Exception as e:
        warn(f"PDF text extraction failed: {e}")
        return ""

# ----------------------------
# Airtable read/write
# ----------------------------

def airtable_list_records_missing_atc() -> List[Dict]:
    """
    Récupère tous les enregistrements dont {Code ATC} est vide.
    """
    records = []
    offset = None

    # formule: Code cis non vide et Code ATC vide
    formula = f"AND({{{FIELD_CIS}}}!='', OR({{{FIELD_ATC}}}='', {{{FIELD_ATC}}}=BLANK()))"

    while True:
        params = {
            "pageSize": 100,
            "filterByFormula": formula,
        }
        if offset:
            params["offset"] = offset

        url = f"{AIRTABLE_API_URL}/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_TABLE_NAME, safe='')}"
        r = SESSION.get(url, headers=HEADERS_AIRTABLE, params=params, timeout=HTTP_TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError(f"Airtable list failed: {r.status_code} {r.text[:300]}")

        data = r.json()
        batch = data.get("records", [])
        records.extend(batch)
        offset = data.get("offset")
        if not offset:
            break

    return records

def airtable_batch_update(updates: List[Dict]):
    """
    updates: list of {"id": "...", "fields": {...}}
    """
    if not updates:
        return

    url = f"{AIRTABLE_API_URL}/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_TABLE_NAME, safe='')}"
    payload = {"records": updates, "typecast": True}

    r = SESSION.patch(url, headers=HEADERS_AIRTABLE, data=json.dumps(payload), timeout=HTTP_TIMEOUT)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Airtable update failed: {r.status_code} {r.text[:500]}")

# ----------------------------
# Core enrichment logic
# ----------------------------

def cis_to_base_urls(cis: str) -> List[str]:
    cis = str(cis).strip()
    # Fragments #tab-xxx ne servent à rien côté serveur, mais on garde une URL propre.
    base = f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait"
    # parfois certains contenus peuvent être sur /medicament/{cis}
    alt = f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}"
    return [base, alt]

def enrich_one_cis_find_atc(cis: str, rcp_link: Optional[str] = None) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Retourne (atc7, atc_l4, label_local, source)
    source in {"ficheinfo_html","rcp_html","pdf"}
    """
    urls_to_try = []
    if rcp_link:
        urls_to_try.append(normalize_url(rcp_link))
    urls_to_try.extend(cis_to_base_urls(cis))

    # 1) HTML (fiche info + rcp html = même page /extrait dans les faits)
    for u in urls_to_try:
        html = fetch_html(u)
        if not html:
            continue

        txt = soup_text(html)

        atc, label = extract_atc_from_text(txt)
        if atc:
            return atc, atc_level4(atc), label, "rcp_html"

        # 2) fallback PDF : trouver liens PDF dans la page
        pdf_urls = find_pdf_urls_in_html(html, u)
        for pu in pdf_urls:
            pdf_bytes = download_pdf(pu)
            if not pdf_bytes:
                continue

            # IMPORTANT: tu peux mettre max_pages=0 (toutes pages).
            # Pour perf, on lit souvent les premières pages seulement, MAIS tu as demandé "sans limitation":
            pdf_text = extract_text_from_pdf_bytes(pdf_bytes, max_pages=0)
            atc2, label2 = extract_atc_from_text(pdf_text)
            if atc2:
                return atc2, atc_level4(atc2), (label2 or label), "pdf"

    return None, None, None, None

def write_pdf_report(rows: List[Dict], out_dir: str = "reports") -> Optional[str]:
    if not rows:
        return None
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"atc_added_from_pdf_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["record_id", "cis", "atc7", "atc_l4", "label_local", "source"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path

def main():
    info("Lecture Airtable (records ATC manquants) ...")
    records = airtable_list_records_missing_atc()
    total = len(records)
    info(f"Records à enrichir (ATC manquant): {total}")

    if total == 0:
        info("Rien à faire.")
        return

    updates_buffer = []
    pdf_report_rows = []

    rcp_checks = 0
    pdf_hits = 0
    pdf_added = 0
    updated = 0

    start = time.time()

    for i, rec in enumerate(records, start=1):
        rid = rec.get("id")
        fields = rec.get("fields", {})
        cis = fields.get(FIELD_CIS)
        if not cis:
            continue

        rcp_link = fields.get(FIELD_RCP_LINK) if FIELD_RCP_LINK else None
        if isinstance(rcp_link, list):
            # au cas où Airtable renvoie une liste
            rcp_link = rcp_link[0] if rcp_link else None

        rcp_checks += 1
        try:
            atc7, atc_l4, label_local, source = enrich_one_cis_find_atc(str(cis), rcp_link=rcp_link)

            if atc7:
                patch_fields = {
                    FIELD_ATC: atc7,
                }
                if atc_l4 and FIELD_ATC_L4:
                    patch_fields[FIELD_ATC_L4] = atc_l4

                # Optionnel: écrire un libellé "local" si tu as un champ dédié
                if FIELD_ATC_LABEL:
                    # on ne remplace pas si déjà rempli
                    existing_label = fields.get(FIELD_ATC_LABEL)
                    if (not existing_label) and label_local:
                        patch_fields[FIELD_ATC_LABEL] = label_local

                updates_buffer.append({"id": rid, "fields": patch_fields})
                updated += 1

                if source == "pdf":
                    pdf_hits += 1
                    pdf_added += 1
                    pdf_report_rows.append({
                        "record_id": rid,
                        "cis": str(cis),
                        "atc7": atc7,
                        "atc_l4": atc_l4 or "",
                        "label_local": label_local or "",
                        "source": source
                    })

            # flush batch
            if len(updates_buffer) >= 10:
                airtable_batch_update(updates_buffer)
                updates_buffer.clear()
                info("Batch updates: 10")

        except Exception as e:
            warn(f"Enrich KO CIS={cis}: {e} (on continue)")

        # Heartbeat
        if i % 50 == 0:
            elapsed = time.time() - start
            speed = i / elapsed if elapsed > 0 else 0
            info(f"Heartbeat: {i}/{total} (CIS={cis}) | {speed:.2f} rec/s | RCP checks: {rcp_checks} | PDF hits: {pdf_hits} | PDF ATC added: {pdf_added} | Updated: {updated}")

    # flush last
    if updates_buffer:
        airtable_batch_update(updates_buffer)
        info(f"Batch updates final: {len(updates_buffer)}")

    report_path = write_pdf_report(pdf_report_rows)
    if report_path:
        info(f"Rapport PDF ATC écrit: {report_path} (lignes={len(pdf_report_rows)})")
    else:
        info("Aucun ATC ajouté via PDF -> rapport non généré.")

    info(f"Terminé. Updated={updated}, PDF_added={pdf_added}, RCP_checks={rcp_checks}")

if __name__ == "__main__":
    main()
