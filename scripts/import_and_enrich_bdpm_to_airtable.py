#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
import time
import json
import random
import logging
from io import BytesIO
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup
import xlrd  # lit les .xls (pas xlsx)

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger("bdpm")

# ---------------------------
# ENV
# ---------------------------
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

STOP_ON_RCP_ERROR = os.getenv("STOP_ON_RCP_ERROR", "true").lower() in ("1", "true", "yes", "y")
MAX_RCP_PER_RUN = int(os.getenv("MAX_RCP_PER_RUN", "300"))
RCP_SLEEP_MIN = float(os.getenv("RCP_SLEEP_MIN", "0.2"))
RCP_SLEEP_MAX = float(os.getenv("RCP_SLEEP_MAX", "0.6"))

# ---------------------------
# SOURCES OFFICIELLES
# ---------------------------
URL_CIS_BDPM = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
URL_CIS_CIP_BDPM = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
URL_CIS_CPD_BDPM = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"

ANSM_PAGE = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; BDPM-AirtableSync/1.0)",
        "Accept": "*/*",
    }
)

# ---------------------------
# Airtable helpers
# ---------------------------
def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json",
    }

def require_env():
    missing = []
    if not AIRTABLE_API_TOKEN:
        missing.append("AIRTABLE_API_TOKEN")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if not AIRTABLE_TABLE_NAME:
        missing.append("AIRTABLE_CIS_TABLE_NAME")
    if missing:
        raise SystemExit(f"❌ Variables manquantes: {', '.join(missing)}")

def airtable_url() -> str:
    # table name peut contenir des espaces -> quote
    from urllib.parse import quote
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(AIRTABLE_TABLE_NAME)}"

def airtable_list_records() -> Dict[str, str]:
    """Map CIS -> record_id"""
    url = airtable_url()
    params = {"pageSize": 100}
    out: Dict[str, str] = {}
    offset = None
    while True:
        if offset:
            params["offset"] = offset
        r = SESSION.get(url, headers=airtable_headers(), params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            cis = str(fields.get("Code cis", "")).strip()
            if cis:
                out[cis] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
    return out

def airtable_batch_create(records: List[Dict], max_batch=10):
    url = airtable_url()
    for i in range(0, len(records), max_batch):
        batch = {"records": records[i : i + max_batch]}
        r = SESSION.post(url, headers=airtable_headers(), data=json.dumps(batch), timeout=120)
        if r.status_code >= 400:
            LOG.error("❌ Airtable create error: %s", r.text[:5000])
            r.raise_for_status()

def airtable_batch_update(updates: List[Dict], max_batch=10):
    url = airtable_url()
    for i in range(0, len(updates), max_batch):
        batch = {"records": updates[i : i + max_batch]}
        r = SESSION.patch(url, headers=airtable_headers(), data=json.dumps(batch), timeout=120)
        if r.status_code >= 400:
            LOG.error("❌ Airtable update error: %s", r.text[:5000])
            r.raise_for_status()

def airtable_batch_delete(record_ids: List[str], max_batch=10):
    url = airtable_url()
    for i in range(0, len(record_ids), max_batch):
        params = [("records[]", rid) for rid in record_ids[i : i + max_batch]]
        r = SESSION.delete(url, headers=airtable_headers(), params=params, timeout=120)
        if r.status_code >= 400:
            LOG.error("❌ Airtable delete error: %s", r.text[:5000])
            r.raise_for_status()

# ---------------------------
# Download helpers
# ---------------------------
def download_text(url: str) -> str:
    r = SESSION.get(url, timeout=180)
    r.raise_for_status()
    # fichiers BDPM souvent latin-1
    return r.content.decode("latin-1", errors="replace")

# ---------------------------
# Parse BDPM
# ---------------------------
def parse_cis_bdpm(text: str) -> Dict[str, Dict]:
    d: Dict[str, Dict] = {}
    reader = csv.reader(text.splitlines(), delimiter="\t")
    for row in reader:
        if not row:
            continue
        cis = row[0].strip()
        if not cis:
            continue
        titulaire = row[10].strip() if len(row) > 10 else ""
        d[cis] = {
            "Code cis": cis,
            "Spécialité": row[1].strip() if len(row) > 1 else "",
            "Forme": row[2].strip() if len(row) > 2 else "",
            "Voie d'administration": row[3].strip() if len(row) > 3 else "",
            # tu veux des noms type "Arrow" -> c’est le Titulaire dans CIS_bdpm
            "Laboratoire": titulaire,
        }
    return d

def parse_cis_cip_bdpm(text: str) -> Tuple[Dict[str, str], Set[str]]:
    """
    CIP13 + présence taux remboursement (ville).
    Méthode plus robuste: on parcourt toutes les colonnes:
      - CIP13 = 13 chiffres
      - taux rembours. = motif "xx%" ou valeurs usuelles
    """
    cis_to_cip13: Dict[str, str] = {}
    cis_with_reimb: Set[str] = set()

    for line in text.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        cis = parts[0].strip()
        if not cis.isdigit():
            continue

        cip13 = ""
        for p in parts[1:]:
            digits = re.sub(r"\D", "", p)
            if len(digits) == 13:
                cip13 = digits
                break
        if cip13:
            cis_to_cip13[cis] = cip13

        joined = " ".join(parts)
        if re.search(r"\b\d{1,3}\s?%\b", joined) or re.search(r"\b(15|30|35|40|50|55|60|65|70|80|90|100)\b", joined):
            cis_with_reimb.add(cis)

    return cis_to_cip13, cis_with_reimb

def parse_cis_cpd_bdpm(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    reader = csv.reader(text.splitlines(), delimiter="\t")
    for row in reader:
        if not row:
            continue
        cis = row[0].strip()
        if not cis:
            continue
        txt = "\n".join([c.strip() for c in row[1:] if c.strip()])
        txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
        if txt:
            out[cis] = txt
    return out

# ---------------------------
# ANSM rétrocession
# ---------------------------
def find_ansm_xls_url() -> str:
    r = SESSION.get(ANSM_PAGE, timeout=60)
    r.raise_for_status()
    html = r.text
    m = re.findall(r'https?://ansm\.sante\.fr/[^"\s]+?\.(?:xls|xlsx)', html, flags=re.I)
    if m:
        return m[0]
    m2 = re.findall(r'(/uploads/[^"\s]+?\.(?:xls|xlsx))', html, flags=re.I)
    if m2:
        return "https://ansm.sante.fr" + m2[0]
    raise RuntimeError("Impossible de trouver le fichier ANSM (.xls/.xlsx).")

def parse_ansm_retro_cis(xls_url: str) -> Set[str]:
    r = SESSION.get(xls_url, timeout=180)
    r.raise_for_status()

    if xls_url.lower().endswith(".xlsx"):
        raise RuntimeError("Le fichier ANSM est en .xlsx (xlrd ne le lit pas).")

    book = xlrd.open_workbook(file_contents=r.content)
    sh = book.sheet_by_index(0)

    cis_set: Set[str] = set()
    for rx in range(sh.nrows):
        row = sh.row_values(rx)
        if len(row) < 3:
            continue
        v = re.sub(r"\D", "", str(row[2]).strip())
        if len(v) >= 6:
            cis_set.add(v)
    return cis_set

# ---------------------------
# RCP scraping (corrigé)
# ---------------------------
def normalize_rcp_url(url: str) -> str:
    """
    Transforme:
      https://.../extrait#tab-rcp
    en:
      https://.../extrait?tab=rcp
    """
    if not url:
        return url

    u = url.strip()

    # supprime ancre
    u = re.sub(r"#.*$", "", u)

    # force tab=rcp
    if "extrait" in u and "tab=rcp" not in u:
        if "?" in u:
            u += "&tab=rcp"
        else:
            u += "?tab=rcp"
    return u


def fetch_rcp_and_extract_cpd(rcp_url: str) -> str:
    url = normalize_rcp_url(rcp_url)
    r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return extract_cpd_from_rcp_html(r.text)

def strip_accents_lower(s: str) -> str:
    s = s.lower()
    return (
        s.replace("é", "e")
         .replace("è", "e")
         .replace("ê", "e")
         .replace("à", "a")
         .replace("ù", "u")
         .replace("î", "i")
         .replace("ï", "i")
         .replace("ô", "o")
         .replace("ç", "c")
    )


def extract_cpd_from_rcp_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # Texte rendu, ligne par ligne
    raw = soup.get_text("\n", strip=True)
    lines = [re.sub(r"\s+", " ", l).strip() for l in raw.split("\n") if l.strip()]

    # Trouver le titre "CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE"
    idx = -1
    for i, l in enumerate(lines):
        ll = strip_accents_lower(l)
        if ll == "conditions de prescription et de delivrance":
            idx = i
            break
        if "conditions de prescription" in ll and "delivrance" in ll:
            idx = i
            break

    if idx == -1:
        return ""

    out = []
    for l in lines[idx + 1:]:
        ll = strip_accents_lower(l)

        # Stops: footer + navigation
        if ll in ("haut de page",):
            break
        if "ministere du travail" in ll or "ministere de la sante" in ll:
            break
        if ll.startswith("legifrance") or ll.startswith("gouvernement") or ll.startswith("service-public"):
            break

        # Stops: retour à un gros titre numéroté type "10. DATE..." / "11. DOSIMETRIE"
        if re.match(r"^\d+\.\s", l):
            break

        # Stops: certains pages ont des titres en MAJUSCULES pour sections
        # (on garde "Liste I." etc, mais si on voit un gros titre très long en caps, on stop)
        if len(l) > 8 and l == l.upper() and "CONDITIONS DE PRESCRIPTION" not in l:
            break

        out.append(l)

    text = "\n".join(out).strip()
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

def is_homeopathy(html: str) -> bool:
    t = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    t = strip_accents_lower(t)
    return "homeopathi" in t  # couvre "homéopathique", "homéopathie", etc.

def has_hospital_only_mention(html: str) -> bool:
    t = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    t = strip_accents_lower(t)
    keywords = [
        "reserve a l usage hospitalier",
        "reserve a l'usage hospitalier",
        "usage hospitalier",
        "prescription hospitaliere",
        "medicament soumis a prescription hospitaliere",
    ]
    return any(k in t for k in keywords)

# ---------------------------
# Disponibilité (règles finales)
# ---------------------------
def compute_availability(
    cis_in_retro_ansm: bool,
    cis_has_reimb: bool,
    rcp_html: Optional[str],
) -> str:
    in_ville = cis_has_reimb

    if rcp_html and is_homeopathy(rcp_html):
        in_ville = True

    if cis_in_retro_ansm and in_ville:
        return "Disponible en ville et en rétrocession hospitalière"
    if cis_in_retro_ansm:
        return "Disponible en rétrocession hospitalière"

    if rcp_html and has_hospital_only_mention(rcp_html):
        return "Réservé à l'usage hospitalier"

    if in_ville:
        return "Disponible en pharmacie de ville"

    return "Pas d'informations mentionnées"

# ---------------------------
# Main
# ---------------------------
def main():
    require_env()

    LOG.info("1) Téléchargements BDPM ...")
    cis_bdpm_txt = download_text(URL_CIS_BDPM)
    LOG.info("✅ BDPM CIS OK")
    cis_cip_txt = download_text(URL_CIS_CIP_BDPM)
    LOG.info("✅ BDPM CIS_CIP OK")
    cis_cpd_txt = download_text(URL_CIS_CPD_BDPM)
    LOG.info("✅ BDPM CIS_CPD OK")

    LOG.info("2) ANSM rétrocession ...")
    ansm_xls_url = find_ansm_xls_url()
    LOG.info(f"✅ Lien ANSM trouvé : {ansm_xls_url}")
    retro_cis = parse_ansm_retro_cis(ansm_xls_url)
    LOG.info(f"✅ CIS ANSM rétrocession : {len(retro_cis)}")

    LOG.info("3) Parsing ...")
    cis_rows = parse_cis_bdpm(cis_bdpm_txt)
    cis_to_cip13, cis_has_reimb = parse_cis_cip_bdpm(cis_cip_txt)
    cis_to_cpd_fallback = parse_cis_cpd_bdpm(cis_cpd_txt)

    bdpm_cis_set = set(cis_rows.keys())
    LOG.info(f"✅ CIS BDPM: {len(bdpm_cis_set)}")

    LOG.info("4) Inventaire Airtable ...")
    airtable_map = airtable_list_records()
    airtable_cis_set = set(airtable_map.keys())
    LOG.info(f"✅ CIS Airtable: {len(airtable_cis_set)}")

    to_delete = sorted(list(airtable_cis_set - bdpm_cis_set))
    to_create = sorted(list(bdpm_cis_set - airtable_cis_set))
    to_update = sorted(list(bdpm_cis_set & airtable_cis_set))

    LOG.info(f"→ À créer: {len(to_create)} | À supprimer: {len(to_delete)} | À mettre à jour: {len(to_update)}")

    if to_delete:
        LOG.info("5) Suppression Airtable (absents BDPM) ...")
        delete_ids = [airtable_map[cis] for cis in to_delete if cis in airtable_map]
        airtable_batch_delete(delete_ids)
        LOG.info(f"✅ Supprimés: {len(delete_ids)}")

    if to_create:
        LOG.info("6) Création Airtable ...")
        payload = []
        for cis in to_create:
            base = cis_rows[cis]
            fields = {
                "Code cis": base.get("Code cis", ""),
                "Spécialité": base.get("Spécialité", ""),
                "Forme": base.get("Forme", ""),
                "Voie d'administration": base.get("Voie d'administration", ""),
                "Laboratoire": base.get("Laboratoire", ""),
                "Lien vers RCP": f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait?tab=rcp",
                "CIP 13": cis_to_cip13.get(cis, ""),
            }
            fields = {k: v for k, v in fields.items() if str(v).strip() != ""}
            payload.append({"fields": fields})
        airtable_batch_create(payload)
        LOG.info(f"✅ Créés: {len(payload)}")
        airtable_map = airtable_list_records()

    LOG.info("7) Enrichissement (CPD + CIP13 + Disponibilité) ...")
    updates = []
    scraped = 0

    random.shuffle(to_update)

    for cis in to_update:
        rid = airtable_map.get(cis)
        if not rid:
            continue

        rcp_url = normalize_rcp_url(f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp")
        rcp_html = None
        cpd_text = ""

        if scraped < MAX_RCP_PER_RUN:
            try:
                rcp_html = fetch_rcp_html(rcp_url)
                scraped += 1
                cpd_text = extract_cpd_from_rcp(rcp_html)
            except Exception as e:
                if STOP_ON_RCP_ERROR:
                    raise SystemExit(f"❌ RCP inaccessible pour CIS={cis} ({rcp_url}) -> STOP. Détail: {e}")
                cpd_text = cis_to_cpd_fallback.get(cis, "")
        else:
            cpd_text = cis_to_cpd_fallback.get(cis, "")

        dispo = compute_availability(
            cis_in_retro_ansm=(cis in retro_cis),
            cis_has_reimb=(cis in cis_has_reimb),
            rcp_html=rcp_html,
        )

        fields = {
            "CIP 13": cis_to_cip13.get(cis, ""),
            "Disponibilité du traitement": dispo,
            "Conditions de prescription et délivrance": cpd_text,
        }

        fields = {k: v for k, v in fields.items() if str(v).strip() != ""}

        # si tout est vide (rare), on évite un patch inutile
        if fields:
            updates.append({"id": rid, "fields": fields})

        if rcp_html is not None:
            time.sleep(random.uniform(RCP_SLEEP_MIN, RCP_SLEEP_MAX))

        if len(updates) >= 50:
            airtable_batch_update(updates)
            updates = []

    if updates:
        airtable_batch_update(updates)

    LOG.info(f"✅ Terminé. RCP scrapés ce run: {scraped}")

if __name__ == "__main__":
    main()
