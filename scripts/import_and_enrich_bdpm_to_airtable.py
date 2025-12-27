#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import math
import shutil
import zipfile
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Iterable, Set

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# ----------------------------
# Config
# ----------------------------

CIS_BDPM_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
CIS_CIP_BDPM_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
CIS_CPD_BDPM_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"
ANSM_PAGE_URL = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

RCP_URL_TEMPLATE = "https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

# Throttling / safety
HTTP_TIMEOUT = 60
BDPM_ENCODING = "utf-8"  # les fichiers BDPM sont en général UTF-8, mais on protège avec errors="replace"
AIRT_BATCH_SIZE = 10     # Airtable API: max 10 records per request
AIRT_SLEEP_BETWEEN_REQ = 0.25

# RCP scraping (attention: long sur 15k lignes)
RCP_FETCH_ENABLED = os.getenv("RCP_FETCH_ENABLED", "1").strip() == "1"
RCP_STRICT = os.getenv("RCP_STRICT", "1").strip() == "1"  # si 1: au moindre RCP inaccessible => arrêt sans toucher Airtable
RCP_SLEEP = float(os.getenv("RCP_SLEEP", "0.25"))         # ralentit les requêtes BDPM pages
RCP_MAX_PER_RUN = int(os.getenv("RCP_MAX_PER_RUN", "0"))  # 0 = pas de limite. (optionnel si tu veux tester)

# ----------------------------
# Helpers
# ----------------------------

def eprint(*args):
    print(*args, file=sys.stderr)

def die(msg: str, code: int = 1):
    eprint(f"❌ {msg}")
    sys.exit(code)

def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        die(f"Variable d'environnement manquante: {name}")
    return v

def normalize_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

def safe_float_fr(x: str) -> Optional[float]:
    if not x:
        return None
    x = x.strip().replace(",", ".")
    try:
        return float(x)
    except Exception:
        return None

def requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "pharmacie-projet/1.0 (+github actions) requests"
    })
    return s

def download_file(session: requests.Session, url: str, dest_path: str) -> None:
    r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
    if r.status_code != 200 or not r.content:
        raise RuntimeError(f"Download failed {url} status={r.status_code}")
    with open(dest_path, "wb") as f:
        f.write(r.content)

def fetch_html(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.text

# ----------------------------
# Airtable API
# ----------------------------

@dataclass
class AirtableConfig:
    api_token: str
    base_id: str
    table_name: str

class AirtableClient:
    def __init__(self, cfg: AirtableConfig):
        self.cfg = cfg
        self.session = requests_session()
        self.session.headers.update({
            "Authorization": f"Bearer {cfg.api_token}",
            "Content-Type": "application/json",
        })

    def _url(self) -> str:
        # table name can contain spaces => must be url-encoded by requests when passed as params,
        # but for path we keep raw and requests will handle. Safer: replace spaces with %20.
        table = requests.utils.requote_uri(self.cfg.table_name)
        return f"{AIRTABLE_API_BASE}/{self.cfg.base_id}/{table}"

    def list_all_records(self, fields: Optional[List[str]] = None) -> List[dict]:
        url = self._url()
        out = []
        offset = None
        params = {}
        if fields:
            # Airtable expects fields[]=...
            for i, f in enumerate(fields):
                params[f"fields[{i}]"] = f

        while True:
            p = dict(params)
            if offset:
                p["offset"] = offset
            r = self.session.get(url, params=p, timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                raise RuntimeError(f"Airtable list failed {r.status_code}: {r.text[:500]}")
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(AIRT_SLEEP_BETWEEN_REQ)
        return out

    def batch_create(self, records: List[dict]) -> None:
        url = self._url()
        for i in range(0, len(records), AIRT_BATCH_SIZE):
            chunk = records[i:i + AIRT_BATCH_SIZE]
            payload = {"records": [{"fields": rec} for rec in chunk]}
            r = self.session.post(url, data=json.dumps(payload), timeout=HTTP_TIMEOUT)
            if r.status_code not in (200, 201):
                raise RuntimeError(f"Airtable create failed {r.status_code}: {r.text[:500]}")
            time.sleep(AIRT_SLEEP_BETWEEN_REQ)

    def batch_update(self, records: List[Tuple[str, dict]]) -> None:
        url = self._url()
        for i in range(0, len(records), AIRT_BATCH_SIZE):
            chunk = records[i:i + AIRT_BATCH_SIZE]
            payload = {"records": [{"id": rid, "fields": fields} for rid, fields in chunk]}
            r = self.session.patch(url, data=json.dumps(payload), timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                raise RuntimeError(f"Airtable update failed {r.status_code}: {r.text[:500]}")
            time.sleep(AIRT_SLEEP_BETWEEN_REQ)

    def batch_delete(self, record_ids: List[str]) -> None:
        url = self._url()
        for i in range(0, len(record_ids), AIRT_BATCH_SIZE):
            chunk = record_ids[i:i + AIRT_BATCH_SIZE]
            # Airtable DELETE uses query params records[]=...
            params = {}
            for j, rid in enumerate(chunk):
                params[f"records[{j}]"] = rid
            r = self.session.delete(url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                raise RuntimeError(f"Airtable delete failed {r.status_code}: {r.text[:500]}")
            time.sleep(AIRT_SLEEP_BETWEEN_REQ)

# ----------------------------
# Parsing BDPM files
# ----------------------------

@dataclass
class CisBaseRow:
    cis: str
    specialite: str
    forme: str
    voie: str
    laboratoire: str

def parse_cis_bdpm(path: str) -> Dict[str, CisBaseRow]:
    """
    CIS_bdpm.txt columns (tab-separated):
    1 CIS
    2 Denomination / Spécialité
    3 Forme pharmaceutique
    4 Voies d'administration (séparées par ;)
    5 Statut AMM
    6 Type de procédure
    7 Etat de commercialisation
    8 Date AMM
    9 StatutBdm
    10 Num autorisation Européenne
    11 Titulaire
    12 Surveillance renforcée
    => On garde 1..4 + titulaire/labo (selon ton tableau Airtable: "Laboratoire")
    """
    out: Dict[str, CisBaseRow] = {}
    with open(path, "r", encoding=BDPM_ENCODING, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            cis = parts[0].strip()
            specialite = parts[1].strip() if len(parts) > 1 else ""
            forme = parts[2].strip() if len(parts) > 2 else ""
            voie = parts[3].strip() if len(parts) > 3 else ""
            # "Laboratoire": dans CIS_bdpm c'est plutôt "titulaire" en col 11
            labo = parts[10].strip() if len(parts) > 10 else ""
            if cis:
                out[cis] = CisBaseRow(cis=cis, specialite=specialite, forme=forme, voie=voie, laboratoire=labo)
    return out

@dataclass
class CipInfo:
    cip13_list: List[str]
    agrement_collectivites: Optional[str]  # "oui"/"non"
    has_reimbursement_rate: bool           # présence d'un taux de remboursement sur au moins une présentation
    cpd_text: Optional[str]                # CPD (si utile pour détecter hospitalier)

def parse_cis_cip_bdpm(path: str) -> Dict[str, CipInfo]:
    """
    CIS_CIP_bdpm.txt contains (observed):
    col1 CIS
    col2 CIP7
    col3 Libellé présentation
    ...
    col6 CIP13
    col7 Agrément collectivités (oui/non)
    col8 Taux de remboursement (ex 65% / 100%)
    col9 Prix HT
    col10 Prix TTC
    col11 Taux TVA
    """
    tmp_cip13: Dict[str, Set[str]] = {}
    tmp_agr: Dict[str, str] = {}
    tmp_reimb: Set[str] = set()

    with open(path, "r", encoding=BDPM_ENCODING, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 1:
                continue
            cis = parts[0].strip()
            if not cis:
                continue

            cip13 = parts[5].strip() if len(parts) > 5 else ""
            agr = parts[6].strip().lower() if len(parts) > 6 else ""
            taux = parts[7].strip() if len(parts) > 7 else ""

            if cip13:
                tmp_cip13.setdefault(cis, set()).add(cip13)

            if agr in ("oui", "non"):
                # si plusieurs lignes, on garde "oui" si au moins une présentation est "oui"
                prev = tmp_agr.get(cis)
                if prev == "oui":
                    pass
                else:
                    tmp_agr[cis] = agr

            if taux:
                # un taux non vide => "ville" selon ta règle
                tmp_reimb.add(cis)

    out: Dict[str, CipInfo] = {}
    all_cis = set(tmp_cip13.keys()) | set(tmp_agr.keys()) | set(tmp_reimb)
    for cis in all_cis:
        out[cis] = CipInfo(
            cip13_list=sorted(list(tmp_cip13.get(cis, set()))),
            agrement_collectivites=tmp_agr.get(cis),
            has_reimbursement_rate=(cis in tmp_reimb),
            cpd_text=None
        )
    return out

def parse_cis_cpd_bdpm(path: str) -> Dict[str, str]:
    """
    CIS_CPD_bdpm.txt : Conditions de prescription et de délivrance (texte court)
    """
    out: Dict[str, str] = {}
    with open(path, "r", encoding=BDPM_ENCODING, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            cis = parts[0].strip() if len(parts) > 0 else ""
            txt = parts[1].strip() if len(parts) > 1 else ""
            if cis:
                out[cis] = txt
    return out

# ----------------------------
# ANSM retrocession list
# ----------------------------

def find_ansm_excel_link(session: requests.Session) -> str:
    """
    Scrape ANSM page and pick an .xls/.xlsx link that looks like the retrocession file.
    """
    html = fetch_html(session, ANSM_PAGE_URL)
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        if href.lower().endswith((".xls", ".xlsx")) and "retrocession" in href.lower():
            # absolute
            links.append(requests.compat.urljoin(ANSM_PAGE_URL, href))
    if not links:
        # fallback: sometimes link doesn't contain retrocession in href
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().endswith((".xls", ".xlsx")):
                links.append(requests.compat.urljoin(ANSM_PAGE_URL, href))
    if not links:
        raise RuntimeError("Impossible de trouver le fichier Excel de rétrocession sur la page ANSM.")
    # Heuristic: take the last (often latest), otherwise first
    return links[-1]

def parse_ansm_retrocession_xls(path: str) -> Set[str]:
    """
    Règle donnée: la 3ème colonne = Code CIS.
    On récupère tous les CIS trouvés (en strings).
    """
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    cis_set: Set[str] = set()

    for row in ws.iter_rows(values_only=True):
        if not row or len(row) < 3:
            continue
        v = row[2]
        if v is None:
            continue
        # v peut être numérique
        cis = str(v).strip()
        cis = cis.replace(".0", "")  # si Excel a converti en float
        if cis.isdigit():
            cis_set.add(cis)
    return cis_set

# ----------------------------
# RCP scraping: extract CPD block
# ----------------------------

def extract_cpd_from_rcp_html(html: str) -> Optional[str]:
    """
    Cherche le bloc sous "CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE"
    et renvoie le texte (concaténé) jusqu'au prochain titre.
    """
    soup = BeautifulSoup(html, "lxml")

    # texte cible sans accents / casefold
    target = normalize_text("CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE")

    # On cherche un élément qui contient ce titre
    candidates = soup.find_all(string=True)
    title_node = None
    for s in candidates:
        if not s or not isinstance(s, str):
            continue
        if target in normalize_text(s):
            title_node = s
            break

    if not title_node:
        return None

    # On remonte à un conteneur "section"
    title_el = title_node.parent
    container = title_el
    # remonter un peu si c'est un span etc
    for _ in range(4):
        if container and container.name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            break
        if container and container.parent:
            container = container.parent

    # Maintenant on récupère le texte après ce titre dans le flux DOM
    texts: List[str] = []

    # stratégie: parcourir les éléments suivants dans le document, arrêter sur un prochain heading
    for el in container.find_all_next():
        if el == container:
            continue
        if el.name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            break
        # éviter menus/nav
        if el.name in ("script", "style"):
            continue
        # récupérer paragraphes / divs qui ont du texte
        t = el.get_text(" ", strip=True) if hasattr(el, "get_text") else ""
        if t:
            # éviter de répéter le titre lui-même
            if target in normalize_text(t):
                continue
            texts.append(t)
        # limiter pour éviter d'aspirer toute la page
        if len(" ".join(texts)) > 1500:
            break

    # Nettoyage
    out = " ".join(texts).strip()
    out = re.sub(r"\s+", " ", out)
    return out or None

def rcp_contains_markers(html: str) -> Tuple[bool, bool]:
    """
    Retourne:
    - contains_hospitalier (mention usage hospitalier)
    - contains_homeopathie (homéopathi)
    """
    t = normalize_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))
    contains_homeo = "homeopathi" in t  # capture homéopathie/homéopathique
    # hospitalier: on cherche "usage hospitalier" ou "réservé à l'usage hospitalier"
    contains_hosp = ("usage hospitalier" in t) or ("reserve a l'usage hospitalier" in t) or ("reserve a l’usage hospitalier" in t)
    return contains_hosp, contains_homeo

# ----------------------------
# Availability / "Rétrocession" rules
# ----------------------------

def compute_availability_status(
    cis: str,
    in_ansm_retro: bool,
    has_reimb_rate: bool,
    rcp_html: Optional[str],
    cpd_text: Optional[str],
) -> str:
    """
    Ton ordre final:
    1) Si taux remboursement => "Disponible en pharmacie de ville"
    2) Ensuite si présent ANSM rétrocession => "Disponible en rétrocession hospitalière"
    3) Ensuite si RCP contient "homéopathi" => "Disponible en pharmacie de ville"
    4) Ensuite si RCP mentionne usage hospitalier OU CPD le marque => "Réservé à l'usage hospitalier"
    5) Sinon => "Pas d'informations mentionnées"
    """
    if has_reimb_rate:
        return "Disponible en pharmacie de ville"

    if in_ansm_retro:
        return "Disponible en rétrocession hospitalière"

    contains_hosp = False
    contains_homeo = False
    if rcp_html:
        contains_hosp, contains_homeo = rcp_contains_markers(rcp_html)
        if contains_homeo:
            return "Disponible en pharmacie de ville"

    # fallback CPD file (utile si le texte CPD mentionne hospitalier)
    txt = normalize_text(cpd_text or "")
    if contains_hosp or ("usage hospitalier" in txt) or ("reserve" in txt and "hospital" in txt):
        return "Réservé à l'usage hospitalier"

    return "Pas d'informations mentionnées"

# ----------------------------
# Main workflow
# ----------------------------

def main():
    # Airtable config (variable renommée pour éviter ton conflit)
    api_token = must_env("AIRTABLE_API_TOKEN")
    base_id = must_env("AIRTABLE_BASE_ID")
    table_name = must_env("AIRTABLE_CIS_TABLE_NAME")

    cfg = AirtableConfig(api_token=api_token, base_id=base_id, table_name=table_name)
    airt = AirtableClient(cfg)
    session = requests_session()

    os.makedirs("data", exist_ok=True)

    cis_bdpm_path = os.path.join("data", "CIS_bdpm.txt")
    cis_cip_path = os.path.join("data", "CIS_CIP_bdpm.txt")
    cis_cpd_path = os.path.join("data", "CIS_CPD_bdpm.txt")
    ansm_xls_path = os.path.join("data", "ansm_retrocession.xlsx")

    print("1) Téléchargements (tout d'abord, sans toucher Airtable)...")

    try:
        download_file(session, CIS_BDPM_URL, cis_bdpm_path)
        print(f"✅ BDPM CIS: {CIS_BDPM_URL}")

        download_file(session, CIS_CIP_BDPM_URL, cis_cip_path)
        print(f"✅ BDPM CIS_CIP: {CIS_CIP_BDPM_URL}")

        download_file(session, CIS_CPD_BDPM_URL, cis_cpd_path)
        print(f"✅ BDPM CIS_CPD: {CIS_CPD_BDPM_URL}")

        ansm_link = find_ansm_excel_link(session)
        download_file(session, ansm_link, ansm_xls_path)
        print(f"✅ ANSM Excel: {ansm_link}")

    except Exception as ex:
        die(f"Download impossible -> arrêt sans modification Airtable. Détail: {ex}")

    print("2) Parsing fichiers...")
    cis_rows = parse_cis_bdpm(cis_bdpm_path)
    cip_info = parse_cis_cip_bdpm(cis_cip_path)
    cpd_map = parse_cis_cpd_bdpm(cis_cpd_path)
    ansm_cis = parse_ansm_retrocession_xls(ansm_xls_path)

    print(f"📦 CIS BDPM: {len(cis_rows)}")
    print(f"📦 CIS avec CIP infos: {len(cip_info)}")
    print(f"📦 CIS avec CPD: {len(cpd_map)}")
    print(f"📦 CIS ANSM rétrocession: {len(ansm_cis)}")

    # Merge view of cis
    bdpm_cis_set = set(cis_rows.keys())

    print("3) Lecture Airtable (inventaire)...")
    # IMPORTANT: on récupère record_id + Code cis
    existing_records = airt.list_all_records(fields=["Code cis", "Lien vers RCP"])
    airt_by_cis: Dict[str, Tuple[str, dict]] = {}
    for rec in existing_records:
        rid = rec.get("id")
        fields = rec.get("fields", {}) or {}
        cis = str(fields.get("Code cis", "")).strip()
        if cis:
            airt_by_cis[cis] = (rid, fields)

    airt_cis_set = set(airt_by_cis.keys())

    to_delete_cis = sorted(list(airt_cis_set - bdpm_cis_set))
    to_add_cis = sorted(list(bdpm_cis_set - airt_cis_set))
    to_update_cis = sorted(list(bdpm_cis_set & airt_cis_set))

    print(f"🧹 À supprimer (absents BDPM): {len(to_delete_cis)}")
    print(f"➕ À créer (absents Airtable): {len(to_add_cis)}")
    print(f"🔁 À mettre à jour: {len(to_update_cis)}")

    # Optional: prefetch RCP HTML for ALL impacted CIS if enabled,
    # and abort before Airtable modifications if any failure (strict)
    rcp_cache: Dict[str, str] = {}
    cpd_extracted: Dict[str, Optional[str]] = {}

    if RCP_FETCH_ENABLED:
        print("4) Récupération RCP (avant modifications Airtable)...")
        impacted = to_add_cis + to_update_cis
        if RCP_MAX_PER_RUN > 0:
            impacted = impacted[:RCP_MAX_PER_RUN]

        for idx, cis in enumerate(impacted, start=1):
            url = RCP_URL_TEMPLATE.format(cis=cis)
            try:
                html = fetch_html(session, url)
                rcp_cache[cis] = html
                cpd_extracted[cis] = extract_cpd_from_rcp_html(html)
            except Exception as ex:
                msg = f"RCP inaccessible pour CIS={cis} ({url}) -> {ex}"
                if RCP_STRICT:
                    die(msg)
                else:
                    eprint(f"⚠️ {msg}")
                    rcp_cache[cis] = ""
                    cpd_extracted[cis] = None
            time.sleep(RCP_SLEEP)

        print(f"✅ RCP récupérés: {len(rcp_cache)}/{len(impacted)}")

    # --- APPLY CHANGES TO AIRTABLE ---
    print("5) Application des changements Airtable...")

    # 5.1 Delete
    if to_delete_cis:
        del_ids = [airt_by_cis[cis][0] for cis in to_delete_cis if cis in airt_by_cis]
        print(f"🧹 Suppression Airtable: {len(del_ids)}")
        if del_ids:
            airt.batch_delete(del_ids)

    # Helper to build fields for a CIS
    def build_fields(cis: str) -> dict:
        row = cis_rows.get(cis)
        ci = cip_info.get(cis)

        cip13_joined = ""
        agr = ""
        has_reimb = False

        if ci:
            cip13_joined = ";".join(ci.cip13_list) if ci.cip13_list else ""
            agr = ci.agrement_collectivites or ""
            has_reimb = ci.has_reimbursement_rate

        cpd_txt_file = cpd_map.get(cis, "")

        # RCP link
        rcp_link = RCP_URL_TEMPLATE.format(cis=cis)

        # RCP html & extraction
        html = rcp_cache.get(cis) if RCP_FETCH_ENABLED else None
        if RCP_FETCH_ENABLED and html == "":
            html = None

        # CPD from RCP (priority)
        cpd_from_rcp = cpd_extracted.get(cis) if RCP_FETCH_ENABLED else None
        # si rien trouvé dans RCP, on garde CPD fichier comme fallback dans la colonne
        cpd_final = (cpd_from_rcp or "").strip()
        if not cpd_final:
            cpd_final = (cpd_txt_file or "").strip()

        # Status in "Rétrocession"
        status = compute_availability_status(
            cis=cis,
            in_ansm_retro=(cis in ansm_cis),
            has_reimb_rate=has_reimb,
            rcp_html=html,
            cpd_text=cpd_txt_file
        )

        fields = {
            "Code cis": cis,
            "Spécialité": row.specialite if row else "",
            "Forme": row.forme if row else "",
            "Voie d'administration": row.voie if row else "",
            "Laboratoire": row.laboratoire if row else "",
            "Lien vers RCP": rcp_link,
            "Agrément aux collectivités": agr,
            "CIP 13": cip13_joined,
            "Rétrocession": status,
            "Conditions de prescription et délivrance": cpd_final,
        }

        # Nettoyage: Airtable n'aime pas les NaN/Inf et parfois pas les None
        for k in list(fields.keys()):
            v = fields[k]
            if v is None:
                fields[k] = ""
        return fields

    # 5.2 Create new
    if to_add_cis:
        print(f"➕ Création Airtable: {len(to_add_cis)}")
        create_payload = []
        for cis in to_add_cis:
            create_payload.append(build_fields(cis))
        if create_payload:
            airt.batch_create(create_payload)

    # 5.3 Update existing
    if to_update_cis:
        print(f"🔁 Mise à jour Airtable: {len(to_update_cis)}")
        updates: List[Tuple[str, dict]] = []
        for cis in to_update_cis:
            rid = airt_by_cis[cis][0]
            updates.append((rid, build_fields(cis)))
        if updates:
            airt.batch_update(updates)

    print("✅ Terminé.")

if __name__ == "__main__":
    main()
