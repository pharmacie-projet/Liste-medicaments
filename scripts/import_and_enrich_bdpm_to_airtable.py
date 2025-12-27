#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import math
import random
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Excel readers
import xlrd
from openpyxl import load_workbook

# -----------------------------
# Config
# -----------------------------
BDPM_CIS_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
BDPM_CIS_CIP_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
BDPM_CIS_CPD_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"

ANSM_PAGE_URL = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

RCP_URL_TEMPLATE = "https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"

# Airtable field names (doivent exister)
FIELD_CIS = "Code cis"
FIELD_SPECIALITE = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE_ADMIN = "Voie d'administration"
FIELD_LABO = "Laboratoire"
FIELD_CPD = "Conditions de prescription et délivrance"
FIELD_RCP_LINK = "Lien vers RCP"
FIELD_AGREMENT = "Agrément aux collectivités"
FIELD_CIP13 = "CIP 13"
FIELD_DISPO = "Disponibilité du traitement"

# Airtable batch sizes
AIRTABLE_BATCH = 10

# Requests
DEFAULT_TIMEOUT = 60
USER_AGENT = "Mozilla/5.0 (compatible; BDPM-Airtable-Bot/1.0; +https://github.com/)"

# Stop on ANSM failure BEFORE touching Airtable
STOP_IF_ANSM_FAIL = True

# -----------------------------
# Helpers
# -----------------------------

def env_required(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"Variable manquante: {name}")
    return v

def safe_sleep(base: float = 0.3) -> None:
    time.sleep(base + random.random() * base)

def normalize_ws(s: str) -> str:
    s = s.replace("\r", "")
    # keep newlines but normalize spaces
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def decode_bytes(b: bytes) -> str:
    """
    BDPM peut varier. On essaie utf-8 strict, sinon latin-1 (évite les '�').
    """
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("latin-1", errors="replace")

def download_text(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return decode_bytes(r.content)

def download_binary(session: requests.Session, url: str) -> bytes:
    r = session.get(url, timeout=120)
    r.raise_for_status()
    # protection contre HTML au lieu du fichier
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ctype or (len(r.content) < 4000 and b"<html" in r.content[:2000].lower()):
        raise RuntimeError(f"Le lien ne renvoie pas un fichier binaire attendu (probable HTML). URL: {url}")
    return r.content

# -----------------------------
# ANSM - find Excel link (robuste)
# -----------------------------

def find_ansm_excel_url(session: requests.Session) -> str:
    r = session.get(ANSM_PAGE_URL, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    hrefs = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href:
            hrefs.append(href)

    urls = [urljoin(ANSM_PAGE_URL, h) for h in hrefs]

    candidates: List[str] = []
    for u in urls:
        low = u.lower()
        if (low.endswith(".xls") or low.endswith(".xlsx")) and ("retrocession" in low or "uploads" in low):
            candidates.append(u)

    if not candidates:
        raw = re.findall(r'https?://[^\s"<>]+?\.(?:xls|xlsx)', r.text, flags=re.IGNORECASE)
        candidates.extend(raw)

    # de-dup
    seen = set()
    cand = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            cand.append(c)

    print(f"ANSM: {len(cand)} candidat(s) .xls/.xlsx")
    for i, c in enumerate(cand[:10], start=1):
        print(f"  - {i}: {c}")

    if not cand:
        raise RuntimeError("Impossible de trouver le fichier ANSM (.xls/.xlsx) sur la page.")

    def score(u: str) -> int:
        s = 0
        if re.search(r"\b20\d{2}/\d{2}/\d{2}\b", u):
            s += 10
        if re.search(r"\b20\d{2}\d{2}\d{2}\b", u):
            s += 8
        if "retrocession" in u.lower():
            s += 5
        return s

    cand.sort(key=score, reverse=True)
    return cand[0]

def parse_ansm_excel_cis(excel_bytes: bytes, is_xls: bool) -> Set[str]:
    """
    La 3e colonne (index 2) = Code CIS.
    Utilise xlrd (xls) ou openpyxl (xlsx) sans pandas (évite les soucis numpy).
    """
    cis_set: Set[str] = set()

    if is_xls:
        book = xlrd.open_workbook(file_contents=excel_bytes)
        sheet = book.sheet_by_index(0)
        for r in range(sheet.nrows):
            val = sheet.cell_value(r, 2)  # 3e colonne
            s = str(val).strip()
            # xlrd peut retourner float -> "61234567.0"
            s = s.replace(".0", "")
            if s.isdigit():
                cis_set.add(s)
    else:
        # xlsx
        from io import BytesIO
        wb = load_workbook(filename=BytesIO(excel_bytes), read_only=True, data_only=True)
        ws = wb.worksheets[0]
        for row in ws.iter_rows(values_only=True):
            if not row or len(row) < 3:
                continue
            val = row[2]
            if val is None:
                continue
            s = str(val).strip()
            s = s.replace(".0", "")
            if s.isdigit():
                cis_set.add(s)

    return cis_set

def load_ansm_retrocession_cis(session: requests.Session) -> Set[str]:
    print("Recherche lien Excel ANSM ...")
    url = find_ansm_excel_url(session)
    print(f"Lien ANSM trouvé : {url}")

    data = download_binary(session, url)
    is_xls = url.lower().endswith(".xls") and not url.lower().endswith(".xlsx")
    cis = parse_ansm_excel_cis(data, is_xls=is_xls)
    if not cis:
        raise RuntimeError("Fichier ANSM lu mais aucun CIS détecté.")
    print(f"CIS ANSM rétrocession: {len(cis)}")
    return cis

# -----------------------------
# BDPM Parsers
# -----------------------------

@dataclass
class CisRow:
    cis: str
    specialite: str
    forme: str
    voie_admin: str
    laboratoire: str  # titulaire

@dataclass
class CipInfo:
    cis: str
    cip13: str
    agrement: str
    remboursement_present: bool

def parse_bdpm_cis(text: str) -> Dict[str, CisRow]:
    """
    CIS_bdpm.txt : séparateur TAB, latin-1/utf-8 selon source.
    Colonnes importantes:
    0 CIS
    1 Dénomination
    2 Forme pharmaceutique
    3 Voies d'administration
    10 Titulaire
    """
    out: Dict[str, CisRow] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 11:
            continue
        cis = parts[0].strip()
        if not cis.isdigit():
            continue

        denom = parts[1].strip()
        forme = parts[2].strip()
        voie = parts[3].strip()
        titulaire = parts[10].strip()

        out[cis] = CisRow(
            cis=cis,
            specialite=denom,
            forme=forme,
            voie_admin=voie,
            laboratoire=titulaire
        )
    return out

def extract_first_cip13(parts: List[str]) -> str:
    # cherche un bloc de 13 chiffres dans toute la ligne
    joined = "\t".join(parts)
    m = re.search(r"\b(\d{13})\b", joined)
    return m.group(1) if m else ""

def parse_bdpm_cis_cip(text: str) -> Dict[str, CipInfo]:
    """
    CIS_CIP_bdpm.txt (TAB).
    Besoins:
    - Agrément aux collectivités = 7e colonne (index 6) selon ta consigne
    - présence taux remboursement = colonnes 8/9/10 (positions 8-9-10) => index 7/8/9
    - CIP 13: extraction robuste (13 digits)
    """
    out: Dict[str, CipInfo] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        cis = parts[0].strip()
        if not cis.isdigit():
            continue

        agrement = parts[6].strip() if len(parts) > 6 else ""
        # colonnes 8/9/10 => index 7/8/9
        col8 = parts[7].strip() if len(parts) > 7 else ""
        col9 = parts[8].strip() if len(parts) > 8 else ""
        col10 = parts[9].strip() if len(parts) > 9 else ""
        remboursement_present = any([col8, col9, col10])

        cip13 = extract_first_cip13(parts)

        # On garde une seule entrée par CIS (si multiples lignes, on privilégie:
        # - remboursement_present True
        # - cip13 non vide
        # - agrement non vide)
        prev = out.get(cis)
        if prev is None:
            out[cis] = CipInfo(cis=cis, cip13=cip13, agrement=agrement, remboursement_present=remboursement_present)
        else:
            best = prev
            def score(ci: CipInfo) -> int:
                return (2 if ci.remboursement_present else 0) + (1 if ci.cip13 else 0) + (1 if ci.agrement else 0)
            cand = CipInfo(cis=cis, cip13=cip13, agrement=agrement, remboursement_present=remboursement_present)
            if score(cand) > score(best):
                out[cis] = cand
    return out

def parse_bdpm_cis_cpd(text: str) -> Dict[str, str]:
    """
    CIS_CPD_bdpm.txt : colonnes TAB
    col0 CIS
    col1 Conditions
    (fallback uniquement si RCP inaccessible/indisponible pour le CPD)
    """
    out: Dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        cis = parts[0].strip()
        if not cis.isdigit():
            continue
        cpd = parts[1].strip()
        if cpd:
            out[cis] = cpd
    return out

# -----------------------------
# RCP parsing for CPD + signals
# -----------------------------

@dataclass
class RcpSignals:
    cpd_text: str
    has_hospital_usage: bool
    has_homeopathy: bool

def fetch_rcp_and_extract(session: requests.Session, cis: str) -> RcpSignals:
    url = RCP_URL_TEMPLATE.format(cis=cis)
    r = session.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    full_text = normalize_ws(soup.get_text("\n"))
    low = full_text.lower()

    has_homeopathy = ("homéopathi" in low) or ("homeopathi" in low)
    has_hospital_usage = ("usage hospitalier" in low) or ("réservé à l’usage hospitalier" in low) or ("reserve a l'usage hospitalier" in low)

    # Trouver le bloc "CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE"
    # On cherche un élément contenant cette chaîne puis on prend le texte après.
    target_re = re.compile(r"conditions\s+de\s+prescription\s+et\s+de\s+d[ée]livrance", re.IGNORECASE)

    cpd_text = ""
    # 1) Chercher dans les headings/strong
    candidates = soup.find_all(string=target_re)
    if candidates:
        node = candidates[0]
        # Remonter au conteneur visuel
        container = node.parent
        # On essaye d'aller au parent supérieur si c'est juste <span> etc.
        for _ in range(3):
            if container and container.parent:
                container = container.parent

        # Extraire les paragraphes suivants dans ce conteneur, jusqu’au prochain titre en majuscules
        # Méthode simple: récupérer tout le texte du conteneur puis couper après le header
        container_text = normalize_ws(container.get_text("\n"))
        m = target_re.search(container_text)
        if m:
            after = container_text[m.end():].strip()
            # couper si on retombe sur un gros titre probable (ex: "13." ou "ANNEXE" etc.)
            after = re.split(r"\n\s*(?:\d+\.\s+[A-ZÉÈÀÙÂÊÎÔÛÇ]|ANNEXE|CONDITIONS\s+D[’']UTILISATION)\b", after)[0]
            cpd_text = normalize_ws(after)

    # Fallback: regex dans tout le texte
    if not cpd_text:
        m = re.search(r"CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE\s*(.+)$", full_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            after = m.group(1)
            # couper à la fin si un nouveau titre apparaît
            after = re.split(r"\n\s*\d+\.\s+", after)[0]
            cpd_text = normalize_ws(after)

    return RcpSignals(
        cpd_text=cpd_text,
        has_hospital_usage=has_hospital_usage,
        has_homeopathy=has_homeopathy
    )

def compute_disponibilite(
    remboursement_present: bool,
    is_retro_ansm: bool,
    has_hospital_usage: bool,
    has_homeopathy: bool
) -> str:
    ville = remboursement_present or has_homeopathy

    if ville and is_retro_ansm:
        return "Disponible en ville et en rétrocession hospitalière"
    if is_retro_ansm:
        return "Disponible en rétrocession hospitalière"
    if has_hospital_usage:
        return "Réservé à l'usage hospitalier"
    if ville:
        return "Disponible en pharmacie de ville"
    return "Pas d'informations mentionnées"

# -----------------------------
# Airtable client (retry / batch)
# -----------------------------

class AirtableClient:
    def __init__(self, api_token: str, base_id: str, table_name: str):
        self.api_token = api_token
        self.base_id = base_id
        self.table_name = table_name
        self.base_url = f"https://api.airtable.com/v0/{base_id}/{requests.utils.quote(table_name, safe='')}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT
        })

    def _request(self, method: str, url: str, **kwargs) -> dict:
        for attempt in range(1, 8):
            resp = self.session.request(method, url, timeout=DEFAULT_TIMEOUT, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(30, (2 ** attempt) * 0.5) + random.random()
                print(f"Airtable {resp.status_code} -> retry dans {wait:.1f}s")
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f"Airtable error {resp.status_code}: {resp.text[:800]}")
            return resp.json()
        raise RuntimeError("Airtable: trop de retries")

    def list_all_records(self, fields: Optional[List[str]] = None) -> List[dict]:
        records = []
        offset = None
        params = {}
        if fields:
            for f in fields:
                params.setdefault("fields[]", []).append(f)

        while True:
            p = dict(params)
            if offset:
                p["offset"] = offset
            data = self._request("GET", self.base_url, params=p)
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            safe_sleep(0.2)
        return records

    def batch_create(self, rows: List[dict]) -> None:
        for i in range(0, len(rows), AIRTABLE_BATCH):
            chunk = rows[i:i + AIRTABLE_BATCH]
            payload = {"records": [{"fields": r} for r in chunk]}
            self._request("POST", self.base_url, data=json.dumps(payload))
            safe_sleep(0.35)

    def batch_update(self, updates: List[Tuple[str, dict]]) -> None:
        for i in range(0, len(updates), AIRTABLE_BATCH):
            chunk = updates[i:i + AIRTABLE_BATCH]
            payload = {"records": [{"id": rid, "fields": fields} for rid, fields in chunk]}
            self._request("PATCH", self.base_url, data=json.dumps(payload))
            safe_sleep(0.35)

    def batch_delete(self, record_ids: List[str]) -> None:
        for i in range(0, len(record_ids), AIRTABLE_BATCH):
            chunk = record_ids[i:i + AIRTABLE_BATCH]
            # delete uses query params records[]=id
            params = [("records[]", rid) for rid in chunk]
            self._request("DELETE", self.base_url, params=params)
            safe_sleep(0.35)

# -----------------------------
# Main sync logic
# -----------------------------

def main() -> None:
    # Env
    api_token = env_required("AIRTABLE_API_TOKEN")
    base_id = env_required("AIRTABLE_BASE_ID")
    table_name = env_required("AIRTABLE_CIS_TABLE_NAME")  # tu veux garder ce nom

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    print("1) Téléchargements BDPM ...")
    cis_txt = download_text(session, BDPM_CIS_URL)
    print(f"BDPM CIS OK ({len(cis_txt)} chars)")

    cis_cip_txt = download_text(session, BDPM_CIS_CIP_URL)
    print(f"BDPM CIS_CIP OK ({len(cis_cip_txt)} chars)")

    cis_cpd_txt = download_text(session, BDPM_CIS_CPD_URL)
    print(f"BDPM CIS_CPD OK ({len(cis_cpd_txt)} chars)")

    print("2) Recherche + téléchargement ANSM ...")
    try:
        cis_ansm_retro = load_ansm_retrocession_cis(session)
    except Exception as e:
        print(f"❌ Impossible de trouver/lire le fichier ANSM. Aucune action Airtable. Détail: {e}")
        if STOP_IF_ANSM_FAIL:
            raise
        cis_ansm_retro = set()

    print("3) Parsing fichiers BDPM ...")
    cis_map = parse_bdpm_cis(cis_txt)
    cip_map = parse_bdpm_cis_cip(cis_cip_txt)
    cpd_file_map = parse_bdpm_cis_cpd(cis_cpd_txt)

    print(f"CIS BDPM: {len(cis_map)}")
    print(f"CIS_CIP infos: {len(cip_map)}")
    print(f"CIS_CPD infos: {len(cpd_file_map)}")

    # Airtable client
    at = AirtableClient(api_token, base_id, table_name)

    print("4) Lecture inventaire Airtable ...")
    existing = at.list_all_records(fields=[FIELD_CIS])
    cis_to_id: Dict[str, str] = {}
    for rec in existing:
        fields = rec.get("fields", {})
        cis = str(fields.get(FIELD_CIS, "")).strip()
        if cis:
            cis_to_id[cis] = rec["id"]

    existing_set = set(cis_to_id.keys())
    bdpm_set = set(cis_map.keys())

    to_delete_cis = sorted(existing_set - bdpm_set)
    to_create_cis = sorted(bdpm_set - existing_set)
    to_keep_cis = sorted(existing_set & bdpm_set)

    print(f"Airtable existants: {len(existing_set)}")
    print(f"A créer: {len(to_create_cis)}")
    print(f"A supprimer: {len(to_delete_cis)}")
    print(f"A conserver: {len(to_keep_cis)}")

    # 5) Suppressions (seulement si ANSM OK, sinon on ne touche pas Airtable)
    if to_delete_cis:
        print("5) Suppression des CIS absents BDPM ...")
        ids = [cis_to_id[cis] for cis in to_delete_cis if cis in cis_to_id]
        at.batch_delete(ids)
        print(f"Supprimés: {len(ids)}")

        # mettre à jour cis_to_id si besoin (optionnel)
        for cis in to_delete_cis:
            cis_to_id.pop(cis, None)

    # 6) Créations
    if to_create_cis:
        print("6) Création des CIS manquants ...")
        new_rows = []
        for cis in to_create_cis:
            row = cis_map[cis]
            new_rows.append({
                FIELD_CIS: row.cis,
                FIELD_SPECIALITE: row.specialite,
                FIELD_FORME: row.forme,
                FIELD_VOIE_ADMIN: row.voie_admin,
                FIELD_LABO: row.laboratoire,
                FIELD_RCP_LINK: RCP_URL_TEMPLATE.format(cis=row.cis),
            })
        at.batch_create(new_rows)
        print(f"Créés: {len(new_rows)}")

        # Recharger ids (obligatoire pour updater ensuite)
        existing = at.list_all_records(fields=[FIELD_CIS])
        cis_to_id = {}
        for rec in existing:
            fields = rec.get("fields", {})
            cis = str(fields.get(FIELD_CIS, "")).strip()
            if cis:
                cis_to_id[cis] = rec["id"]

    # 7) Enrichissements / updates
    print("7) Enrichissement des champs (CIP13 / agrément / dispo / CPD depuis RCP) ...")

    updates: List[Tuple[str, dict]] = []
    total = len(cis_map)

    # On peut limiter la charge RCP si besoin, mais tu as demandé "totalité"
    for idx, cis in enumerate(sorted(cis_map.keys()), start=1):
        rid = cis_to_id.get(cis)
        if not rid:
            continue

        base = cis_map[cis]
        cip = cip_map.get(cis)
        remboursement_present = cip.remboursement_present if cip else False
        agrement = cip.agrement if cip else ""
        cip13 = cip.cip13 if cip else ""

        is_retro = cis in cis_ansm_retro

        # RCP extraction
        try:
            sig = fetch_rcp_and_extract(session, cis)
            cpd_text = sig.cpd_text
            has_hosp = sig.has_hospital_usage
            has_homeo = sig.has_homeopathy
        except Exception as e:
            # On ne stoppe pas tout l’import BDPM si un RCP est inaccessible.
            # MAIS tu peux choisir de stopper : mets "raise" ici si tu veux.
            print(f"⚠️ RCP inaccessible pour CIS={cis}: {e}")
            cpd_text = ""
            has_hosp = False
            has_homeo = False

        # fallback CPD file si RCP vide
        if not cpd_text:
            cpd_text = cpd_file_map.get(cis, "")

        dispo = compute_disponibilite(
            remboursement_present=remboursement_present,
            is_retro_ansm=is_retro,
            has_hospital_usage=has_hosp,
            has_homeopathy=has_homeo
        )

        fields_update = {
            # toujours remettre à jour ces champs (corrige encodage et erreurs)
            FIELD_SPECIALITE: base.specialite,
            FIELD_FORME: base.forme,
            FIELD_VOIE_ADMIN: base.voie_admin,
            FIELD_LABO: base.laboratoire,
            FIELD_RCP_LINK: RCP_URL_TEMPLATE.format(cis=cis),
            FIELD_DISPO: dispo,
        }

        if agrement:
            fields_update[FIELD_AGREMENT] = agrement
        if cip13:
            fields_update[FIELD_CIP13] = cip13
        if cpd_text:
            fields_update[FIELD_CPD] = cpd_text

        updates.append((rid, fields_update))

        if idx % 200 == 0:
            print(f"  - préparé {idx}/{total} updates")

    if updates:
        print(f"Envoi updates Airtable: {len(updates)}")
        at.batch_update(updates)
    print("✅ Terminé.")

if __name__ == "__main__":
    main()
