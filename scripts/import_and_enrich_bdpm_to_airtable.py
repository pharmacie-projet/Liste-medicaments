#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import math
import hashlib
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ============ CONFIG ============

BDPM_CIS_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
BDPM_CIS_CIP_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
BDPM_CIS_CPD_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"

ANSM_PAGE_URL = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

DOWNLOAD_DIR = "data"
BDPM_CIS_PATH = os.path.join(DOWNLOAD_DIR, "CIS_bdpm.txt")
BDPM_CIS_CIP_PATH = os.path.join(DOWNLOAD_DIR, "CIS_CIP_bdpm.txt")
BDPM_CIS_CPD_PATH = os.path.join(DOWNLOAD_DIR, "CIS_CPD_bdpm.txt")
ANSM_XLS_PATH = os.path.join(DOWNLOAD_DIR, "ansm_retrocession.xls")  # extension volontaire, peut être .xls ou .xlsx

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

# Airtable field names (doivent correspondre à tes colonnes)
FIELD_CIS = "Code cis"
FIELD_SPECIALITE = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE = "Voie d'administration"
FIELD_LABO = "Laboratoire"
FIELD_RCP_LINK = "Lien vers RCP"
FIELD_CIP13 = "CIP 13"
FIELD_AGREMENT = "Agrément aux collectivités"
FIELD_RETRO = "Rétrocession"
FIELD_COND_PRESC = "Conditions de prescription et délivrance"

# Throttle Airtable
AIRTABLE_REQ_SLEEP = 0.26  # ~3-4 req/sec

# Max records per Airtable batch (API v0 supports up to 10 per request)
BATCH_SIZE = 10

# User rules text
RETRO_VILLE = "Disponible en pharmacie de ville"
RETRO_RETROCESSION = "Disponible en rétrocession hospitalière"
RETRO_HOSP = "Réservé à l'usage hospitalier"
RETRO_UNKNOWN = "Pas d'informations mentionnées"

# ============ HELPERS ============

def die(msg: str, code: int = 1):
    print(f"❌ {msg}")
    sys.exit(code)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def sha1_file(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def assert_not_html_file(path: str, label: str):
    with open(path, "rb") as f:
        head = f.read(1024).lower()
    if b"<html" in head or b"<!doctype html" in head:
        raise RuntimeError(f"{label} téléchargé mais c'est du HTML (blocage / redirection probable).")

def http_get(session: requests.Session, url: str, timeout: int = 60) -> requests.Response:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r

def download_file(session: requests.Session, url: str, dest_path: str, label: str):
    print(f"⬇️  Téléchargement {label} : {url}")
    r = http_get(session, url)
    with open(dest_path, "wb") as f:
        f.write(r.content)
    assert_not_html_file(dest_path, label)
    print(f"✅ {label} OK ({os.path.getsize(dest_path)} bytes)")

def find_ansm_excel_link(session: requests.Session) -> str:
    """
    Sur la page ANSM, le fichier est téléchargeable via une icône (enveloppe).
    On cherche un lien vers .xls ou .xlsx dans la page.
    """
    print(f"🔎 Recherche lien Excel ANSM sur : {ANSM_PAGE_URL}")
    html = http_get(session, ANSM_PAGE_URL).text
    soup = BeautifulSoup(html, "lxml")
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    # parfois liens relatifs
    candidates = []
    for href in links:
        if not href:
            continue
        if ".xls" in href.lower() or ".xlsx" in href.lower():
            candidates.append(href)

    if not candidates:
        # fallback: recherche directe dans le texte
        m = re.findall(r'https?://[^\s"\']+\.xls[x]?', html, flags=re.I)
        candidates.extend(m)

    if not candidates:
        raise RuntimeError("Impossible de trouver un lien .xls/.xlsx sur la page ANSM (structure modifiée ou lien non visible).")

    # Priorité aux liens ansm.sante.fr
    candidates = sorted(candidates, key=lambda x: (0 if "ansm.sante.fr" in x else 1, len(x)))
    href = candidates[0]

    if href.startswith("/"):
        href = "https://ansm.sante.fr" + href
    print(f"✅ Lien ANSM trouvé : {href}")
    return href

def parse_ansm_retrocession_excel(path: str) -> set:
    import xlrd

    cis_set = set()
    book = xlrd.open_workbook(path)

    for sheet in book.sheets():
        for r in range(sheet.nrows):
            if sheet.ncols < 3:
                continue
            v = sheet.cell_value(r, 2)
            if v is None:
                continue
            cis = str(v).strip().replace(".0", "")
            if cis.isdigit():
                cis_set.add(cis)

    return cis_set


def safe_strip(s: str) -> str:
    return (s or "").strip()

# ============ BDPM PARSING ============

@dataclass
class CisRecord:
    cis: str
    specialite: str
    forme: str
    voie: str
    labo: str

def parse_cis_bdpm(path: str) -> Dict[str, CisRecord]:
    """
    CIS_bdpm.txt
    mapping colonnes (selon ta règle):
    1 = Code CIS
    2 = Spécialité
    3 = Forme
    4 = Voie d'administration
    avant-dernière = Laboratoire (on prend la dernière colonne non vide si besoin)
    """
    records: Dict[str, CisRecord] = {}

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue

            cis = safe_strip(parts[0])
            if not cis.isdigit():
                continue

            specialite = safe_strip(parts[1]) if len(parts) > 1 else ""
            forme = safe_strip(parts[2]) if len(parts) > 2 else ""
            voie = safe_strip(parts[3]) if len(parts) > 3 else ""

            # labo = "avant-dernière"
            labo = ""
            if len(parts) >= 2:
                labo_candidate = safe_strip(parts[-2])
                if labo_candidate:
                    labo = labo_candidate
                else:
                    # fallback: dernière non vide
                    for p in reversed(parts):
                        p = safe_strip(p)
                        if p:
                            labo = p
                            break

            records[cis] = CisRecord(
                cis=cis,
                specialite=specialite,
                forme=forme,
                voie=voie,
                labo=labo
            )
    return records

def parse_cis_cpd(path: str) -> Dict[str, str]:
    """
    CIS_CPD_bdpm.txt
    1 = CIS, 2 = texte CPD (Conditions de prescription et délivrance)
    """
    out: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            cis = safe_strip(parts[0])
            if not cis.isdigit():
                continue
            out[cis] = safe_strip(parts[1])
    return out

def parse_cis_cip_bdpm(path: str) -> Tuple[Dict[str, str], Dict[str, bool]]:
    """
    CIS_CIP_bdpm.txt
    - On récupère CIP13 (si présent) pour remplir "CIP 13"
    - On détecte un "taux de remboursement" via la présence de valeurs non vides dans les colonnes 8, 9, 10 (index 7,8,9).
      Si une des colonnes 8-9-10 est non vide => ville.
    """
    cis_to_cip13: Dict[str, str] = {}
    cis_has_remb: Dict[str, bool] = {}

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 1:
                continue
            cis = safe_strip(parts[0])
            if not cis.isdigit():
                continue

            # CIP13: on tente de trouver un champ 13 chiffres quelque part
            cip13 = ""
            for p in parts:
                p2 = re.sub(r"\D", "", p or "")
                if len(p2) == 13:
                    cip13 = p2
                    break
            if cip13:
                cis_to_cip13[cis] = cip13

            # remb col 8/9/10 (positions humaines)
            remb = False
            for idx in [7, 8, 9]:
                if idx < len(parts):
                    if safe_strip(parts[idx]) != "":
                        remb = True
                        break
            cis_has_remb[cis] = remb

    return cis_to_cip13, cis_has_remb

# ============ RCP PARSING (HTML) ============

def fetch_rcp_html(session: requests.Session, url: str, timeout: int = 60) -> str:
    r = session.get(url, timeout=timeout, allow_redirects=True, headers={
        "User-Agent": "Mozilla/5.0 (compatible; bdpm-airtable-bot/1.0)"
    })
    r.raise_for_status()
    return r.text

def extract_conditions_from_rcp(html: str) -> Tuple[Optional[str], bool, bool]:
    """
    Retourne:
      - texte CPD extrait depuis le bloc "CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE" (si trouvé)
      - hospital_flag: True si on trouve 'réservé à l’usage hospitalier' / 'usage hospitalier'
      - homeo_flag: True si on trouve 'homéopathi' (pour classer ville)
    """
    text_lower = re.sub(r"\s+", " ", BeautifulSoup(html, "lxml").get_text(" ", strip=True)).lower()

    homeo_flag = ("homéopathi" in text_lower) or ("homeopathi" in text_lower)
    hospital_flag = ("réservé à l’usage hospitalier" in text_lower) or ("reserve a l'usage hospitalier" in text_lower) or ("usage hospitalier" in text_lower)

    soup = BeautifulSoup(html, "lxml")

    # Recherche d'un titre contenant "CONDITIONS DE PRESCRIPTION"
    # Sur la BDPM, c'est souvent en majuscules dans un bloc proche du texte.
    title = None
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "strong", "p", "div"]):
        t = tag.get_text(" ", strip=True)
        if not t:
            continue
        tl = t.lower()
        if "conditions de prescription" in tl and "délivrance" in tl or "delivrance" in tl:
            title = tag
            break

    if not title:
        return (None, hospital_flag, homeo_flag)

    # Le texte utile est souvent dans les éléments suivants (siblings) jusqu'au prochain gros titre
    collected: List[str] = []
    cur = title

    # On avance dans le DOM
    for _ in range(60):
        cur = cur.find_next()
        if cur is None:
            break
        if cur.name in ["h1", "h2", "h3", "h4"]:
            # stop sur nouveau titre
            break
        t = cur.get_text(" ", strip=True)
        if not t:
            continue

        # On ignore les répétitions du titre
        tl = t.lower()
        if "conditions de prescription" in tl and ("delivrance" in tl or "délivrance" in tl):
            continue

        # Heuristique : on stop si on tombe sur une autre rubrique structurante très longue
        if re.match(r"^\d+\.\s", t.strip()):
            break

        collected.append(t)

        # On s'arrête dès qu'on a une phrase "propre" (souvent 1-3 lignes)
        if len(" ".join(collected)) > 400:
            break

    out = " ".join(collected).strip()
    out = re.sub(r"\s+", " ", out)
    if out == "":
        out = None
    return (out, hospital_flag, homeo_flag)

# ============ AIRTABLE CLIENT ============

class Airtable:
    def __init__(self, token: str, base_id: str, table_name: str):
        self.token = token
        self.base_id = base_id
        self.table_name = table_name
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        })

    def _url(self, suffix: str = "") -> str:
        # Important: table name must be URL-encoded
        from urllib.parse import quote
        return f"{AIRTABLE_API_BASE}/{self.base_id}/{quote(self.table_name)}{suffix}"

    def list_all(self, fields: Optional[List[str]] = None) -> List[dict]:
        out = []
        offset = None
        params = {}
        if fields:
            for i, f in enumerate(fields):
                params[f"fields[{i}]"] = f

        while True:
            if offset:
                params["offset"] = offset
            r = self.session.get(self._url(""), params=params)
            if r.status_code >= 400:
                raise RuntimeError(f"Airtable list error {r.status_code}: {r.text}")
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            time.sleep(AIRTABLE_REQ_SLEEP)
        return out

    def batch_create(self, records: List[dict]):
        for i in range(0, len(records), BATCH_SIZE):
            chunk = records[i:i+BATCH_SIZE]
            payload = {"records": [{"fields": r} for r in chunk]}
            r = self.session.post(self._url(""), data=json.dumps(payload))
            if r.status_code >= 400:
                raise RuntimeError(f"Airtable create error {r.status_code}: {r.text}")
            time.sleep(AIRTABLE_REQ_SLEEP)

    def batch_update(self, updates: List[Tuple[str, dict]]):
        for i in range(0, len(updates), BATCH_SIZE):
            chunk = updates[i:i+BATCH_SIZE]
            payload = {"records": [{"id": rid, "fields": fields} for rid, fields in chunk]}
            r = self.session.patch(self._url(""), data=json.dumps(payload))
            if r.status_code >= 400:
                raise RuntimeError(f"Airtable update error {r.status_code}: {r.text}")
            time.sleep(AIRTABLE_REQ_SLEEP)

    def batch_delete(self, record_ids: List[str]):
        # delete uses query params records[]=id
        for i in range(0, len(record_ids), BATCH_SIZE):
            chunk = record_ids[i:i+BATCH_SIZE]
            params = [("records[]", rid) for rid in chunk]
            r = self.session.delete(self._url(""), params=params)
            if r.status_code >= 400:
                raise RuntimeError(f"Airtable delete error {r.status_code}: {r.text}")
            time.sleep(AIRTABLE_REQ_SLEEP)

# ============ BUSINESS LOGIC ============

def rcp_link_for_cis(cis: str) -> str:
    # modèle demandé
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"

def compute_retro_status(
    cis: str,
    cis_in_ansm: bool,
    has_remb: bool,
    hospital_flag: bool,
    homeo_flag: bool
) -> str:
    """
    Ordre des règles (selon tes consignes finales) :
    1) Si taux de remboursement présent => "Disponible en pharmacie de ville"
       (mais si ANSM rétrocession => priorité rétrocession)
    2) Si présent dans liste ANSM => "Disponible en rétrocession hospitalière"
    3) Sinon si RCP mention "usage hospitalier" => "Réservé à l'usage hospitalier"
    4) Si homéopathie détectée => "Disponible en pharmacie de ville"
    5) Sinon => "Pas d'informations mentionnées"
    """
    if cis_in_ansm:
        return RETRO_RETROCESSION
    if has_remb:
        return RETRO_VILLE
    if homeo_flag:
        return RETRO_VILLE
    if hospital_flag:
        return RETRO_HOSP
    return RETRO_UNKNOWN

def main():
    load_dotenv()

    token = os.getenv("AIRTABLE_API_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table_name = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

    missing = []
    if not token:
        missing.append("AIRTABLE_API_TOKEN")
    if not base_id:
        missing.append("AIRTABLE_BASE_ID")
    if not table_name:
        missing.append("AIRTABLE_CIS_TABLE_NAME")
    if missing:
        die("Variables d'environnement manquantes: " + ", ".join(missing))

    ensure_dir(DOWNLOAD_DIR)

    session = requests.Session()

    # 1) Télécharger TOUS les fichiers d'abord. Si un fichier manque => STOP, aucune modif Airtable.
    try:
        download_file(session, BDPM_CIS_URL, BDPM_CIS_PATH, "BDPM CIS")
        download_file(session, BDPM_CIS_CIP_URL, BDPM_CIS_CIP_PATH, "BDPM CIS_CIP")
        download_file(session, BDPM_CIS_CPD_URL, BDPM_CIS_CPD_PATH, "BDPM CIS_CPD")

        ansm_excel_url = find_ansm_excel_link(session)
        download_file(session, ansm_excel_url, ANSM_XLS_PATH, "ANSM Excel")
    except Exception as e:
        die(f"Échec téléchargement (aucune action sur Airtable). Détail: {e}")

    # 2) Parser fichiers
    print("📦 Parsing fichiers...")
    cis_records = parse_cis_bdpm(BDPM_CIS_PATH)
    cis_to_cip13, cis_has_remb = parse_cis_cip_bdpm(BDPM_CIS_CIP_PATH)
    cis_to_cpd_file = parse_cis_cpd(BDPM_CIS_CPD_PATH)

    try:
        cis_ansm = parse_ansm_retrocession_excel(ANSM_XLS_PATH)
    except Exception as e:
        die(f"Impossible de lire le fichier ANSM (aucune action sur Airtable). Détail: {e}")

    print(f"✅ CIS BDPM: {len(cis_records)}")
    print(f"✅ CIS avec taux remboursement (ville): {sum(1 for v in cis_has_remb.values() if v)}")
    print(f"✅ CIS ANSM rétrocession: {len(cis_ansm)}")

    # 3) Lecture Airtable (inventaire) + sync add/delete (sans vider la table)
    at = Airtable(token=token, base_id=base_id, table_name=table_name)

    print("📥 Lecture Airtable (inventaire)...")
    existing = at.list_all(fields=[FIELD_CIS, FIELD_RCP_LINK, FIELD_RETRO])
    cis_to_recordid: Dict[str, str] = {}
    for rec in existing:
        fields = rec.get("fields", {})
        cis = str(fields.get(FIELD_CIS, "")).strip()
        if cis.isdigit():
            cis_to_recordid[cis] = rec["id"]

    bdpm_cis_set = set(cis_records.keys())
    airtable_cis_set = set(cis_to_recordid.keys())

    to_add = sorted(list(bdpm_cis_set - airtable_cis_set))
    to_del = sorted(list(airtable_cis_set - bdpm_cis_set))

    print(f"➕ À ajouter: {len(to_add)}")
    print(f"➖ À supprimer: {len(to_del)}")

    # 3a) Ajouter
    if to_add:
        create_payload: List[dict] = []
        for cis in to_add:
            r = cis_records[cis]
            create_payload.append({
                FIELD_CIS: r.cis,
                FIELD_SPECIALITE: r.specialite,
                FIELD_FORME: r.forme,
                FIELD_VOIE: r.voie,
                FIELD_LABO: r.labo,
                FIELD_RCP_LINK: rcp_link_for_cis(r.cis),
            })
        print("🧾 Création des nouveaux enregistrements...")
        at.batch_create(create_payload)

        # recharger mapping ids après création
        existing = at.list_all(fields=[FIELD_CIS, FIELD_RCP_LINK])
        cis_to_recordid = {}
        for rec in existing:
            fields = rec.get("fields", {})
            cis = str(fields.get(FIELD_CIS, "")).strip()
            if cis.isdigit():
                cis_to_recordid[cis] = rec["id"]

    # 3b) Supprimer
    if to_del:
        print("🗑️ Suppression des enregistrements absents BDPM...")
        at.batch_delete([cis_to_recordid[cis] for cis in to_del if cis in cis_to_recordid])

    # 4) Enrichissement (CIP13, agrément, rétrocession, CPD)
    #    + extraction RCP CPD et détection hospitalier/homeo (STOP si RCP inaccessible)
    print("🧠 Enrichissement (CIP13 / CPD / statut / RCP scraping)...")
    updates: List[Tuple[str, dict]] = []

    # On ne scrape pas tout si énorme: ici on le fait pour tous, comme tu l'as demandé.
    # (ça peut être long en CI). Si tu veux limiter, dis-moi et on met un plafond.
    all_cis_sorted = sorted(list(set(cis_to_recordid.keys()) & bdpm_cis_set))

    rcp_session = requests.Session()

    for idx, cis in enumerate(all_cis_sorted, start=1):
        rid = cis_to_recordid[cis]
        r = cis_records.get(cis)
        if not r:
            continue

        # CIP13
        cip13 = cis_to_cip13.get(cis, "")

        # Agrément aux collectivités : si tu as une source fiable ailleurs, branche-la ici.
        # Pour l'instant on ne l'invente pas (vide si pas dispo).
        # (Tu avais parlé d'une 7ème colonne d'un fichier joint, si tu veux on le rebranche proprement)
        agrement = ""  # garder vide si pas de donnée

        # CPD depuis fichier (fallback si RCP non extractible)
        cpd_from_file = cis_to_cpd_file.get(cis)

        # RCP link
        rcp_url = rcp_link_for_cis(cis)

        # Fetch RCP + extract
        try:
            html = fetch_rcp_html(rcp_session, rcp_url, timeout=60)
        except Exception as e:
            die(f"RCP inaccessible pour CIS={cis} ({rcp_url}). STOP (aucune suite). Détail: {e}")

        cpd_from_rcp, hospital_flag, homeo_flag = extract_conditions_from_rcp(html)

        has_remb = bool(cis_has_remb.get(cis, False))
        cis_in_ansm = cis in cis_ansm

        retro_status = compute_retro_status(
            cis=cis,
            cis_in_ansm=cis_in_ansm,
            has_remb=has_remb,
            hospital_flag=hospital_flag,
            homeo_flag=homeo_flag
        )

        # CPD final: priorité au RCP si on a extrait quelque chose, sinon fichier CIS_CPD
        final_cpd = cpd_from_rcp or cpd_from_file or ""

        fields_update = {
            FIELD_RCP_LINK: rcp_url,
            FIELD_RETRO: retro_status,
        }
        if cip13:
            fields_update[FIELD_CIP13] = cip13
        if agrement:
            fields_update[FIELD_AGREMENT] = agrement
        if final_cpd:
            fields_update[FIELD_COND_PRESC] = final_cpd

        # Mise à jour des champs “base” aussi (au cas où BDPM a changé)
        fields_update[FIELD_SPECIALITE] = r.specialite
        fields_update[FIELD_FORME] = r.forme
        fields_update[FIELD_VOIE] = r.voie
        fields_update[FIELD_LABO] = r.labo

        updates.append((rid, fields_update))

        if idx % 250 == 0:
            print(f"… {idx}/{len(all_cis_sorted)}")

    print(f"✍️ Updates à pousser: {len(updates)}")
    if updates:
        at.batch_update(updates)

    print("✅ Terminé.")

if __name__ == "__main__":
    main()
