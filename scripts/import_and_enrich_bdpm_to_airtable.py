import os
import re
import sys
import time
import json
from typing import Dict, List, Tuple, Optional, Set

import requests
import xlrd
from bs4 import BeautifulSoup

# -----------------------------
# CONFIG
# -----------------------------
BDPM_CIS_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
BDPM_CIS_CIP_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
BDPM_CIS_CPD_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"
ANSM_PAGE_URL = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

FIELD_CIS = "Code cis"
FIELD_SPEC = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE = "Voie d'administration"
FIELD_LABO = "Laboratoire"
FIELD_RCP_LINK = "Lien vers RCP"
FIELD_CIP13 = "CIP 13"
FIELD_AGREMENT = "Agrément aux collectivités"
FIELD_CPD = "Conditions de prescription et délivrance"

# ✅ Nouveau champ demandé (remplace “Rétrocession”)
FIELD_DISPO = "Disponibilité du traitement"

# Valeurs attendues
DISPO_VILLE = "Disponible en pharmacie de ville"
DISPO_RETRO = "Disponible en rétrocession hospitalière"
DISPO_VILLE_ET_RETRO = "Disponible en ville et en rétrocession hospitalière"
DISPO_HOSP = "Réservé à l'usage hospitalier"
DISPO_NONE = "Pas d'informations mentionnées"

USER_AGENT = "Mozilla/5.0 (compatible; BDPM-AirtableBot/1.0)"


# -----------------------------
# UTILS
# -----------------------------
def die(msg: str, code: int = 1):
    print(f"❌ {msg}")
    sys.exit(code)

def ok(msg: str):
    print(f"✅ {msg}")

def info(msg: str):
    print(f"ℹ️ {msg}")

def warn(msg: str):
    print(f"⚠️ {msg}")

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\r", "")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def download_bytes(url: str, timeout: int = 60) -> bytes:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    r.raise_for_status()
    return r.content

def decode_text(b: bytes) -> str:
    # BDPM est souvent UTF-8, parfois caractères bizarres -> on sécurise
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("latin-1", errors="replace")

def chunks(lst, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]


# -----------------------------
# ANSM RETRO LIST (XLS)
# -----------------------------
def find_ansm_xls_url(html: str) -> Optional[str]:
    # Sur la page ANSM, on retrouve généralement un lien /uploads/...retrocession...xls
    # (le bouton “enveloppe”)
    m = re.search(r'https://ansm\.sante\.fr/uploads/[0-9/]+[^"\']*retrocession[^"\']*\.xls', html, flags=re.I)
    return m.group(0) if m else None

def parse_ansm_xls_get_cis_set(xls_bytes: bytes) -> Set[str]:
    # ANSM fournit un .xls (pas .xlsx). On lit via xlrd.
    book = xlrd.open_workbook(file_contents=xls_bytes)
    sh = book.sheet_by_index(0)

    cis_set = set()
    for rx in range(sh.nrows):
        row = sh.row_values(rx)
        if len(row) < 3:
            continue
        val = str(row[2]).strip()  # 3ème colonne (index 2)
        # conserve uniquement les chiffres
        cis = re.sub(r"\D", "", val)
        if len(cis) >= 6:
            cis_set.add(cis)
    return cis_set


# -----------------------------
# BDPM PARSERS (TXT tab-separated)
# -----------------------------
def parse_bdpm_cis(txt: str) -> Dict[str, Dict[str, str]]:
    """
    CIS_bdpm.txt : colonnes officielles (tab). On extrait:
    - CIS
    - dénomination
    - forme
    - voies
    - titulaire (utilisé comme laboratoire)
    """
    data: Dict[str, Dict[str, str]] = {}
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        # CIS est en 1ère colonne
        cis = parts[0].strip()
        if not cis.isdigit():
            continue

        denom = parts[1].strip() if len(parts) > 1 else ""
        forme = parts[2].strip() if len(parts) > 2 else ""
        voies = parts[3].strip() if len(parts) > 3 else ""

        # le titulaire est généralement en colonne 8 ou proche selon versions.
        # On prend un fallback robuste : on cherche un champ "titulaire" plausible en fin.
        titulaire = ""
        if len(parts) >= 8:
            titulaire = parts[7].strip()
        if not titulaire and len(parts) >= 10:
            titulaire = parts[9].strip()

        data[cis] = {
            FIELD_CIS: cis,
            FIELD_SPEC: denom,
            FIELD_FORME: forme,
            FIELD_VOIE: voies,
            FIELD_LABO: titulaire,
            FIELD_RCP_LINK: f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp",
        }
    return data

def parse_bdpm_cis_cip(txt: str) -> Tuple[Dict[str, str], Dict[str, bool], Dict[str, str]]:
    """
    CIS_CIP_bdpm.txt :
    - on détecte CIP13 (un nombre 13 chiffres)
    - on détecte présence d'un taux de remboursement via colonnes 8/9/10 (index 7/8/9)
    - on prend la 7e colonne (index 6) pour 'Agrément aux collectivités' si présente (ton besoin précédent)
    """
    cis_to_cip13: Dict[str, str] = {}
    cis_has_rate: Dict[str, bool] = {}
    cis_agrement: Dict[str, str] = {}

    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        cis = parts[0].strip() if parts else ""
        if not cis.isdigit():
            continue

        # CIP13 : première séquence 13 chiffres dans la ligne
        cip13 = ""
        m = re.search(r"\b\d{13}\b", line)
        if m:
            cip13 = m.group(0)
            cis_to_cip13[cis] = cip13

        # Agrément collectivités : 7e colonne (index 6)
        if len(parts) > 6:
            agr = normalize_text(parts[6])
            if agr:
                cis_agrement[cis] = agr

        # Taux de remboursement : colonnes 8-9-10 => index 7/8/9
        rate_present = False
        for idx in (7, 8, 9):
            if len(parts) > idx and normalize_text(parts[idx]):
                rate_present = True
                break
        cis_has_rate[cis] = cis_has_rate.get(cis, False) or rate_present

    return cis_to_cip13, cis_has_rate, cis_agrement


# -----------------------------
# RCP fetch + CPD extraction with line breaks preserved
# -----------------------------
def fetch_rcp_html(url: str, timeout: int = 40) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    r.raise_for_status()
    # requests -> unicode correct, garde accents
    return r.text

def extract_cpd_from_rcp(html: str) -> str:
    """
    Extrait le bloc sous 'CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE'
    en conservant paragraphes + lignes vides (comme ta capture).
    """
    soup = BeautifulSoup(html, "lxml")

    title_node = soup.find(string=re.compile(
        r"CONDITIONS\s+DE\s+PRESCRIPTION\s+ET\s+DE\s+DELIVRANCE",
        re.IGNORECASE
    ))
    if not title_node:
        return ""

    anchor = title_node.parent
    collected: List[str] = []
    started = False

    stop_re = re.compile(
        r"^\s*(\d+\.\s+|POSOLOGIE|CONTRE-INDICATION|MISES EN GARDE|"
        r"EFFETS INDESIRABLES|PROPRIETES|INSTRUCTIONS|DATE DE MISE A JOUR|SURDOSAGE)\b",
        re.IGNORECASE
    )

    for el in anchor.next_elements:
        if getattr(el, "name", None) in ("script", "style"):
            continue

        if not started:
            if isinstance(el, str) and re.search(
                r"CONDITIONS\s+DE\s+PRESCRIPTION\s+ET\s+DE\s+DELIVRANCE",
                el,
                re.I
            ):
                started = True
            continue

        if getattr(el, "name", None) in ("h1", "h2", "h3", "h4"):
            txt = normalize_text(el.get_text(" ", strip=True))
            if txt and stop_re.match(txt):
                break

        if getattr(el, "name", None) in ("p", "li"):
            txt = normalize_text(el.get_text(" ", strip=True))
            if txt:
                collected.append(txt)

        if getattr(el, "name", None) == "br":
            if collected and collected[-1] != "":
                collected.append("")

    # reconstruit avec lignes vides
    lines: List[str] = []
    for item in collected:
        if item == "":
            if lines and lines[-1] != "":
                lines.append("")
        else:
            lines.append(item)
    while lines and lines[-1] == "":
        lines.pop()

    return "\n".join(lines).strip()

def rcp_mentions_hospital_use(html: str) -> bool:
    t = BeautifulSoup(html, "lxml").get_text(" ", strip=True).lower()
    # on se contente de "usage hospitalier" (ton critère)
    return "usage hospitalier" in t

def rcp_mentions_homeopathy(html: str) -> bool:
    t = BeautifulSoup(html, "lxml").get_text(" ", strip=True).lower()
    return "homéopathi" in t or "homeopathi" in t


# -----------------------------
# Disponibilité logic (UPDATED)
# -----------------------------
def compute_disponibilite(
    cis: str,
    has_rate: bool,
    is_retro_ansm: bool,
    rcp_html: Optional[str]
) -> str:
    """
    Règles demandées (dans cet ordre) :

    1) Si taux de remboursement présent => "Disponible en pharmacie de ville"
    2) Si ANSM rétrocession => "Disponible en rétrocession hospitalière"
       MAIS si (1) et (2) => "Disponible en ville et en rétrocession hospitalière"
    3) Sinon, si RCP contient "usage hospitalier" => "Réservé à l'usage hospitalier"
    4) Sinon, si RCP contient "homéopathi" => "Disponible en pharmacie de ville"
    5) Sinon => "Pas d'informations mentionnées"
    """
    # combo ville + rétro
    if has_rate and is_retro_ansm:
        return DISPO_VILLE_ET_RETRO
    if is_retro_ansm:
        return DISPO_RETRO
    if has_rate:
        return DISPO_VILLE

    if rcp_html:
        if rcp_mentions_hospital_use(rcp_html):
            return DISPO_HOSP
        if rcp_mentions_homeopathy(rcp_html):
            return DISPO_VILLE

    return DISPO_NONE


# -----------------------------
# AIRTABLE API
# -----------------------------
def airtable_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def airtable_list_records(token: str, base_id: str, table: str, view: Optional[str] = None) -> List[Dict]:
    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table, safe='')}"
    records = []
    params = {"pageSize": 100}
    if view:
        params["view"] = view

    offset = None
    while True:
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=airtable_headers(token), params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records

def airtable_batch_create(token: str, base_id: str, table: str, records_fields: List[Dict]) -> None:
    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table, safe='')}"
    for batch in chunks(records_fields, 10):
        payload = {"records": [{"fields": f} for f in batch], "typecast": True}
        r = requests.post(url, headers=airtable_headers(token), data=json.dumps(payload), timeout=60)
        r.raise_for_status()

def airtable_batch_update(token: str, base_id: str, table: str, updates: List[Tuple[str, Dict]]) -> None:
    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table, safe='')}"
    for batch in chunks(updates, 10):
        payload = {
            "records": [{"id": rid, "fields": fields} for rid, fields in batch],
            "typecast": True
        }
        r = requests.patch(url, headers=airtable_headers(token), data=json.dumps(payload), timeout=60)
        r.raise_for_status()

def airtable_batch_delete(token: str, base_id: str, table: str, record_ids: List[str]) -> None:
    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table, safe='')}"
    for batch in chunks(record_ids, 10):
        params = [("records[]", rid) for rid in batch]
        r = requests.delete(url, headers=airtable_headers(token), params=params, timeout=60)
        r.raise_for_status()


# -----------------------------
# MAIN
# -----------------------------
def main():
    token = os.getenv("AIRTABLE_API_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()
    view = os.getenv("AIRTABLE_VIEW", "").strip() or None
    max_rcp_fetch = int(os.getenv("MAX_RCP_FETCH", "300"))

    if not token or not base_id or not table:
        die("Variables manquantes: AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME")

    # 1) Téléchargements (STOP si un seul fichier échoue)
    try:
        info("Téléchargement BDPM CIS ...")
        cis_txt = decode_text(download_bytes(BDPM_CIS_URL))
        ok(f"BDPM CIS OK ({len(cis_txt)} chars)")

        info("Téléchargement BDPM CIS_CIP ...")
        cis_cip_txt = decode_text(download_bytes(BDPM_CIS_CIP_URL))
        ok(f"BDPM CIS_CIP OK ({len(cis_cip_txt)} chars)")

        info("Téléchargement BDPM CIS_CPD ...")
        cis_cpd_txt = decode_text(download_bytes(BDPM_CIS_CPD_URL))
        ok(f"BDPM CIS_CPD OK ({len(cis_cpd_txt)} chars)")

        info("Recherche lien Excel ANSM ...")
        ansm_page_html = decode_text(download_bytes(ANSM_PAGE_URL))
        ansm_xls_url = find_ansm_xls_url(ansm_page_html)
        if not ansm_xls_url:
            die("Impossible de trouver le fichier ANSM (lien .xls). Aucune action Airtable.")
        ok(f"Lien ANSM trouvé : {ansm_xls_url}")

        info("Téléchargement Excel ANSM ...")
        ansm_xls_bytes = download_bytes(ansm_xls_url)
        ok(f"ANSM Excel OK ({len(ansm_xls_bytes)} bytes)")
    except Exception as e:
        die(f"Erreur téléchargement fichier(s). Aucune action Airtable. Détail: {e}")

    # 2) Parsing (sans pandas, pour éviter les erreurs numpy/pyarrow)
    info("Parsing fichiers ...")
    bdpm = parse_bdpm_cis(cis_txt)
    cis_to_cip13, cis_has_rate, cis_agrement = parse_bdpm_cis_cip(cis_cip_txt)

    try:
        ansm_retro_cis = parse_ansm_xls_get_cis_set(ansm_xls_bytes)
    except Exception as e:
        die(f"Impossible de lire le fichier ANSM (XLS). Aucune action Airtable. Détail: {e}")

    ok(f"CIS BDPM: {len(bdpm)} | CIS avec taux (ville): {sum(1 for v in cis_has_rate.values() if v)} | CIS ANSM rétrocession: {len(ansm_retro_cis)}")

    # 3) Lecture Airtable
    info("Lecture Airtable ...")
    records = airtable_list_records(token, base_id, table, view=view)
    ok(f"Airtable records: {len(records)}")

    airtable_by_cis: Dict[str, Dict] = {}
    for rec in records:
        fields = rec.get("fields", {})
        cis = str(fields.get(FIELD_CIS, "")).strip()
        if cis:
            airtable_by_cis[cis] = rec

    bdpm_cis_set = set(bdpm.keys())
    airtable_cis_set = set(airtable_by_cis.keys())

    to_delete = sorted(list(airtable_cis_set - bdpm_cis_set))
    to_create = sorted(list(bdpm_cis_set - airtable_cis_set))
    to_consider_update = sorted(list(bdpm_cis_set & airtable_cis_set))

    info(f"Inventaire: à supprimer={len(to_delete)} | à créer={len(to_create)} | à vérifier/maj={len(to_consider_update)}")

    # 4) Prépare créations
    create_payload: List[Dict] = []
    for cis in to_create:
        base_fields = dict(bdpm[cis])

        # CIP13
        cip13 = cis_to_cip13.get(cis, "")
        if cip13:
            base_fields[FIELD_CIP13] = cip13

        # Agrément
        agr = cis_agrement.get(cis, "")
        if agr:
            base_fields[FIELD_AGREMENT] = agr

        # Disponibilité (sans RCP au départ)
        dispo = compute_disponibilite(
            cis=cis,
            has_rate=cis_has_rate.get(cis, False),
            is_retro_ansm=(cis in ansm_retro_cis),
            rcp_html=None
        )
        base_fields[FIELD_DISPO] = dispo

        create_payload.append(base_fields)

    # 5) Applique suppressions / créations / MAJ (sans RCP lourd)
    # Suppressions
    if to_delete:
        info("Suppression des CIS absents BDPM ...")
        delete_ids = [airtable_by_cis[c]["id"] for c in to_delete if c in airtable_by_cis]
        airtable_batch_delete(token, base_id, table, delete_ids)
        ok(f"Supprimés: {len(delete_ids)}")

    # Créations
    if create_payload:
        info("Création des nouveaux enregistrements ...")
        airtable_batch_create(token, base_id, table, create_payload)
        ok(f"Créés: {len(create_payload)}")

    # Re-liste Airtable (pour avoir ids à jour après create)
    records = airtable_list_records(token, base_id, table, view=view)
    airtable_by_cis = {}
    for rec in records:
        fields = rec.get("fields", {})
        cis = str(fields.get(FIELD_CIS, "")).strip()
        if cis:
            airtable_by_cis[cis] = rec

    # 6) MAJ des champs BDPM + disponibilité (toujours sans RCP)
    updates: List[Tuple[str, Dict]] = []
    for cis in to_consider_update:
        rec = airtable_by_cis.get(cis)
        if not rec:
            continue
        rid = rec["id"]

        new_fields = dict(bdpm[cis])

        cip13 = cis_to_cip13.get(cis, "")
        if cip13:
            new_fields[FIELD_CIP13] = cip13

        agr = cis_agrement.get(cis, "")
        if agr:
            new_fields[FIELD_AGREMENT] = agr

        new_fields[FIELD_DISPO] = compute_disponibilite(
            cis=cis,
            has_rate=cis_has_rate.get(cis, False),
            is_retro_ansm=(cis in ansm_retro_cis),
            rcp_html=None
        )

        updates.append((rid, new_fields))

    if updates:
        info("Mise à jour BDPM + Disponibilité (sans RCP) ...")
        airtable_batch_update(token, base_id, table, updates)
        ok(f"Mises à jour: {len(updates)}")

    # 7) Enrichissement RCP (CPD + hospital/homeopath si besoin)
    # On limite pour éviter blocage / bannissement.
    info(f"Enrichissement RCP (max {max_rcp_fetch}) ...")
    rcp_updates: List[Tuple[str, Dict]] = []
    fetched = 0

    # On ne va chercher le RCP que si:
    # - CPD manquant
    # OU
    # - Disponibilité == "Pas d'informations mentionnées" (donc dépend du texte RCP)
    for cis, rec in airtable_by_cis.items():
        fields = rec.get("fields", {})
        rid = rec["id"]

        cpd_present = bool(str(fields.get(FIELD_CPD, "")).strip())
        dispo_current = str(fields.get(FIELD_DISPO, "")).strip()
        rcp_url = str(fields.get(FIELD_RCP_LINK, "")).strip()

        if not rcp_url:
            continue

        if cpd_present and dispo_current != DISPO_NONE:
            continue

        if fetched >= max_rcp_fetch:
            warn("Limite MAX_RCP_FETCH atteinte, arrêt de l’enrichissement RCP.")
            break

        try:
            html = fetch_rcp_html(rcp_url)
        except Exception as e:
            # STOP demandé si un RCP est inaccessible
            die(f"RCP inaccessible pour CIS={cis} ({rcp_url}). Stop mise à jour. Détail: {e}")

        fetched += 1

        cpd_txt = extract_cpd_from_rcp(html)
        dispo = compute_disponibilite(
            cis=cis,
            has_rate=cis_has_rate.get(cis, False),
            is_retro_ansm=(cis in ansm_retro_cis),
            rcp_html=html
        )

        patch = {}
        if cpd_txt:
            patch[FIELD_CPD] = cpd_txt
        # met à jour si changé
        if dispo and dispo != dispo_current:
            patch[FIELD_DISPO] = dispo

        if patch:
            rcp_updates.append((rid, patch))

        # petite pause pour éviter de se faire bloquer
        time.sleep(0.2)

        # envoi par batch pour éviter de tout garder en mémoire
        if len(rcp_updates) >= 50:
            airtable_batch_update(token, base_id, table, rcp_updates)
            ok(f"RCP enrichis (batch): {len(rcp_updates)}")
            rcp_updates = []

    if rcp_updates:
        airtable_batch_update(token, base_id, table, rcp_updates)
        ok(f"RCP enrichis (final): {len(rcp_updates)}")

    ok("Terminé.")


if __name__ == "__main__":
    main()
