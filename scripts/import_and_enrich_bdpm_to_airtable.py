import os
import re
import sys
import time
import json
import math
import csv
import io
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Iterable, Set

import requests
from bs4 import BeautifulSoup

# ----------------------------
# CONFIG
# ----------------------------
BDPM_CIS_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
BDPM_CIS_CIP_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
BDPM_CIS_CPD_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"

ANSM_PAGE_URL = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"
ANSM_XLS_REGEX = re.compile(r"https://ansm\.sante\.fr/uploads/\d{4}/\d{2}/\d{2}/\d{8}-retrocession-[a-zA-Z0-9_-]+-\d{4}\.xls")

AIRTABLE_API_URL = "https://api.airtable.com/v0"

# Airtable field names (doivent matcher exactement tes colonnes)
FIELD_CIS = "Code cis"
FIELD_SPECIALITE = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE = "Voie d'administration"
FIELD_LABO = "Laboratoire"
FIELD_LIEN_RCP = "Lien vers RCP"
FIELD_AGREMENT = "Agrément aux collectivités"
FIELD_CIP13 = "CIP 13"
FIELD_RETROCESSION = "Rétrocession"
FIELD_CPD = "Conditions de prescription et délivrance"

# ----------------------------
# UTILS: robust download + decoding
# ----------------------------
def die(msg: str) -> None:
    print(f"❌ {msg}", flush=True)
    sys.exit(1)

def info(msg: str) -> None:
    print(f"ℹ️ {msg}", flush=True)

def ok(msg: str) -> None:
    print(f"✅ {msg}", flush=True)

def http_get(url: str, timeout: int = 60) -> requests.Response:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r

def download_bytes(url: str) -> bytes:
    info(f"Téléchargement: {url}")
    r = http_get(url)
    ok(f"OK ({len(r.content)} bytes)")
    return r.content

def decode_bytes_best_effort(data: bytes) -> str:
    """
    BDPM: souvent CP1252/Latin-1. On essaye plusieurs encodages en STRICT.
    Si ça échoue, on retombe sur cp1252 avec erreurs='replace' (mais normalement on n'en a plus besoin).
    """
    candidates = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
    for enc in candidates:
        try:
            return data.decode(enc, errors="strict")
        except UnicodeDecodeError:
            continue
    # dernier recours (évite crash mais peut injecter �)
    return data.decode("cp1252", errors="replace")

def fix_mojibake(s: str) -> str:
    """
    Corrige les cas "Ã©" etc si jamais une double-conversion a eu lieu.
    On applique seulement si on détecte des marqueurs typiques.
    """
    if "Ã" in s or "Â" in s:
        try:
            # retransforme comme si c'était du latin1 mal décodé
            return s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            return s
    return s

def clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = fix_mojibake(s)
    # supprime caractères de contrôle invisibles
    s = re.sub(r"[\u0000-\u0008\u000B\u000C\u000E-\u001F]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def guess_delimiter(sample_line: str) -> str:
    # BDPM est le plus souvent TAB. Parfois ; ou |
    candidates = ["\t", ";", "|", ","]
    best = "\t"
    best_count = -1
    for d in candidates:
        c = sample_line.count(d)
        if c > best_count:
            best_count = c
            best = d
    return best

def parse_text_table(text: str) -> Tuple[str, List[List[str]]]:
    """
    Retourne (delimiter, rows)
    """
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return ("\t", [])
    delim = guess_delimiter(lines[0])
    rows: List[List[str]] = []
    reader = csv.reader(lines, delimiter=delim)
    for row in reader:
        rows.append([clean_text(x) for x in row])
    return (delim, rows)

# ----------------------------
# ANSM: find and download retrocession xls
# ----------------------------
def find_ansm_xls_link() -> str:
    info(f"Recherche lien Excel ANSM sur: {ANSM_PAGE_URL}")
    html = http_get(ANSM_PAGE_URL).text
    # 1) regex direct
    m = ANSM_XLS_REGEX.search(html)
    if m:
        ok(f"Lien ANSM trouvé (regex): {m.group(0)}")
        return m.group(0)
    # 2) parse page + chercher .xls
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".xls") and "retrocession" in href:
            if href.startswith("/"):
                href = "https://ansm.sante.fr" + href
            ok(f"Lien ANSM trouvé (HTML): {href}")
            return href
    die("Impossible de trouver le lien .xls ANSM (rétrocession).")

def parse_ansm_xls_cis_set(xls_bytes: bytes) -> Set[str]:
    """
    Lecture .xls via xlrd (sans pandas) pour éviter des problèmes d'engine.
    La 3e colonne contient le CIS (selon ta règle).
    """
    import xlrd
    with io.BytesIO(xls_bytes) as bio:
        book = xlrd.open_workbook(file_contents=bio.read())
    cis_set: Set[str] = set()
    for sheet in book.sheets():
        for r in range(sheet.nrows):
            if sheet.ncols < 3:
                continue
            v = sheet.cell_value(r, 2)
            if v is None:
                continue
            cis = str(v).strip()
            cis = cis.replace(".0", "")
            cis = re.sub(r"\D", "", cis)
            if cis.isdigit():
                cis_set.add(cis)
    ok(f"CIS ANSM rétrocession: {len(cis_set)}")
    return cis_set

# ----------------------------
# BDPM parsing
# ----------------------------
@dataclass
class CisRecord:
    cis: str
    specialite: str
    forme: str
    voie: str
    labo: str
    lien_rcp: str

def parse_cis_bdpm(cis_text: str) -> Dict[str, CisRecord]:
    _, rows = parse_text_table(cis_text)
    # CIS_bdpm.txt: colonnes attendues:
    # 1 CIS, 2 Dénomination, 3 Forme, 4 Voies d'administration, 5 Statut AMM, 6 Type procédure, 7 Etat commercialisation, 8 Date AMM, 9 Statut BSM, 10 Numéro autorisation etc...
    # (ça varie mais les 5 premières nous suffisent)
    out: Dict[str, CisRecord] = {}
    for row in rows:
        if len(row) < 5:
            continue
        cis = re.sub(r"\D", "", row[0])
        if not cis:
            continue
        specialite = row[1]
        forme = row[2] if len(row) > 2 else ""
        voie = row[3] if len(row) > 3 else ""
        labo = row[4] if len(row) > 4 else ""
        lien_rcp = f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"
        out[cis] = CisRecord(
            cis=cis,
            specialite=specialite,
            forme=forme,
            voie=voie,
            labo=labo,
            lien_rcp=lien_rcp,
        )
    ok(f"BDPM CIS parsés: {len(out)}")
    return out

def parse_cis_cip_bdpm(cip_text: str) -> Tuple[Dict[str, str], Dict[str, bool]]:
    """
    On utilise CIS_CIP_bdpm pour:
    - CIP13 (colonne CIP)
    - "taux de remboursement" : tu m'as dit colonnes 8/9/10 -> si une des 3 colonnes non vide => ville.
      Attention : selon version, la position exacte peut varier, donc on détecte par taille de ligne
      et on regarde les colonnes 7/8/9 (index 7..9) si présentes.
    """
    _, rows = parse_text_table(cip_text)
    cis_to_cip13: Dict[str, str] = {}
    cis_has_refund: Dict[str, bool] = {}

    for row in rows:
        if len(row) < 2:
            continue
        cis = re.sub(r"\D", "", row[0])
        if not cis:
            continue

        # CIP13: on prend le champ qui ressemble le plus à 13 chiffres
        cip13 = ""
        for cell in row[1:]:
            digits = re.sub(r"\D", "", cell)
            if len(digits) == 13:
                cip13 = digits
                break
        if cip13:
            cis_to_cip13[cis] = cip13

        # Remboursement: colonnes 8-9-10 (position humaine) => index 7-8-9
        has_refund = False
        for idx in (7, 8, 9):
            if idx < len(row) and clean_text(row[idx]) != "":
                has_refund = True
                break
        cis_has_refund[cis] = has_refund

    ok(f"BDPM CIS_CIP: CIP13 trouvés: {len(cis_to_cip13)} | CIS avec remboursement: {sum(1 for v in cis_has_refund.values() if v)}")
    return cis_to_cip13, cis_has_refund

def parse_cis_cpd_bdpm(cpd_text: str) -> Tuple[Dict[str, str], Set[str]]:
    """
    CIS_CPD_bdpm: conditions de prescription & délivrance.
    On s'en sert pour:
    - remplir le champ CPD (texte complet)
    - détecter 'usage hospitalier' / 'réservé à l'usage hospitalier' etc
    """
    _, rows = parse_text_table(cpd_text)
    cis_to_cpd: Dict[str, str] = {}
    cis_hospital: Set[str] = set()

    for row in rows:
        if len(row) < 2:
            continue
        cis = re.sub(r"\D", "", row[0])
        if not cis:
            continue
        cpd = row[1]
        cpd = clean_text(cpd)
        if cpd:
            cis_to_cpd[cis] = cpd
            low = cpd.lower()
            # détection "usage hospitalier" (sans sur-généraliser)
            if "usage hospitalier" in low or "réservé à l’usage hospitalier" in low or "réservé à l'usage hospitalier" in low:
                cis_hospital.add(cis)

    ok(f"BDPM CIS_CPD: CPD trouvés: {len(cis_to_cpd)} | marqués hospitalier: {len(cis_hospital)}")
    return cis_to_cpd, cis_hospital

# ----------------------------
# Airtable client (batch, retry, throttle)
# ----------------------------
class Airtable:
    def __init__(self, api_token: str, base_id: str, table_name: str):
        self.api_token = api_token
        self.base_id = base_id
        self.table_name = table_name

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    def _url(self) -> str:
        return f"{AIRTABLE_API_URL}/{self.base_id}/{requests.utils.quote(self.table_name)}"

    def list_all(self) -> List[dict]:
        out = []
        offset = None
        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset
            r = requests.get(self._url(), headers=self._headers(), params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return out

    def batch_create(self, records: List[dict]) -> None:
        # Airtable: max 10
        for i in range(0, len(records), 10):
            chunk = records[i:i+10]
            payload = {"records": chunk}
            self._post_with_retry(payload)
            time.sleep(0.25)

    def batch_update(self, records: List[dict]) -> None:
        for i in range(0, len(records), 10):
            chunk = records[i:i+10]
            payload = {"records": chunk}
            self._patch_with_retry(payload)
            time.sleep(0.25)

    def batch_delete(self, record_ids: List[str]) -> None:
        for i in range(0, len(record_ids), 10):
            chunk = record_ids[i:i+10]
            params = [("records[]", rid) for rid in chunk]
            self._delete_with_retry(params)
            time.sleep(0.25)

    def _post_with_retry(self, payload: dict) -> None:
        for attempt in range(1, 6):
            r = requests.post(self._url(), headers=self._headers(), data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * attempt)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"Airtable create error {r.status_code}: {r.text}")
            return
        raise RuntimeError("Airtable create: trop de tentatives (rate limit / serveur).")

    def _patch_with_retry(self, payload: dict) -> None:
        for attempt in range(1, 6):
            r = requests.patch(self._url(), headers=self._headers(), data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * attempt)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"Airtable update error {r.status_code}: {r.text}")
            return
        raise RuntimeError("Airtable update: trop de tentatives (rate limit / serveur).")

    def _delete_with_retry(self, params: list) -> None:
        for attempt in range(1, 6):
            r = requests.delete(self._url(), headers=self._headers(), params=params, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * attempt)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"Airtable delete error {r.status_code}: {r.text}")
            return
        raise RuntimeError("Airtable delete: trop de tentatives (rate limit / serveur).")

# ----------------------------
# Business rules
# ----------------------------
def compute_retro_label(
    cis: str,
    has_refund: bool,
    in_ansm_retro: bool,
    marked_hospitalier: bool,
) -> str:
    # priorité ANSM
    if in_ansm_retro:
        return "Disponible en rétrocession hospitalière"
    # remboursement -> ville
    if has_refund:
        return "Disponible en pharmacie de ville"
    # hospitalier explicite -> hospitalier
    if marked_hospitalier:
        return "Réservé à l'usage hospitalier"
    # sinon
    return "Pas d'informations mentionnées"

# ----------------------------
# MAIN SYNC (inventory)
# ----------------------------
def main():
    api_token = os.getenv("AIRTABLE_API_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table_name = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

    if not api_token or not base_id or not table_name:
        die("Variables manquantes: AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_CIS_TABLE_NAME")

    airtable = Airtable(api_token, base_id, table_name)

    # 1) Télécharger tous les fichiers d'abord (sinon on STOP)
    try:
        cis_bytes = download_bytes(BDPM_CIS_URL)
        cip_bytes = download_bytes(BDPM_CIS_CIP_URL)
        cpd_bytes = download_bytes(BDPM_CIS_CPD_URL)

        ansm_link = find_ansm_xls_link()
        ansm_bytes = download_bytes(ansm_link)

    except Exception as e:
        die(f"Échec téléchargement d'un fichier. Aucune action sur Airtable. Détail: {e}")

    # 2) Parsing avec encodage robuste
    cis_text = decode_bytes_best_effort(cis_bytes)
    cip_text = decode_bytes_best_effort(cip_bytes)
    cpd_text = decode_bytes_best_effort(cpd_bytes)

    cis_map = parse_cis_bdpm(cis_text)
    cis_to_cip13, cis_has_refund = parse_cis_cip_bdpm(cip_text)
    cis_to_cpd, cis_hospital = parse_cis_cpd_bdpm(cpd_text)
    ansm_cis_set = parse_ansm_xls_cis_set(ansm_bytes)

    # 3) Charger inventaire Airtable
    info("Lecture Airtable...")
    existing = airtable.list_all()
    existing_by_cis: Dict[str, dict] = {}
    for rec in existing:
        fields = rec.get("fields", {})
        cis = str(fields.get(FIELD_CIS, "")).strip()
        cis = re.sub(r"\D", "", cis)
        if cis:
            existing_by_cis[cis] = rec

    target_cis_set = set(cis_map.keys())

    # 4) Supprimer: présent Airtable mais absent BDPM
    to_delete = []
    for cis, rec in existing_by_cis.items():
        if cis not in target_cis_set:
            to_delete.append(rec["id"])

    # 5) Ajouter: présent BDPM mais absent Airtable
    to_create = []
    for cis, cr in cis_map.items():
        if cis in existing_by_cis:
            continue

        has_refund = cis_has_refund.get(cis, False)
        in_ansm = cis in ansm_cis_set
        marked_hosp = cis in cis_hospital
        retro = compute_retro_label(cis, has_refund, in_ansm, marked_hosp)

        fields = {
            FIELD_CIS: cis,
            FIELD_SPECIALITE: cr.specialite,
            FIELD_FORME: cr.forme,
            FIELD_VOIE: cr.voie,
            FIELD_LABO: cr.labo,
            FIELD_LIEN_RCP: cr.lien_rcp,
            FIELD_AGREMENT: "",  # si tu l'as depuis un autre fichier -> on l'ajoute après
            FIELD_CIP13: cis_to_cip13.get(cis, ""),
            FIELD_RETROCESSION: retro,
            FIELD_CPD: cis_to_cpd.get(cis, ""),
        }
        # nettoyage final
        for k, v in list(fields.items()):
            if isinstance(v, str):
                fields[k] = clean_text(v)

        to_create.append({"fields": fields})

    # 6) Mettre à jour: présent des deux côtés (mise à jour champs)
    to_update = []
    for cis, cr in cis_map.items():
        if cis not in existing_by_cis:
            continue
        rec = existing_by_cis[cis]
        rec_id = rec["id"]

        has_refund = cis_has_refund.get(cis, False)
        in_ansm = cis in ansm_cis_set
        marked_hosp = cis in cis_hospital
        retro = compute_retro_label(cis, has_refund, in_ansm, marked_hosp)

        fields = {
            FIELD_SPECIALITE: cr.specialite,
            FIELD_FORME: cr.forme,
            FIELD_VOIE: cr.voie,
            FIELD_LABO: cr.labo,
            FIELD_LIEN_RCP: cr.lien_rcp,
            FIELD_CIP13: cis_to_cip13.get(cis, ""),
            FIELD_RETROCESSION: retro,
            FIELD_CPD: cis_to_cpd.get(cis, ""),
        }
        for k, v in list(fields.items()):
            if isinstance(v, str):
                fields[k] = clean_text(v)

        to_update.append({"id": rec_id, "fields": fields})

    # 7) Appliquer en batch (delete -> create -> update)
    info(f"Suppression Airtable: {len(to_delete)}")
    if to_delete:
        airtable.batch_delete(to_delete)
        ok("Suppression OK")

    info(f"Création Airtable: {len(to_create)}")
    if to_create:
        airtable.batch_create(to_create)
        ok("Création OK")

    info(f"Mise à jour Airtable: {len(to_update)}")
    if to_update:
        airtable.batch_update(to_update)
        ok("Mise à jour OK")

    ok("Terminé ✅")

if __name__ == "__main__":
    main()
