#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import math
import random
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable, Set

import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIG
# ============================================================

BDPM_CIS_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt"
BDPM_CIS_CIP_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
BDPM_CIS_CPD_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt"

ANSM_RETRO_PAGE = "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession"

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

# Airtable: 5 req/s conseillé -> on throttle
AIRTABLE_MIN_DELAY_S = 0.25
AIRTABLE_BATCH_SIZE = 10

# Pour éviter de “bloquer”
REQUEST_TIMEOUT = 30
MAX_RETRIES = 4

# ============================================================
# UTIL
# ============================================================

def die(msg: str, code: int = 1):
    print(f"❌ {msg}")
    raise SystemExit(code)

def info(msg: str):
    print(f"ℹ️ {msg}")

def ok(msg: str):
    print(f"✅ {msg}")

def warn(msg: str):
    print(f"⚠️ {msg}")

def sleep_throttle():
    time.sleep(AIRTABLE_MIN_DELAY_S)

def retry_sleep(attempt: int):
    # backoff simple
    time.sleep(min(8, 0.6 * (2 ** (attempt - 1))) + random.random() * 0.2)

def safe_text(s: str) -> str:
    # Nettoie encodage / caractères bizarres
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = s.replace("\uFFFD", "")  # symbole "�"
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def normalize_ws(s: str) -> str:
    s = safe_text(s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def chunked(lst: List, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ============================================================
# DOWNLOAD
# ============================================================

def http_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=timeout)
            return r
        except Exception as e:
            last_err = e
            retry_sleep(attempt)
    raise RuntimeError(f"GET failed: {url} / {last_err}")

def download_text(url: str, encoding: str = "latin-1") -> str:
    r = http_get(url)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    r.encoding = encoding  # BDPM txt souvent latin-1
    return r.text

def download_bytes(url: str) -> bytes:
    r = http_get(url)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.content

# ============================================================
# ANSM retrocession link discovery + parse
# ============================================================

def find_ansm_retro_excel_link() -> str:
    r = http_get(ANSM_RETRO_PAGE)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} {ANSM_RETRO_PAGE}")
    soup = BeautifulSoup(r.text, "lxml")

    # Cherche un lien .xls/.xlsx dans /uploads/ ... retrocession ...
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        if href.startswith("/"):
            href = "https://ansm.sante.fr" + href
        if "ansm.sante.fr/uploads/" in href and "retrocession" in href.lower() and re.search(r"\.xlsx?$", href.lower()):
            links.append(href)

    # Heuristique: prendre le plus récent (souvent contient date)
    if not links:
        raise RuntimeError("Lien Excel ANSM (retrocession) introuvable sur la page")

    # tri par présence de date YYYY/MM/DD dans l'URL
    def score(u: str) -> Tuple[int, str]:
        m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", u)
        if m:
            return (1, f"{m.group(1)}{m.group(2)}{m.group(3)}")
        return (0, u)

    links.sort(key=score, reverse=True)
    return links[0]

def parse_ansm_retrocession_cis(excel_bytes: bytes, url_hint: str = "") -> Set[str]:
    """
    Le fichier ANSM est souvent .xls.
    On lit via xlrd pour .xls, openpyxl pour .xlsx.
    La 3ème colonne (index 2) = Code CIS selon ta consigne.
    """
    cis_set: Set[str] = set()

    # détecter extension
    ext = ""
    if url_hint:
        ext = url_hint.lower().split("?")[0].split("#")[0]
        ext = os.path.splitext(ext)[1].lower()

    if ext == ".xlsx":
        from openpyxl import load_workbook
        import io
        wb = load_workbook(io.BytesIO(excel_bytes), read_only=True, data_only=True)
        ws = wb.worksheets[0]
        for row in ws.iter_rows(values_only=True):
            if not row or len(row) < 3:
                continue
            v = row[2]
            if v is None:
                continue
            v = re.sub(r"\D", "", str(v))
            if len(v) == 8:  # CIS = 8 chiffres
                cis_set.add(v)
        return cis_set

    # par défaut .xls (ou inconnu)
    import io
    try:
        import xlrd
    except Exception:
        raise RuntimeError("xlrd manquant (nécessaire pour lire .xls)")

    book = xlrd.open_workbook(file_contents=excel_bytes)
    sheet = book.sheet_by_index(0)
    for rx in range(sheet.nrows):
        row = sheet.row_values(rx)
        if len(row) < 3:
            continue
        v = row[2]
        if v is None:
            continue
        v = re.sub(r"\D", "", str(v))
        if len(v) == 8:
            cis_set.add(v)

    return cis_set

# ============================================================
# BDPM parse
# ============================================================

@dataclass
class CisRow:
    cis: str
    specialite: str
    forme: str
    voie_admin: str
    titulaire: str
    surveillance: str

def parse_bdpm_cis(txt: str) -> Dict[str, CisRow]:
    """
    Format CIS_bdpm.txt : séparateur TAB.
    Colonnes usuelles (rappel) :
    0 CIS
    1 DENOMINATION
    2 FORME PHARMACEUTIQUE
    3 VOIES D'ADMINISTRATION
    ...
    10 TITULAIRE
    11 SURVEILLANCE RENFORCEE
    """
    out: Dict[str, CisRow] = {}
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        cis = re.sub(r"\D", "", parts[0].strip())
        if len(cis) != 8:
            continue
        denom = safe_text(parts[1]) if len(parts) > 1 else ""
        forme = safe_text(parts[2]) if len(parts) > 2 else ""
        voie = safe_text(parts[3]) if len(parts) > 3 else ""
        titulaire = safe_text(parts[10]) if len(parts) > 10 else ""
        surveil = safe_text(parts[11]) if len(parts) > 11 else ""
        out[cis] = CisRow(cis=cis, specialite=denom, forme=forme, voie_admin=voie, titulaire=titulaire, surveillance=surveil)
    return out

def normalize_lab_name(titulaire: str) -> str:
    """
    Objectif: obtenir un nom court type "Arrow".
    Heuristique: prendre le 1er segment significatif, nettoyer suffixes juridiques.
    """
    t = titulaire or ""
    t = t.replace(",", " ").replace(";", " ")
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return ""

    # enlever mentions juridiques fréquentes
    t = re.sub(r"\b(SAS|SA|SARL|S\.A\.|S\.A\.S\.|GMBH|LTD|INC|BV|AG|SPA|S\.P\.A\.)\b", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s+", " ", t).strip()

    # prendre premier "mot" si c'est typiquement un labo (Arrow, Viatris, Sandoz...)
    first = t.split(" ")[0].strip()
    # casse: Arrow plutôt que ARROW
    if first.isupper() and len(first) > 2:
        first = first.capitalize()

    # cas "LABORATOIRES XXX" -> XXX
    if first.lower() in {"laboratoires", "laboratoire"} and len(t.split(" ")) > 1:
        nxt = t.split(" ")[1]
        if nxt.isupper() and len(nxt) > 2:
            nxt = nxt.capitalize()
        return nxt

    return first

@dataclass
class CipInfo:
    cip13: str
    has_taux: bool
    agrement_collectivites: str

def looks_like_taux(val: str) -> bool:
    v = (val or "").strip()
    if not v:
        return False
    v2 = v.replace(",", ".").replace("%", "").strip()
    if not re.fullmatch(r"\d{1,3}(\.\d+)?", v2):
        return False
    try:
        x = float(v2)
    except:
        return False
    # taux usuels
    return x in {0, 15, 30, 35, 65, 100}

def parse_bdpm_cis_cip(txt: str) -> Dict[str, CipInfo]:
    """
    CIS_CIP_bdpm.txt contient CIP7/CIP13 etc.
    On en déduit:
    - CIP13: premier CIP13 rencontré
    - has_taux: si une des colonnes ressemble à un taux (heuristique)
    - agrement_collectivites: heuristique (si une colonne contient "collectiv" / "agrément" etc)
    """
    out: Dict[str, CipInfo] = {}
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        cis = re.sub(r"\D", "", parts[0].strip())
        if len(cis) != 8:
            continue

        # CIP13 est souvent en colonne 1 ou 2 selon fichiers; on prend le champ le plus long de type 13 chiffres
        cip13 = ""
        for p in parts:
            d = re.sub(r"\D", "", p)
            if len(d) == 13:
                cip13 = d
                break

        # taux: heuristique sur toutes colonnes
        has_taux = any(looks_like_taux(p) for p in parts)

        # agrément collectivités: heuristique sur texte
        agrement = ""
        joined = " ".join(parts).lower()
        if "collectiv" in joined or "agrément" in joined or "agrement" in joined:
            agrement = "Oui"

        # conserver 1er CIP13
        if cis not in out:
            out[cis] = CipInfo(cip13=cip13, has_taux=has_taux, agrement_collectivites=agrement)
        else:
            if not out[cis].cip13 and cip13:
                out[cis].cip13 = cip13
            out[cis].has_taux = out[cis].has_taux or has_taux
            if not out[cis].agrement_collectivites and agrement:
                out[cis].agrement_collectivites = agrement

    return out

def parse_bdpm_cis_cpd(txt: str) -> Dict[str, str]:
    """
    CIS_CPD_bdpm.txt : on l'utilise comme “indice” hospitalier si texte mentionne hospitalier.
    (Le texte complet CPD sera extrait depuis le RCP HTML comme demandé.)
    """
    out: Dict[str, str] = {}
    for line in txt.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        cis = re.sub(r"\D", "", parts[0].strip())
        if len(cis) != 8:
            continue
        # tout le reste
        text = safe_text(" ".join(parts[1:]))
        out[cis] = text
    return out

def rcp_url_from_cis(cis: str) -> str:
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"

# ============================================================
# RCP HTML fetch + extraction
# ============================================================

def fetch_rcp_html(url: str, timeout: int = 20, max_retries: int = 3) -> str:
    if not url or not url.startswith("http"):
        raise RuntimeError(f"RCP invalide: {url}")

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=timeout)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}")
            if not r.text or len(r.text) < 200:
                raise RuntimeError("HTML vide/trop court")
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.0 * attempt)

    raise RuntimeError(f"RCP inaccessible: {url}. Détail: {last_err}")

def extract_cpd_from_rcp_html(html: str) -> str:
    """
    Extrait toutes les lignes sous:
    CONDITIONS DE PRESCRIPTION ET DE DELIVRANCE
    et conserve les sauts de ligne (format proche de la page).
    """
    soup = BeautifulSoup(html, "lxml")

    # Trouver un bloc qui contient le titre, puis prendre ses voisins
    text = soup.get_text("\n", strip=True)

    # Repérage du titre
    m = re.search(r"CONDITIONS DE PRESCRIPTION ET DE D[ÉE]LIVRANCE", text, flags=re.IGNORECASE)
    if not m:
        return ""

    after = text[m.start():]

    # Enlever la ligne du titre
    after = re.sub(r"^.*CONDITIONS.*D[ÉE]LIVRANCE.*\n", "", after, flags=re.IGNORECASE)

    # Couper à la prochaine section typique (numérotée)
    cut = re.split(
        r"\n(?:\d{1,2}\.\s)|\n(?:10\.|11\.|12\.)|\n(?:INSTRUCTIONS|DOSIMETRIE|DATE DE MISE A JOUR|MISE A JOUR)",
        after,
        maxsplit=1
    )
    bloc = cut[0]
    bloc = normalize_ws(bloc)

    # Si tout petit, on considère vide
    if len(bloc) < 3:
        return ""

    # Limite de taille raisonnable
    if len(bloc) > 2500:
        bloc = bloc[:2500].rstrip() + "…"

    return bloc

def rcp_mentions_homeopathy(html: str) -> bool:
    t = html.lower()
    return "homéopathi" in t or "homeopathi" in t

def rcp_mentions_hospital(html: str) -> bool:
    t = html.lower()
    # mentions “usage hospitalier” etc.
    keys = [
        "réservé à l’usage hospitalier",
        "réservé à l'usage hospitalier",
        "usage hospitalier",
        "prescription hospitalière",
        "médicament soumis à prescription hospitalière",
        "médicament réservé à l’usage hospitalier",
        "médicament réservé à l'usage hospitalier",
    ]
    return any(k in t for k in keys)

# ============================================================
# DISPONIBILITE RULES
# ============================================================

def compute_disponibilite(
    has_taux_ville: bool,
    is_ansm_retro: bool,
    is_homeopathy: bool,
    has_hospital_mention: bool
) -> str:
    """
    Règles consolidées:
    - Si taux remboursement OU homéopathie => "Disponible en pharmacie de ville" (ville=True)
    - Puis rétrocession:
        - si rétrocession + ville => "Disponible en ville et en rétrocession hospitalière"
        - si rétrocession seule => "Disponible en rétrocession hospitalière"
    - Sinon si mention hospitalière => "Réservé à l'usage hospitalier"
    - Sinon => "Pas d'informations mentionnées"
    """
    ville = bool(has_taux_ville or is_homeopathy)

    if is_ansm_retro and ville:
        return "Disponible en ville et en rétrocession hospitalière"
    if is_ansm_retro and not ville:
        return "Disponible en rétrocession hospitalière"
    if has_hospital_mention:
        return "Réservé à l'usage hospitalier"
    if ville:
        return "Disponible en pharmacie de ville"
    return "Pas d'informations mentionnées"

# ============================================================
# AIRTABLE CLIENT
# ============================================================

class AirtableClient:
    def __init__(self, api_token: str, base_id: str, table_name: str):
        self.api_token = api_token
        self.base_id = base_id
        self.table_name = table_name

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        })

    @property
    def table_url(self) -> str:
        t = urllib.parse.quote(self.table_name, safe="")
        return f"{AIRTABLE_API_BASE}/{self.base_id}/{t}"

    def _request(self, method: str, url: str, **kwargs):
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sleep_throttle()
                r = self.session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                if r.status_code >= 400:
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
                return r
            except Exception as e:
                last_err = e
                retry_sleep(attempt)
        raise RuntimeError(f"Airtable request failed: {method} {url} / {last_err}")

    def list_all_records(self, fields: Optional[List[str]] = None) -> List[dict]:
        out = []
        params = {}
        if fields:
            for f in fields:
                params.setdefault("fields[]", [])
                params["fields[]"].append(f)

        offset = None
        while True:
            if offset:
                params["offset"] = offset
            r = self._request("GET", self.table_url, params=params)
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return out

    def create_records(self, records: List[dict]) -> None:
        for batch in chunked(records, AIRTABLE_BATCH_SIZE):
            payload = {"records": batch, "typecast": True}
            self._request("POST", self.table_url, data=json.dumps(payload))

    def update_records(self, records: List[dict]) -> None:
        for batch in chunked(records, AIRTABLE_BATCH_SIZE):
            payload = {"records": batch, "typecast": True}
            self._request("PATCH", self.table_url, data=json.dumps(payload))

    def delete_records(self, record_ids: List[str]) -> None:
        for batch in chunked(record_ids, AIRTABLE_BATCH_SIZE):
            params = [("records[]", rid) for rid in batch]
            self._request("DELETE", self.table_url, params=params)

# ============================================================
# MAIN
# ============================================================

def main():
    api_token = os.getenv("AIRTABLE_API_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table_name = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

    if not api_token or not base_id or not table_name:
        die("Variables manquantes: AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME")

    # 1) Télécharger TOUS les fichiers d'abord (aucune action Airtable avant)
    info("Téléchargement BDPM CIS ...")
    cis_txt = download_text(BDPM_CIS_URL, encoding="latin-1")
    ok(f"BDPM CIS OK ({len(cis_txt)} chars)")

    info("Téléchargement BDPM CIS_CIP ...")
    cis_cip_txt = download_text(BDPM_CIS_CIP_URL, encoding="latin-1")
    ok(f"BDPM CIS_CIP OK ({len(cis_cip_txt)} chars)")

    info("Téléchargement BDPM CIS_CPD ...")
    cis_cpd_txt = download_text(BDPM_CIS_CPD_URL, encoding="latin-1")
    ok(f"BDPM CIS_CPD OK ({len(cis_cpd_txt)} chars)")

    info("Recherche lien Excel ANSM (rétrocession) ...")
    ansm_link = find_ansm_retro_excel_link()
    ok(f"Lien ANSM trouvé: {ansm_link}")

    info("Téléchargement Excel ANSM ...")
    ansm_bytes = download_bytes(ansm_link)
    ok(f"ANSM Excel OK ({len(ansm_bytes)} bytes)")

    # 2) Parser les fichiers
    info("Parsing fichiers BDPM ...")
    cis_map = parse_bdpm_cis(cis_txt)
    cip_map = parse_bdpm_cis_cip(cis_cip_txt)
    cpd_hint_map = parse_bdpm_cis_cpd(cis_cpd_txt)
    ansm_retro_cis = parse_ansm_retrocession_cis(ansm_bytes, url_hint=ansm_link)

    ok(f"CIS BDPM: {len(cis_map)}")
    ok(f"CIS avec taux remboursement (ville) (heuristique): {sum(1 for k,v in cip_map.items() if v.has_taux)}")
    ok(f"CIS ANSM rétrocession: {len(ansm_retro_cis)}")

    # 3) Connexion Airtable
    at = AirtableClient(api_token, base_id, table_name)

    info("Inventaire Airtable ...")
    needed_fields = [
        "Code cis",
        "Lien vers RCP",
        "CIP 13",
        "Agrément aux collectivités",
        "Disponibilité du traitement",
        "Conditions de prescription et délivrance",
        "Laboratoire",
    ]
    records = at.list_all_records(fields=needed_fields)
    ok(f"CIS Airtable: {len(records)}")

    airtable_by_cis: Dict[str, dict] = {}
    for rec in records:
        cis = str(rec.get("fields", {}).get("Code cis", "")).strip()
        cis = re.sub(r"\D", "", cis)
        if len(cis) == 8:
            airtable_by_cis[cis] = rec

    bdpm_cis_set = set(cis_map.keys())
    airtable_cis_set = set(airtable_by_cis.keys())

    to_create = sorted(list(bdpm_cis_set - airtable_cis_set))
    to_delete = sorted(list(airtable_cis_set - bdpm_cis_set))
    to_keep = sorted(list(bdpm_cis_set & airtable_cis_set))

    info(f"À créer: {len(to_create)} | À supprimer: {len(to_delete)} | À conserver: {len(to_keep)}")

    # 4) Créations
    if to_create:
        info("Création des enregistrements manquants ...")
        new_recs = []
        for cis in to_create:
            row = cis_map[cis]
            cip = cip_map.get(cis)
            titulaire = row.titulaire
            labo = normalize_lab_name(titulaire)

            fields = {
                "Code cis": cis,
                "Spécialité": safe_text(row.specialite),
                "Forme": safe_text(row.forme),
                "Voie d'administration": safe_text(row.voie_admin),
                "Laboratoire": labo,
                "Lien vers RCP": rcp_url_from_cis(cis),
            }

            if cip and cip.cip13:
                fields["CIP 13"] = cip.cip13
            if cip and cip.agrement_collectivites:
                fields["Agrément aux collectivités"] = cip.agrement_collectivites

            new_recs.append({"fields": fields})

        at.create_records(new_recs)
        ok(f"Créés: {len(new_recs)}")

        # relire pour avoir IDs (optionnel, on relit tout pour simplifier)
        records = at.list_all_records(fields=needed_fields + ["Spécialité", "Forme", "Voie d'administration"])
        airtable_by_cis = {}
        for rec in records:
            cis = str(rec.get("fields", {}).get("Code cis", "")).strip()
            cis = re.sub(r"\D", "", cis)
            if len(cis) == 8:
                airtable_by_cis[cis] = rec

    # 5) Suppressions (après téléchargement + parsing OK)
    if to_delete:
        info("Suppression des enregistrements Airtable absents de BDPM ...")
        ids = [airtable_by_cis[c]["id"] for c in to_delete if c in airtable_by_cis]
        at.delete_records(ids)
        ok(f"Supprimés: {len(ids)}")

        # refresh map
        records = at.list_all_records(fields=needed_fields + ["Spécialité", "Forme", "Voie d'administration"])
        airtable_by_cis = {}
        for rec in records:
            cis = str(rec.get("fields", {}).get("Code cis", "")).strip()
            cis = re.sub(r"\D", "", cis)
            if len(cis) == 8:
                airtable_by_cis[cis] = rec

    # 6) Enrichissement (RCP + dispo + CPD + cip13 + agrément)
    info("Enrichissement (RCP + CPD + Disponibilité + CIP13 + Agrément) ...")
    updates = []

    # On met à jour pour tous les CIS existants
    for cis, rec in airtable_by_cis.items():
        fields_cur = rec.get("fields", {}) or {}

        # URL RCP : priorité Airtable (si vide, fallback cis)
        rcp_url = str(fields_cur.get("Lien vers RCP", "")).strip()
        if not rcp_url:
            rcp_url = rcp_url_from_cis(cis)

        # CIP13 / agrément depuis CIS_CIP
        cip = cip_map.get(cis)
        cip13 = cip.cip13 if cip else ""
        agrement = cip.agrement_collectivites if cip else ""
        has_taux = cip.has_taux if cip else False

        # retrocession ANSM ?
        is_retro = cis in ansm_retro_cis

        # Indice hospitalier depuis CPD fichier
        cpd_hint = (cpd_hint_map.get(cis, "") or "").lower()
        hint_hosp = ("hospital" in cpd_hint)

        # RCP obligatoire pour:
        # - extraire CPD texte
        # - détecter homeopathie
        # - détecter mention hospitalier
        try:
            html = fetch_rcp_html(rcp_url)
        except Exception as e:
            die(f"RCP inaccessible pour CIS={cis} ({rcp_url}) -> STOP. Détail: {e}")

        cpd_text = extract_cpd_from_rcp_html(html)
        is_homeo = rcp_mentions_homeopathy(html)
        has_hosp = hint_hosp or rcp_mentions_hospital(html)

        dispo = compute_disponibilite(
            has_taux_ville=has_taux,
            is_ansm_retro=is_retro,
            is_homeopathy=is_homeo,
            has_hospital_mention=has_hosp,
        )

        # Préparer update (uniquement si différent / manquant)
        upd_fields = {}

        if cip13 and str(fields_cur.get("CIP 13", "")).strip() != cip13:
            upd_fields["CIP 13"] = cip13

        if agrement and str(fields_cur.get("Agrément aux collectivités", "")).strip() != agrement:
            upd_fields["Agrément aux collectivités"] = agrement

        if str(fields_cur.get("Lien vers RCP", "")).strip() != rcp_url:
            upd_fields["Lien vers RCP"] = rcp_url

        # Disponibilité du traitement
        if str(fields_cur.get("Disponibilité du traitement", "")).strip() != dispo:
            upd_fields["Disponibilité du traitement"] = dispo

        # CPD : on veut tout le bloc, avec retours à la ligne
        if cpd_text:
            cur_cpd = str(fields_cur.get("Conditions de prescription et délivrance", "")).strip()
            if cur_cpd != cpd_text:
                upd_fields["Conditions de prescription et délivrance"] = cpd_text

        # Laboratoire : normaliser depuis CIS_bdpm (titulaire)
        row = cis_map.get(cis)
        if row:
            labo = normalize_lab_name(row.titulaire)
            if labo and str(fields_cur.get("Laboratoire", "")).strip() != labo:
                upd_fields["Laboratoire"] = labo

        if upd_fields:
            updates.append({"id": rec["id"], "fields": upd_fields})

        # envoyer par batch pour éviter mémoire + limiter la durée
        if len(updates) >= 200:
            at.update_records(updates)
            ok(f"Batch updates: {len(updates)}")
            updates = []

    if updates:
        at.update_records(updates)
        ok(f"Updates finaux: {len(updates)}")

    ok("Terminé.")

if __name__ == "__main__":
    main()
