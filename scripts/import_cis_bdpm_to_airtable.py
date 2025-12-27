#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
from typing import Dict, List, Tuple, Optional, Set

import requests
import pandas as pd


# =========================
# ENV / CONFIG
# =========================
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_CIS_TABLE_NAME = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

# Airtable field names (must match exactly your Airtable columns)
FIELD_CIS = "Code cis"
FIELD_SPECIALITE = "Spécialité"
FIELD_FORME = "Forme"
FIELD_VOIE = "Voie d'administration"
FIELD_LABO = "Laboratoire"
FIELD_CPD = "Conditions de prescription et délivrance"
FIELD_RCP_LINK = "Lien vers RCP"
FIELD_AGREMENT = "Agrément aux collectivités"
FIELD_CIP13 = "CIP 13"
FIELD_RETRO = "Rétrocession"

# Behavior toggles
STOP_ON_RCP_ERROR = os.getenv("STOP_ON_RCP_ERROR", "true").strip().lower() == "true"
AIRTABLE_SLEEP = float(os.getenv("AIRTABLE_SLEEP", "0.25"))
AIRTABLE_MAX_RETRIES = int(os.getenv("AIRTABLE_MAX_RETRIES", "8"))

RCP_TIMEOUT = int(os.getenv("RCP_TIMEOUT", "45"))
RCP_MAX_RETRIES = int(os.getenv("RCP_MAX_RETRIES", "4"))
RCP_SLEEP = float(os.getenv("RCP_SLEEP", "0.40"))

# Sources
CIS_URL = os.getenv(
    "CIS_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_bdpm.txt",
).strip()

CIS_CPD_URL = os.getenv(
    "CIS_CPD_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CPD_bdpm.txt",
).strip()

CIS_CIP_URL = os.getenv(
    "CIS_CIP_URL",
    "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt",
).strip()

ANSM_RETRO_PAGE_URL = os.getenv(
    "ANSM_RETRO_PAGE_URL",
    "https://ansm.sante.fr/documents/reference/medicaments-en-retrocession",
).strip()

DATA_DIR = os.getenv("DATA_DIR", "data").strip()


# =========================
# HTTP helpers
# =========================
def http_get(session: requests.Session, url: str, timeout: int = 60) -> requests.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CIS-Airtable-Bot/1.0)",
        "Accept": "*/*",
    }
    return session.get(url, headers=headers, timeout=timeout)


def download_to(session: requests.Session, url: str, path: str, timeout: int = 120) -> None:
    resp = http_get(session, url, timeout=timeout)
    resp.raise_for_status()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(resp.content)


# =========================
# Parse BDPM files
# =========================
def parse_cis_bdpm_txt(path: str) -> Dict[str, Dict[str, str]]:
    """
    CIS_bdpm.txt columns:
    1 Code CIS
    2 Dénomination (Spécialité)
    3 Forme pharmaceutique
    4 Voies d'administration
    5 Titulaire (Laboratoire)
    tab-separated
    """
    cis_map: Dict[str, Dict[str, str]] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 5:
                continue
            cis = parts[0].strip()
            if not cis:
                continue
            cis_map[cis] = {
                FIELD_CIS: cis,
                FIELD_SPECIALITE: (parts[1] or "").strip(),
                FIELD_FORME: (parts[2] or "").strip(),
                FIELD_VOIE: (parts[3] or "").strip(),
                FIELD_LABO: (parts[4] or "").strip(),
            }
    return cis_map


def parse_cis_cpd_txt(path: str) -> Dict[str, str]:
    """
    CIS_CPD_bdpm.txt columns:
    1 Code CIS
    2 Conditions de prescription et délivrance
    """
    cpd: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            cis = parts[0].strip()
            txt = (parts[1] or "").strip()
            if cis:
                cpd[cis] = txt
    return cpd


def parse_cis_cip_txt(path: str) -> Dict[str, Dict[str, Optional[str]]]:
    """
    CIS_CIP_bdpm.txt is tab-separated with 13 columns in practice.
    Example start (from your file):
      CIS | CIP7 | ... | CIP13 | agrement | taux_remb | ... | ...
    We extract for each CIS:
      - CIP13 (first non-empty found)
      - Agrément aux collectivités (oui/non) if present
      - Taux remboursement (non-empty => reimbursed)
    """
    info: Dict[str, Dict[str, Optional[str]]] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 9:
                continue

            cis = (parts[0] or "").strip()
            if not cis:
                continue

            # In observed structure:
            # ... date ... then CIP13 then agrement then taux_remb then prices...
            # We locate CIP13 by pattern 13 digits in the line (safe).
            cip13_match = re.search(r"\b\d{13}\b", line)
            cip13 = cip13_match.group(0) if cip13_match else ""

            # Agrément often appears as 'oui'/'non' right after CIP13 in this file.
            # We try a heuristic: find the token right after the CIP13 token in split parts.
            agrement = None
            taux_remb = None

            if cip13:
                # find index where cip13 occurs in parts
                try:
                    idx = parts.index(cip13)
                    if idx + 1 < len(parts):
                        agrement = (parts[idx + 1] or "").strip().lower()
                    if idx + 2 < len(parts):
                        taux_remb = (parts[idx + 2] or "").strip()
                except ValueError:
                    pass

            # Normalize
            if agrement in ("oui", "non"):
                agrement_norm = agrement
            else:
                agrement_norm = None

            taux_norm = (taux_remb or "").strip()
            if not taux_norm:
                taux_norm = None

            cur = info.setdefault(cis, {"cip13": None, "agrement": None, "taux_remb": None})
            if cip13 and not cur["cip13"]:
                cur["cip13"] = cip13
            if agrement_norm and not cur["agrement"]:
                cur["agrement"] = agrement_norm
            # we keep "any reimbursed" logic
            if taux_norm:
                cur["taux_remb"] = taux_norm

    return info


# =========================
# ANSM retro list
# =========================
def find_ansm_retro_excel_url(session: requests.Session, page_url: str) -> str:
    """
    Scrape the ANSM page and find a .xls/.xlsx link.
    The page shows an envelope icon; behind it is a downloadable file under ansm.sante.fr/uploads/...
    """
    resp = http_get(session, page_url, timeout=60)
    resp.raise_for_status()
    html = resp.text

    # Prefer links containing "retrocession" and xls/xlsx
    candidates = re.findall(r'href="([^"]+\.(?:xls|xlsx))"', html, flags=re.IGNORECASE)
    # Convert relative to absolute if needed
    abs_candidates = []
    for href in candidates:
        if href.startswith("http"):
            abs_candidates.append(href)
        else:
            abs_candidates.append("https://ansm.sante.fr" + href)

    # Filter to likely file
    scored = []
    for u in abs_candidates:
        s = 0
        if "retrocession" in u.lower():
            s += 5
        if "uploads" in u.lower():
            s += 2
        scored.append((s, u))
    scored.sort(reverse=True)

    if not scored:
        raise RuntimeError("Impossible de trouver le lien Excel de rétrocession sur la page ANSM.")

    return scored[0][1]


def parse_ansm_retro_cis(excel_path: str) -> Set[str]:
    """
    User: the 3rd column contains the Code CIS.
    """
    df = pd.read_excel(excel_path, engine="openpyxl")
    if df.shape[1] < 3:
        raise RuntimeError("Le fichier ANSM rétrocession ne contient pas 3 colonnes.")
    cis_series = df.iloc[:, 2].astype(str).str.strip()
    # keep only digits
    cis_set = set(cis_series[cis_series.str.match(r"^\d+$")].tolist())
    return cis_set


# =========================
# RCP text probing
# =========================
def normalize_text(s: str) -> str:
    return (s or "").lower()


def fetch_rcp_text(session: requests.Session, url: str) -> str:
    """
    Fetch the RCP extract page and return text.
    Retries on transient errors.
    """
    last_err = None
    for attempt in range(1, RCP_MAX_RETRIES + 1):
        try:
            resp = http_get(session, url, timeout=RCP_TIMEOUT)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            # crude HTML->text: keep it simple to avoid heavy deps
            html = resp.text
            # strip scripts/styles
            html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.DOTALL | re.IGNORECASE)
            # remove tags
            text = re.sub(r"<[^>]+>", " ", html)
            # collapse whitespace
            text = re.sub(r"\s+", " ", text).strip()
            return text
        except Exception as e:
            last_err = e
            time.sleep(RCP_SLEEP * attempt)
    raise RuntimeError(f"RCP inaccessible: {url} ({last_err})")


def decide_retro_status(
    cis: str,
    in_ansm_retro: bool,
    has_reimbursement: bool,
    rcp_url: Optional[str],
    session: requests.Session,
) -> str:
    """
    Priority:
    1) ANSM retro
    2) reimbursement -> ville
    3) RCP contains homeopathy -> ville
    4) RCP contains hospital usage -> hospital
    5) else -> no info
    """
    if in_ansm_retro:
        return "Disponible en rétrocession hospitalière"

    if has_reimbursement:
        return "Disponible en pharmacie de ville"

    # No retro, no reimbursement => we may need RCP to decide (homeopathy or hospital mention)
    if not rcp_url:
        return "Pas d'informations mentionnées"

    text = fetch_rcp_text(session, rcp_url)
    t = normalize_text(text)

    if "homéopathi" in t or "homeopathi" in t:
        return "Disponible en pharmacie de ville"

    # explicit hospital mention
    if (
        "réservé à l'usage hospitalier" in t
        or "reserve a l'usage hospitalier" in t
        or "usage hospitalier" in t
        or "médicament hospitalier" in t
        or "medicament hospitalier" in t
        or "réservé à l’hôpital" in t
        or "reserve a l’hopital" in t
        or "réservé a l'hopital" in t
        or "reserve a l'hopital" in t
    ):
        return "Réservé à l'usage hospitalier"

    return "Pas d'informations mentionnées"


# =========================
# Airtable API
# =========================
def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json",
    }


def airtable_url() -> str:
    # Table name can include spaces; Airtable supports it in the URL path.
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_CIS_TABLE_NAME, safe='')}"


def airtable_request(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    last = None
    for attempt in range(1, AIRTABLE_MAX_RETRIES + 1):
        try:
            resp = session.request(method, url, headers=airtable_headers(), timeout=60, **kwargs)
            # retry on 429/5xx
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(AIRTABLE_SLEEP * attempt)
                last = resp
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last = e
            time.sleep(AIRTABLE_SLEEP * attempt)
    raise RuntimeError(f"Airtable request failed after retries: {method} {url} ({last})")


def airtable_list_all(session: requests.Session, fields: List[str]) -> List[Dict]:
    records = []
    offset = None
    params = [("pageSize", "100")]
    for f in fields:
        params.append(("fields[]", f))

    while True:
        p = list(params)
        if offset:
            p.append(("offset", offset))
        resp = airtable_request(session, "GET", airtable_url(), params=p)
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(AIRTABLE_SLEEP)
    return records


def airtable_batch_create(session: requests.Session, records: List[Dict]) -> None:
    # Airtable batch create: max 10
    for i in range(0, len(records), 10):
        chunk = records[i : i + 10]
        payload = {"records": [{"fields": r} for r in chunk]}
        airtable_request(session, "POST", airtable_url(), data=json.dumps(payload))
        time.sleep(AIRTABLE_SLEEP)


def airtable_batch_update(session: requests.Session, updates: List[Tuple[str, Dict]]) -> None:
    # updates: (record_id, fields)
    for i in range(0, len(updates), 10):
        chunk = updates[i : i + 10]
        payload = {"records": [{"id": rid, "fields": f} for rid, f in chunk]}
        airtable_request(session, "PATCH", airtable_url(), data=json.dumps(payload))
        time.sleep(AIRTABLE_SLEEP)


def airtable_batch_delete(session: requests.Session, record_ids: List[str]) -> None:
    for i in range(0, len(record_ids), 10):
        chunk = record_ids[i : i + 10]
        params = [("records[]", rid) for rid in chunk]
        airtable_request(session, "DELETE", airtable_url(), params=params)
        time.sleep(AIRTABLE_SLEEP)


# =========================
# Main pipeline
# =========================
def ensure_env() -> None:
    missing = []
    if not AIRTABLE_API_TOKEN:
        missing.append("AIRTABLE_API_TOKEN")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if not AIRTABLE_CIS_TABLE_NAME:
        missing.append("AIRTABLE_CIS_TABLE_NAME")

    if missing:
        raise RuntimeError(
            "Variables d'environnement manquantes: " + ", ".join(missing)
        )


def build_rcp_link(cis: str) -> str:
    # Per your required model
    return f"https://base-donnees-publique.medicaments.gouv.fr/medicament/{cis}/extrait#tab-rcp"


def main() -> None:
    ensure_env()
    os.makedirs(DATA_DIR, exist_ok=True)

    print("1) Téléchargements...")

    with requests.Session() as session:
        # Download BDPM files
        cis_path = os.path.join(DATA_DIR, "CIS_bdpm.txt")
        cpd_path = os.path.join(DATA_DIR, "CIS_CPD_bdpm.txt")
        cip_path = os.path.join(DATA_DIR, "CIS_CIP_bdpm.txt")

        download_to(session, CIS_URL, cis_path, timeout=180)
        download_to(session, CIS_CPD_URL, cpd_path, timeout=180)
        download_to(session, CIS_CIP_URL, cip_path, timeout=180)

        # Download ANSM retro excel (dynamic link)
        ansm_xls_url = find_ansm_retro_excel_url(session, ANSM_RETRO_PAGE_URL)
        ansm_xls_path = os.path.join(DATA_DIR, "ansm_retrocession.xlsx")
        download_to(session, ansm_xls_url, ansm_xls_path, timeout=180)
        print(f"✅ ANSM Excel: {ansm_xls_url}")

        print("2) Parsing...")
        cis_map = parse_cis_bdpm_txt(cis_path)  # base roster
        cpd_map = parse_cis_cpd_txt(cpd_path)
        cip_info = parse_cis_cip_txt(cip_path)
        retro_cis = parse_ansm_retro_cis(ansm_xls_path)

        # Determine reimbursed CIS (any line with a non-empty taux_remb)
        reimbursed_cis: Set[str] = set()
        agrement_map: Dict[str, str] = {}
        cip13_map: Dict[str, str] = {}

        for cis, d in cip_info.items():
            if d.get("taux_remb"):
                reimbursed_cis.add(cis)
            if d.get("agrement") in ("oui", "non"):
                agrement_map[cis] = d["agrement"]  # type: ignore
            if d.get("cip13"):
                cip13_map[cis] = d["cip13"]  # type: ignore

        print(f"📌 CIS total (CIS_bdpm): {len(cis_map)}")
        print(f"📌 CIS avec taux remboursement: {len(reimbursed_cis)}")
        print(f"📌 CIS ANSM rétrocession: {len(retro_cis)}")

        # Build desired Airtable records in memory (NO WRITE YET)
        desired_by_cis: Dict[str, Dict] = {}

        # We will only fetch RCP for CIS not in retro and not reimbursed
        rcp_needed: List[Tuple[str, str]] = []

        for cis, fields in cis_map.items():
            rec = dict(fields)

            # CPD
            rec[FIELD_CPD] = cpd_map.get(cis, "")

            # RCP link
            rcp_link = build_rcp_link(cis)
            rec[FIELD_RCP_LINK] = rcp_link

            # Agrément + CIP13
            if cis in agrement_map:
                rec[FIELD_AGREMENT] = agrement_map[cis]
            if cis in cip13_map:
                rec[FIELD_CIP13] = cip13_map[cis]

            in_retro = cis in retro_cis
            has_reimb = cis in reimbursed_cis

            # compute status
            if in_retro:
                rec[FIELD_RETRO] = "Disponible en rétrocession hospitalière"
            elif has_reimb:
                rec[FIELD_RETRO] = "Disponible en pharmacie de ville"
            else:
                # will require RCP fetch to decide homeopathy/hospital/no info
                rcp_needed.append((cis, rcp_link))
                rec[FIELD_RETRO] = None  # placeholder

            desired_by_cis[cis] = rec

        # Pre-flight: fetch required RCP texts BEFORE modifying Airtable
        print(f"3) Pré-contrôle RCP (nécessaires): {len(rcp_needed)}")
        # If STOP_ON_RCP_ERROR, we stop on first inaccessible.
        for idx, (cis, url) in enumerate(rcp_needed, start=1):
            try:
                status = decide_retro_status(
                    cis=cis,
                    in_ansm_retro=False,
                    has_reimbursement=False,
                    rcp_url=url,
                    session=session,
                )
                desired_by_cis[cis][FIELD_RETRO] = status
            except Exception as e:
                msg = f"❌ RCP inaccessible pour CIS={cis} url={url} erreur={e}"
                print(msg)
                if STOP_ON_RCP_ERROR:
                    raise RuntimeError(msg) from e
                # else mark no info and continue
                desired_by_cis[cis][FIELD_RETRO] = "Pas d'informations mentionnées"

            if idx % 50 == 0:
                print(f"   ...RCP traités: {idx}/{len(rcp_needed)}")

        # At this point, ALL needed downloads + RCP fetch done => safe to sync Airtable
        print("4) Lecture Airtable + inventaire...")
        at_fields = [
            FIELD_CIS,
            FIELD_SPECIALITE,
            FIELD_FORME,
            FIELD_VOIE,
            FIELD_LABO,
            FIELD_CPD,
            FIELD_RCP_LINK,
            FIELD_AGREMENT,
            FIELD_CIP13,
            FIELD_RETRO,
        ]
        existing = airtable_list_all(session, at_fields)

        existing_by_cis: Dict[str, Dict] = {}
        for r in existing:
            f = r.get("fields", {})
            cis = str(f.get(FIELD_CIS, "")).strip()
            if cis:
                existing_by_cis[cis] = {"id": r["id"], "fields": f}

        desired_cis_set = set(desired_by_cis.keys())
        existing_cis_set = set(existing_by_cis.keys())

        to_create_cis = sorted(list(desired_cis_set - existing_cis_set))
        to_delete_cis = sorted(list(existing_cis_set - desired_cis_set))
        to_consider_update_cis = sorted(list(desired_cis_set & existing_cis_set))

        print(f"✅ À créer: {len(to_create_cis)}")
        print(f"✅ À supprimer: {len(to_delete_cis)}")
        print(f"✅ À vérifier/mettre à jour: {len(to_consider_update_cis)}")

        # Prepare creates
        creates = [desired_by_cis[cis] for cis in to_create_cis]

        # Prepare updates (only if fields differ)
        updates: List[Tuple[str, Dict]] = []
        for cis in to_consider_update_cis:
            desired_fields = desired_by_cis[cis]
            existing_rec = existing_by_cis[cis]
            rid = existing_rec["id"]
            cur_fields = existing_rec["fields"]

            patch: Dict = {}
            for k, v in desired_fields.items():
                # Normalize None => ""? No: keep None as "no change"? We want consistent.
                if v is None:
                    v_comp = ""
                else:
                    v_comp = v

                cur_val = cur_fields.get(k, "")
                if cur_val != v_comp:
                    patch[k] = v_comp

            if patch:
                updates.append((rid, patch))

        delete_ids = [existing_by_cis[cis]["id"] for cis in to_delete_cis]

        print("5) Synchronisation Airtable...")
        # Create first
        if creates:
            airtable_batch_create(session, creates)
            print(f"➕ Créés: {len(creates)}")

        # Update
        if updates:
            airtable_batch_update(session, updates)
            print(f"♻️ Mis à jour: {len(updates)}")

        # Delete last
        if delete_ids:
            airtable_batch_delete(session, delete_ids)
            print(f"➖ Supprimés: {len(delete_ids)}")

        print("✅ Terminé.")


if __name__ == "__main__":
    main()
