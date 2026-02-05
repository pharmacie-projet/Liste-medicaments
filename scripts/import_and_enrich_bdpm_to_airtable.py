#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import random
import urllib.parse
import unicodedata
from typing import Dict, Any, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag


# ============================================================
# CONFIG
# ============================================================

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH_SIZE = 10

AIRTABLE_MIN_DELAY_S = float(os.getenv("AIRTABLE_MIN_DELAY_S", "0.25"))
HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "25"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))

# IMPORTANT : limite par run (sinon ça ne finira jamais en GitHub Actions)
RCP_MAX_PER_RUN = int(os.getenv("RCP_MAX_PER_RUN", "200"))

UPDATE_FLUSH_THRESHOLD = int(os.getenv("UPDATE_FLUSH_THRESHOLD", "200"))
HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "50"))

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
}

# Airtable fields
FIELD_CIS = "Code cis"
FIELD_RCP_URL = "Lien vers RCP"

FIELD_INDICATIONS_RCP = "Indications RCP"      # 4.1
FIELD_POSOLOGIE_RCP = "Posologie RCP"          # 4.2
FIELD_INTERACTIONS_RCP = "Interactions RCP"    # 4.5


# ============================================================
# LOG
# ============================================================

def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def info(msg: str):
    print(f"[{_ts()}] ℹ️ {msg}", flush=True)

def ok(msg: str):
    print(f"[{_ts()}] ✅ {msg}", flush=True)

def warn(msg: str):
    print(f"[{_ts()}] ⚠️ {msg}", flush=True)

def die(msg: str, code: int = 1):
    print(f"[{_ts()}] ❌ {msg}", flush=True)
    raise SystemExit(code)

def sleep_throttle():
    time.sleep(AIRTABLE_MIN_DELAY_S)

def retry_sleep(attempt: int):
    time.sleep(min(10, 0.6 * (2 ** (attempt - 1))) + random.random() * 0.25)


# ============================================================
# TEXT / NORMALIZATION
# ============================================================

def safe_text(x: Any) -> str:
    if x is None:
        return ""
    if not isinstance(x, str):
        x = str(x)
    x = x.replace("\uFFFD", "")
    x = x.replace("\r\n", "\n").replace("\r", "\n")
    return x.strip()

def strip_accents(s: str) -> str:
    s = safe_text(s)
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def norm_key(s: str) -> str:
    # lower + sans accents + espaces normalisés
    s = strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_rcp_url(url: str) -> str:
    """
    Problème principal : le fragment #tab-rcp n'est PAS envoyé au serveur.
    Donc on transforme en extrait?tab=rcp (côté serveur).
    """
    url = safe_text(url)
    if not url:
        return ""

    p = urllib.parse.urlsplit(url)
    # enlever fragment
    p = p._replace(fragment="")

    # si déjà un query tab=rcp, ok
    qs = urllib.parse.parse_qs(p.query, keep_blank_values=True)
    if "tab" in qs and qs["tab"] and qs["tab"][0].lower() == "rcp":
        return urllib.parse.urlunsplit(p)

    # cas classique: .../extrait (ou .../extrait?xxx) => on force tab=rcp
    qs["tab"] = ["rcp"]
    new_query = urllib.parse.urlencode(qs, doseq=True)
    p = p._replace(query=new_query)
    return urllib.parse.urlunsplit(p)

def looks_like_header_4x(txt: str) -> bool:
    """
    Détecte un header type "4.3." / "4.4." / "4.5." etc.
    """
    t = norm_key(txt)
    return bool(re.match(r"^4\.\d+\.", t))


# ============================================================
# HTTP
# ============================================================

def http_get(url: str) -> str:
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                url,
                headers=HEADERS_WEB,
                timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
                allow_redirects=True,
            )
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}")
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            last = e
            warn(f"GET KO (attempt {attempt}/{MAX_RETRIES}) url={url} err={e}")
            retry_sleep(attempt)
    raise RuntimeError(f"GET failed after retries: {last}")


# ============================================================
# RCP EXTRACTION (copier-coller brut sous le header)
# ============================================================

TARGETS = [
    (FIELD_INDICATIONS_RCP, "indications therapeutiques"),
    (FIELD_POSOLOGIE_RCP, "posologie et mode d'administration"),
    (FIELD_INTERACTIONS_RCP, "interactions avec d'autres medicaments et autres formes d'interactions"),
]

def find_heading_tag(soup: BeautifulSoup, key_phrase: str) -> Optional[Tag]:
    """
    Trouve le tag qui contient la phrase cible (sans accents, insensible à la casse).
    On accepte h1..h6, strong, b, p, div (car le site varie parfois).
    """
    key = norm_key(key_phrase)

    candidates = soup.find_all(["h1","h2","h3","h4","h5","h6","strong","b","p","div","span"])
    for tag in candidates:
        txt = tag.get_text(" ", strip=True)
        if not txt:
            continue
        nt = norm_key(txt)
        if key in nt:
            return tag
        # tolérance : "4.1. Indications thérapeutiques"
        if ("indications therapeutiques" in key) and ("indications therapeutiques" in nt):
            return tag
        if ("posologie et mode d'administration" in key) and ("posologie et mode d'administration" in nt):
            return tag
        if ("interactions avec d'autres medicaments" in key) and ("interactions avec d'autres medicaments" in nt):
            return tag
    return None

def iter_after(tag: Tag):
    """
    Itère en ordre document sur les éléments après 'tag' (en restant dans le contenu principal).
    """
    cur = tag
    while True:
        cur = cur.find_next()
        if cur is None:
            return
        if isinstance(cur, Tag):
            yield cur

def extract_under_heading_until_next_4x(soup: BeautifulSoup, heading_phrase: str) -> str:
    """
    1) repère le header
    2) récupère tout le texte qui suit
    3) stop au prochain header "4.x." (autre rubrique 4.y)
    """
    h = find_heading_tag(soup, heading_phrase)
    if h is None:
        return ""

    collected: List[str] = []
    started = False

    for el in iter_after(h):
        # stop si on tombe sur un nouveau header 4.x (et qu’on a commencé à collecter)
        if el.name in ("h1","h2","h3","h4","h5","h6"):
            ht = el.get_text(" ", strip=True)
            if looks_like_header_4x(ht) and started:
                break
            # aussi stop si on rencontre l'un des 3 headers cibles (évite d'englober)
            nt = norm_key(ht)
            if started and (
                "indications therapeutiques" in nt
                or "posologie et mode d'administration" in nt
                or "interactions avec d'autres medicaments" in nt
            ):
                break

        # on saute les éléments "menu" / sommaire (rare mais ça arrive)
        if el.get("class") and any("sommaire" in c.lower() for c in el.get("class", [])):
            continue

        # textes réellement utiles : p, li, table, div, etc.
        if el.name in ("p","li","table","tbody","tr","td","div","span","ul","ol"):
            txt = el.get_text("\n", strip=True)
            txt = safe_text(txt)
            if txt:
                started = True
                collected.append(txt)

        # sécurité: si on a commencé et qu’on accumule beaucoup, on continue quand même,
        # mais on évite les doublons exacts
    # nettoyage final
    # - supprime doublons consécutifs
    out: List[str] = []
    prev = ""
    for chunk in collected:
        if chunk == prev:
            continue
        out.append(chunk)
        prev = chunk

    result = "\n\n".join(out).strip()
    return result


def extract_rcp_sections_from_html(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")

    results: Dict[str, str] = {}
    for field_name, heading in TARGETS:
        results[field_name] = extract_under_heading_until_next_4x(soup, heading)

    return results


# ============================================================
# AIRTABLE API
# ============================================================

def airtable_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

def airtable_list_records(token: str, base_id: str, table: str) -> List[Dict[str, Any]]:
    """
    Récupère tous les records (pagination).
    """
    url = f"{AIRTABLE_API_BASE}/{base_id}/{urllib.parse.quote(table)}"
    out: List[Dict[str, Any]] = []
    offset = None

    while True:
        params = {}
        if offset:
            params["offset"] = offset
        # on ne filtre pas côté Airtable pour rester simple et robuste
        r = requests.get(url, headers=airtable_headers(token), params=params, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Airtable list HTTP {r.status_code}: {r.text[:200]}")
        data = r.json()
        recs = data.get("records", [])
        out.extend(recs)
        offset = data.get("offset")
        if not offset:
            break
        sleep_throttle()

    return out

def airtable_batch_update(token: str, base_id: str, table: str, updates: List[Dict[str, Any]]):
    """
    updates: [{ "id": "...", "fields": {...} }, ...]
    """
    if not updates:
        return
    url = f"{AIRTABLE_API_BASE}/{base_id}/{urllib.parse.quote(table)}"
    payload = {"records": updates, "typecast": False}
    r = requests.patch(url, headers=airtable_headers(token), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable update HTTP {r.status_code}: {r.text[:400]}")
    sleep_throttle()


# ============================================================
# MAIN
# ============================================================

def main():
    token = os.getenv("AIRTABLE_API_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()

    if not token or not base_id or not table:
        die("Variables manquantes: AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME")

    info(f"Chargement Airtable table='{table}' ...")
    records = airtable_list_records(token, base_id, table)
    ok(f"Records Airtable chargés: {len(records)}")

    # traitement
    to_update: List[Dict[str, Any]] = []
    updated = 0
    checked = 0
    empty_or_missing_url = 0
    fetched = 0
    parse_ok = 0
    parse_empty = 0
    failures = 0

    rcp_done_this_run = 0

    for idx, rec in enumerate(records, start=1):
        fields = rec.get("fields", {}) or {}

        cis = safe_text(fields.get(FIELD_CIS))
        rcp_url_raw = safe_text(fields.get(FIELD_RCP_URL))

        # si pas de lien => skip
        if not rcp_url_raw:
            empty_or_missing_url += 1
            continue

        # si déjà rempli sur les 3 champs => skip
        already_ind = safe_text(fields.get(FIELD_INDICATIONS_RCP))
        already_pos = safe_text(fields.get(FIELD_POSOLOGIE_RCP))
        already_int = safe_text(fields.get(FIELD_INTERACTIONS_RCP))
        if already_ind and already_pos and already_int:
            continue

        # limite par run
        if rcp_done_this_run >= RCP_MAX_PER_RUN:
            break

        checked += 1
        rcp_done_this_run += 1

        try:
            rcp_url = normalize_rcp_url(rcp_url_raw)
            html = http_get(rcp_url)
            fetched += 1

            sections = extract_rcp_sections_from_html(html)

            # si tout est vide => parfois le site renvoie encore une page sans RCP
            if not (sections[FIELD_INDICATIONS_RCP] or sections[FIELD_POSOLOGIE_RCP] or sections[FIELD_INTERACTIONS_RCP]):
                parse_empty += 1
                warn(f"RCP vide après parse | CIS={cis or 'NA'} | url={rcp_url}")
                continue

            parse_ok += 1

            patch: Dict[str, Any] = {}
            # on n’écrase pas si déjà rempli
            if not already_ind and sections[FIELD_INDICATIONS_RCP]:
                patch[FIELD_INDICATIONS_RCP] = sections[FIELD_INDICATIONS_RCP]
            if not already_pos and sections[FIELD_POSOLOGIE_RCP]:
                patch[FIELD_POSOLOGIE_RCP] = sections[FIELD_POSOLOGIE_RCP]
            if not already_int and sections[FIELD_INTERACTIONS_RCP]:
                patch[FIELD_INTERACTIONS_RCP] = sections[FIELD_INTERACTIONS_RCP]

            if patch:
                to_update.append({"id": rec["id"], "fields": patch})

            # flush
            if len(to_update) >= UPDATE_FLUSH_THRESHOLD:
                info(f"Flush updates: {len(to_update)}")
                # batch 10
                for i in range(0, len(to_update), AIRTABLE_BATCH_SIZE):
                    airtable_batch_update(token, base_id, table, to_update[i:i + AIRTABLE_BATCH_SIZE])
                updated += len(to_update)
                to_update = []
                ok(f"Flush OK (total updated so far: {updated})")

        except Exception as e:
            failures += 1
            warn(f"RCP parse KO | CIS={cis or 'NA'} | err={e} (on continue)")
            continue

        if checked % HEARTBEAT_EVERY == 0:
            info(
                f"Heartbeat: checked={checked} fetched={fetched} parse_ok={parse_ok} parse_empty={parse_empty} "
                f"failures={failures} updates_buffer={len(to_update)}"
            )

    # final flush
    if to_update:
        info(f"Final flush updates: {len(to_update)}")
        for i in range(0, len(to_update), AIRTABLE_BATCH_SIZE):
            airtable_batch_update(token, base_id, table, to_update[i:i + AIRTABLE_BATCH_SIZE])
        updated += len(to_update)
        ok(f"Final flush OK (total updated: {updated})")

    ok(
        f"Done. rcp_checked={checked} fetched={fetched} parse_ok={parse_ok} parse_empty={parse_empty} "
        f"failures={failures} updated={updated} missing_url={empty_or_missing_url} "
        f"limit_per_run={RCP_MAX_PER_RUN}"
    )


if __name__ == "__main__":
    main()
