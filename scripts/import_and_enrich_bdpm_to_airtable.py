#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import random
import urllib.parse
import unicodedata
from typing import Dict, Any, List, Optional

import requests
from bs4 import BeautifulSoup

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH_SIZE = 10
AIRTABLE_MIN_DELAY_S = float(os.getenv("AIRTABLE_MIN_DELAY_S", "0.25"))
RCP_MAX_PER_RUN = int(os.getenv("RCP_MAX_PER_RUN", "100"))
UPDATE_FLUSH_THRESHOLD = int(os.getenv("UPDATE_FLUSH_THRESHOLD", "100"))
HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "20"))

FIELD_CIS = "Code cis"
FIELD_RCP_URL = "Lien vers RCP"
FIELD_INDICATIONS_RCP = "Indications RCP"
FIELD_POSOLOGIE_RCP = "Posologie RCP"
FIELD_INTERACTIONS_RCP = "Interactions RCP"

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

def safe_text(x: Any) -> str:
    if x is None:
        return ""
    if not isinstance(x, str):
        x = str(x)
    x = x.replace("\uFFFD", "")
    x = x.replace("\r\n", "\n").replace("\r", "\n")
    # normalise apostrophes/espaces insécables (important pour matcher)
    x = x.replace("\u2019", "'").replace("\u2018", "'").replace("\u02BC", "'")
    x = x.replace("\u00A0", " ").replace("\u202F", " ")
    return x.strip()

def strip_accents(s: str) -> str:
    s = safe_text(s)
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm(s: str) -> str:
    s = strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_like_header_4x(txt: str) -> bool:
    # accepte "4.2", "4.2.", etc.
    return bool(re.match(r"^4\.\d+\b", norm(txt)))

TARGETS = [
    (FIELD_INDICATIONS_RCP, "indications therapeutiques"),
    (FIELD_POSOLOGIE_RCP, "posologie et mode d'administration"),
    (FIELD_INTERACTIONS_RCP, "interactions avec d'autres medicaments et autres formes d'interactions"),
]

def extract_section_by_heading(html: str, heading_key: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # on cherche un tag dont le texte contient heading_key (normalisé)
    key = norm(heading_key)

    candidates = soup.find_all(["h1","h2","h3","h4","h5","h6","strong","b","p","div","span","a","button"])
    h = None
    for t in candidates:
        txt = t.get_text(" ", strip=True)
        if not txt:
            continue
        nt = norm(txt)
        if key in nt:
            h = t
            break
        # tolérances
        if "indications therapeutiques" in key and "indications therapeutiques" in nt:
            h = t; break
        if "posologie et mode d'administration" in key and "posologie et mode d'administration" in nt:
            h = t; break
        if "interactions avec d'autres medicaments" in key and "interactions avec d'autres medicaments" in nt:
            h = t; break

    if h is None:
        return ""

    out = []
    started = False

    cur = h
    while True:
        cur = cur.find_next()
        if cur is None:
            break

        if getattr(cur, "name", None) in ("h1","h2","h3","h4","h5","h6"):
            ht = cur.get_text(" ", strip=True)
            nt = norm(ht)
            if started and looks_like_header_4x(ht):
                break
            if started and (
                "indications therapeutiques" in nt
                or "posologie et mode d'administration" in nt
                or "interactions avec d'autres medicaments" in nt
            ):
                break

        if getattr(cur, "name", None) in ("p","li","div","span","ul","ol","table","tbody","tr","td"):
            txt = safe_text(cur.get_text("\n", strip=True))
            if txt:
                started = True
                out.append(txt)

    # dédoublonnage léger
    cleaned = []
    prev = ""
    for x in out:
        if x == prev:
            continue
        cleaned.append(x)
        prev = x

    return "\n\n".join(cleaned).strip()

def extract_all_sections(html: str) -> Dict[str, str]:
    res = {}
    for field, key in TARGETS:
        res[field] = extract_section_by_heading(html, key)
    return res

def airtable_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def airtable_list_records(token: str, base_id: str, table: str) -> List[Dict[str, Any]]:
    url = f"{AIRTABLE_API_BASE}/{base_id}/{urllib.parse.quote(table)}"
    out = []
    offset = None
    while True:
        params = {}
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=airtable_headers(token), params=params, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Airtable list HTTP {r.status_code}: {r.text[:200]}")
        data = r.json()
        out.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        sleep_throttle()
    return out

def airtable_batch_update(token: str, base_id: str, table: str, updates: List[Dict[str, Any]]):
    if not updates:
        return
    url = f"{AIRTABLE_API_BASE}/{base_id}/{urllib.parse.quote(table)}"
    payload = {"records": updates, "typecast": False}
    r = requests.patch(url, headers=airtable_headers(token), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable update HTTP {r.status_code}: {r.text[:400]}")
    sleep_throttle()

def fetch_html_with_fragment(url_with_hash: str, timeout_ms: int = 45000) -> str:
    """
    Ouvre EXACTEMENT l'URL avec #tab-rcp via Chromium headless.
    """
    url_with_hash = safe_text(url_with_hash)
    if not url_with_hash:
        return ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(url_with_hash, wait_until="domcontentloaded", timeout=timeout_ms)
            # petit wait pour que l'onglet s'affiche si la page ajuste le contenu
            page.wait_for_timeout(800)
            html = page.content()
            return html
        finally:
            browser.close()

def main():
    token = os.getenv("AIRTABLE_API_TOKEN", "").strip()
    base_id = os.getenv("AIRTABLE_BASE_ID", "").strip()
    table = os.getenv("AIRTABLE_CIS_TABLE_NAME", "").strip()
    if not token or not base_id or not table:
        die("Variables manquantes: AIRTABLE_API_TOKEN / AIRTABLE_BASE_ID / AIRTABLE_CIS_TABLE_NAME")

    records = airtable_list_records(token, base_id, table)
    ok(f"Records Airtable: {len(records)}")

    to_update = []
    checked = fetched = parse_ok = parse_empty = failures = updated = 0
    done = 0

    for rec in records:
        fields = rec.get("fields", {}) or {}
        rcp_url = safe_text(fields.get(FIELD_RCP_URL))
        if not rcp_url:
            continue

        # ne traite que si au moins un champ est vide
        already_ind = safe_text(fields.get(FIELD_INDICATIONS_RCP))
        already_pos = safe_text(fields.get(FIELD_POSOLOGIE_RCP))
        already_int = safe_text(fields.get(FIELD_INTERACTIONS_RCP))
        if already_ind and already_pos and already_int:
            continue

        if done >= RCP_MAX_PER_RUN:
            break

        checked += 1
        done += 1

        try:
            html = fetch_html_with_fragment(rcp_url)
            fetched += 1
            if not html or len(html) < 500:
                parse_empty += 1
                warn(f"HTML trop court | url={rcp_url}")
                continue

            sections = extract_all_sections(html)
            if not (sections[FIELD_INDICATIONS_RCP] or sections[FIELD_POSOLOGIE_RCP] or sections[FIELD_INTERACTIONS_RCP]):
                parse_empty += 1
                warn(f"RCP vide après parse | url={rcp_url}")
                continue

            patch = {}
            if not already_ind and sections[FIELD_INDICATIONS_RCP]:
                patch[FIELD_INDICATIONS_RCP] = sections[FIELD_INDICATIONS_RCP]
            if not already_pos and sections[FIELD_POSOLOGIE_RCP]:
                patch[FIELD_POSOLOGIE_RCP] = sections[FIELD_POSOLOGIE_RCP]
            if not already_int and sections[FIELD_INTERACTIONS_RCP]:
                patch[FIELD_INTERACTIONS_RCP] = sections[FIELD_INTERACTIONS_RCP]

            if patch:
                to_update.append({"id": rec["id"], "fields": patch})
                parse_ok += 1

            if len(to_update) >= UPDATE_FLUSH_THRESHOLD:
                info(f"Flush updates: {len(to_update)}")
                for i in range(0, len(to_update), AIRTABLE_BATCH_SIZE):
                    airtable_batch_update(token, base_id, table, to_update[i:i + AIRTABLE_BATCH_SIZE])
                updated += len(to_update)
                to_update = []
                ok(f"Flush OK total_updated={updated}")

        except PWTimeoutError:
            failures += 1
            warn(f"Timeout Playwright | url={rcp_url}")
        except Exception as e:
            failures += 1
            warn(f"Erreur | url={rcp_url} | err={e}")

        if checked % HEARTBEAT_EVERY == 0:
            info(f"Heartbeat checked={checked} fetched={fetched} parse_ok={parse_ok} parse_empty={parse_empty} failures={failures} buffer={len(to_update)}")

    if to_update:
        info(f"Final flush updates: {len(to_update)}")
        for i in range(0, len(to_update), AIRTABLE_BATCH_SIZE):
            airtable_batch_update(token, base_id, table, to_update[i:i + AIRTABLE_BATCH_SIZE])
        updated += len(to_update)

    ok(f"Done. checked={checked} fetched={fetched} parse_ok={parse_ok} parse_empty={parse_empty} failures={failures} updated={updated} limit={RCP_MAX_PER_RUN}")

if __name__ == "__main__":
    main()
