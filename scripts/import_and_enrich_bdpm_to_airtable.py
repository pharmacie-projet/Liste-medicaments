#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import random
import unicodedata
from typing import Dict, Any, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

# Playwright (obligatoire si tu veux conserver #tab-rcp)
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# ============================================================
# CONFIG
# ============================================================

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

# ⚠️ Pour éviter Airtable 413 : 1 record / requête
AIRTABLE_BATCH_SIZE = int(os.getenv("AIRTABLE_BATCH_SIZE", "1"))

AIRTABLE_MIN_DELAY_S = float(os.getenv("AIRTABLE_MIN_DELAY_S", "0.25"))
HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "25"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))

# IMPORTANT : limite par run (sinon ça ne finira jamais en GitHub Actions)
RCP_MAX_PER_RUN = int(os.getenv("RCP_MAX_PER_RUN", "50"))

UPDATE_FLUSH_THRESHOLD = int(os.getenv("UPDATE_FLUSH_THRESHOLD", "25"))
HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "25"))

# Pour éviter limites Airtable (cellule + payload)
AIRTABLE_TEXT_MAX_CHARS = int(os.getenv("AIRTABLE_TEXT_MAX_CHARS", "90000"))

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
    s = strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_like_header_4x(txt: str) -> bool:
    """
    Détecte un header type "4.3." / "4.4." / "4.5." etc.
    """
    t = norm_key(txt)
    return bool(re.match(r"^4\.\d+\.", t))

def truncate_for_airtable(s: str, limit: int = AIRTABLE_TEXT_MAX_CHARS) -> str:
    s = safe_text(s)
    if not s:
        return ""
    if len(s) <= limit:
        return s
    # On garde le début (le plus important) + marqueur clair
    return s[:limit - 2000].rstrip() + "\n\n[...] (tronqué pour limite Airtable)\n"


# ============================================================
# PLAYWRIGHT FETCH (respecte #tab-rcp)
# ============================================================

def pw_get_html(page, url: str) -> str:
    """
    Charge l'URL EXACTE (avec #tab-rcp) et renvoie le HTML rendu.
    """
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=int((HTTP_CONNECT_TIMEOUT + HTTP_READ_TIMEOUT) * 1000))
            # attendre un élément caractéristique du RCP (menu rubriques / header "DONNEES CLINIQUES")
            # c'est plus robuste que "networkidle" (souvent instable).
            try:
                page.wait_for_selector("text=DONNÉES CLINIQUES", timeout=8000)
            except PWTimeoutError:
                # certains RCP n'affichent pas immédiatement l'accent ou le texte exact -> on tolère
                pass
            # laisser respirer un peu
            time.sleep(0.2)
            return page.content()
        except Exception as e:
            last = e
            warn(f"Playwright GET KO (attempt {attempt}/{MAX_RETRIES}) url={url} err={e}")
            retry_sleep(attempt)
    raise RuntimeError(f"Playwright GET failed after retries: {last}")


# ============================================================
# RCP EXTRACTION (copier-coller brut sous le header)
# ============================================================

TARGETS = [
    (FIELD_INDICATIONS_RCP, "indications therapeutiques"),
    (FIELD_POSOLOGIE_RCP, "posologie et mode d'administration"),
    (FIELD_INTERACTIONS_RCP, "interactions avec d'autres medicaments et autres formes d'interactions"),
]

def find_heading_tag(soup: BeautifulSoup, key_phrase: str) -> Optional[Tag]:
    key = norm_key(key_phrase)
    candidates = soup.find_all(["h1","h2","h3","h4","h5","h6","strong","b","p","div","span"])
    for tag in candidates:
        txt = tag.get_text(" ", strip=True)
        if not txt:
            continue
        nt = norm_key(txt)
        if key in nt:
            return tag
        # tolérance
        if ("indications therapeutiques" in key) and ("indications therapeutiques" in nt):
            return tag
        if ("posologie et mode d'administration" in key) and ("posologie et mode d'administration" in nt):
            return tag
        if ("interactions avec d'autres medicaments" in key) and ("interactions avec d'autres medicaments" in nt):
            return tag
    return None

def iter_after(tag: Tag):
    cur = tag
    while True:
        cur = cur.find_next()
        if cur is None:
            return
        if isinstance(cur, Tag):
            yield cur

def extract_under_heading_until_next_4x(soup: BeautifulSoup, heading_phrase: str) -> str:
    h = find_heading_tag(soup, heading_phrase)
    if h is None:
        return ""

    collected: List[str] = []
    started = False

    for el in iter_after(h):
        if el.name in ("h1","h2","h3","h4","h5","h6"):
            ht = el.get_text(" ", strip=True)
            if looks_like_header_4x(ht) and started:
                break
            nt = norm_key(ht)
            if started and (
                "indications therapeutiques" in nt
                or "posologie et mode d'administration" in nt
                or "interactions avec d'autres medicaments" in nt
            ):
                break

        # skip sommaire/menu
        cls = el.get("class") or []
        if any("sommaire" in str(c).lower() for c in cls):
            continue

        if el.name in ("p","li","table","tbody","tr","td","div","span","ul","ol"):
            txt = el.get_text("\n", strip=True)
            txt = safe_text(txt)
            if txt:
                started = True
                collected.append(txt)

    # supprime doublons consécutifs
    out: List[str] = []
    prev = ""
    for chunk in collected:
        if chunk == prev:
            continue
        out.append(chunk)
        prev = chunk

    return "\n\n".join(out).strip()

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
    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table)}"
    out: List[Dict[str, Any]] = []
    offset = None

    while True:
        params = {}
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=airtable_headers(token), params=params, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"Airtable list HTTP {r.status_code}: {r.text[:300]}")
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
    Envoi en petites requêtes (batch=1 par défaut) pour éviter 413.
    """
    if not updates:
        return
    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table)}"

    payload = {"records": updates, "typecast": False}
    r = requests.patch(url, headers=airtable_headers(token), json=payload, timeout=120)
    if r.status_code == 413:
        raise RuntimeError("Airtable update HTTP 413 (payload trop gros) -> réduire batch / tronquer textes")
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable update HTTP {r.status_code}: {r.text[:600]}")
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

    to_update: List[Dict[str, Any]] = []

    checked = 0
    empty_or_missing_url = 0
    fetched = 0
    parse_ok = 0
    parse_empty = 0
    failures = 0
    updated = 0

    rcp_done_this_run = 0

    # Playwright : un seul navigateur + une page réutilisée
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="fr-FR")
        page = context.new_page()

        for idx, rec in enumerate(records, start=1):
            fields = rec.get("fields", {}) or {}

            cis = safe_text(fields.get(FIELD_CIS))
            rcp_url = safe_text(fields.get(FIELD_RCP_URL))

            if not rcp_url:
                empty_or_missing_url += 1
                continue

            # skip si déjà rempli
            already_ind = safe_text(fields.get(FIELD_INDICATIONS_RCP))
            already_pos = safe_text(fields.get(FIELD_POSOLOGIE_RCP))
            already_int = safe_text(fields.get(FIELD_INTERACTIONS_RCP))
            if already_ind and already_pos and already_int:
                continue

            if rcp_done_this_run >= RCP_MAX_PER_RUN:
                break

            checked += 1
            rcp_done_this_run += 1

            try:
                # IMPORTANT: on garde EXACTEMENT le lien avec #tab-rcp
                html = pw_get_html(page, rcp_url)
                fetched += 1

                sections = extract_rcp_sections_from_html(html)

                if not (sections[FIELD_INDICATIONS_RCP] or sections[FIELD_POSOLOGIE_RCP] or sections[FIELD_INTERACTIONS_RCP]):
                    parse_empty += 1
                    warn(f"RCP vide après parse | CIS={cis or 'NA'} | url={rcp_url}")
                    continue

                parse_ok += 1

                patch: Dict[str, Any] = {}
                if not already_ind and sections[FIELD_INDICATIONS_RCP]:
                    patch[FIELD_INDICATIONS_RCP] = truncate_for_airtable(sections[FIELD_INDICATIONS_RCP])
                if not already_pos and sections[FIELD_POSOLOGIE_RCP]:
                    patch[FIELD_POSOLOGIE_RCP] = truncate_for_airtable(sections[FIELD_POSOLOGIE_RCP])
                if not already_int and sections[FIELD_INTERACTIONS_RCP]:
                    patch[FIELD_INTERACTIONS_RCP] = truncate_for_airtable(sections[FIELD_INTERACTIONS_RCP])

                if patch:
                    to_update.append({"id": rec["id"], "fields": patch})

                # flush régulier
                if len(to_update) >= UPDATE_FLUSH_THRESHOLD:
                    info(f"Flush updates: {len(to_update)} (batch_size={AIRTABLE_BATCH_SIZE})")
                    for i in range(0, len(to_update), AIRTABLE_BATCH_SIZE):
                        airtable_batch_update(token, base_id, table, to_update[i:i + AIRTABLE_BATCH_SIZE])
                    updated += len(to_update)
                    to_update = []
                    ok(f"Flush OK (total updated so far: {updated})")

            except Exception as e:
                failures += 1
                warn(f"RCP parse KO | CIS={cis or 'NA'} | url={rcp_url} | err={e} (on continue)")
                continue

            if checked % HEARTBEAT_EVERY == 0:
                info(
                    f"Heartbeat: checked={checked} fetched={fetched} parse_ok={parse_ok} parse_empty={parse_empty} "
                    f"failures={failures} updates_buffer={len(to_update)}"
                )

        # final flush
        if to_update:
            info(f"Final flush updates: {len(to_update)} (batch_size={AIRTABLE_BATCH_SIZE})")
            for i in range(0, len(to_update), AIRTABLE_BATCH_SIZE):
                airtable_batch_update(token, base_id, table, to_update[i:i + AIRTABLE_BATCH_SIZE])
            updated += len(to_update)
            ok(f"Final flush OK (total updated: {updated})")

        context.close()
        browser.close()

    ok(
        f"Done. rcp_checked={checked} fetched={fetched} parse_ok={parse_ok} parse_empty={parse_empty} "
        f"failures={failures} updated={updated} missing_url={empty_or_missing_url} "
        f"limit_per_run={RCP_MAX_PER_RUN}"
    )


if __name__ == "__main__":
    main()
