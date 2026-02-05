#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import random
import unicodedata
from typing import Dict, Any, List, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

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

# Airtable fields
FIELD_CIS = "Code cis"
FIELD_RCP_URL = "Lien vers RCP"

FIELD_INDICATIONS_RCP = "Indications RCP"      # 4.1
FIELD_POSOLOGIE_RCP = "Posologie RCP"          # 4.2
FIELD_INTERACTIONS_RCP = "Interactions RCP"    # 4.4 + 4.5 concat

# Airtable limits / safety
AIRTABLE_CELL_SOFT_LIMIT = int(os.getenv("AIRTABLE_CELL_SOFT_LIMIT", "95000"))  # < 100k
AIRTABLE_JSON_SOFT_LIMIT = int(os.getenv("AIRTABLE_JSON_SOFT_LIMIT", str(900_000)))  # ~1MB

# Playwright tuning
PW_NAV_TIMEOUT_MS = int(os.getenv("PW_NAV_TIMEOUT_MS", "45000"))
PW_WAIT_TIMEOUT_MS = int(os.getenv("PW_WAIT_TIMEOUT_MS", "45000"))
PW_HEADLESS = os.getenv("PW_HEADLESS", "1").strip() != "0"

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

def clip_airtable_cell(s: str, limit: int = AIRTABLE_CELL_SOFT_LIMIT) -> str:
    s = safe_text(s)
    if len(s) <= limit:
        return s
    return s[:limit - 40].rstrip() + "\n\n[...TRONQUÉ - limite Airtable...]"

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
    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table)}"
    out: List[Dict[str, Any]] = []
    offset = None

    while True:
        params = {}
        if offset:
            params["offset"] = offset
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

def _payload_size_bytes(obj: Any) -> int:
    return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))

def airtable_batch_update(token: str, base_id: str, table: str, updates: List[Dict[str, Any]]):
    """
    updates: [{ "id": "...", "fields": {...} }, ...]
    """
    if not updates:
        return

    url = f"{AIRTABLE_API_BASE}/{base_id}/{requests.utils.quote(table)}"
    payload = {"records": updates, "typecast": False}

    # Safety: si payload trop gros -> envoyer 1 par 1
    if _payload_size_bytes(payload) > AIRTABLE_JSON_SOFT_LIMIT and len(updates) > 1:
        for u in updates:
            airtable_batch_update(token, base_id, table, [u])
        return

    r = requests.patch(url, headers=airtable_headers(token), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable update HTTP {r.status_code}: {r.text[:400]}")
    sleep_throttle()

# ============================================================
# PLAYWRIGHT EXTRACTION
# ============================================================

def _extract_sections_js() -> str:
    """
    Renvoie une fonction JS (string) qui extrait le texte sous un heading,
    jusqu'au prochain heading 4.x / 5.x (on coupe proprement).
    """
    return r"""
    (phrase) => {
      const norm = (s) => (s || "")
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
        .toLowerCase().replace(/\s+/g, " ").trim();

      const target = norm(phrase);

      // Cherche un heading h1..h6 dont le texte contient la phrase
      const headings = Array.from(document.querySelectorAll("h1,h2,h3,h4,h5,h6"));
      let h = headings.find(x => norm(x.textContent).includes(target));
      if (!h) return "";

      // Conteneur principal: on remonte un peu (souvent section/article/div)
      let root = h.closest("article,section,div") || document.body;

      const isStopHeading = (el) => {
        if (!el || !el.matches) return false;
        if (!el.matches("h1,h2,h3,h4,h5,h6")) return false;
        const t = norm(el.textContent);
        // Stop sur 4.x ou 5.x (rubriques structurantes)
        return /^4\.\d+/.test(t) || /^5\./.test(t);
      };

      // Parcours DOM "suivant" après le heading
      const out = [];
      let cur = h;

      const nextNode = (node) => {
        // depth-first next
        if (node.firstElementChild) return node.firstElementChild;
        while (node) {
          if (node.nextElementSibling) return node.nextElementSibling;
          node = node.parentElement;
        }
        return null;
      };

      // On commence juste après h
      cur = nextNode(cur);

      const pushText = (el) => {
        if (!el) return;
        // On ignore menu/sommaire
        const cls = (el.getAttribute && el.getAttribute("class")) ? el.getAttribute("class") : "";
        if (typeof cls === "string" && cls.toLowerCase().includes("sommaire")) return;

        // On prend les blocs utiles
        if (el.matches("p,li,ul,ol,table,tbody,tr,td,div,span")) {
          const t = (el.innerText || el.textContent || "").trim();
          if (t) out.push(t);
        }
      };

      // Collecte jusqu'au prochain heading stop
      let guard = 0;
      while (cur && guard < 5000) {
        guard++;

        if (isStopHeading(cur)) break;

        pushText(cur);
        cur = nextNode(cur);
      }

      // Nettoyage: supprimer doublons consécutifs
      const cleaned = [];
      let prev = "";
      for (const x of out) {
        if (x === prev) continue;
        cleaned.push(x);
        prev = x;
      }

      return cleaned.join("\n\n").trim();
    }
    """

def fetch_and_extract_rcp_with_playwright(url_with_hash: str) -> Dict[str, str]:
    """
    IMPORTANT: on utilise exactement l'URL fournie (avec #tab-rcp).
    """
    url_with_hash = safe_text(url_with_hash)
    if not url_with_hash:
        return {FIELD_INDICATIONS_RCP: "", FIELD_POSOLOGIE_RCP: "", FIELD_INTERACTIONS_RCP: ""}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PW_HEADLESS)
        context = browser.new_context(locale="fr-FR")
        page = context.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)
        page.set_default_timeout(PW_WAIT_TIMEOUT_MS)

        # goto EXACT URL (avec #)
        page.goto(url_with_hash, wait_until="domcontentloaded")

        # On attend un élément typique du RCP
        # (souvent "4. DONNEES CLINIQUES" ou "Résumé des caractéristiques du produit")
        try:
            page.wait_for_selector("text=Résumé des caractéristiques du produit", timeout=PW_WAIT_TIMEOUT_MS)
        except PWTimeoutError:
            # parfois ça charge quand même; on continue

            pass

        # S'il faut cliquer l'onglet RCP (au cas où)
        # (pas toujours nécessaire, mais ça sauve des cas)
        try:
            tab = page.locator("role=tab[name*='Résumé des caractéristiques du produit']").first
            if tab.count() > 0:
                tab.click(timeout=2000)
        except Exception:
            pass

        # Attendre le début des données cliniques
        try:
            page.wait_for_selector("text=4. DONNÉES CLINIQUES", timeout=PW_WAIT_TIMEOUT_MS)
        except PWTimeoutError:
            # variantes sans accent
            try:
                page.wait_for_selector("text=4. DONNEES CLINIQUES", timeout=PW_WAIT_TIMEOUT_MS)
            except PWTimeoutError:
                # on tente quand même l'extraction
                pass

        extractor = _extract_sections_js()

        def get_section(phrase: str) -> str:
            try:
                txt = page.evaluate(extractor, phrase)
                return safe_text(txt)
            except Exception:
                return ""

        indications = get_section("4.1. Indications thérapeutiques")
        posologie = get_section("4.2. Posologie et mode d'administration")

        # 4.4 + 4.5 concat dans Interactions RCP (comme tu veux)
        prec = get_section("4.4. Mises en garde spéciales et précautions d'emploi")
        inter = get_section("4.5. Interactions avec d'autres médicaments et autres formes d'interactions")

        interactions = ""
        if prec:
            interactions += "4.4. Mises en garde spéciales et précautions d'emploi\n" + prec.strip()
        if inter:
            if interactions:
                interactions += "\n\n"
            interactions += "4.5. Interactions avec d'autres médicaments et autres formes d'interactions\n" + inter.strip()

        context.close()
        browser.close()

        return {
            FIELD_INDICATIONS_RCP: indications,
            FIELD_POSOLOGIE_RCP: posologie,
            FIELD_INTERACTIONS_RCP: interactions,
        }

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
    updated = 0
    checked = 0
    missing_url = 0
    fetched = 0
    parse_ok = 0
    parse_empty = 0
    failures = 0

    rcp_done_this_run = 0

    for idx, rec in enumerate(records, start=1):
        fields = rec.get("fields", {}) or {}

        cis = safe_text(fields.get(FIELD_CIS))
        rcp_url = safe_text(fields.get(FIELD_RCP_URL))

        if not rcp_url:
            missing_url += 1
            continue

        already_ind = safe_text(fields.get(FIELD_INDICATIONS_RCP))
        already_pos = safe_text(fields.get(FIELD_POSOLOGIE_RCP))
        already_int = safe_text(fields.get(FIELD_INTERACTIONS_RCP))

        # Si tout est déjà rempli -> skip
        if already_ind and already_pos and already_int:
            continue

        if rcp_done_this_run >= RCP_MAX_PER_RUN:
            break

        checked += 1
        rcp_done_this_run += 1

        try:
            sections = fetch_and_extract_rcp_with_playwright(rcp_url)  # URL EXACTE avec #

            fetched += 1

            ind = safe_text(sections.get(FIELD_INDICATIONS_RCP))
            pos = safe_text(sections.get(FIELD_POSOLOGIE_RCP))
            inter = safe_text(sections.get(FIELD_INTERACTIONS_RCP))

            if not (ind or pos or inter):
                parse_empty += 1
                warn(f"RCP vide après extraction | CIS={cis or 'NA'} | url={rcp_url}")
                continue

            parse_ok += 1

            patch: Dict[str, Any] = {}

            if not already_ind and ind:
                patch[FIELD_INDICATIONS_RCP] = clip_airtable_cell(ind)
            if not already_pos and pos:
                patch[FIELD_POSOLOGIE_RCP] = clip_airtable_cell(pos)
            if not already_int and inter:
                patch[FIELD_INTERACTIONS_RCP] = clip_airtable_cell(inter)

            if patch:
                to_update.append({"id": rec["id"], "fields": patch})

            # Flush
            if len(to_update) >= UPDATE_FLUSH_THRESHOLD:
                info(f"Flush updates: {len(to_update)}")

                # Batch: on essaye par 10 mais si trop gros -> fonction enverra 1 par 1
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
                f"Heartbeat: checked={checked} fetched={fetched} parse_ok={parse_ok} "
                f"parse_empty={parse_empty} failures={failures} updates_buffer={len(to_update)}"
            )

    # Final flush
    if to_update:
        info(f"Final flush updates: {len(to_update)}")
        for i in range(0, len(to_update), AIRTABLE_BATCH_SIZE):
            airtable_batch_update(token, base_id, table, to_update[i:i + AIRTABLE_BATCH_SIZE])
        updated += len(to_update)
        ok(f"Final flush OK (total updated: {updated})")

    ok(
        f"Done. rcp_checked={checked} fetched={fetched} parse_ok={parse_ok} "
        f"parse_empty={parse_empty} failures={failures} updated={updated} "
        f"missing_url={missing_url} limit_per_run={RCP_MAX_PER_RUN}"
    )

if __name__ == "__main__":
    main()
