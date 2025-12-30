#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch OCR: extrait les codes ATC depuis une arborescence de PDF.

Usage:
  python scripts/ocr_extract_atc_from_pdfs.py --pdf-dir ./pdfs --out reports/atc_ocr_backup.tsv --max-pages 2 --dpi 200

Sortie (TSV):
  pdf_path | med_name | atc | method
"""

import os
import argparse
from typing import List

# On réutilise les fonctions du script principal (évite les doublons)
from import_and_enrich_bdpm_to_airtable import (  # type: ignore
    extract_atc_from_pdf_bytes_with_method,
    canonical_atc7,
)

def iter_pdfs(pdf_dir: str) -> List[str]:
    out: List[str] = []
    for root, _dirs, files in os.walk(pdf_dir):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                out.append(os.path.join(root, fn))
    return sorted(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf-dir", required=True, help="Dossier racine contenant des PDFs")
    ap.add_argument("--out", default="reports/atc_ocr_backup.tsv", help="Chemin du TSV de sortie")
    ap.add_argument("--max-pages-text", type=int, default=30, help="Pages max pour extraction texte")
    args = ap.parse_args()

    pdfs = iter_pdfs(args.pdf_dir)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("pdf_path\tmed_name\tatc\tmethod\n")
        for p in pdfs:
            try:
                with open(p, "rb") as pf:
                    b = pf.read()
                atc, method = extract_atc_from_pdf_bytes_with_method(b, max_pages_text=args.max_pages_text)
                # med_name: nom de fichier par défaut
                med = os.path.splitext(os.path.basename(p))[0]
                atc = (atc or "").strip()
                if canonical_atc7(atc):
                    f.write(f"{p}\t{med}\t{atc}\t{method}\n")
            except Exception:
                continue

    print(f"OK -> {args.out} ({len(pdfs)} PDF scannés)")

if __name__ == "__main__":
    main()
