# Import CIS_bdpm.txt -> Airtable

## Champs Airtable attendus
- Code cis
- Spécialité
- Forme
- Voie d'administration
- Laboratoire

## Variables d'environnement
- AIRTABLE_API_TOKEN
- AIRTABLE_BASE_ID
- AIRTABLE_TABLE_NAME
- (optionnel) INPUT_FILE (par défaut: CIS_bdpm.txt)

## Installation
```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt

## OCR (fallback pour remplir le Code ATC depuis des PDF scannés)

- Active l’OCR via la variable d’environnement `OCR_ENABLE=1`.
- Réglages :
  - `OCR_MAX_PAGES` (défaut: 2) : nombre max de pages à OCR (0 = toutes)
  - `OCR_DPI` (défaut: 200) : résolution de rendu des pages
  - `OCR_PSM` (défaut: 6) : mode tesseract (segmentation)

Le workflow GitHub installe `tesseract-ocr` et `poppler-utils`.
Les ATC trouvés via OCR uniquement sont sauvegardés dans `reports/atc_from_pdf_ocr_YYYY-MM-DD.tsv` (avec le nom de la spécialité).

### OCR batch sur un dossier de PDFs
```bash
python scripts/ocr_extract_atc_from_pdfs.py --pdf-dir ./pdfs --out reports/atc_ocr_backup.tsv
