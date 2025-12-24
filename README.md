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
