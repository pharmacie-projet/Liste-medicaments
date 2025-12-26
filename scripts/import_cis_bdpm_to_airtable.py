name: run-import

on:
  schedule:
    - cron: "0 1 5 * *"   # 01:00 UTC le 5 du mois
  workflow_dispatch:

jobs:
  run-import:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run import (download + reset + enrich)
        env:
          AIRTABLE_API_TOKEN: ${{ secrets.AIRTABLE_API_TOKEN }}
          AIRTABLE_BASE_ID: ${{ secrets.AIRTABLE_BASE_ID }}
          AIRTABLE_CIS_TABLE_NAME: ${{ secrets.AIRTABLE_CIS_TABLE_NAME }}
        run: |
          python scripts/import_cis_bdpm_to_airtable.py
