# Processing of Mediavida forums (rehydration)

This repository provides a **rehydration** script that converts the **dehydrated (IDs-only)** Mediavida dialogue files into **local text** by scraping Mediavida thread pages at runtime.

**Important:** the rehydrated output contains user-generated content retrieved from Mediavida. **Do not redistribute** the rehydrated files.

## Usage

```bash
python rehydrate_mediavida.py \
  --input dehydrated_mediavida.json \
  --output rehydrated_mediavida_text.json \
  --user-agent "esCorpiusDialog-rehydrator/1.0 (contact: <email>)" \
  --sleep 1.0 \
  --timeout 30 \
  --max-pages 2000

