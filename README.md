# Scrape Weed Species (Australia)

Scrapes declared weed species from official Australian government sources, including PDFs, CSVs, and HTML weed lists from state departments and local councils. Consolidates all data into a clean, deduplicated format suitable for downstream processing or AI training.

## Features

- 🗺️ Covers multiple Australian jurisdictions (NSW, VIC, QLD, etc.)
- 🧼 Normalizes and deduplicates botanical names
- 🧾 Parses PDFs, HTML pages, and CSV files
- 📤 Outputs structured JSON with traceable source metadata
- 🧠 Supports integration with WeedScout or other ag-tech tools

## Requirements

- Python 3.8+
- `requests`
- `pandas`
- `pdfplumber`
- `beautifulsoup4`
- `rich`

Install dependencies:

```bash
pip install -r requirements.txt
