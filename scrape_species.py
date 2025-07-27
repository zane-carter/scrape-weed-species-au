"""
Weed Species Aggregator and Validator Script

This script scrapes invasive and environmental weed species from various Australian state, council,
and national sources (PDFs, HTML, CSV, Lucid Keys). It standardizes names using the Kew POWO
API (via `pykew`) and exports a validated, deduplicated list of accepted scientific names.

Output:
    - accepted_species.json: a sorted JSON array of accepted species names (1 per line)

Usage:
    python scrape_weed_species_data.py

Dependencies:
    - requests
    - beautifulsoup4
    - PyPDF2
    - rich
    - pykew
    - selenium (with chromedriver)

Author: Zane Carter
"""

import re
import csv
import json
import time
import difflib
import logging
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from pykew import powo
from pykew.powo_terms import Name
from rich.console import Console
from rich.logging import RichHandler

# --- Setup ---
DATA_DIR = Path("data_sources")
DATA_DIR.mkdir(exist_ok=True)

console = Console()
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    handlers=[RichHandler(console=console, show_path=False)]
)
logger = logging.getLogger("rich")


# === PDF Utilities ===
def download_pdf(state_code, url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/pdf",
            "Referer": "https://nt.gov.au/"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        path = DATA_DIR / f"{state_code}.pdf"
        with open(path, "wb") as f:
            f.write(response.content)
        return path
    except Exception as e:
        logger.warning(f"[{state_code}] PDF download failed: {e}")
        return None

def extract_text_from_pdf(path):
    reader = PdfReader(path)
    return "\n".join(page.extract_text() or "" for page in reader.pages)


# === POWO Name Validation ===
def validate_species_name(name, index=None, total=None):
    prefix = f"[{index}/{total}] " if index is not None and total is not None else ""
    try:
        parts = name.strip().split()
        if len(parts) < 2:
            logger.warning(f"{prefix}[pykew] Skipping malformed name: {name}")
            return None

        genus, species = parts[0], parts[1]
        results = powo.search({Name.genus: genus, Name.species: species})

        for result in results:
            if result.get("rank") != "Species":
                continue

            if result.get("accepted") is True:
                accepted_name = result.get("name")
                if accepted_name:
                    logger.info(f"{prefix}[pykew] '{name}' is accepted as '{accepted_name}'")
                    return accepted_name

            elif result.get("accepted") is False and "synonymOf" in result:
                accepted_entry = result["synonymOf"]
                accepted_name = accepted_entry.get("name")
                if accepted_name:
                    score = difflib.SequenceMatcher(None, name.lower(), accepted_name.lower()).ratio()
                    if score < 0.8:
                        logger.warning(f"{prefix}[pykew] Synonym mismatch: '{name}' → '{accepted_name}' (score {score:.2f})")
                    else:
                        logger.info(f"{prefix}[pykew] '{name}' is a synonym of '{accepted_name}'")
                    return accepted_name

        logger.warning(f"{prefix}[pykew] No accepted species match found for '{name}'")
        return None

    except Exception as e:
        logger.error(f"{prefix}[pykew] Error validating '{name}': {e}")
        return None


# === Scrapers: PDFs, HTML, CSV ===
def scrape_qld_html(category):
    url = f"https://www.business.qld.gov.au/industries/farms-fishing-forestry/agriculture/biosecurity/plants/invasive/{category}"
    logger.info(f"[QLD-{category}] Scraping: {url}")
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    species = []
    for card in soup.select("div.bq-qgds-card"):
        sci = card.select_one("p.scientific")
        if sci:
            name = sci.text.strip()
            if re.match(r"^[A-Z][a-z]+ [a-z\-]+(?: [a-z\-]+)?$", name):
                species.append(name)
    logger.info(f"[QLD-{category}] Extracted {len(species)} species")
    return species

def scrape_nsw_html():
    logger.info("[NSW] Scraping NSW weed list")
    url = "https://weeds.dpi.nsw.gov.au/WeedListPublics/Browse?SNOrder=False"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    species = []
    for span in soup.select("#contentbuffer span"):
        ital = span.find_all("i")
        if len(ital) >= 2:
            sci = f"{ital[0].text.strip()} {ital[1].text.strip()}"
            if re.match(r"^[A-Z][a-z]+ [a-z\-]+$", sci):
                species.append(sci)
    logger.info(f"[NSW] Extracted {len(species)} species")
    return species

def scrape_table_species(url, sci_col_keyword):
    logger.info(f"[TABLE] Scraping: {url}")
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    headers = [th.text.strip().lower() for th in table.find_all("th")]
    sci_idx = next((i for i, h in enumerate(headers) if sci_col_keyword in h), None)
    species = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if sci_idx is not None and len(cells) > sci_idx:
            name = cells[sci_idx].text.strip()
            if re.match(r"^[A-Z][a-z]+ [a-z\-]+", name):
                species.append(name)
    return species

def scrape_wa_csv():
    logger.info("[WA] Scraping WA CSV")
    path = DATA_DIR / "wa-s22.csv"
    species = []
    if path.exists():
        with open(path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()
        reader = csv.DictReader(lines[2:], fieldnames=lines[1].strip().split(","))
        for row in reader:
            name = row.get("Scientific name", "").strip()
            if name:
                species.append(name)
    logger.info(f"[WA] Extracted {len(species)} species")
    return species

def scrape_sa_pdf():
    logger.info("[SA] Scraping SA PDF")
    path = DATA_DIR / "SA.pdf"
    species = []
    if path.exists():
        text = extract_text_from_pdf(path)
        for line in text.splitlines():
            match = re.match(r"^([A-Z][a-z]+ [a-z\-]+(?: [a-z\-]+)?)\s", line.strip())
            if match:
                species.append(match.group(1))
    logger.info(f"[SA] Extracted {len(species)} species")
    return species

def scrape_vic_pdf():
    logger.info("[VIC] Scraping VIC PDF")
    path = DATA_DIR / "VIC.pdf"
    species = []
    if path.exists():
        text = extract_text_from_pdf(path)
        for line in text.splitlines():
            match = re.match(r"^([A-Z][a-z]+ [a-z\-]+(?: [a-z\-\.]+)?)", line.strip())
            if match:
                species.append(match.group(1))
    logger.info(f"[VIC] Extracted {len(species)} species")
    return species

def scrape_nt_pdf():
    logger.info("[NT] Scraping NT PDF")
    path = DATA_DIR / "declared-weeds-in-the-nt-2025.pdf"
    species = []
    if path.exists():
        text = extract_text_from_pdf(path)
        for line in text.splitlines():
            tokens = line.split()
            if len(tokens) >= 2 and tokens[0][0].isupper():
                name = f"{tokens[0]} {tokens[1]}"
                if re.match(r"^[A-Z][a-z]+ [a-z\-]+", name):
                    species.append(name)
    logger.info(f"[NT] Extracted {len(species)} species")
    return species

def scrape_bcc_csv():
    logger.info("[BCC] Scraping Brisbane Council CSV")
    path = DATA_DIR / "bcc_weedlist.csv"
    species = []
    if path.exists():
        with open(path, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                name = row.get("botanicalName", "").strip()
                if re.match(r"^[A-Z][a-z]+ [a-z\-]+(?: [a-z\-]+)?$", name):
                    species.append(name)
    logger.info(f"[BCC] Extracted {len(species)} species")
    return species

def scrape_csiro_weedscan():
    logger.info("[CSIRO] Scraping WeedScan")
    url = "https://weedscan.org.au/Weeds?handler=QueryPartial"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    species = []
    for td in soup.find_all("td"):
        a = td.find("a")
        if a:
            m = re.search(r"\\*([A-Z][a-z]+ [a-z\-]+)\\*", a.get("title", ""))
            if m:
                species.append(m.group(1))
    logger.info(f"[CSIRO] Extracted {len(species)} species")
    return species

def scrape_wons_wikipedia():
    logger.info("[WONS] Scraping Wikipedia")
    url = "https://en.wikipedia.org/wiki/Weeds_of_National_Significance"
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    species = []
    table = soup.find("table", class_="wikitable")
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) >= 2:
            sci = cols[1].text.strip()
            if re.match(r"^[A-Z][a-z]+ [a-z\-]+", sci):
                species.append(sci)
    logger.info(f"[WONS] Extracted {len(species)} species")
    return species

def scrape_lucid_key_entities(url):
    logger.info(f"[LUCID] Scraping Lucid Key: {url}")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=opts)
    driver.get(url)
    time.sleep(5)
    labels = driver.find_elements(By.CLASS_NAME, "Label")
    species = []
    for label in labels:
        match = re.match(r"^([A-Z][a-z]+ [a-z\-]+(?: [a-z\-]+)?)", label.text.strip())
        if match:
            species.append(match.group(1))
    driver.quit()
    logger.info(f"[LUCID] Extracted {len(species)} species")
    return species

def scrape_lucid_key():
    species = scrape_lucid_key_entities("https://keyserver.lucidcentral.org/weeds/player.jsp?keyId=1&featuresChosen=false&entitiesDiscarded=false&gallery=true&viewer=simpleviewer&thumbnails=true")
    species += scrape_lucid_key_entities("https://keyserver.lucidcentral.org/key-server/player.jsp?keyId=39&thumbnails=true&gallery=true")
    logger.info(f"[LUCID] Total extracted: {len(species)}")
    return species


# === Main Execution ===
if __name__ == "__main__":
    logger.info("Loading data sources...")
    all_species = []
    all_species += scrape_qld_html("prohibited")
    all_species += scrape_qld_html("restricted")
    all_species += scrape_nsw_html()
    all_species += scrape_nt_pdf()
    all_species += scrape_vic_pdf()
    all_species += scrape_sa_pdf()
    all_species += scrape_wa_csv()
    all_species += scrape_table_species("https://nre.tas.gov.au/invasive-species/weeds/weeds-index/declared-weeds-index", "scientific")
    all_species += scrape_csiro_weedscan()
    all_species += scrape_wons_wikipedia()
    all_species += scrape_bcc_csv()
    all_species += scrape_lucid_key()

    logger.info(f"Total scraped species (raw): {len(all_species)}")
    raw_names = sorted(set(all_species))
    logger.info(f"Unique species to validate: {len(raw_names)}")

    accepted_species = set()
    for i, raw_name in enumerate(raw_names, start=1):
        validated = validate_species_name(raw_name, index=i, total=len(raw_names))
        if validated:
            accepted_species.add(validated)

    accepted_species = sorted(accepted_species)
    with open("accepted_species.json", "w") as f:
        json.dump(accepted_species, f, indent=2)

    logger.info(f"\n✅ SAVED {len(accepted_species)} ACCEPTED SPECIES NAMES → accepted_species.json")
