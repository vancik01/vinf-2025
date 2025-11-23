# VINF Project - Real Estate & Wikipedia Integration

Integration of US real estate data with Wikipedia information for enhanced property search.

---

## Project Structure

```
vinf_project/
├── 01_crawler.py              # Web scraper for real estate data
├── 02_01_tiktoken.py          # Token counting utility
├── 02_parser.py               # HTML parsing
├── 03_indexer.py              # Lucene indexer with Wikipedia integration
├── 04_search_engine.py        # Search engine interface
├── requirements.txt           # Python dependencies
│
├── data/                      # ⚠️ NOT IN GIT - Scraped data
│   └── scraper_data/
│       └── pages/             # HTML pages from remax.com
│
├── wiki/                      # ⚠️ NOT IN GIT - Wikipedia dumps
│   ├── enwiki-*.xml.bz2       # Wikipedia XML dump (~22 GB)
│   └── enwiki-*.txt.bz2       # Wikipedia index (~227 MB)
│
├── 05_spark/                  # Spark pipeline for Wikipedia integration
│   ├── main.py                # Pipeline entry point
│   ├── wiki_pipeline.py       # Stages 2-8: Wikipedia processing
│   ├── extract_html_properties.py  # Stage 1: HTML extraction
│   │
│   ├── docs/                  # Documentation
│   │   ├── DOKUMENTACIA.md    # Slovak documentation
│   │   ├── DOKUMENTACIA_WIKI.txt  # DokuWiki format
│   │   ├── README.md          # Documentation overview
│   │   ├── VALIDATION_REPORT.md   # Statistics validation
│   │   ├── pipeline-flow.png       # Diagram
│   │   ├── matching-logic.png      # Diagram
│   │   └── joining-architecture.png # Diagram
│   │
│   ├── spark_data/            # ⚠️ NOT IN GIT - Spark outputs
│   │   ├── parsed/            # Stage 1: Parsed properties (15,386)
│   │   ├── wiki_extracted/    # Stage 5: Wikipedia pages (754)
│   │   ├── property_wiki_metadata.jsonl  # Final output (15,295 matches)
│   │   ├── property_wiki_mapping.parquet # Parquet format
│   │   └── wiki_pipeline_summary.json    # Pipeline statistics
│   │
│   └── logs/                  # ⚠️ NOT IN GIT - Pipeline logs
│
├── venv/                      # ⚠️ NOT IN GIT - Python virtual environment
│
├── .gitignore                 # Git ignore rules
└── README.md                  # This file
```

---

## Folder Descriptions

### Code Files (IN GIT)

| File | Description |
|------|-------------|
| `01_crawler.py` | Scrapes real estate listings from remax.com |
| `02_parser.py` | Parses HTML and extracts property data |
| `03_indexer.py` | Creates Lucene index with Wikipedia enrichment |
| `04_search_engine.py` | Search interface with demographic filtering |
| `05_spark/` | Spark pipeline for Wikipedia integration |

### Data Folders (NOT IN GIT - too large)

#### `data/` - Scraped Real Estate Data
- **Contents:** HTML pages from remax.com
- **Size:** ~1-2 GB
- **Created by:** `01_crawler.py`
- **Structure:**
  ```
  data/scraper_data/pages/
  ├── abc123def456.html
  ├── 789ghi012jkl.html
  └── ...
  ```

#### `wiki/` - Wikipedia Dumps
- **Contents:** English Wikipedia dumps (October 2024)
- **Size:** ~22 GB (compressed)
- **Download from:** https://dumps.wikimedia.org/enwiki/
- **Files needed:**
  - `enwiki-20241020-pages-articles-multistream.xml.bz2` (21.8 GB)
  - `enwiki-20241020-pages-articles-multistream-index.txt.bz2` (227 MB)

#### `05_spark/spark_data/` - Pipeline Outputs
- **Contents:** Spark processing results
- **Size:** ~3-5 GB
- **Created by:** `05_spark/main.py`
- **Structure:**
  ```
  spark_data/
  ├── parsed/                  # 15,386 properties in Parquet
  ├── wiki_extracted/          # 754 Wikipedia pages
  │   ├── TX/
  │   │   ├── Dallas,_Texas.xml
  │   │   ├── Dallas,_Texas.json
  │   │   └── ...
  │   ├── CA/
  │   └── ...
  ├── property_wiki_metadata.jsonl  # 15,295 matches (main output)
  └── property_wiki_mapping.parquet # Parquet version
  ```

#### `venv/` - Python Virtual Environment
- **Contents:** Python packages and dependencies
- **Created by:** `python -m venv venv`

---

## Setup Instructions

### 1. Create Virtual Environment

```bash
cd vinf_project
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Download Wikipedia Dumps

```bash
mkdir -p wiki
cd wiki

# Download Wikipedia dump (21.8 GB)
wget https://dumps.wikimedia.org/enwiki/20241020/enwiki-20241020-pages-articles-multistream.xml.bz2

# Download Wikipedia index (227 MB)
wget https://dumps.wikimedia.org/enwiki/20241020/enwiki-20241020-pages-articles-multistream-index.txt.bz2

cd ..
```

### 3. Run Scraper (Optional - or use existing data)

```bash
python 01_crawler.py
# Creates: data/scraper_data/pages/*.html
```

### 4. Run Spark Pipeline

```bash
cd 05_spark

# Full pipeline (extraction + Wikipedia integration)
python main.py full --pages-dir ../data/scraper_data/pages

# Or run in stages:
# Stage 1: Extract properties from HTML
python main.py extraction --pages-dir ../data/scraper_data/pages

# Stages 2-8: Wikipedia integration
python main.py wiki --parsed-data spark_data/parsed/parsed.parquet
```

### 5. Index and Search

```bash
cd ..

# Create Lucene index with Wikipedia enrichment
python 03_indexer.py

# Run search engine
python 04_search_engine.py
```

---

## Pipeline Overview

**Wikipedia Integration Pipeline (8 Stages):**

1. **HTML Parsing** → Extract 15,386 properties
2. **Location Extraction** → 748 cities + 774 school districts
3. **Wikipedia Index Scan** → Find 3,001 pages
4. **Deduplication** → Keep 1,047 unique pages
5. **Content Extraction** → Extract 755 pages (754 unique)
6. **Fuzzy Matching** → Create 15,295 matches (90.8% coverage)
7. **JSONL Export** → `property_wiki_metadata.jsonl`
8. **Statistics** → Generate reports

**Final Output:**
- 13,975 properties matched to Wikipedia (90.8%)
- 754 unique Wikipedia pages used
- 96.5% high confidence matches

---

## Search Features

With Wikipedia integration, you can search using:

**Standard queries:**
- `price:<500000` - properties under $500k
- `beds:3` - 3 bedroom properties
- `city:Dallas` - properties in Dallas

**Wikipedia-enriched queries:**
- `population:>1000000` - cities with >1M people
- `students:<50000` - school districts with <50k students
- `area_sq_mi:>300` - large cities
- `schools:>50` - districts with >50 schools

**Combined:**
- `price:<500000 AND population:>500000` - affordable homes in big cities

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Total properties scraped | 15,386 |
| Properties matched to Wikipedia | 13,975 (90.8%) |
| Unique Wikipedia pages used | 754 |
| Total match records | 15,295 |
| City matches | 13,795 (90.2%) |
| School district matches | 1,500 (9.8%) |
| High confidence matches | 14,767 (96.5%) |

---

## Documentation

Detailed documentation in `05_spark/docs/`:
- **DOKUMENTACIA.md** - Complete pipeline documentation (Slovak)
- **VALIDATION_REPORT.md** - Statistics validation
- **Diagrams** - Pipeline flow, matching logic, architecture

---

## .gitignore

The following folders are **NOT** pushed to git (too large):
- `data/` - Scraped HTML pages
- `wiki/` - Wikipedia dumps
- `spark_data/` - Spark outputs
- `logs/` - Log files
- `venv/` - Virtual environment

Only **code**, **configuration**, and **documentation** are version controlled.

---

## Requirements

- Python 3.8+
- Apache Spark 3.x
- ~25 GB disk space (for Wikipedia dumps + processed data)
- ~8 GB RAM minimum (recommended: 16 GB)

---

## Author

Martin Vančo
FIIT STU, 4th year
Course: VINF (Information Retrieval)
