#!/usr/bin/env python3
"""
HTML Property Extraction - Stage 1

Extracts property data from HTML files using regex and MarkItDown
"""

import re
import logging
from pathlib import Path
from typing import Dict, Optional
from markitdown import MarkItDown


# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================

def extract_property_title(content: str) -> Optional[str]:
    pattern = r'^#\s+(.+)$'
    match = re.search(pattern, content, re.MULTILINE)
    return match.group(1).strip() if match else None


def extract_price(content: str) -> Optional[str]:
    pattern = r'^(?!.*(?:sq\s*ft|acres?)).*(\$[\d,]+(?:\.\d{2})?).*$'
    for line in content.split('\n'):
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_beds(content: str) -> Optional[int]:
    patterns = [r'(\d+)\s+Beds?', r'Bedrooms?\s+Total\s*\n\s*(\d+)']
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def extract_baths(content: str) -> Optional[int]:
    patterns = [
        r'(\d+)\s+Baths?(?!\s+(?:Full|Half|Three Quarter|Total))',
        r'Bathrooms?\s+Total\s*\n\s*(\d+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def extract_sqft(content: str) -> Optional[int]:
    patterns = [r'([\d,]+)\s+Sq\s+Ft(?!\s*/)', r'Living\s+Area\s*\n\s*([\d,]+)\s*SqFt']
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    return None


def extract_status(content: str) -> Optional[str]:
    pattern = r'Status\s+(\w+)'
    match = re.search(pattern, content, re.IGNORECASE)
    return match.group(1) if match else None


def extract_description(content: str) -> Optional[str]:
    patterns = [
        r'MLS#?\s*[A-Z0-9]+\s*\n\n(.+?)\n\nRead More',
        r'MLS#?\s*[A-Z0-9]+\s*\n\n(.+?)(?=\n\n(?:Read More|## Details))'
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def extract_year_built(content: str) -> Optional[int]:
    patterns = [r'Built in (\d{4})', r'Year Built\s*\n\s*(\d{4})']
    for pattern in patterns:
        match = re.search(pattern, content)
        if match:
            year = int(match.group(1))
            if 0 < year <= 2025:
                return year
    return None


def extract_price_per_sqft(content: str) -> Optional[str]:
    pattern = r'\$(\d+(?:,\d{3})*)\s*/\s*Sq\s*Ft'
    match = re.search(pattern, content, re.IGNORECASE)
    return f"${match.group(1)}" if match else None


def extract_lot_size(content: str) -> Optional[float]:
    patterns = [
        r'([\d.]+)\s+acres?\s+lot',
        r'Lot\s+Size\s+Acres\s*\n\s*([\d.]+)',
        r'^([\d.]+)\s+Acres$'
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE | re.MULTILINE)
        if match:
            try:
                size = float(match.group(1))
                if 0 < size < 10000:
                    return size
            except:
                continue
    return None


def extract_city(content: str) -> Optional[str]:
    pattern = r'^City\s*\n\s*(.+)$'
    match = re.search(pattern, content, re.MULTILINE)
    return match.group(1).strip() if match else None


def extract_county(content: str) -> Optional[str]:
    pattern = r'County\s+(?:Or\s+)?Parish\s*\n\s*(.+)$'
    match = re.search(pattern, content, re.MULTILINE | re.IGNORECASE)
    return match.group(1).strip() if match else None


def extract_school_district(content: str) -> Optional[str]:
    patterns = [
        r'School District\s*\n\s*(.+)$',
        r'School\s+District\s*\n\s*(.+?)(?=\n\n|\n[A-Z])'
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.MULTILINE)
        if match:
            district = match.group(1).strip()
            if district and district.lower() not in ['call school board', 'none', 'n/a']:
                return district
    return None


def extract_property_type(content: str) -> Optional[str]:
    pattern = r'Property Type\s*\n\s*(.+)$'
    match = re.search(pattern, content, re.MULTILINE)
    return match.group(1).strip() if match else None


def extract_property_sub_type(content: str) -> Optional[str]:
    pattern = r'Property Sub Type\s*\n\s*(.+)$'
    match = re.search(pattern, content, re.MULTILINE)
    return match.group(1).strip() if match else None


def html_to_markdown(html_file_path: str) -> str:
    """Convert HTML to markdown using MarkItDown"""
    md = MarkItDown()
    result = md.convert(html_file_path)
    return result.text_content


def is_listing_page(markdown_content: str) -> bool:
    """Check if page is a listing page (not a property detail page)"""
    return bool(re.search(r'##\s+[\d,]+\s+results', markdown_content, re.IGNORECASE))


def process_html_file(html_file_path: str) -> Optional[Dict]:
    """Process a single HTML file and extract property data"""
    try:
        if not Path(html_file_path).exists():
            return None

        # Convert HTML to markdown
        markdown_content = html_to_markdown(html_file_path)

        # Skip listing pages
        if is_listing_page(markdown_content):
            return None

        # Extract file ID from filename
        file_id = Path(html_file_path).stem

        # Extract all property data
        property_data = {
            'file_hash': file_id,
            'source_file': str(html_file_path),
            'title': extract_property_title(markdown_content),
            'price': extract_price(markdown_content),
            'beds': extract_beds(markdown_content),
            'baths': extract_baths(markdown_content),
            'sqft': extract_sqft(markdown_content),
            'status': extract_status(markdown_content),
            'description': extract_description(markdown_content),
            'year_built': extract_year_built(markdown_content),
            'price_per_sqft': extract_price_per_sqft(markdown_content),
            'lot_size_acres': extract_lot_size(markdown_content),
            'property_type': extract_property_type(markdown_content),
            'property_sub_type': extract_property_sub_type(markdown_content),
            'city': extract_city(markdown_content),
            'county': extract_county(markdown_content),
            'school_district': extract_school_district(markdown_content),
        }

        return property_data

    except Exception as e:
        return None


# ============================================================================
# MAIN EXTRACTION FUNCTION (Called from PySpark)
# ============================================================================

def extract_properties_from_html(
    spark,
    pages_dir: str,
    output_dir: str,
    num_partitions: int = 8,
    logger=None
):
    """
    Extract property data from HTML files using PySpark

    Returns: DataFrame with property information
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, FloatType
    )

    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("STAGE 1: Parsing HTML Properties")
    logger.info("=" * 80)

    # Get all HTML files
    pages_path = Path(pages_dir)
    if not pages_path.exists():
        raise FileNotFoundError(f"Directory not found: {pages_dir}")

    html_files = list(pages_path.glob("*.html"))
    html_file_paths = [str(f) for f in html_files]

    if not html_file_paths:
        raise ValueError("No HTML files found")

    logger.info(f"Found {len(html_file_paths)} HTML files to process")

    # Create RDD and process in parallel
    file_rdd = spark.sparkContext.parallelize(html_file_paths, num_partitions)
    results_rdd = file_rdd.map(process_html_file).filter(lambda x: x is not None)
    results = results_rdd.collect()

    logger.info(f"Successfully processed {len(results)} properties")

    # Define schema
    schema = StructType([
        StructField("file_hash", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("beds", IntegerType(), True),
        StructField("baths", IntegerType(), True),
        StructField("sqft", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
        StructField("year_built", IntegerType(), True),
        StructField("price_per_sqft", StringType(), True),
        StructField("lot_size_acres", FloatType(), True),
        StructField("property_type", StringType(), True),
        StructField("property_sub_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("county", StringType(), True),
        StructField("school_district", StringType(), True),
    ])

    # Create DataFrame
    df = spark.createDataFrame(results, schema=schema)

    # Save outputs
    parsed_dir = Path(output_dir) / "parsed"
    parsed_dir.mkdir(parents=True, exist_ok=True)

    df.write.mode("overwrite").parquet(str(parsed_dir / "parsed.parquet"))
    df.coalesce(1).write.mode("overwrite").json(str(parsed_dir / "parsed.jsonl"))

    logger.info(f"Saved parsed data to: {parsed_dir}")
    logger.info(f"Stage 1 complete: {len(results)} properties extracted")

    return df
