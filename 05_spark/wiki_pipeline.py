#!/usr/bin/env python3
"""
Wikipedia Integration Pipeline - Stages 2-8

Improved version with:
- Redirect following
- Disambiguation page parsing
- Content-based verification
- Enhanced deduplication
"""

import re
import bz2
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from collections import defaultdict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, LongType
)
from pyspark.sql.functions import (
    col, lower, trim, concat, lit, regexp_extract, count,
    countDistinct, collect_list, collect_set, struct, when,
    length, current_timestamp, avg, row_number, desc, asc
)
from pyspark.sql.window import Window


# ============================================================================
# CONFIGURATION
# ============================================================================

CHUNK_SIZE = 50 * 1024 * 1024  # 50MB for bz2 decompression

STATE_ABBREVIATIONS = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
}


# ============================================================================
# STAGE 2: EXTRACT CITIES AND SCHOOL DISTRICTS
# ============================================================================

def stage2_extract_locations(
    spark: SparkSession,
    properties_df: DataFrame,
    output_dir: str,
    logger=None
) -> Tuple[DataFrame, DataFrame]:
    """
    Stage 2: Extract unique cities and school districts from properties

    Returns: (cities_df, school_districts_df)
    """
    logger.info("=" * 80)
    logger.info("STAGE 2: Extracting Cities and School Districts")
    logger.info("=" * 80)

    # Create state mapping DataFrame
    state_data = [(k, v) for k, v in STATE_ABBREVIATIONS.items()]
    state_schema = StructType([
        StructField("state", StringType(), False),
        StructField("state_full", StringType(), False)
    ])
    state_mapping_df = spark.createDataFrame(state_data, schema=state_schema)

    # Extract cities with state information
    cities_df = properties_df \
        .withColumn("state", regexp_extract(col("title"), r"\b([A-Z]{2})\s+\d{5}", 1)) \
        .filter(col("city").isNotNull() & (col("state") != "")) \
        .groupBy("city", "state") \
        .agg(count("*").alias("property_count")) \
        .join(state_mapping_df, on="state", how="left") \
        .select("city", "state", "state_full", "property_count") \
        .orderBy(desc("property_count"))

    cities_df.cache()
    cities_count = cities_df.count()
    logger.info(f"Found {cities_count} unique cities")

    # Extract school districts
    invalid_districts = ["call school board", "none", "n/a", ""]
    school_districts_df = properties_df \
        .withColumn("state", regexp_extract(col("title"), r"\b([A-Z]{2})\s+\d{5}", 1)) \
        .filter(
            col("school_district").isNotNull() &
            (col("state") != "") &
            ~lower(col("school_district")).isin(invalid_districts)
        ) \
        .groupBy("school_district", "state") \
        .agg(count("*").alias("property_count")) \
        .join(state_mapping_df, on="state", how="left") \
        .select("school_district", "state", "state_full", "property_count") \
        .orderBy(desc("property_count"))

    school_districts_df.cache()
    districts_count = school_districts_df.count()
    logger.info(f"Found {districts_count} unique school districts")

    # Save outputs
    cities_file = Path(output_dir) / "cities_to_extract.json"
    cities_df.coalesce(1).write.mode("overwrite").json(str(cities_file))
    logger.info(f"Saved cities to: {cities_file}")

    districts_file = Path(output_dir) / "school_districts_to_extract.json"
    school_districts_df.coalesce(1).write.mode("overwrite").json(str(districts_file))
    logger.info(f"Saved school districts to: {districts_file}")

    logger.info(f"Stage 2 complete: {cities_count} cities, {districts_count} districts")

    return cities_df, school_districts_df


# ============================================================================
# STAGE 3: FIND WIKIPEDIA PAGES IN INDEX
# ============================================================================

def normalize_title(title: str) -> str:
    """Normalize Wikipedia title for matching"""
    if not title:
        return ""
    return title.lower().strip().replace('_', ' ')


def create_search_lookup(cities_list: List[Dict], districts_list: List[Dict]) -> Dict:
    """Create lookup dictionary for Wikipedia title matching"""
    lookup = {}

    # Add city search terms
    for city_info in cities_list:
        city = city_info['city']
        state = city_info['state']
        state_full = city_info.get('state_full', '')

        # Multiple search term formats
        terms = [
            normalize_title(f"{city}, {state_full}"),  # "Portland, Oregon"
            normalize_title(f"{city}, {state}"),        # "Portland, OR"
            normalize_title(city)                       # "Portland" (disambiguation)
        ]

        for term in terms:
            if term and term not in lookup:  # Avoid overwriting
                lookup[term] = {
                    'entity_name': city,
                    'entity_type': 'city',
                    'state': state,
                    'state_full': state_full
                }

    # Add school district search terms
    for district_info in districts_list:
        district = district_info['school_district']
        state = district_info['state']
        state_full = district_info.get('state_full', '')

        term = normalize_title(district)
        if term and term not in lookup:
            lookup[term] = {
                'entity_name': district,
                'entity_type': 'school_district',
                'state': state,
                'state_full': state_full
            }

    return lookup


def scan_index_lines(lines_iter, search_lookup_bc):
    """Scan index lines for matching Wikipedia pages"""
    results = []
    search_lookup = search_lookup_bc.value

    for line in lines_iter:
        try:
            # Parse index line format: offset:page_id:title
            parts = line.strip().split(':', 2)
            if len(parts) < 3:
                continue

            offset = int(parts[0])
            page_id = parts[1]
            title = parts[2]

            # Normalize and check for match
            title_norm = normalize_title(title)
            if title_norm in search_lookup:
                match_info = search_lookup[title_norm]
                results.append({
                    'offset': offset,
                    'page_id': page_id,
                    'wiki_title': title,
                    'entity_name': match_info['entity_name'],
                    'entity_type': match_info['entity_type'],
                    'state': match_info['state'],
                    'state_full': match_info['state_full']
                })
        except:
            continue

    return results


def stage3_find_wiki_pages(
    spark: SparkSession,
    cities_df: DataFrame,
    school_districts_df: DataFrame,
    wiki_index_path: str,
    output_dir: str,
    num_partitions: int = 20,
    logger=None
) -> DataFrame:
    """
    Stage 3: Find Wikipedia pages in index file

    Returns: DataFrame with found Wikipedia pages
    """
    logger.info("=" * 80)
    logger.info("STAGE 3: Finding Wikipedia Pages in Index")
    logger.info("=" * 80)

    # Collect cities and districts
    cities_list = cities_df.select("city", "state", "state_full").collect()
    districts_list = school_districts_df.select("school_district", "state", "state_full").collect()

    cities_list = [row.asDict() for row in cities_list]
    districts_list = [row.asDict() for row in districts_list]

    logger.info(f"Searching for {len(cities_list)} cities and {len(districts_list)} districts")

    # Create search lookup and broadcast
    search_lookup = create_search_lookup(cities_list, districts_list)
    search_lookup_bc = spark.sparkContext.broadcast(search_lookup)

    logger.info(f"Created search lookup with {len(search_lookup)} terms")
    logger.info(f"Scanning index file: {wiki_index_path}")

    # Read and scan index file in parallel
    index_lines_rdd = spark.sparkContext.textFile(wiki_index_path, minPartitions=num_partitions)

    found_pages_rdd = index_lines_rdd.mapPartitions(
        lambda lines: scan_index_lines(lines, search_lookup_bc)
    )

    # Flatten results
    found_pages_list = found_pages_rdd.collect()

    logger.info(f"Found {len(found_pages_list)} Wikipedia pages")

    # Create DataFrame
    if found_pages_list:
        schema = StructType([
            StructField("offset", LongType(), False),
            StructField("page_id", StringType(), False),
            StructField("wiki_title", StringType(), False),
            StructField("entity_name", StringType(), False),
            StructField("entity_type", StringType(), False),
            StructField("state", StringType(), False),
            StructField("state_full", StringType(), True)
        ])
        found_df = spark.createDataFrame(found_pages_list, schema=schema)
    else:
        raise ValueError("No Wikipedia pages found!")

    # Save output
    output_file = Path(output_dir) / "wiki_pages_found.json"
    found_df.coalesce(1).write.mode("overwrite").json(str(output_file))
    logger.info(f"Saved found pages to: {output_file}")

    logger.info(f"Stage 3 complete: {len(found_pages_list)} pages found")

    return found_df


# ============================================================================
# STAGE 4: DEDUPLICATE WIKIPEDIA PAGES (IMPROVED)
# ============================================================================

def rank_title_quality(title: str, state: str, state_full: str) -> int:
    """
    Rank Wikipedia title quality for deduplication

    Priority (higher is better):
    1. "City, State Full Name" format (e.g., "Portland, Oregon")
    2. "City, ST" format (e.g., "Portland, OR")
    3. Bare city name (e.g., "Portland") - likely disambiguation
    """
    title_lower = title.lower()

    # Check if title contains full state name
    if state_full and state_full.lower() in title_lower:
        return 100  # Highest priority

    # Check if title contains state abbreviation
    if ',' in title and state and f", {state.lower()}" in title_lower:
        return 50  # Medium priority

    # Bare name - likely disambiguation page
    return 1  # Lowest priority


def stage4_deduplicate_wiki_pages(
    spark: SparkSession,
    found_pages_df: DataFrame,
    output_dir: str,
    logger=None
) -> DataFrame:
    """
    Stage 4: Deduplicate Wikipedia pages (keep best match per entity)

    IMPROVED: Prioritizes full article pages over redirects/disambiguations

    Returns: Deduplicated DataFrame
    """
    logger.info("=" * 80)
    logger.info("STAGE 4: Deduplicating Wikipedia Pages (Improved)")
    logger.info("=" * 80)

    initial_count = found_pages_df.count()
    logger.info(f"Initial pages: {initial_count}")

    # Create grouping key and enhanced scoring columns
    dedupe_df = found_pages_df \
        .withColumn("group_key",
            concat(lower(col("entity_name")), lit("|"), col("state"), lit("|"), col("entity_type"))
        ) \
        .withColumn("has_state_full",
            when(col("wiki_title").contains(col("state_full")), 1).otherwise(0)
        ) \
        .withColumn("has_state_abbr",
            when(col("wiki_title").contains(concat(lit(", "), col("state"))), 1).otherwise(0)
        ) \
        .withColumn("title_len", length(col("wiki_title")))

    # Rank within groups
    # Priority: Full state name > State abbreviation > Shorter titles
    window = Window.partitionBy("group_key") \
        .orderBy(desc("has_state_full"), desc("has_state_abbr"), asc("title_len"))

    deduplicated_df = dedupe_df \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1) \
        .drop("group_key", "has_state_full", "has_state_abbr", "title_len", "rank")

    final_count = deduplicated_df.count()
    removed = initial_count - final_count

    logger.info(f"Final pages: {final_count}")
    logger.info(f"Duplicates removed: {removed}")

    # Save output
    output_file = Path(output_dir) / "wiki_pages_found_deduplicated.json"
    deduplicated_df.coalesce(1).write.mode("overwrite").json(str(output_file))
    logger.info(f"Saved deduplicated pages to: {output_file}")

    logger.info(f"Stage 4 complete: {removed} duplicates removed")

    return deduplicated_df


# ============================================================================
# STAGE 5: EXTRACT WIKI CONTENT (IMPROVED WITH REDIRECT/DISAMBIGUATION HANDLING)
# ============================================================================

def is_disambiguation_page(text: str) -> bool:
    """Check if Wikipedia page is a disambiguation page"""
    if not text:
        return False
    text_start = text[:1000].lower()
    return 'may refer to:' in text_start or '{{disambiguation' in text_start


def is_redirect_page(text: str) -> bool:
    """Check if Wikipedia page is a redirect page"""
    if not text:
        return False
    text_start = text[:100].strip()
    return text_start.upper().startswith('#REDIRECT')


def extract_redirect_target(text: str) -> Optional[str]:
    """Extract target page from redirect"""
    if not text:
        return None
    match = re.match(r'#REDIRECT\s*\[\[([^\]]+)\]\]', text, re.IGNORECASE)
    if match:
        target = match.group(1)
        # Remove section links (e.g., "City#History" -> "City")
        if '#' in target:
            target = target.split('#')[0]
        return target.strip()
    return None


def extract_disambiguation_target(text: str, entity_name: str, state: str, state_full: str) -> Optional[str]:
    """
    Parse disambiguation page to find correct target based on state

    Example patterns:
    * [[Portland, Oregon]], the largest city in Oregon
    * [[Portland, Texas]], a city in San Patricio County, Texas
    """
    if not is_disambiguation_page(text):
        return None

    # Look for links that mention our state
    # Pattern: [[Link text]] followed by description mentioning state
    pattern = r'\[\[([^\]]+)\]\]([^\n]*(?:' + re.escape(state_full) + r'|,\s*' + re.escape(state) + r'))'

    matches = re.findall(pattern, text, re.IGNORECASE)

    for link, description in matches:
        # Remove section links
        if '#' in link:
            link = link.split('#')[0]

        # Verify the link contains our entity name
        if entity_name.lower() in link.lower():
            return link.strip()

    return None


def verify_location_content(text: str, entity_name: str, state: str, state_full: str) -> float:
    """
    Score how well a Wikipedia article matches the expected location

    Returns confidence score 0.0-1.0
    """
    if not text or len(text) < 100:
        return 0.0

    score = 0.0
    text_lower = text[:5000].lower()  # Check first 5000 chars

    # Check for state mentions (weighted heavily)
    if state_full and state_full.lower() in text_lower:
        score += 0.4
    if state and re.search(rf'\b{re.escape(state)}\b', text):
        score += 0.3

    # Check for location indicators
    location_keywords = ['city in', 'town in', 'located in', 'county', 'population', 'municipality']
    for keyword in location_keywords:
        if keyword in text_lower:
            score += 0.05

    # Check infobox (Wikipedia articles have structured data)
    if '{{infobox' in text_lower or '{{geobox' in text_lower:
        score += 0.2

    return min(score, 1.0)


def clean_wiki_value(value: str) -> str:
    """Clean Wikipedia value by removing markup and refs"""
    if not value:
        return ""

    # Remove HTML comments (both <!-- --> and encoded versions)
    value = re.sub(r'<!--.*?-->', '', value)
    value = re.sub(r'&lt;!--.*?--&gt;', '', value)

    # Remove references - multiple passes to catch all variations
    # Remove closing ref tags: <ref>...</ref>
    value = re.sub(r'<ref[^>]*>.*?</ref>', '', value, flags=re.DOTALL)
    # Remove self-closing ref tags: <ref ... />
    value = re.sub(r'<ref[^>]*/>', '', value)
    # Remove orphan closing tags
    value = re.sub(r'</ref>', '', value)

    # Remove encoded references: &lt;ref&gt;...&lt;/ref&gt;
    value = re.sub(r'&lt;ref[^&]*?&gt;.*?&lt;/ref&gt;', '', value, flags=re.DOTALL)
    # Remove encoded self-closing: &lt;ref ... /&gt;
    value = re.sub(r'&lt;ref[^&]*?/&gt;', '', value)
    # Remove orphan encoded closing tags
    value = re.sub(r'&lt;/ref&gt;', '', value)

    # Remove HTML entities
    value = value.replace('&amp;nbsp;', ' ')
    value = value.replace('&nbsp;', ' ')
    value = value.replace('&lt;br /&gt;', ' ')
    value = value.replace('&lt;br&gt;', ' ')
    value = value.replace('<br />', ' ')
    value = value.replace('<br>', ' ')

    # Remove wiki links [[text]] or [[link|text]]
    value = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'\2', value)
    value = re.sub(r'\[\[([^\]]+)\]\]', r'\1', value)

    # Remove templates {{...}}
    value = re.sub(r'\{\{[^\}]+\}\}', '', value)

    # Clean whitespace
    value = re.sub(r'\s+', ' ', value).strip()

    return value


def extract_numeric_value(value: str) -> Optional[str]:
    """Extract only numeric value (numbers, commas, decimals) from a string"""
    if not value:
        return None

    # First clean the value
    cleaned = clean_wiki_value(value)

    # Extract first occurrence of number (with optional commas and decimal point)
    # Matches: 123, 1,234, 1234.56, 1,234.56
    match = re.search(r'([\d,]+\.?\d*)', cleaned)
    if match:
        result = match.group(1)
        # Remove trailing dots if present
        result = result.rstrip('.')
        return result if result else None

    return None


def extract_infobox_field(text: str, field_names: list) -> Optional[str]:
    """Extract a field from Wikipedia infobox by field name"""
    for field in field_names:
        # Pattern: | field = value (until next | or }})
        pattern = rf'\|\s*{re.escape(field)}\s*=\s*([^\|\{{]+?)(?:\n\||$)'
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)

        if match:
            value = match.group(1).strip()
            return clean_wiki_value(value)

    return None


def extract_city_properties(text: str) -> Dict:
    """Extract properties from a city Wikipedia page"""
    props = {}

    if not text:
        return props

    # Extract population (numeric only)
    pop = extract_infobox_field(text, [
        'population_total', 'population', 'pop_est', 'pop'
    ])
    if pop:
        pop_numeric = extract_numeric_value(pop)
        if pop_numeric:
            props['population'] = pop_numeric

    # Extract area in km2 only (numeric only)
    area_km2 = extract_infobox_field(text, [
        'area_total_km2', 'area_km2'
    ])

    if area_km2:
        area_km2_numeric = extract_numeric_value(area_km2)
        if area_km2_numeric:
            props['area_km2'] = area_km2_numeric

    # Extract elevation (numeric only)
    elev_m = extract_infobox_field(text, [
        'elevation_m', 'elevation_m2'
    ])
    elev_ft = extract_infobox_field(text, [
        'elevation_ft', 'elevation_feet'
    ])

    if elev_m:
        elev_m_numeric = extract_numeric_value(elev_m)
        if elev_m_numeric:
            props['elevation_m'] = elev_m_numeric
    if elev_ft:
        elev_ft_numeric = extract_numeric_value(elev_ft)
        if elev_ft_numeric:
            props['elevation_ft'] = elev_ft_numeric

    # Extract timezone (keep as text)
    tz = extract_infobox_field(text, [
        'timezone', 'timezone1'
    ])
    if tz:
        props['timezone'] = tz

    # Extract founded/established (keep as text, just clean)
    founded = extract_infobox_field(text, [
        'established_date', 'founded', 'established'
    ])
    if founded:
        # Clean but keep text (years, dates)
        founded_clean = clean_wiki_value(founded)
        if founded_clean and founded_clean != '':
            props['founded'] = founded_clean

    return props


def extract_district_properties(text: str) -> Dict:
    """Extract properties from a school district Wikipedia page"""
    props = {}

    if not text:
        return props

    # Extract students/enrollment (numeric only)
    students = extract_infobox_field(text, [
        'students', 'enrollment', 'num_students'
    ])
    if students:
        students_numeric = extract_numeric_value(students)
        if students_numeric:
            props['students'] = students_numeric

    # Extract number of schools (numeric only)
    schools = extract_infobox_field(text, [
        'schools', 'number_of_schools', 'num_schools'
    ])
    if schools:
        schools_numeric = extract_numeric_value(schools)
        if schools_numeric:
            props['schools'] = schools_numeric

    # Extract grades (keep as text - e.g., "K-12", "9-12")
    grades = extract_infobox_field(text, [
        'grades', 'grade', 'grade_levels'
    ])
    if grades:
        grades_clean = clean_wiki_value(grades)
        if grades_clean and grades_clean != '':
            props['grades'] = grades_clean

    # Extract budget (numeric only)
    budget = extract_infobox_field(text, [
        'budget', 'revenue'
    ])
    if budget:
        budget_numeric = extract_numeric_value(budget)
        if budget_numeric:
            props['budget'] = budget_numeric

    # Extract superintendent (keep as text)
    superintendent = extract_infobox_field(text, [
        'superintendent', 'admin', 'leader_name'
    ])
    if superintendent:
        superintendent_clean = clean_wiki_value(superintendent)
        if superintendent_clean and superintendent_clean != '':
            props['superintendent'] = superintendent_clean

    # Extract established (keep as text, just clean)
    established = extract_infobox_field(text, [
        'established', 'founded', 'established_date'
    ])
    if established:
        established_clean = clean_wiki_value(established)
        if established_clean and established_clean != '':
            props['established'] = established_clean

    return props


def extract_page_from_dump_improved(dump_path: str, offset: int, pages_info: List[Dict]) -> List[Dict]:
    """
    Extract Wikipedia pages from dump file with redirect/disambiguation handling

    IMPROVED: Handles redirects and disambiguation pages
    """
    results = []
    redirect_cache = {}  # Cache for resolved redirects in this chunk

    try:
        with open(dump_path, 'rb') as f:
            f.seek(offset)
            compressed_data = f.read(CHUNK_SIZE)

            if not compressed_data:
                return results

            # Decompress bz2 data
            decompressor = bz2.BZ2Decompressor()
            decompressed = decompressor.decompress(compressed_data)

            # Parse XML
            xml_str = b'<mediawiki>' + decompressed + b'</mediawiki>'
            root = ET.fromstring(xml_str)
            pages = root.findall('.//page')

            # Build a cache of all pages in this chunk
            pages_by_title = {}
            for page in pages:
                title_elem = page.find('title')
                if title_elem is not None:
                    pages_by_title[title_elem.text] = page

            # Process each requested page
            for page_info in pages_info:
                target_title = page_info['wiki_title']
                entity_name = page_info['entity_name']
                state = page_info['state']
                state_full = page_info['state_full']

                # Try to find the page (may need to follow redirects)
                current_title = target_title
                attempts = 0
                max_attempts = 5  # Prevent infinite redirect loops

                while attempts < max_attempts:
                    if current_title in pages_by_title:
                        page = pages_by_title[current_title]

                        page_id_elem = page.find('id')
                        text_elem = page.find('.//text')
                        page_text = text_elem.text if text_elem is not None else ''

                        # Check if it's a redirect
                        if is_redirect_page(page_text):
                            redirect_target = extract_redirect_target(page_text)
                            if redirect_target and redirect_target in pages_by_title:
                                current_title = redirect_target
                                attempts += 1
                                continue  # Follow the redirect
                            else:
                                # Redirect target not in this chunk
                                break

                        # Check if it's a disambiguation page
                        if is_disambiguation_page(page_text):
                            disambig_target = extract_disambiguation_target(
                                page_text, entity_name, state, state_full
                            )
                            if disambig_target and disambig_target in pages_by_title:
                                current_title = disambig_target
                                attempts += 1
                                continue  # Follow to specific page
                            else:
                                # Could not resolve disambiguation
                                break

                        # Verify content matches our location
                        content_score = verify_location_content(
                            page_text, entity_name, state, state_full
                        )

                        if content_score < 0.3:
                            # Content doesn't match our location well
                            break

                        # We have a valid article!
                        page_xml = ET.tostring(page, encoding='unicode')

                        # Extract properties based on entity type
                        extracted_properties = {}
                        if page_info['entity_type'] == 'city':
                            extracted_properties = extract_city_properties(page_text)
                        elif page_info['entity_type'] == 'school_district':
                            extracted_properties = extract_district_properties(page_text)

                        results.append({
                            'wiki_title': current_title,  # Use resolved title
                            'original_title': target_title,
                            'page_id': page_id_elem.text if page_id_elem is not None else None,
                            'text': page_text,
                            'xml': page_xml,
                            'entity_name': entity_name,
                            'entity_type': page_info['entity_type'],
                            'state': state,
                            'state_full': state_full,
                            'offset': offset,
                            'text_length': len(page_text) if page_text else 0,
                            'content_score': content_score,
                            'redirects_followed': attempts,
                            'extracted_properties': json.dumps(extracted_properties) if extracted_properties else '{}'
                        })
                        break

                    else:
                        # Page not found in this chunk
                        break

                    attempts += 1

    except Exception as e:
        pass

    return results


def stage5_extract_wiki_content(
    spark: SparkSession,
    deduplicated_df: DataFrame,
    wiki_dump_path: str,
    output_dir: str,
    num_partitions: int = 20,
    logger=None
) -> DataFrame:
    """
    Stage 5: Extract Wikipedia content from dump file

    IMPROVED: Follows redirects and parses disambiguation pages

    Returns: DataFrame with extracted Wikipedia pages
    """
    logger.info("=" * 80)
    logger.info("STAGE 5: Extracting Wikipedia Content (Improved)")
    logger.info("=" * 80)

    pages_to_extract = deduplicated_df.count()
    logger.info(f"Pages to extract: {pages_to_extract}")

    # Group by offset for efficient extraction
    offset_groups = deduplicated_df \
        .groupBy("offset") \
        .agg(collect_list(struct(
            "wiki_title", "page_id", "entity_name", "entity_type",
            "state", "state_full"
        )).alias("pages"))

    # Convert to RDD of (offset, pages_list)
    tasks_rdd = offset_groups.rdd.map(lambda row: (row.offset, [p.asDict() for p in row.pages]))

    # Broadcast dump path
    dump_path_bc = spark.sparkContext.broadcast(wiki_dump_path)

    # Extract in parallel with improved logic
    def extract_batch(task):
        offset, pages_info = task
        return extract_page_from_dump_improved(dump_path_bc.value, offset, pages_info)

    extracted_rdd = tasks_rdd.flatMap(extract_batch)
    extracted_list = extracted_rdd.collect()

    logger.info(f"Successfully extracted: {len(extracted_list)} pages")
    logger.info(f"Pages not extracted: {pages_to_extract - len(extracted_list)}")

    # Create DataFrame
    if extracted_list:
        schema = StructType([
            StructField("wiki_title", StringType(), False),
            StructField("original_title", StringType(), True),
            StructField("page_id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("xml", StringType(), True),
            StructField("entity_name", StringType(), False),
            StructField("entity_type", StringType(), False),
            StructField("state", StringType(), False),
            StructField("state_full", StringType(), True),
            StructField("offset", LongType(), False),
            StructField("text_length", IntegerType(), True),
            StructField("content_score", FloatType(), True),
            StructField("redirects_followed", IntegerType(), True),
            StructField("extracted_properties", StringType(), True)
        ])
        extracted_df = spark.createDataFrame(extracted_list, schema=schema)
    else:
        raise ValueError("No pages extracted!")

    # Save XML and JSON files by state
    wiki_dir = Path(output_dir) / "wiki_extracted"
    wiki_dir.mkdir(parents=True, exist_ok=True)

    for page_data in extracted_list:
        state = page_data['state']
        title = page_data['wiki_title'].replace('/', '_').replace(':', '_')

        state_dir = wiki_dir / state
        state_dir.mkdir(exist_ok=True)

        # Save XML
        with open(state_dir / f"{title}.xml", 'w', encoding='utf-8') as f:
            f.write(page_data['xml'])

        # Save JSON metadata
        extracted_props = json.loads(page_data.get('extracted_properties', '{}'))
        with open(state_dir / f"{title}.json", 'w', encoding='utf-8') as f:
            json.dump({
                'wiki_title': page_data['wiki_title'],
                'original_title': page_data.get('original_title'),
                'page_id': page_data['page_id'],
                'entity_name': page_data['entity_name'],
                'entity_type': page_data['entity_type'],
                'state': page_data['state'],
                'state_full': page_data['state_full'],
                'text_length': page_data['text_length'],
                'content_score': page_data.get('content_score', 0.0),
                'redirects_followed': page_data.get('redirects_followed', 0),
                'offset': page_data['offset'],
                'extracted_properties': extracted_props
            }, f, indent=2)

    logger.info(f"Saved extracted pages to: {wiki_dir}")
    logger.info(f"Stage 5 complete: {len(extracted_list)} pages extracted")

    return extracted_df


# ============================================================================
# IMPORT STAGES 6-8 FROM ORIGINAL FILE
# ============================================================================

def normalize_for_fuzzy_matching(name: str) -> str:
    """
    Aggressively normalize names for fuzzy matching

    Removes common suffixes, numbers, special characters
    """
    if not name:
        return ""

    name = name.lower().strip()

    # Remove common school district suffixes (order matters - longest first)
    district_suffixes = [
        ' unified school district',
        ' independent school district',
        ' consolidated school district',
        ' community school district',
        ' school district',
        ' public schools',
        ' city schools',
        ' county schools',
        ' community schools',
        ' area schools',
        ' school system',
        ' school corporation',
        ' schools',
        ' unified district',
        ' unified',
        ' independent',
        ' isd',
        ' usd',
        ' district',
    ]

    for suffix in district_suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()

    # Remove district numbers (e.g., "11 - Conroe" -> "conroe", "Cherry Creek 5" -> "cherry creek")
    name = re.sub(r'^\d+\s*-\s*', '', name)  # Remove leading "11 - "
    name = re.sub(r'\s+\d+$', '', name)       # Remove trailing " 5"
    name = re.sub(r'#\d+', '', name)          # Remove "#80"
    name = re.sub(r'\s+\d+\s+', ' ', name)    # Remove middle numbers

    # Remove state abbreviations
    name = re.sub(r',\s*[a-z]{2}$', '', name)
    name = re.sub(r'\s+[a-z]{2}$', '', name)

    # Remove special characters
    name = re.sub(r'[^\w\s]', ' ', name)

    # Normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    return name


def calculate_match_score(prop_name: str, wiki_name: str) -> float:
    """
    Calculate fuzzy match score between property and wiki names

    Returns score from 0.0 to 1.0
    """
    if not prop_name or not wiki_name:
        return 0.0

    prop_norm = normalize_for_fuzzy_matching(prop_name)
    wiki_norm = normalize_for_fuzzy_matching(wiki_name)

    # Exact match after normalization
    if prop_norm == wiki_norm:
        return 1.0

    # One contains the other
    if prop_norm in wiki_norm or wiki_norm in prop_norm:
        return 0.9

    # Word-based similarity (Jaccard)
    prop_words = set(prop_norm.split())
    wiki_words = set(wiki_norm.split())

    if not prop_words or not wiki_words:
        return 0.0

    intersection = prop_words.intersection(wiki_words)
    union = prop_words.union(wiki_words)

    jaccard_score = len(intersection) / len(union) if union else 0.0

    # Bonus if all property words are in wiki name (property is subset)
    if prop_words.issubset(wiki_words):
        jaccard_score = max(jaccard_score, 0.85)

    return jaccard_score


def stage6_create_property_wiki_mapping(
    spark: SparkSession,
    properties_df: DataFrame,
    extracted_wiki_df: DataFrame,
    output_dir: str,
    logger=None,
    fuzzy_threshold: float = 0.7
) -> DataFrame:
    """
    Stage 6: Create mappings between properties and Wikipedia pages using fuzzy matching

    Uses aggressive normalization and Jaccard similarity to match:
    1. School districts (prioritized)
    2. Cities (fallback)

    Args:
        fuzzy_threshold: Minimum score (0.0-1.0) to accept a match (default: 0.7)

    Returns: DataFrame with property-wiki mappings including match_score and match_confidence
    """
    logger.info("=" * 80)
    logger.info("STAGE 6: Creating Property-Wiki Mappings (Fuzzy Matching)")
    logger.info("=" * 80)
    logger.info(f"Fuzzy match threshold: {fuzzy_threshold}")

    total_properties = properties_df.count()
    logger.info(f"Total properties: {total_properties}")

    # Extract state abbreviation from property title
    properties_with_state = properties_df \
        .withColumn("state_abbr", regexp_extract(col("title"), r"\b([A-Z]{2})\s+\d{5}", 1))

    # Collect wiki pages to driver
    wiki_pages = extracted_wiki_df.select(
        "wiki_title", "page_id", "entity_name", "entity_type", "state", "state_full", "text_length", "extracted_properties"
    ).collect()

    logger.info(f"Collected {len(wiki_pages)} wiki pages for fuzzy matching")

    # Separate by type for efficient matching
    district_pages = [row.asDict() for row in wiki_pages if row['entity_type'] == 'school_district']
    city_pages = [row.asDict() for row in wiki_pages if row['entity_type'] == 'city']

    logger.info(f"  - {len(district_pages)} school district pages")
    logger.info(f"  - {len(city_pages)} city pages")

    # Broadcast wiki pages to all executors
    broadcast_districts = spark.sparkContext.broadcast(district_pages)
    broadcast_cities = spark.sparkContext.broadcast(city_pages)

    def find_all_fuzzy_matches(property_row):
        """
        Find all fuzzy matches for a property (both school district AND city if available)
        Returns a list of matches (can be 0, 1, or 2 records)
        """
        prop_dict = property_row.asDict()
        property_id = prop_dict.get('file_hash')
        school_district = prop_dict.get('school_district', '')
        city = prop_dict.get('city', '')
        state_abbr = prop_dict.get('state_abbr', '')

        matches = []

        # Strategy 1: Try school_district fuzzy matching
        best_district_match = None
        best_district_score = 0.0

        if school_district:
            for wiki in broadcast_districts.value:
                # Only match within same state
                if wiki.get('state') != state_abbr:
                    continue

                score = calculate_match_score(school_district, wiki.get('entity_name', ''))
                if score >= fuzzy_threshold and score > best_district_score:
                    best_district_score = score
                    best_district_match = wiki

        # Add school district match if found
        if best_district_match:
            # Determine confidence level
            if best_district_score >= 0.95:
                confidence = 'high'
            elif best_district_score >= 0.8:
                confidence = 'medium'
            else:
                confidence = 'low'

            matches.append({
                'property_id': property_id,
                **prop_dict,
                'wiki_title': best_district_match['wiki_title'],
                'wiki_page_id': best_district_match['page_id'],
                'wiki_state': best_district_match['state'],
                'wiki_state_full': best_district_match['state_full'],
                'wiki_text_length': best_district_match['text_length'],
                'extracted_properties': best_district_match.get('extracted_properties', '{}'),
                'match_type': 'school_district_fuzzy',
                'match_score': float(best_district_score),
                'match_confidence': confidence
            })

        # Strategy 2: Try city fuzzy matching (independent of district)
        best_city_match = None
        best_city_score = 0.0

        if city:
            for wiki in broadcast_cities.value:
                # Only match within same state
                if wiki.get('state') != state_abbr:
                    continue

                score = calculate_match_score(city, wiki.get('entity_name', ''))
                if score >= fuzzy_threshold and score > best_city_score:
                    best_city_score = score
                    best_city_match = wiki

        # Add city match if found
        if best_city_match:
            # Determine confidence level
            if best_city_score >= 0.95:
                confidence = 'high'
            elif best_city_score >= 0.8:
                confidence = 'medium'
            else:
                confidence = 'low'

            matches.append({
                'property_id': property_id,
                **prop_dict,
                'wiki_title': best_city_match['wiki_title'],
                'wiki_page_id': best_city_match['page_id'],
                'wiki_state': best_city_match['state'],
                'wiki_state_full': best_city_match['state_full'],
                'wiki_text_length': best_city_match['text_length'],
                'extracted_properties': best_city_match.get('extracted_properties', '{}'),
                'match_type': 'city_fuzzy',
                'match_score': float(best_city_score),
                'match_confidence': confidence
            })

        return matches

    # Apply fuzzy matching
    logger.info("Performing fuzzy matching across all properties...")
    logger.info("NOTE: Each property can match BOTH city AND school district")

    matched_rows = properties_with_state.rdd \
        .flatMap(find_all_fuzzy_matches) \
        .collect()

    logger.info(f"Fuzzy matching complete: {len(matched_rows)} matches found")

    # Convert back to DataFrame
    if matched_rows:
        all_mappings = spark.createDataFrame(matched_rows) \
            .withColumn("mapping_timestamp", current_timestamp())

        # Statistics
        mapping_count = all_mappings.count()
        district_fuzzy = all_mappings.filter(col("match_type") == "school_district_fuzzy").count()
        city_fuzzy = all_mappings.filter(col("match_type") == "city_fuzzy").count()
        high_conf = all_mappings.filter(col("match_confidence") == "high").count()
        medium_conf = all_mappings.filter(col("match_confidence") == "medium").count()
        low_conf = all_mappings.filter(col("match_confidence") == "low").count()

        # Calculate unique properties with mappings
        unique_properties_with_mappings = all_mappings.select("property_id").distinct().count()

        # Count properties with both city AND district mappings
        properties_with_both = all_mappings.groupBy("property_id") \
            .agg(countDistinct("match_type").alias("mapping_types")) \
            .filter(col("mapping_types") == 2) \
            .count()

        coverage = (unique_properties_with_mappings / total_properties) * 100 if total_properties > 0 else 0

        logger.info(f"Total mapping records created: {mapping_count}")
        logger.info(f"Unique properties with mappings: {unique_properties_with_mappings}/{total_properties} ({coverage:.1f}%)")
        logger.info(f"Properties with BOTH city AND district: {properties_with_both}")
        logger.info(f"Mapping type breakdown:")
        logger.info(f"  - School district mappings: {district_fuzzy}")
        logger.info(f"  - City mappings: {city_fuzzy}")
        logger.info(f"Confidence distribution:")
        logger.info(f"  - High (≥0.95): {high_conf}")
        logger.info(f"  - Medium (≥0.80): {medium_conf}")
        logger.info(f"  - Low (≥{fuzzy_threshold}): {low_conf}")

        # Save outputs
        mapping_file = Path(output_dir) / "property_wiki_mapping.jsonl"
        all_mappings.coalesce(1).write.mode("overwrite").json(str(mapping_file))

        parquet_file = Path(output_dir) / "property_wiki_mapping.parquet"
        all_mappings.write.mode("overwrite").parquet(str(parquet_file))

        logger.info(f"Saved mappings to: {mapping_file}")
        logger.info(f"Stage 6 complete: {mapping_count} mappings created")

        return all_mappings
    else:
        logger.warning("No fuzzy matches found!")
        # Return empty DataFrame with schema
        schema = properties_with_state.schema
        return spark.createDataFrame([], schema)


def stage7_generate_jsonl_metadata(
    spark: SparkSession,
    mapping_df: DataFrame,
    output_dir: str,
    logger=None
) -> None:
    """
    Stage 7: Generate JSONL metadata file with extracted Wikipedia properties

    Format (each line is a JSON object):
    For city matches:
    {
      "property_id": "hash...",
      "wiki_page_path": "spark_data/wiki_extracted/TX/Dallas.xml",
      "match_type": "city",
      "matched_based_on": "Dallas",
      "match_score": 1.0,
      "match_confidence": "high",
      "city": {
        "population": "1,304,379",
        "area_sq_mi": "385.8",
        ...
      }
    }

    For school district matches:
    {
      "property_id": "hash...",
      "wiki_page_path": "spark_data/wiki_extracted/TX/Frisco_ISD.xml",
      "match_type": "school_district",
      "matched_based_on": "Frisco Independent School District",
      "match_score": 1.0,
      "match_confidence": "high",
      "school_district": {
        "students": "67226",
        "schools": "72",
        ...
      }
    }
    """
    logger.info("=" * 80)
    logger.info("STAGE 7: Generating JSONL Metadata with Extracted Properties")
    logger.info("=" * 80)

    # Collect all mappings to convert to JSONL
    mappings = mapping_df \
        .filter(col("wiki_page_id").isNotNull()) \
        .select(
            col("property_id"),
            col("city"),
            col("school_district"),
            col("wiki_title"),
            col("wiki_state"),
            col("match_type"),
            col("match_score"),
            col("match_confidence"),
            col("extracted_properties")
        ) \
        .orderBy("property_id", "match_type") \
        .collect()

    logger.info(f"Generating JSONL for {len(mappings)} mappings...")

    # Convert to JSONL format
    jsonl_records = []
    for mapping in mappings:
        # Determine matched_based_on field
        matched_based_on = mapping['school_district'] if 'school_district' in mapping['match_type'] else mapping['city']

        # Clean up match_type to just "city" or "school_district"
        match_type = "school_district" if "school_district" in mapping['match_type'] else "city"

        # Build wiki page path
        wiki_title_clean = mapping['wiki_title'].replace('/', '_').replace(':', '_')
        wiki_page_path = f"spark_data/wiki_extracted/{mapping['wiki_state']}/{wiki_title_clean}.xml"

        # Parse extracted properties
        wiki_metadata = {}
        if mapping['extracted_properties']:
            try:
                wiki_metadata = json.loads(mapping['extracted_properties'])
            except:
                wiki_metadata = {}

        # Build final record with entity-specific field
        record = {
            "property_id": mapping['property_id'],
            "wiki_page_path": wiki_page_path,
            "match_type": match_type,
            "matched_based_on": matched_based_on,
            "match_score": float(mapping['match_score']),
            "match_confidence": mapping['match_confidence']
        }

        # Add entity-specific metadata field
        if match_type == "city":
            record["city"] = wiki_metadata
        elif match_type == "school_district":
            record["school_district"] = wiki_metadata

        jsonl_records.append(record)

    # Save as JSONL
    output_file = Path(output_dir) / "property_wiki_metadata.jsonl"

    with open(output_file, 'w', encoding='utf-8') as f:
        for record in jsonl_records:
            f.write(json.dumps(record) + '\n')

    logger.info(f"Saved JSONL metadata to: {output_file}")
    logger.info(f"Stage 7 complete: {len(jsonl_records)} records")

    # Print sample records
    logger.info("\nSample records:")
    for i, record in enumerate(jsonl_records[:3], 1):
        logger.info(f"\n  Record {i}:")
        logger.info(f"    Property: {record['property_id'][:16]}...")
        logger.info(f"    Match type: {record['match_type']}")
        logger.info(f"    Matched on: {record['matched_based_on']}")
        logger.info(f"    Score: {record['match_score']:.2f} ({record['match_confidence']})")

        # Show entity-specific properties
        if 'city' in record and record['city']:
            logger.info(f"    City properties: {list(record['city'].keys())}")
        elif 'school_district' in record and record['school_district']:
            logger.info(f"    School district properties: {list(record['school_district'].keys())}")


def stage8_generate_school_district_stats(
    spark: SparkSession,
    mapping_df: DataFrame,
    output_dir: str,
    logger=None
) -> None:
    """
    Stage 8: Generate school district statistics

    Count wiki pages per school district
    """
    logger.info("=" * 80)
    logger.info("STAGE 8: Generating School District Statistics")
    logger.info("=" * 80)

    # Filter for school district matches
    stats_df = mapping_df \
        .filter(
            (col("match_type").contains("school_district")) &
            col("wiki_page_id").isNotNull()
        ) \
        .groupBy("school_district", "wiki_state") \
        .agg(
            countDistinct("property_id").alias("property_count"),
            countDistinct("wiki_page_id").alias("wiki_page_count"),
            collect_set("wiki_title").alias("wiki_pages"),
            avg("wiki_text_length").alias("avg_wiki_text_length")
        ) \
        .withColumn(
            "avg_properties_per_wiki",
            col("property_count") / col("wiki_page_count")
        ) \
        .orderBy(desc("property_count"))

    districts_count = stats_df.count()
    logger.info(f"School districts with wiki pages: {districts_count}")

    # Convert to JSON
    stats_list = stats_df.collect()
    stats_json = []

    for row in stats_list:
        stats_json.append({
            'school_district': row['school_district'],
            'state': row['wiki_state'],
            'property_count': row['property_count'],
            'wiki_page_count': row['wiki_page_count'],
            'wiki_pages': row['wiki_pages'],
            'avg_wiki_text_length': round(row['avg_wiki_text_length'], 2) if row['avg_wiki_text_length'] else 0,
            'avg_properties_per_wiki': round(row['avg_properties_per_wiki'], 2)
        })

    # Save output
    output_file = Path(output_dir) / "school_district_wiki_stats.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(stats_json, f, indent=2)

    logger.info(f"Saved statistics to: {output_file}")
    logger.info(f"Stage 8 complete: {districts_count} districts analyzed")
