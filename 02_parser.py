import re
import json
from pathlib import Path
from typing import Dict, Optional, List
from markitdown import MarkItDown


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
    patterns = [
        r'(\d+)\s+Beds?',
        r'Bedrooms?\s+Total\s*\n\s*(\d+)'
    ]
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
    patterns = [
        r'([\d,]+)\s+Sq\s+Ft(?!\s*/)',
        r'Living\s+Area\s*\n\s*([\d,]+)\s*SqFt'
    ]
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
    patterns = [
        r'Built in (\d{4})',
        r'Year Built\s*\n\s*(\d{4})'
    ]
    for pattern in patterns:
        match = re.search(pattern, content)
        if match:
            year = int(match.group(1))
            if year > 0 and year <= 2025:
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
                if size > 0 and size < 10000:
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


def extract_property(content: str, file_id: Optional[str] = None) -> Dict:
    property_data = {
        'title': extract_property_title(content),
        'price': extract_price(content),
        'beds': extract_beds(content),
        'baths': extract_baths(content),
        'sqft': extract_sqft(content),
        'status': extract_status(content),
        'description': extract_description(content),
        'year_built': extract_year_built(content),
        'price_per_sqft': extract_price_per_sqft(content),
        'lot_size_acres': extract_lot_size(content),
        'property_type': extract_property_type(content),
        'property_sub_type': extract_property_sub_type(content),
        'city': extract_city(content),
        'county': extract_county(content),
        'school_district': extract_school_district(content),
        'file_hash': file_id,
    }

    return property_data


def html_to_markdown(html_file_path: str) -> str:
    md = MarkItDown()
    result = md.convert(html_file_path)
    return result.text_content


def process_html_file(html_file_path: str, log_file=None) -> Optional[Dict]:
    if not Path(html_file_path).exists():
        print(f"Error: Input file '{html_file_path}' not found")
        return None

    print(f"Converting HTML to markdown: {html_file_path}")
    try:
        markdown_content = html_to_markdown(html_file_path)

        if re.search(r'##\s+[\d,]+\s+results', markdown_content, re.IGNORECASE):
            print(f"  ⊘ Skipping listing page")
            if log_file:
                log_file.write(f"\n{'='*80}\n")
                log_file.write(f"Source: {html_file_path}\n")
                log_file.write(f"SKIPPED: Listing page detected\n")
                log_file.write(f"{'='*80}\n\n")
                log_file.flush()
            return None

        file_id = Path(html_file_path).stem
        property_data = extract_property(markdown_content, file_id)
        property_data['source_file'] = str(html_file_path)

        if log_file:
            log_file.write(f"\n{'='*80}\n")
            log_file.write(f"Source: {html_file_path}\n")
            log_file.write(f"{'='*80}\n\n")
            log_file.write("MARKDOWN CONTENT:\n")
            log_file.write("-" * 80 + "\n")
            log_file.write(markdown_content)
            log_file.write("\n\n")
            log_file.write("EXTRACTED DATA:\n")
            log_file.write("-" * 80 + "\n")
            log_file.write(json.dumps(property_data, ensure_ascii=False, indent=2))
            log_file.write("\n\n")
            log_file.flush()

        return property_data
    except Exception as e:
        print(f"Error processing {html_file_path}: {e}")
        return None


def get_all_html_files(pages_dir: str) -> List[str]:
    pages_path = Path(pages_dir)
    if not pages_path.exists():
        print(f"Error: Directory '{pages_dir}' not found")
        return []

    html_files = list(pages_path.glob("*.html"))
    return [str(f) for f in html_files]


def process_multiple_html_files(pages_dir: str = "data/scraper_data/pages",
                                output_jsonl_path: str = "data/parsed.jsonl",
                                log_path: str = "data/scraper_data/extraction_log.txt",
                                num_files: int = -1) -> None:
    html_files = get_all_html_files(pages_dir)

    if not html_files:
        print("No HTML files found")
        return

    if num_files:
        if num_files > len(html_files):
            print(f"Warning: Requested {num_files} files but only {len(html_files)} available")
            num_files = len(html_files)
        selected_files = html_files[:num_files]
    else:
        selected_files = html_files

    print(f"Processing {len(selected_files)} HTML files...")
    if log_path:
        print(f"Logging to: {log_path}")

    successful = 0
    with open(output_jsonl_path, 'w', encoding='utf-8') as f:
        log_file = open(log_path, 'w', encoding='utf-8') if log_path else None

        try:
            if log_file:
                log_file.write("=" * 80 + "\n")
                log_file.write("DATA EXTRACTION LOG\n")

            for i, html_file in enumerate(selected_files, 1):
                print(f"\n[{i}/{len(selected_files)}] Processing: {Path(html_file).name}")
                property_data = process_html_file(html_file, log_file)

                if property_data:
                    json.dump(property_data, f, ensure_ascii=False)
                    f.write('\n')
                    successful += 1
                    print(f"  ✓ Extracted {len([v for v in property_data.values() if v is not None])} fields")

            if log_file:
                log_file.write("\n" + "=" * 80 + "\n")
                log_file.write(f"EXTRACTION COMPLETE\n")
                log_file.write(f"Successfully processed: {successful}/{len(selected_files)} files\n")
                log_file.write("=" * 80 + "\n")

        finally:
            if log_file:
                log_file.close()

    print(f"\n{'='*60}")
    print(f"Extraction complete!")
    print(f"Successfully processed: {successful}/{len(selected_files)} files")
    print(f"Output written to: {output_jsonl_path}")
    if log_path:
        print(f"Log written to: {log_path}")
    print(f"{'='*60}")


def main():
    process_multiple_html_files()


if __name__ == "__main__":
    main()
