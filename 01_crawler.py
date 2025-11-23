import requests
import hashlib
import csv
import os
import re
import time
import random
import json
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse, quote

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

def setup_logging(log_file):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    logger.handlers = []

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s',
                                         datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.remax.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }

def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

def build_root_page_url(city_data, page = 1):
    city = city_data['city']
    state_code = city_data['state_code']
    city_slug = city_data['city_slug']
    place_key = city_data['place_key']

    base_url = f"https://www.remax.com/homes-for-sale/{state_code}/{city_slug}/city/{place_key}"

    search_query = {
        "place": {
            "city": city.lower(),
            "placeId": place_key,
            "placeType": "places",
            "stateOrProvince": state_code
        },
        "filters": {
            "city": city_slug,
            "stateOrProvince": state_code,
            "uiTransactionType": "Sale"
        },
        "sortKey": "0",
        "sortDirection": "1",
        "hasPolygon": False,
        "mapState": "hidden-all",
        "pageNumber": page
    }

    encoded_query = quote(json.dumps(search_query))
    return f"{base_url}?searchQuery={encoded_query}"

def clean_html(html_content, tags_to_remove=None):
    if tags_to_remove is None:
        tags_to_remove = {
            'style': 'with_content',
            'script': 'with_content',
            'svg': 'with_content',
            'link': 'self_closing',
            'script': 'self_closing'
        }

    cleaned_html = html_content
    for tag, tag_type in tags_to_remove.items():
        if tag_type == 'with_content':
            pattern = rf'<{tag}[^>]*>.*?</{tag}>'
            cleaned_html = re.sub(pattern, '', cleaned_html, flags=re.DOTALL | re.IGNORECASE)
        elif tag_type == 'self_closing':
            pattern = rf'<{tag}[^>]*/?>'
            cleaned_html = re.sub(pattern, '', cleaned_html, flags=re.IGNORECASE)
    return cleaned_html

def extract_property_urls(html_content, base_url, state_code, city_slug):
    html_content = clean_html(html_content)

    urls = set()
    href_pattern = r'<a[^>]+href=["\']([^"\']+)["\']'
    matches = re.findall(href_pattern, html_content, re.IGNORECASE)

    property_url_pattern = r'/home-details/'

    for href in matches:
        if not href or href.startswith('#') or href.startswith('javascript:'):
            continue

        absolute_url = urljoin(base_url, href)

        if 'www.remax.com' in absolute_url and property_url_pattern in absolute_url:
            urls.add(absolute_url)

    return list(urls)

def download_page(url, data_dir, session, max_retries=5):
    base_delay = 10
    robot_check_string = "In order to continue, we need to verify that you're not a robot."

    for attempt in range(max_retries):
        start_time = time.time()
        response = session.get(url, timeout=30)
        response.raise_for_status()
        response_time_ms = (time.time() - start_time) * 1000

        html_content = response.text

        if robot_check_string in html_content:
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)
                print(f"Robot detection! Waiting {wait_time}s before retry {attempt + 1}/{max_retries - 1}")
                time.sleep(wait_time)
                continue
            else:
                raise Exception("Robot detection: max retries exceeded")

        pages_dir = os.path.join(data_dir, 'pages')
        os.makedirs(pages_dir, exist_ok=True)

        url_hash_value = hash_url(url)
        filename = f"{url_hash_value}.html"
        filepath = os.path.join(pages_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)

        return {
            'url_hash': url_hash_value,
            'url': url,
            'domain': urlparse(url).netloc,
            'download_timestamp': datetime.now().isoformat(),
            'status_code': response.status_code,
            'content_length': len(html_content),
            'encoding': response.encoding or 'utf-8',
            'html_file_path': filepath,
            'content_type': response.headers.get('content-type', 'text/html'),
            'response_time_ms': response_time_ms
        }

    raise Exception("Failed to download page after retries")

def save_metadata(metadata, csv_path):
    headers = ['url_hash', 'url', 'domain', 'download_timestamp', 'status_code',
               'content_length', 'encoding', 'html_file_path', 'content_type', 'response_time_ms']

    file_exists = os.path.exists(csv_path)

    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: metadata.get(k, '') for k in headers})

def print_stack_status(url_stack, logger):
    total = len(url_stack)
    logger.debug(f"Stack status: {total} URLs remaining")

    if total == 0:
        return

    depth_counts = {}
    for url, depth in url_stack:
        depth_counts[depth] = depth_counts.get(depth, 0) + 1

    for depth in sorted(depth_counts.keys()):
        logger.debug(f"  Depth {depth}: {depth_counts[depth]} URLs")

    logger.debug("Stack contents:")
    for idx, (url, depth) in enumerate(url_stack, 1):
        logger.debug(f"  {idx}. ({url}, depth={depth})")

def main(cities_file="all_city_urls.json", data_dir="data/scraper_data", limit=1, start_index=0, max_depth=3, num_root_pages=2):
    csv_path = os.path.join(data_dir, "metadata.csv")
    log_path = os.path.join(data_dir, "crawler.log")
    min_delay = 1.0
    max_delay = 3.0

    os.makedirs(data_dir, exist_ok=True)

    logger = setup_logging(log_path)
    logger.info("="*60)
    logger.info("Starting crawler")
    logger.info(f"Max depth: {max_depth}")
    logger.info(f"Root pages per city: {num_root_pages}")
    logger.info("="*60)

    with open(cities_file, 'r') as f:
        cities = json.load(f)

    valid_cities = [city for city in cities if city.get('error') is None and city.get('place_key')]
    cities_to_process = valid_cities[start_index:start_index + limit]

    logger.info(f"Processing {len(cities_to_process)} cities (start_index={start_index}, limit={limit})")

    session = requests.Session()

    for idx, city_data in enumerate(cities_to_process, start=start_index):
        city_name = city_data['city']
        state_name = city_data['state']
        state_code = city_data['state_code']
        city_slug = city_data['city_slug']

        logger.info("")
        logger.info("="*60)
        logger.info(f"Processing city index {idx}: {city_name}, {state_name}")
        logger.info(f"URL pattern: /{state_code}/{city_slug}/home-details/")
        logger.info("="*60)

        try:
            url_stack = []
            visited_urls = set()
            pages_processed = 0

            logger.info(f"Adding {num_root_pages} root page(s) to stack")
            for page_num in range(1, num_root_pages + 1):
                root_url = build_root_page_url(city_data, page=page_num)
                url_stack.append((root_url, 0))
                logger.info(f"  Page {page_num}: {root_url}")
            logger.info(f"Initialized stack with {num_root_pages} root page(s) at depth 0")

            while url_stack:
                current_url, current_depth = url_stack.pop(0)

                if current_url in visited_urls:
                    logger.debug(f"Skipping already visited URL: {current_url}")
                    continue

                pages_processed += 1
                logger.info(f"[Page {pages_processed}] Processing depth {current_depth}: {current_url}")

                try:
                    session.headers.update(get_headers())
                    metadata = download_page(current_url, data_dir, session)
                    save_metadata(metadata, csv_path)
                    visited_urls.add(current_url)

                    logger.info(f"  Downloaded successfully (size: {metadata['content_length']} bytes)")

                    if current_depth < max_depth:
                        with open(metadata['html_file_path'], 'r', encoding='utf-8') as f:
                            html_content = f.read()

                        discovered_urls = extract_property_urls(html_content, current_url, state_code, city_slug)
                        logger.debug(f"  Discovered {len(discovered_urls)} URLs on page")

                        new_urls = [url for url in discovered_urls if url not in visited_urls]
                        skipped_count = len(discovered_urls) - len(new_urls)

                        new_depth = current_depth + 1
                        for url in new_urls:
                            url_stack.append((url, new_depth))

                        logger.info(f"  Found {len(discovered_urls)} URLs: {len(new_urls)} added to stack (depth {new_depth}), {skipped_count} skipped (already visited)")
                    else:
                        logger.info(f"  Max depth reached, not extracting URLs from this page")

                    print_stack_status(url_stack, logger)

                    delay = random.uniform(min_delay, max_delay)
                    time.sleep(delay)

                except Exception as e:
                    logger.error(f"  Error processing {current_url}: {e}")
                    visited_urls.add(current_url)
                    continue

            logger.info(f"Completed {city_name}: {pages_processed} pages processed, {len(visited_urls)} unique URLs visited")

        except Exception as e:
            logger.error(f"Error processing {city_name}: {e}")
            continue

if __name__ == "__main__":
    main(limit=350, start_index=0, max_depth=3, num_root_pages=2)
