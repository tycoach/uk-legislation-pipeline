import os
import re
import time
import logging
import requests
from typing import List, Dict, Optional, Generator, Any
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import concurrent.futures
import json
import  hashlib



class LegislationScraper:
    """
    Scraper for UK legislation from legislation.gov.uk
    
    """
    
    BASE_URL = "https://www.legislation.gov.uk"
    SEARCH_URL = f"{BASE_URL}/all"
    
    def __init__(self, cache_dir: str = "/data/cache"):
        self.logger = logging.getLogger(__name__)
        self.cache_dir = cache_dir
        self.session = requests.Session()
        os.makedirs(self.cache_dir, exist_ok=True)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        })

    def _get_cache_path(self, url: str) -> str:
        filename = re.sub(r'[^a-zA-Z0-9]', '_', url) + '.html'
        return os.path.join(self.cache_dir, filename)

    def _fetch_with_cache(self, url: str, force_refresh: bool = False) -> str:
        cache_path = self._get_cache_path(url)
        if os.path.exists(cache_path) and not force_refresh:
            self.logger.debug(f"Loading from cache: {url}")
            with open(cache_path, 'r', encoding='utf-8') as f:
                return f.read()
        
        max_retries = 3
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Fetching from web: {url}")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                return response.text
            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    self.logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    raise

    def search_legislation(
        self, 
        time_period: str,
        category: str, 
        max_results: int = 100
    ) -> List[Dict[str, str]]:
        self.logger.info(f"Searching legislation for period: {time_period}, category: {category}")
        try:
            month, year = time_period.split('/')
            month_num = datetime.strptime(month, '%B').month
            year_num = int(year)
        except (ValueError, AttributeError):
            self.logger.error(f"Invalid time period format: {time_period}. Expected format: 'Month/Year'")
            raise ValueError(f"Invalid time period format. Expected 'Month/Year', got: {time_period}")
        
        search_url = f"{self.SEARCH_URL}/{year_num}?title={quote(category)}"
        self.logger.info(f"Using search URL: {search_url}")
        
        html_content = self._fetch_with_cache(search_url)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []
        result_rows = soup.select('tbody tr')
        self.logger.info(f"Found {len(result_rows)} result rows in search page")
        
        for row in result_rows[:max_results]:
            try:
                title_cell = row.select_one('td:nth-child(1)')
                if not title_cell or not title_cell.a:
                    continue
                title = title_cell.a.text.strip()
                url = urljoin(self.BASE_URL, title_cell.a['href'])
                year_cell = row.select_one('td:nth-child(2)')
                year = year_cell.text.strip() if year_cell else ''
                number_cell = row.select_one('td:nth-child(3)')
                number = number_cell.text.strip() if number_cell else ''
                type_cell = row.select_one('td:nth-child(4)')
                leg_type = type_cell.text.strip() if type_cell else ''
                doc_id = url.split(self.BASE_URL + '/')[1] if self.BASE_URL in url else ''
                legislation_meta = {
                    'title': title,
                    'url': url,
                    'doc_id': doc_id,
                    'year': year,
                    'number': number,
                    'type': leg_type,
                    'month': month_num
                }
                results.append(legislation_meta)
                self.logger.debug(f"Found legislation: {title} - {doc_id}")
            except Exception as e:
                self.logger.warning(f"Error parsing search result row: {str(e)}")
        self.logger.info(f"Total legislation items found: {len(results)}")
        return results

    def fetch_single_section_content(self, url: str) -> str:
        self.logger.info(f"Fetching single section content: {url}")
        try:
            html = self._fetch_with_cache(url)
            soup = BeautifulSoup(html, 'html.parser')
            content_div = soup.select_one('#content, .LegContent, .legislation-body')
            if not content_div:
                self.logger.warning("Content container not found")
                return ""
            paragraphs = []
            for elem in content_div.find_all(['p', 'div', 'span'], recursive=True):
                classes = elem.get('class', [])
                if any(cls in ['LegLabel', 'LegNav', 'watermark'] for cls in classes):
                    continue
                text = elem.get_text(strip=True)
                if text:
                    paragraphs.append(text)
            full_text = '\n\n'.join(paragraphs)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text).strip()
            return full_text
        except Exception as e:
            self.logger.error(f"Error fetching section content: {e}")
            return ""
    def _generate_legislation_id(self, legislation_meta: Dict[str, Any]) -> str:
        """
        Generate a unique ID for legislation, based on URL or doc_id.
        """
        base_string = legislation_meta.get('url') or legislation_meta.get('doc_id') or str(time.time())
        return hashlib.sha256(base_string.encode('utf-8')).hexdigest()
    def fetch_legislation_content(self, legislation_meta: Dict[str, str]) -> Dict[str, Any]:
        url = legislation_meta.get('url')
        doc_id = legislation_meta.get('doc_id')
        if not url or not doc_id:
            self.logger.error("Missing URL or document ID in legislation metadata")
            return None
        self.logger.info(f"Fetching legislation content: {legislation_meta.get('title')}")
        try:
            html_content = self._fetch_with_cache(url)
            result = {
                **legislation_meta,
                'html_content': html_content,
                'sections': [],
                'fetch_timestamp': datetime.now().isoformat()
            }
            # Generate and add stable unique ID
            result['id'] = self._generate_legislation_id(result)
            soup = BeautifulSoup(html_content, 'html.parser')
            toc_items = soup.select('.LegContents li a, #legContents li a')
            if not toc_items:
                self.logger.info("No table of contents found, extracting main content")
                main_text = self.fetch_single_section_content(url)
                if main_text:
                    result['sections'].append({
                        'title': 'Main Content',
                        'url': url,
                        'content': main_text,
                        'html': ''
                    })
                return result

            self.logger.info(f"Found {len(toc_items)} table of contents items")

            # Use ThreadPoolExecutor for concurrent fetching of sections
            def fetch_section(item):
                section_url = urljoin(self.BASE_URL, item.get('href', ''))
                section_title = item.text.strip()
                if '#' in section_url and section_url.split('#')[0] == url:
                    return None
                content = self.fetch_single_section_content(section_url)
                if content:
                    return {
                        'title': section_title,
                        'url': section_url,
                        'content': content,
                        'html': ''
                    }
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(fetch_section, item) for item in toc_items]
                for future in concurrent.futures.as_completed(futures):
                    section = future.result()
                    if section:
                        result['sections'].append(section)
                        self.logger.debug(f"Added section: {section['title']}")

            total_chars = sum(len(s['content']) for s in result['sections'])
            self.logger.info(f"Total content extracted: {total_chars} characters in {len(result['sections'])} sections")
            return result
        except Exception as e:
            self.logger.error(f"Error fetching legislation content for {url}: {str(e)}")
            return None
        
   
        
    def fetch_all_legislation(
        self, 
        time_period: str,
        category: str,
        max_items: Optional[int] = None,
        batch_size: int = 10
    ) -> Generator[List[Dict[str, Any]], None, None]:
        items = self.search_legislation(time_period, category)
        if max_items is not None:
            items = items[:max_items]
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} items)")
            results = []
            for item in batch:
                try:
                    res = self.fetch_legislation_content(item)
                    if res:
                        results.append(res)
                except Exception as e:
                    self.logger.error(f"Error processing item {item.get('doc_id')}: {str(e)}")
            yield results


