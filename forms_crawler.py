import asyncio
import csv
import os
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime
import logging
from bs4 import BeautifulSoup
import aiohttp
from typing import Dict

logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG for more verbose output
logger = logging.getLogger(__name__)

class WebCrawler:
    def __init__(self, base_url: str, batch_size: int = 5, max_pages: int = 500, timeout_minutes: int = 30):
        self.base_url = self._normalize_url(base_url)
        self.visited = set()
        self.queue = []
        self.forms_found = []
        self.batch_size = batch_size
        self.max_pages = max_pages
        self.timeout_minutes = timeout_minutes
        self.all_urls = set()
        self.start_time = None

        # Known form paths and their expected IDs/classes
        self.form_configs = {
            '/': {'name': 'Get Free Recommendations', 'selectors': ['form#home-recommendations']},
            'vendor-profile-page-software': {
                'name': 'Software Profile Forms',
                'selectors': [
                    'form.get-pricing-form',
                    'form.compare-pricing-form',
                    'form.watch-demo-form',
                    'form.write-review-form',
                    'form.fit-check-form'
                ]
            },
            'vendor-profile-page-services': {
                'name': 'Services Profile Forms',
                'selectors': [
                    'form.get-quote-form',
                    'form.download-portfolio-form'
                ]
            },
            'lead-generation-page': {'name': 'Get Started Form', 'selectors': ['form.vendor-signup-form']},
            'subcategory-page': {
                'name': 'Subcategory Forms',
                'selectors': [
                    'form.pricing-guide-form',
                    'form.download-list-form'
                ]
            },
            'vendor-comparison-page': {'name': 'Comparison Form', 'selectors': ['form.comparison-form']},
            'whitepaper-article-page': {'name': 'Whitepaper Form', 'selectors': ['form.whitepaper-form']},
            'register-now': {'name': 'Webinar Registration', 'selectors': ['form.webinar-form']},
            'watch-now-webinar': {'name': 'Watch Webinar', 'selectors': ['form.watch-webinar-form']},
            'category-page': {
                'name': 'Category Page Forms',
                'selectors': [
                    'form.advice-form',
                    'form.help-form'
                ]
            },
            'get-free-advice': {
                'name': 'Advice Forms',
                'selectors': [
                    'form.deciding-help-form',
                    'form.software-search-form'
                ]
            },
            'contact-us': {'name': 'Contact Form', 'selectors': ['form.contact-form']}
        }

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to prevent duplicates"""
        parsed = urlparse(url)
        clean_path = re.sub(r'/+', '/', parsed.path.rstrip('/'))
        return f"{parsed.scheme}://{parsed.netloc.lower()}{clean_path}"

    def _should_crawl_url(self, url: str) -> bool:
        """Filter URLs to crawl with priority for form-related paths"""
        parsed = urlparse(url)
        path = parsed.path.lower()

        # Skip non-HTML resources
        if any(ext in path for ext in [
            '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip', '.css', '.js',
            '.ico', '.xml', '.txt', '.doc', '.docx', '.xls', '.xlsx'
        ]):
            return False

        # Skip certain patterns that indicate duplicate content
        if any(pattern in path for pattern in [
            '/feed/', '/rss/', '/atom/', '/api/',
            '/print/', '/trackback/',
            'offset=', 'limit=', 'start='
        ]):
            return False

        # Prioritize paths that might contain forms
        if any(form_path in path for form_path in self.form_configs.keys()):
            logger.debug(f"Found potential form page: {url}")
            return True

        # Allow other HTML pages but with lower priority
        return True

    def _extract_meta_info(self, soup, url: str) -> Dict:
        """Extract and analyze meta tag information"""
        meta_info = {
            'url': url,
            'has_meta_keywords': False,
            'has_meta_description': False,
            'meta_keywords': '',
            'meta_description': '',
            'seo_issues': []
        }

        # Check meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            content = meta_desc.get('content', '').strip()
            if content:
                meta_info['has_meta_description'] = True
                meta_info['meta_description'] = content
            else:
                meta_info['seo_issues'].append("Empty meta description")
        else:
            meta_info['seo_issues'].append("Missing meta description")

        # Check meta keywords
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords:
            content = meta_keywords.get('content', '').strip()
            if content:
                meta_info['has_meta_keywords'] = True
                meta_info['meta_keywords'] = content
            else:
                meta_info['seo_issues'].append("Empty meta keywords")
        else:
            meta_info['seo_issues'].append("Missing meta keywords")

        # Log findings
        logger.info(f"Meta analysis for {url}:")
        logger.info(f"- Description present: {meta_info['has_meta_description']}")
        logger.info(f"- Keywords present: {meta_info['has_meta_keywords']}")
        if meta_info['seo_issues']:
            logger.warning(f"SEO issues found: {', '.join(meta_info['seo_issues'])}")

        return meta_info

    def _extract_form_info(self, url: str, html: str) -> list:
        """Extract form information from HTML"""
        forms = []
        meta_info = None

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # First analyze meta tags
            meta_info = self._extract_meta_info(soup, url)

            # Check each path configuration
            for path, config in self.form_configs.items():
                if path in url or path == '/':
                    logger.debug(f"Checking for {config['name']} forms at {url}")

                    for selector in config['selectors']:
                        form = soup.select_one(selector)
                        if form:
                            logger.info(f"Found {config['name']} form using selector: {selector}")

                            # Extract form fields
                            fields = []
                            for field in form.find_all(['input', 'textarea', 'select']):
                                field_type = field.get('type', 'text') if field.name == 'input' else field.name
                                field_name = field.get('name', '')
                                required = field.has_attr('required') or field.has_attr('data-required')

                                # Skip hidden and submit fields
                                if field_type not in ['hidden', 'submit']:
                                    field_data = {
                                        'selector': f"{field.name}[name='{field_name}']",
                                        'type': field_type,
                                        'required': required
                                    }
                                    fields.append(field_data)

                            if fields:  # Only include forms with visible input fields
                                form_data = {
                                    'url': url,
                                    'form_id': form.get('id', '') or selector.replace('form', '').strip('.#'),
                                    'form_selector': selector,
                                    'form_type': config['name'],
                                    'fields': fields,
                                    'meta_info': meta_info  # Include meta tag analysis with each form
                                }
                                forms.append(form_data)
                                logger.info(f"Added form data for {form_data['form_id']} with {len(fields)} fields")

        except Exception as e:
            logger.error(f"Error extracting form info from {url}: {e}")

        return forms

    async def _process_url(self, session: aiohttp.ClientSession, url: str):
        """Process a single URL to find forms and analyze meta tags"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch {url}: Status {response.status}")
                    return []

                html = await response.text()
                logger.debug(f"Got HTML response for {url} (length: {len(html)})")

                # Extract meta information and forms
                soup = BeautifulSoup(html, 'html.parser')
                meta_info = self._extract_meta_info(soup, url)

                forms = []
                # Check each path configuration
                for path, config in self.form_configs.items():
                    if path in url or path == '/':
                        logger.debug(f"Checking for {config['name']} forms at {url}")

                        for selector in config['selectors']:
                            form = soup.select_one(selector)
                            if form:
                                logger.info(f"Found {config['name']} form using selector: {selector}")

                                # Extract form fields
                                fields = []
                                for field in form.find_all(['input', 'textarea', 'select']):
                                    field_type = field.get('type', 'text') if field.name == 'input' else field.name
                                    field_name = field.get('name', '')
                                    required = field.has_attr('required') or field.has_attr('data-required')

                                    # Skip hidden and submit fields
                                    if field_type not in ['hidden', 'submit']:
                                        field_data = {
                                            'selector': f"{field.name}[name='{field_name}']",
                                            'type': field_type,
                                            'required': required
                                        }
                                        fields.append(field_data)

                                if fields:  # Only include forms with visible input fields
                                    form_data = {
                                        'url': url,
                                        'form_id': form.get('id', '') or selector.replace('form', '').strip('.#'),
                                        'form_selector': selector,
                                        'form_type': config['name'],
                                        'fields': fields,
                                        'meta_info': meta_info  # Include meta tag analysis
                                    }
                                    forms.append(form_data)
                                    if form_data not in self.forms_found:
                                        self.forms_found.append(form_data)
                                    logger.info(f"Added form data for {form_data['form_id']} with {len(fields)} fields")

                # Extract new URLs to crawl
                new_urls = []
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if not href or href.startswith('#') or href.startswith('mailto:'):
                        continue

                    try:
                        full_url = urljoin(url, href)
                        normalized_url = self._normalize_url(full_url)
                        if (normalized_url.startswith(self.base_url) and 
                            self._should_crawl_url(full_url) and 
                            normalized_url not in self.all_urls):
                            # Prioritize form-related URLs
                            if any(form_path in normalized_url.lower() for form_path in self.form_configs.keys()):
                                new_urls.insert(0, normalized_url)
                            else:
                                new_urls.append(normalized_url)
                            self.all_urls.add(normalized_url)
                    except Exception as e:
                        logger.warning(f"Error processing link {href}: {str(e)}")

                return new_urls

        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            return []

    def save_forms_to_csv(self, filename: str = 'form_specs.csv'):
        """Save discovered forms to CSV"""
        if not self.forms_found:
            logger.warning("No forms found to save")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Form ID', 'URL', 'Form Selector', 'Success Message Selector', 'Fields',
                'Form Type', 'Has Meta Description', 'Meta Description',
                'Has Meta Keywords', 'Meta Keywords', 'SEO Issues'
            ])

            for form in self.forms_found:
                fields_str = ';'.join([
                    f"{field['selector']}|{field['type']}|{'required' if field['required'] else 'optional'}"
                    for field in form['fields']
                ])
                meta = form.get('meta_info', {})
                writer.writerow([
                    form['form_id'],
                    form['url'],
                    form['form_selector'],
                    '.success-message',  # Default success message selector
                    fields_str,
                    form['form_type'],
                    meta.get('has_meta_description', False),
                    meta.get('meta_description', '')[:200],  # Truncate long descriptions
                    meta.get('has_meta_keywords', False),
                    meta.get('meta_keywords', '')[:200],  # Truncate long keywords
                    '; '.join(meta.get('seo_issues', []))
                ])

            logger.info(f"Saved {len(self.forms_found)} forms to {filename}")

    def _should_stop_crawling(self) -> bool:
        """Check if we should stop crawling"""
        if len(self.visited) >= self.max_pages:
            logger.info(f"Reached maximum pages limit: {self.max_pages}")
            return True

        elapsed_minutes = (datetime.now() - self.start_time).total_seconds() / 60
        if elapsed_minutes >= self.timeout_minutes:
            logger.info(f"Reached timeout after {elapsed_minutes:.1f} minutes")
            return True

        return False

    async def crawl(self):
        """Main crawl method"""
        logger.info(f"Starting crawl of {self.base_url}")
        self.start_time = datetime.now()

        async with aiohttp.ClientSession() as session:
            self.queue = [self.base_url]
            self.visited.add(self.base_url)
            self.all_urls.add(self.base_url)

            while self.queue and not self._should_stop_crawling():
                current_batch = self.queue[:self.batch_size]
                self.queue = self.queue[self.batch_size:]

                # Process URLs in parallel
                tasks = [self._process_url(session, url) for url in current_batch]
                results = await asyncio.gather(*tasks)

                # Add new URLs to queue
                for new_urls in results:
                    self.queue.extend([url for url in new_urls if url not in self.visited])
                    self.visited.update(url for url in new_urls if url not in self.visited)

                logger.info(f"Processed {len(self.visited)} pages, {len(self.queue)} remaining")

            logger.info(f"Crawl completed. Found {len(self.forms_found)} forms across {len(self.visited)} pages")
            self.save_forms_to_csv()

    async def get_session(self):
        """Get an aiohttp session for form testing"""
        return aiohttp.ClientSession()
