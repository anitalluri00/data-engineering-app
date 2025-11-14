import requests
from bs4 import BeautifulSoup
import urllib.robotparser
from urllib.parse import urljoin, urlparse
import time
import logging
from datetime import datetime
from database.models import DataStorage
from ingestion.file_processor import FileProcessor

class WebCrawler:
    def __init__(self, delay=1.0):
        self.delay = delay  # Delay between requests in seconds
        self.visited_urls = set()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataEngineeringBot/1.0 (+http://example.com/bot)'
        })
        self.file_processor = FileProcessor()
        self.db = DataStorage()
        
    def crawl_website(self, base_url, max_pages=50, allowed_domains=None):
        """Crawl a website and extract content"""
        try:
            if allowed_domains is None:
                allowed_domains = [urlparse(base_url).netloc]
            
            # Check robots.txt
            if not self._check_robots_txt(base_url):
                logging.warning(f"Robots.txt disallows crawling for {base_url}")
                return []
            
            to_visit = [base_url]
            crawled_data = []
            
            while to_visit and len(crawled_data) < max_pages:
                url = to_visit.pop(0)
                
                if url in self.visited_urls:
                    continue
                
                try:
                    # Respect crawl delay
                    time.sleep(self.delay)
                    
                    # Fetch page
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    
                    # Parse content
                    content_type = response.headers.get('content-type', '')
                    if 'text/html' in content_type:
                        page_data = self._parse_html_page(response.text, url)
                        
                        if page_data:
                            # Store in database
                            file_id = self._store_web_content(page_data, response.content)
                            page_data['file_id'] = file_id
                            crawled_data.append(page_data)
                            
                            # Extract new links
                            new_links = self._extract_links(response.text, url, allowed_domains)
                            to_visit.extend(new_links)
                    
                    self.visited_urls.add(url)
                    logging.info(f"Crawled: {url} - Total: {len(crawled_data)}")
                    
                except Exception as e:
                    logging.error(f"Error crawling {url}: {e}")
                    continue
            
            return crawled_data
            
        except Exception as e:
            logging.error(f"Error in website crawling: {e}")
            return []
    
    def _check_robots_txt(self, base_url):
        """Check robots.txt for crawling permissions"""
        try:
            parsed_url = urlparse(base_url)
            robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
            
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            
            return rp.can_fetch('*', base_url)
        except:
            # If robots.txt can't be read, proceed with caution
            return True
    
    def _parse_html_page(self, html_content, url):
        """Parse HTML content and extract relevant information"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Extract title
            title = soup.title.string if soup.title else ""
            
            # Extract main content (try to get meaningful text)
            main_content = ""
            
            # Try to find article content
            article = soup.find('article')
            if article:
                main_content = article.get_text()
            else:
                # Fallback to body content
                body = soup.find('body')
                if body:
                    main_content = body.get_text()
                else:
                    main_content = soup.get_text()
            
            # Clean up text
            main_content = self._clean_text(main_content)
            
            # Extract metadata
            metadata = {
                'url': url,
                'title': title,
                'crawled_at': datetime.now().isoformat(),
                'content_type': 'web_page',
                'word_count': len(main_content.split()),
                'char_count': len(main_content)
            }
            
            # Extract links for sitemap
            links = soup.find_all('a', href=True)
            metadata['internal_links'] = len([link for link in links if self._is_internal_link(link['href'], url)])
            metadata['external_links'] = len([link for link in links if not self._is_internal_link(link['href'], url)])
            
            return {
                'filename': f"webpage_{hash(url)}.html",
                'content': main_content,
                'metadata': metadata,
                'raw_html': html_content
            }
            
        except Exception as e:
            logging.error(f"Error parsing HTML page {url}: {e}")
            return None
    
    def _clean_text(self, text):
        """Clean and normalize text"""
        # Remove extra whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _is_internal_link(self, href, base_url):
        """Check if link is internal to the same domain"""
        try:
            full_url = urljoin(base_url, href)
            base_domain = urlparse(base_url).netloc
            link_domain = urlparse(full_url).netloc
            return base_domain == link_domain
        except:
            return False
    
    def _extract_links(self, html_content, base_url, allowed_domains):
        """Extract and filter links from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                
                # Filter URLs
                if (self._is_valid_url(full_url, allowed_domains) and 
                    full_url not in self.visited_urls and
                    full_url not in links):
                    links.append(full_url)
            
            return links
            
        except Exception as e:
            logging.error(f"Error extracting links: {e}")
            return []
    
    def _is_valid_url(self, url, allowed_domains):
        """Check if URL is valid and allowed"""
        try:
            parsed = urlparse(url)
            return (parsed.scheme in ['http', 'https'] and
                    parsed.netloc in allowed_domains and
                    not self._is_file_url(parsed.path))
        except:
            return False
    
    def _is_file_url(self, path):
        """Check if URL points to a file rather than a page"""
        file_extensions = ['.pdf', '.doc', '.docx', '.jpg', '.png', '.gif', '.zip']
        return any(path.lower().endswith(ext) for ext in file_extensions)
    
    def _store_web_content(self, page_data, raw_content):
        """Store web content in database"""
        try:
            file_id = f"web_{hash(page_data['metadata']['url'])}"
            filename = page_data['filename']
            
            # Use file processor to store content
            self.file_processor.process_file(
                raw_content,
                filename,
                source_type="web_crawl"
            )
            
            return file_id
            
        except Exception as e:
            logging.error(f"Error storing web content: {e}")
            raise
    
    def crawl_multiple_sources(self, sources_config):
        """Crawl multiple websites based on configuration"""
        results = {}
        
        for source_name, config in sources_config.items():
            try:
                logging.info(f"Starting crawl for {source_name}")
                
                crawled_data = self.crawl_website(
                    base_url=config['base_url'],
                    max_pages=config.get('max_pages', 50),
                    allowed_domains=config.get('allowed_domains')
                )
                
                results[source_name] = {
                    'success': True,
                    'pages_crawled': len(crawled_data),
                    'data': crawled_data
                }
                
                logging.info(f"Completed crawl for {source_name}: {len(crawled_data)} pages")
                
            except Exception as e:
                results[source_name] = {
                    'success': False,
                    'error': str(e),
                    'pages_crawled': 0
                }
                logging.error(f"Error crawling {source_name}: {e}")
        
        return results