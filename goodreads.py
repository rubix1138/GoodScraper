import multiprocessing, pprint, requests, re, csv
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from os.path import exists
from threading import Lock

class MultiThreadedCrawler:

    def __init__(self, seed_url):
        self.field_names = ["Title", "ISBN", "ISBN13", "Author"]
        self.filename = "export.csv"
        self.writer_lock = Lock()
        self.seed_url = seed_url
        self.root_url = '{}://{}'.format(urlparse(self.seed_url).scheme, urlparse(self.seed_url).netloc)
        self.pool = ThreadPoolExecutor(max_workers=5)
        self.scraped_pages = set([])
        self.crawl_queue = Queue()
        self.crawl_queue.put(self.seed_url)

    def parse_links(self, html):
        soup = BeautifulSoup(html, 'html5lib')
        Anchor_Tags = soup.find_all('a', href=True, class_='bookTitle')
        for link in Anchor_Tags:
            url = link['href']
            if url.startswith('/') or url.startswith(self.root_url):
                url = urljoin(self.root_url, url)
                if url not in self.scraped_pages:
                    self.crawl_queue.put(url)

    def scrape_info(self, html, URL):
        book = dict()
        regex_isbn10 = r"\D(\d{10})\D"
        regex_isbn13 = r"\D(\d{13})\D"

        soup = BeautifulSoup(html, "html.parser")
        # Find the title
        p = soup.find("div", string="Original Title")
        try:
            v = p.find_next("div").string
            book["Title"] = v
        except:
            print(f"Couldn't find title in {URL}")

        #Find the ISBN numbers
        p = soup.find("div", string="ISBN")
        try:
            v = str(p.find_next("div"))
            match_isbn10 = re.finditer(regex_isbn10, v, re.MULTILINE)
            for o in match_isbn10:
                isbn10 = o.group(1)
            match_isbn13 = re.finditer(regex_isbn13, v, re.MULTILINE)
            for o in match_isbn13:
                isbn13 = o.group(1)
            book['ISBN'] = isbn10
            book['ISBN13'] = isbn13
        except:
            print(f"Couldn't find ISBN in {URL})")
        #alternitive way to get ISBN13
        if 'ISBN13' not in book:
            p = soup.find("meta", property="books:isbn")
            try:
                book['ISBN13'] = p["content"]
            except:
                print(f"Couldn't find ISBN13 in {URL}")

        #Find the author
        p = soup.find("a", class_="authorName")
        try:
            book['Author'] = p.string
        except:
            print(f"Couldn't find author in {URL}")
        return book

    def post_scrape_callback(self, res):
        result = res.result()
        if result and result.status_code == 200:
            self.parse_links(result.text)
            row = self.scrape_info(result.text, result.url)
            if row:
                self.write_to_file(row)

    def scrape_page(self, url):
        try:
            res = requests.get(url, timeout=(3, 30))
            return res
        except requests.RequestException:
            return

    def run_web_crawler(self):
        while True:
            try:
                print("\n Name of the current executing process: ", multiprocessing.current_process().name, '\n')
                target_url = self.crawl_queue.get(timeout=60)
                if target_url not in self.scraped_pages:
                    print("Scraping URL: {}".format(target_url))
                    self.current_scraping_url = "{}".format(target_url)
                    self.scraped_pages.add(target_url)
                    job = self.pool.submit(self.scrape_page, target_url)
                    job.add_done_callback(self.post_scrape_callback)
            except Empty:
                return
            except Exception as e:
                print(e)
                continue

    def info(self):
        print('\n Seed URL is: ', self.seed_url, '\n')
        print('Scraped pages are: ', self.scraped_pages, '\n')

    def write_to_file(self, row):
        with self.writer_lock:
            if exists(self.filename):
                try:
                    with open(self.filename, 'a') as my_file:
                        writer = csv.DictWriter(my_file, fieldnames=self.field_names)
                        writer.writerow(row)
                        my_file.flush()
                except:
                    print(f"Couldn't open {self.filename} for appending")
                    exit(1)
            else:
                try:
                    with open(self.filename, 'w') as my_file:
                        writer = csv.DictWriter(my_file, fieldnames=self.field_names)
                        writer.writeheader()
                        writer.writerow(row)
                        my_file.flush()
                except:
                    print(f"Couldn't open {self.filename} for writing")
                    exit(1)



if __name__ == '__main__':
    cc = MultiThreadedCrawler("https://www.goodreads.com/shelf/show/technology?page=1")
    cc.run_web_crawler()
    cc.info()
