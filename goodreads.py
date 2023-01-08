import multiprocessing, pprint, requests, re, csv, threading
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from os.path import exists
#from threading import Lock

class MultiThreadedCrawler:

    def __init__(self, seed_url):
        self.field_names = ["Title", "ISBN", "ISBN13", "Author", "URL"]
        self.stats = {} # will be a nested dict { URL : { stat : value }}
        self.filename = "export.csv"
        self.errorlog = "errorlog"
        self.writer_lock = threading.Lock()
        #self.error_lock = threading.Lock()
        self.seed_url = seed_url
        self.root_url = '{}://{}'.format(urlparse(self.seed_url).scheme, urlparse(self.seed_url).netloc)
        self.pool = ThreadPoolExecutor(max_workers=100)
        self.scraped_pages = set([])
        self.crawl_queue = Queue()
        self.crawl_queue.put(self.seed_url)

    #def increment_stat(self, URL, stat):


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
        # if it's not a book URL, bail
        if not URL.__contains__("/book/show/"):
            return
        print(f"Trying to find book details in {URL}. Current Thread count is {threading.active_count()}")
        book = {}
        regex_isbn10 = r"\D(\d{10})\D"
        regex_isbn13 = r"\D(\d{13})\D"
        book['URL'] = URL

        soup = BeautifulSoup(html, "html5lib")

        ###### Title Logic #######
        # Find the title looking for <h1 id="bookTitle" class="gr-h1 gr-h1--serif" itemprop="name">
        p = soup.find(id="bookTitle")
        try:
            book["Title"] = str(p.string.strip())
        except:
            #print(f"Couldn't find title in {URL} using id='bookTitle'")
            pass
        # Find the title looking for <div class="infoBoxRowTitle">Original Title</div>
        if 'Title' not in book:
            p = soup.find("div", string="Original Title")
            try:
                book["Title"] = p.find_next("div").string
            except:
                #print(f"Couldn't find title in {URL} using Original Title")
                pass
        # alt title search, looking for <div class="BookPageTitleSection__title">
        if 'Title' not in book:
            p = soup.find("h1", class_="BookPageTitleSection__title")
            try:
                book["Title"] = str(p.string.strip())
            except:
                #print(f"Couldn't find title in {URL} using BookPageTitleSection__title")
                pass

        ###### ISBN Logic #######
        #Find the ISBN
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
            #print(f"Couldn't find ISBN in {URL}")
            pass
        #alternitive way to get ISBN13
        if 'ISBN13' not in book:
            p = soup.find("meta", property="books:isbn")
            try:
                book['ISBN13'] = p["content"]
            except:
                #print(f"Couldn't find ISBN13 in {URL}")
                pass
        #sometimes goodreads doesn't have the ISBN.  It puts "null" in it's data.  This cleans that out.
        if 'ISBN13' in book.keys() and book['ISBN13'] == "null":
            book.pop('ISBN13', None)

        ###### Author Logic #######
        #Find author using <div class="authorName__container">
        p = soup.find("a", class_="authorName__container")
        try:
            book['Author'] = str(p.string.strip())
        except:
            #print(f"Couldn't find author in {URL} using authorName__container")
            pass
        #Find the author using <div class_="authorName">
        if 'Author' not in book:
            p = soup.find("a", class_="authorName")
            try:
                book['Author'] = str(p.string.strip())
            except:
                #print(f"Couldn't find author in {URL} using authorName")
                pass

        return book

    def post_scrape_callback(self, res):
        result = res.result()
        if result and result.status_code == 200:
            if result.url.__contains__("/book/show/"):
                #this page is a book page and we should scrape for data
                row = self.scrape_info(result.text, result.url)
                #if only the URL field exists, we weren't able to scrape anything.
                #adding logic to add the url back to the job queue.
                #because this could fire after the main thread is over, we can ignore errors
                if row and len(row.values()) == 1:
                    print(f"Error scraping {result.url} - Adding back on queue. Current Thread count is {threading.active_count()}")
                    try:
                        job = self.pool.submit(self.scrape_page, result.url)
                        job.add_done_callback(self.post_scrape_callback)
                    except:
                        pass
                else:
                    self.write_to_file(row)
            # this is lising page and we need to go parse more links
            else:
                self.parse_links(result.text)

    def scrape_page(self, url):
        try:
            res = requests.get(url, timeout=(3, 30))
            return res
        except requests.RequestException:
            return

    def run_web_crawler(self):
        while True:
            try:
                target_url = self.crawl_queue.get(timeout=60)
                if target_url not in self.scraped_pages:
                    #print("Retrieving URL: {}".format(target_url))
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
        if not row:
            return
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

    def write_error(self, text):
        with self.error_lock:
            try:
                with open(self.errorlog, 'a') as my_file:
                    my_file.write(f"\n{text}")
                    my_file.flush()
            except:
                print(f"Couldn't open {self.errorlog} for appending")
                exit(1)


if __name__ == '__main__':
    cc = MultiThreadedCrawler("https://www.goodreads.com/shelf/show/technology?page=1")
    cc.run_web_crawler()
    cc.info()
