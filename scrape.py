"""
The scraper file, run to begin scraping

To run:

Fill seed_urls.csv with urls you want the scraper to scrape first and get links from to continue scraping
(in the form of)
https://www.bbc.com/news/articles/c865weg99pwo|404
https://www.nytimes.com/

Run the file
"""

import scraper
import time

SLEEP_TIME = 0.5  # seconds between requests to avoid hammering servers
TIMEOUT_TIME = 10  # seconds to wait for fetching a page before skipping

scraper.create_database()

total_scraped = 0

# Seed URLs into DB-backed queue (skip those already in DB)
"""
# This is legacy from running on a single machine, we can probably delete this now that we have a shared queue
with open("seed_urls.csv", "r") as f:
    for line in f:
        url = line.strip()
        if url and not scraper.exists(url, 'url'):
            scraper.enqueue_url(url)
"""

scraper.log("Started scraping")

timed = time.time()
start = timed

while True:
    print(1, time.time()-timed)
    timed = time.time()
    
    url = scraper.pop_next_url()
    scraper.log(f"Starting scraping {url}")
    print(2, time.time()-timed)
    timed = time.time()

    if url is None:
        # either rotated due to domain-balancing or queue empty
        if scraper.queue_size() == 0:
            break
        else:
            time.sleep(SLEEP_TIME)
            continue
    print(3, time.time()-timed)
    timed = time.time()

    if scraper.exists(url, 'url'):
        continue

    print(4, time.time()-timed)
    timed = time.time()

    try:
        # enforce a network/read timeout for page fetch and parsing
        links_to_scrape = scraper.store(url, timeout=TIMEOUT_TIME)
        print(5, time.time()-timed)
        timed = time.time()
        total_links=0

        links_to_add_to_queue = []
        # Clean, deduplicate and filter links in bulk for performance
        raw_links = [i for i in links_to_scrape if "mailto:" not in i]

        seen = set()
        cleaned = []
        for link in raw_links:
            total_links += 1
            clean_link = link.split('?', 1)[0]
            if clean_link not in seen:
                seen.add(clean_link)
                cleaned.append(clean_link)

        # filter_new_urls checks both the queue and stored urls in one go
        links_to_add_to_queue = scraper.filter_new_urls(cleaned)

        scraper.enqueue_urls(links_to_add_to_queue)

        print(6, "links:", total_links, time.time()-timed)
        timed = time.time()


        scraper.log(f"Scraped {url}")
        total_scraped += 1
        print(7, time.time()-timed)
        timed = time.time()

    except Exception as e:
        scraper.log(f"Error scraping {url}: {e}")

    print(8, time.time()-timed)
    timed = time.time()


    if total_scraped % 10 == 0:
        print(f"Scraped {total_scraped} pages. {scraper.queue_size()} URLs left in queue")
    print(f"Scraped {total_scraped} pages. {scraper.queue_size()} URLs left in queue", time.time()-start)

    time.sleep(SLEEP_TIME)

scraper.log("Finished scraping")