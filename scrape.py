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

SLEEP_TIME = 2  # seconds between requests to avoid hammering servers

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
    #print(1, time.time()-timed)
    timed = time.time()
    
    url = scraper.pop_next_url()
    print(url)
    #print(2, time.time()-timed)
    timed = time.time()

    if url is None:
        # either rotated due to domain-balancing or queue empty
        if scraper.queue_size() == 0:
            break
        else:
            time.sleep(SLEEP_TIME)
            continue
    #print(3, time.time()-timed)
    timed = time.time()


    if scraper.exists(url, 'url'):
        continue

    #print(4, time.time()-timed)
    timed = time.time()


    try:
        links_to_scrape = scraper.store(url)
        #print(5, time.time()-timed)
        timed = time.time()
        total_links=0

        links_to_add_to_queue = []


        for i in links_to_scrape:
            total_links+=1

            # Logic to filter out urls we don't want
            if not scraper.exists(i, 'url') and "mailto:" not in i:

                clean_url = url.split('?', 1)[0] # Takes out post data

                links_to_add_to_queue.append(clean_url)

        scraper.enqueue_urls(links_to_add_to_queue)

        #print(6, "links:", total_links, time.time()-timed)
        timed = time.time()


        scraper.log(f"Scraped {url}")
        total_scraped += 1
        #print(7, time.time()-timed)
        timed = time.time()

    except Exception as e:
        scraper.log(f"Error scraping {url}: {e}")

    #print(8, time.time()-timed)
    timed = time.time()


    if total_scraped % 10 == 0:
        print(f"Scraped {total_scraped} pages. {scraper.queue_size()} URLs left in queue")
    print(f"Scraped {total_scraped} pages. {scraper.queue_size()} URLs left in queue", time.time()-start)

    time.sleep(SLEEP_TIME)

scraper.log("Finished scraping")