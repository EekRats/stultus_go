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
with open("seed_urls.csv", "r") as f:
    for line in f:
        url = line.strip()
        if url and not scraper.exists(url, 'url'):
            scraper.enqueue_url(url)

scraper.log("Started scraping")

while True:
    url = scraper.pop_next_url()
    if url is None:
        # either rotated due to domain-balancing or queue empty
        if scraper.queue_size() == 0:
            break
        else:
            time.sleep(SLEEP_TIME)
            continue

    if scraper.exists(url, 'url'):
        continue

    try:
        links_to_scrape = scraper.store(url)

        for i in links_to_scrape:
            if not scraper.exists(i, 'url'):
                scraper.enqueue_url(i)

        scraper.log(f"Scraped {url}")
        total_scraped += 1
    except Exception as e:
        scraper.log(f"Error scraping {url}: {e}")

    if total_scraped % 10 == 0:
        print(f"Scraped {total_scraped} pages. {scraper.queue_size()} URLs left in queue")

    time.sleep(SLEEP_TIME)

scraper.log("Finished scraping")