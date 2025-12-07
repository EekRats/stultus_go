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

urls_queue = []
scraped_urls = scraper.get_scraped_urls()
total_scraped = 0

# Seed URLs
with open("seed_urls.csv", "r") as f:
    for line in f:
        url = line.strip()
        if url and url not in urls_queue and url not in scraped_urls:
            urls_queue.append(url)

scraper.log("Started scraping")

while len(urls_queue)>0:
    url = urls_queue.pop(0)

    if url in scraped_urls:
        continuep
    elif scraper.get_base_domain(url) == scraper.get_base_domain(urls_queue[0]):
        # This makes sure we don't hammer a single domain with a bunch of requests, it doesn't scrape the same domain twice in a row
        # But this also means that if there is only one domain left in the queue, it will never scrape and the process will continue in an infinite loop
        urls_queue.append(url)
        continue

    try:
        links_to_scrape = scraper.store(url)

        for i in links_to_scrape:
            if i not in urls_queue and i not in scraped_urls:
                urls_queue.append(i)

        scraper.log(f"Scraped {url}")
        scraped_urls.add(url)
        total_scraped += 1
    except Exception as e:
        scraper.log(f"Error scraping {url}: {e}")
        scraped_urls.add(url)

    if total_scraped % 10 == 0:
        print(f"Scraped {total_scraped} pages. {len(urls_queue)} URLs left in queue")

    time.sleep(SLEEP_TIME)

scraper.log("Finished scraping")