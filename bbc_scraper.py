# THIS IS ANOTHER SHITTY SCRAPER JUST FOR BBC NEWS (lots of diverse data, scraper friendly), INTENDED FOR TESTING AND DEVELOPMENT OF THE DATABASE, IT WILL BE CHANGED






import time
import requests
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from collections import deque
import re
import main

# -----------------------------------------------------
# Robots.txt setup
# -----------------------------------------------------
rp = RobotFileParser()
rp.set_url("https://www.bbc.com/robots.txt")
rp.read()

def allowed(url):
    return rp.can_fetch("*", url)


# -----------------------------------------------------
# Helper: determine if a URL looks like a BBC article
# -----------------------------------------------------
article_pattern = re.compile(
    r"https:\/\/www\.bbc\.com\/news\/[a-zA-Z0-9\-/]*[0-9]{6,}$"
)

def is_article(url):
    return bool(article_pattern.search(url))


# -----------------------------------------------------
# The crawler
# -----------------------------------------------------
def crawl(start_url="https://www.bbc.com/news/"):

    import sqlite3
    visited = set()

    # Path to your database
    db_path = "database.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM urls;")
    rows = cursor.fetchall()
    for (url,) in rows:
        visited.add(url)
    conn.close()
    
    visited.add("https://www.bbc.com/undefined")
    queue = deque([start_url])

    while queue:
        url = queue.popleft()

        if url in visited:
            continue

        visited.add(url)

        # Respect robots.txt
        if not allowed(url):
            print(f"[BLOCKED by robots.txt] {url}")
            continue

        print(f"[CRAWL] Fetching: {url}")

        try:
            resp = requests.get(
                url,
                timeout=10,
                headers={"User-Agent": "RespectfulBBCBot/1.0"},
            )
        except Exception as e:
            print(f"[ERROR] Could not fetch {url}: {e}")
            continue

        # Continue when 404 or any non-200
        if resp.status_code != 200:
            print(f"[SKIP] HTTP {resp.status_code} at {url}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract all links
        for tag in soup.find_all("a", href=True):
            link = tag["href"]
            link = urljoin(url, link)
            link = link.split("#")[0]

            # Only crawl BBC.com
            if not link.startswith("https://www.bbc.com"):
                continue

            if link not in visited:
                print(f"[FOUND] {link}")
                
                # If it's an article, store it
                main.store(link)
                    

                # Add to queue for crawling
                queue.append(link)

        # Be extra polite to avoid getting blocked
        time.sleep(2)   # 0.5 second between requests


if __name__ == "__main__":
    crawl()
