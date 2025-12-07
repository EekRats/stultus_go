"""
Provides scraping functions for scraper
"""

import requests
from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request
import time
import sqlite3
import tldextract
from urllib.parse import urlparse
import tokenizer
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin

DATABASE_PATH = "database.db"
USER_AGENT = "SearchEngineProjectBot/1.0 (+https://github.com/ThisIsNotANamepng/search_engine; hagenjj4111@uwec.edu)"

def create_database():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    ##TODO: Why is the text the primary key? Should it be the id?

    cursor.execute("CREATE TABLE IF NOT EXISTS words (word VARCHAR(64) NOT NULL, id INT NOT NULL,PRIMARY KEY (word));")
    cursor.execute("CREATE TABLE IF NOT EXISTS bigrams (bigram CHAR(2) PRIMARY KEY, id INT NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS trigrams (trigram CHAR(3) PRIMARY KEY, id INT NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS prefixes (prefix VARCHAR(64) NOT NULL, id INT NOT NULL,PRIMARY KEY (prefix));")
    cursor.execute("CREATE TABLE IF NOT EXISTS urls (url VARCHAR(64) NOT NULL, id INT NOT NULL,PRIMARY KEY (url));")

    cursor.execute("CREATE TABLE IF NOT EXISTS bigram_urls (bigram_id INT NOT NULL, url_id INT NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS trigram_urls (trigram_id INT NOT NULL, url_id INT NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS prefix_urls (prefix_id INT NOT NULL, url_id INT NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS word_urls (word_id INT NOT NULL, url_id INT NOT NULL);")

    cursor.execute("CREATE TABLE IF NOT EXISTS weights (type TEXT NOT NULL UNIQUE, weight FLOAT NOT NULL);")
    set_deafult_weights()

    """
    # I didn't build the count logic for the tokenizers because I'm lazy, we'll probably want to add counts later
    cursor.execute("CREATE TABLE bigram_urls (bigram_id INT NOT NULL, url_id INT NOT NULL, count INT NOT NULL);")
    cursor.execute("CREATE TABLE trigram_urls (trigram_id INT NOT NULL, url_id INT NOT NULL, count INT NOT NULL);")
    cursor.execute("CREATE TABLE prefix_urls (prefix_id INT NOT NULL, url_id INT NOT NULL, count INT NOT NULL);")
    cursor.execute("CREATE TABLE word_urls (word_id INT NOT NULL, url_id INT NOT NULL, count INT NOT NULL);")
    """

    #cursor.execute("CREATE TABLE typos (text TEXT, word TEXT)")
    connection.commit()
    connection.close()

def set_deafult_weights():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("INSERT OR REPLACE INTO weights (type, weight) VALUES ('word', 1.7);")
    cursor.execute("INSERT OR REPLACE INTO weights (type, weight) VALUES ('bigram', 1.2);")
    cursor.execute("INSERT OR REPLACE INTO weights (type, weight) VALUES ('trigram', 1.3);")
    cursor.execute("INSERT OR REPLACE INTO weights (type, weight) VALUES ('prefix', 1.2);")

    connection.commit()
    connection.close()

def exists(text, type):
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    if type=="word":
        res = cursor.execute("SELECT * FROM words WHERE word=?;", (text,)).fetchall()
    elif type=="bigram":
        res = cursor.execute("SELECT * FROM bigrams WHERE bigram=?;", (text,)).fetchall()
    elif type=="trigram":
        res = cursor.execute("SELECT * FROM trigrams WHERE trigram=?;", (text,)).fetchall()
    elif type=="prefix":
        res = cursor.execute("SELECT * FROM prefixes WHERE prefix=?;", (text,)).fetchall()
    elif type=="url":
        res = cursor.execute("SELECT * FROM urls WHERE url=?;", (text,)).fetchall()
    else:
        print("Invalid type")
        return False

    connection.close()

    if len(res)>0:
        return True
    else:
        return False

# Source - https://stackoverflow.com/a
# Posted by jbochi, modified by community. See post 'Timeline' for change history
# Retrieved 2025-12-04, License - CC BY-SA 3.0
# ----

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body, url):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.find_all(string=True)
    visible_texts = filter(tag_visible, texts)

    links = []

    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"            

    for link in soup.find_all('a', href=True):
        full_url = urljoin(base_url, link['href'])
        links.append(full_url)

    return [u" ".join(t.strip() for t in visible_texts), links]
# ----

def allowed_by_robots(url, user_agent):
    """
    Returns True if user_agent is allowed to fetch the URL according to robots.txt.
    """
    parsed = urlparse(url)
    robots_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", "robots.txt")

    rp = RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
    except Exception:
        # If robots.txt cannot be fetched, default to *allow*
        return True

    return rp.can_fetch(user_agent, url)

def get_main_text(url):
    # Gets the main text, checks robots.txt

    if not allowed_by_robots(url, USER_AGENT):
        log(f"Blocked by robots.txt: {url}")
        return ""
    
    headers = {
        'User-Agent': 'SearchEngineProjectBot/1.0 (+https://github.com/ThisIsNotANamepng/search_engine; hagenjj4111@uwec.edu)',
        'From': 'hagenjj4111@uwec.edu'
    }
    return(text_from_html(requests.get(url, headers=headers).content, url))

def log(message):
    with open("scraper.log", "a") as f:
        f.write(str(time.time()) + ": " + message + "\n")

def get_scraped_urls():

    visited = set()

    db_path = "database.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM urls;")
    rows = cursor.fetchall()
    for (url,) in rows:
        visited.add(url)
    conn.close()

    return visited

def get_base_domain(url):
    """
    Return the base (registrable) domain for a given URL, e.g.:
      "https://a.b.example.co.uk/x" -> "example.co.uk"
    """
    # Ensure parseable URL
    if "://" not in url:
        url = "http://" + url
    host = urlparse(url).hostname
    if not host:
        return ""
    # For IPs or localhost, tldextract returns empty suffix; just return host
    ext = tldextract.extract(host)
    if ext.registered_domain:
        return ext.registered_domain
    return host

def store(url):
    text = get_main_text(url)[0]
    links = get_main_text(url)[1]

    tokens = tokenizer.tokenize_all(text)
    
    if text:
        #print("Storing article text")
        
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()

        if not exists(url,"url"):

            try: new_id = cursor.execute("SELECT id FROM urls ORDER BY rowid DESC LIMIT 1;").fetchall()[0][0]
            except: new_id = 0
            cursor.execute("INSERT INTO urls (url,id) VALUES (?,?);", (url,new_id+1))

            url_id = new_id+1

            try: new_id = cursor.execute("SELECT id FROM words ORDER BY rowid DESC LIMIT 1;").fetchall()[0][0]
            except: new_id = 0
            words = set(tokens[0]) # Set() to make unique
            for i in words:
                if not exists(i,"word"):
                    new_id += 1
                    cursor.execute("INSERT INTO words (word,id) VALUES (?,?);", (i,new_id))


            try: new_id = cursor.execute("SELECT id FROM bigrams ORDER BY rowid DESC LIMIT 1;").fetchall()[0][0]
            except: new_id = 0
            bigrams = tokens[1]
            for i in bigrams:  # bigrams
                if not exists(i,"bigram"):
                    new_id += 1
                    cursor.execute("INSERT INTO bigrams (bigram,id) VALUES (?,?);", (i,new_id))


            try: new_id = cursor.execute("SELECT id FROM trigrams ORDER BY rowid DESC LIMIT 1;").fetchall()[0][0]
            except: new_id = 0
            trigrams = tokens[2]
            for i in trigrams:  # trigrams
                if not exists(i,"trigram"):
                    new_id += 1
                    cursor.execute("INSERT INTO trigrams (trigram,id) VALUES (?,?);", (i,new_id))


            try: new_id = cursor.execute("SELECT id FROM prefixes ORDER BY rowid DESC LIMIT 1;").fetchall()[0][0]
            except: new_id = 0
            prefixes = tokens[3]
            for i in prefixes:  # prefixes
                if not exists(i,"prefix"):
                    new_id += 1
                    cursor.execute("INSERT INTO prefixes (prefix,id) VALUES (?,?);", (i,new_id))


            for i in words:
                cursor.execute("INSERT INTO word_urls (word_id,url_id) VALUES ((SELECT id FROM words WHERE word=?),?);", (i,url_id))
            for i in bigrams:
                cursor.execute("INSERT INTO bigram_urls (bigram_id,url_id) VALUES ((SELECT id FROM bigrams WHERE bigram=?),?);", (i,url_id))
            for i in trigrams:
                cursor.execute("INSERT INTO trigram_urls (trigram_id,url_id) VALUES ((SELECT id FROM trigrams WHERE trigram=?),?);", (i,url_id))
            for i in prefixes:
                cursor.execute("INSERT INTO prefix_urls (prefix_id,url_id) VALUES ((SELECT id FROM prefixes WHERE prefix=?),?);", (i,url_id))


        connection.commit()
        connection.close()

    else:
        print("Failed to retrieve article text.")

    return links


def delete_url(url):
    """
    Deletes a URL and all references to it from the database.
    Assumes: cursor is from an open MySQL connection (or SQLite with identical schema).
    """

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()


    # 1. Get the URL's ID
    cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
    row = cursor.fetchone()
    if not row:
        return  # URL does not exist; nothing to delete

    url_id = row[0]

    # -------------------------
    # 2. Delete all relationships in mapping tables
    # -------------------------
    cursor.execute("DELETE FROM word_urls WHERE url_id = ?", (url_id,))
    cursor.execute("DELETE FROM bigram_urls WHERE url_id = ?", (url_id,))
    cursor.execute("DELETE FROM trigram_urls WHERE url_id = ?", (url_id,))
    cursor.execute("DELETE FROM prefix_urls WHERE url_id = ?", (url_id,))

    # -------------------------
    # 3. Delete the URL itself
    # -------------------------
    cursor.execute("DELETE FROM urls WHERE id = ?", (url_id,))

    # -------------------------
    # 4. OPTIONAL: Cleanup orphaned words / n-grams / prefixes
    #    (items that are no longer linked to any URL)
    # -------------------------
    # Words
    cursor.execute("""
        DELETE FROM words
        WHERE id NOT IN (SELECT DISTINCT word_id FROM word_urls)
    """)

    # Bigrams
    cursor.execute("""
        DELETE FROM bigrams
        WHERE id NOT IN (SELECT DISTINCT bigram_id FROM bigram_urls)
    """)

    # Trigrams
    cursor.execute("""
        DELETE FROM trigrams
        WHERE id NOT IN (SELECT DISTINCT trigram_id FROM trigram_urls)
    """)

    # Prefixes
    cursor.execute("""
        DELETE FROM prefixes
        WHERE id NOT IN (SELECT DISTINCT prefix_id FROM prefix_urls)
    """)

    connection.commit()
    connection.close()
