"""
PostgreSQL-backed scraper module (ported from sqlite3_scraper.py).

Usage:
 - Set `DATABASE_URL` environment variable (or PGHOST/PGUSER/PGPASSWORD/PGDATABASE/PGPORT).
 - Install dependency: `psycopg2-binary`.

This file mirrors the original SQLite implementation but uses psycopg2/Postgres.
"""

import os
import time
import requests
import socket
from bs4 import BeautifulSoup
from bs4.element import Comment
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
import tldextract
import tokenizer
import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras
from psycopg2.extras import execute_values

USER_AGENT = "SearchEngineProjectBot/1.0 (+https://github.com/ThisIsNotANamepng/search_engine; hagenjj4111@uwec.edu)"
# export DATABASE_URL="postgres://postgres:DlIR9P2EcH3140xzJojd1B5QK50sh3FxQIxORB59hAK1U@172.233.221.151:5432/search_engine"

def get_conn():
	"""Return a new psycopg2 connection using `DATABASE_URL` or PG_* env vars."""
	#database_url = os.getenv("DATABASE_URL")
	#if database_url:
	#	return psycopg2.connect(database_url)

	# Build from individual env vars with sane defaults
	host = os.getenv("PGHOST", "172.233.221.151")
	port = os.getenv("PGPORT", "5432")
	user = os.getenv("PGUSER", "postgres")
	password = os.getenv("PGPASSWORD", "DlIR9P2EcH3140xzJojd1B5QK50sh3FxQIxORB59hAK1U")
	dbname = os.getenv("PGDATABASE", "search_engine")

	return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)


def create_database():
	conn = get_conn()
	cur = conn.cursor()

	# create sequences for stable id generation and tables with id defaults
	cur.execute("""
	CREATE SEQUENCE IF NOT EXISTS words_id_seq;
	CREATE SEQUENCE IF NOT EXISTS bigrams_id_seq;
	CREATE SEQUENCE IF NOT EXISTS trigrams_id_seq;
	CREATE SEQUENCE IF NOT EXISTS prefixes_id_seq;
	CREATE SEQUENCE IF NOT EXISTS urls_id_seq;

	CREATE TABLE IF NOT EXISTS words (
	    word VARCHAR(64) NOT NULL PRIMARY KEY,
	    id INT NOT NULL DEFAULT nextval('words_id_seq')
	);
	CREATE TABLE IF NOT EXISTS bigrams (
	    bigram CHAR(2) PRIMARY KEY,
	    id INT NOT NULL DEFAULT nextval('bigrams_id_seq')
	);
	CREATE TABLE IF NOT EXISTS trigrams (
	    trigram CHAR(3) PRIMARY KEY,
	    id INT NOT NULL DEFAULT nextval('trigrams_id_seq')
	);
	CREATE TABLE IF NOT EXISTS prefixes (
	    prefix VARCHAR(64) NOT NULL PRIMARY KEY,
	    id INT NOT NULL DEFAULT nextval('prefixes_id_seq')
	);
	CREATE TABLE IF NOT EXISTS urls (
	    url VARCHAR(2048) NOT NULL PRIMARY KEY,
	    id INT NOT NULL DEFAULT nextval('urls_id_seq')
	);

	CREATE TABLE IF NOT EXISTS bigram_urls (bigram_id INT NOT NULL, url_id INT NOT NULL);
	CREATE TABLE IF NOT EXISTS trigram_urls (trigram_id INT NOT NULL, url_id INT NOT NULL);
	CREATE TABLE IF NOT EXISTS prefix_urls (prefix_id INT NOT NULL, url_id INT NOT NULL);
	CREATE TABLE IF NOT EXISTS word_urls (word_id INT NOT NULL, url_id INT NOT NULL);

	CREATE TABLE IF NOT EXISTS weights (type TEXT PRIMARY KEY, weight FLOAT NOT NULL);
	""")

	# create queue and logs tables as well
	_extend_create_database_tables(cur)

	conn.commit()
	cur.close()
	conn.close()

	set_default_weights()


def set_default_weights():
	conn = get_conn()
	cur = conn.cursor()

	# Upsert default weights
	cur.execute("""
	INSERT INTO weights (type, weight) VALUES (%s, %s)
	ON CONFLICT (type) DO UPDATE SET weight = EXCLUDED.weight;
	""", ("word", 1.7))
	cur.execute("""
	INSERT INTO weights (type, weight) VALUES (%s, %s)
	ON CONFLICT (type) DO UPDATE SET weight = EXCLUDED.weight;
	""", ("bigram", 1.2))
	cur.execute("""
	INSERT INTO weights (type, weight) VALUES (%s, %s)
	ON CONFLICT (type) DO UPDATE SET weight = EXCLUDED.weight;
	""", ("trigram", 1.3))
	cur.execute("""
	INSERT INTO weights (type, weight) VALUES (%s, %s)
	ON CONFLICT (type) DO UPDATE SET weight = EXCLUDED.weight;
	""", ("prefix", 1.2))

	conn.commit()
	cur.close()
	conn.close()


def exists(text, type_):
	# Keep function for compatibility but prefer upserts/bulk operations.
	conn = get_conn()
	cur = conn.cursor()

	if type_ == "word":
		cur.execute("SELECT 1 FROM words WHERE word = %s;", (text,))
	elif type_ == "bigram":
		cur.execute("SELECT 1 FROM bigrams WHERE bigram = %s;", (text,))
	elif type_ == "trigram":
		cur.execute("SELECT 1 FROM trigrams WHERE trigram = %s;", (text,))
	elif type_ == "prefix":
		cur.execute("SELECT 1 FROM prefixes WHERE prefix = %s;", (text,))
	elif type_ == "url":
		cur.execute("SELECT 1 FROM urls WHERE url = %s;", (text,))
	else:
		cur.close()
		conn.close()
		return False

	found = cur.fetchone() is not None

	cur.close()
	conn.close()
	return found


# HTML text extraction utilities (copied from original file)
def tag_visible(element):
	if element.parent.name in ["style", "script", "head", "title", "meta", "[document]"]:
		return False
	if isinstance(element, Comment):
		return False
	return True


def text_from_html(body, url):
	soup = BeautifulSoup(body, "html.parser")
	texts = soup.find_all(string=True)
	visible_texts = filter(tag_visible, texts)

	links = []

	parsed = urlparse(url)
	base_url = f"{parsed.scheme}://{parsed.netloc}"

	for link in soup.find_all("a", href=True):
		full_url = urljoin(base_url, link["href"])
		links.append(full_url)

	return [u" ".join(t.strip() for t in visible_texts), links]


def allowed_by_robots(url, user_agent):
	parsed = urlparse(url)
	robots_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", "robots.txt")

	rp = RobotFileParser()
	try:
		rp.set_url(robots_url)
		rp.read()
	except Exception:
		return True

	return rp.can_fetch(user_agent, url)


def get_main_text(url):
	if not allowed_by_robots(url, USER_AGENT):
		log(f"Blocked by robots.txt: {url}")
		return "", []

	headers = {
		"User-Agent": USER_AGENT,
		"From": "hagenjj4111@uwec.edu"
	}
	r = requests.get(url, headers=headers)
	return text_from_html(r.content, url)


def log(message):
	# write to local file
	with open("scraper.log", "a") as f:
		f.write(str(time.time()) + ": " + message + "\n")
	# attempt to write to DB logs table; don't raise if DB unavailable
	try:
		log_db(message)
	except Exception:
		pass


def get_scraped_urls():
	visited = set()
	conn = get_conn()
	cur = conn.cursor()
	cur.execute("SELECT url FROM urls;")
	rows = cur.fetchall()
	for (url,) in rows:
		visited.add(url)
	cur.close()
	conn.close()
	return visited


def get_base_domain(url):
	if "://" not in url:
		url = "http://" + url
	host = urlparse(url).hostname
	if not host:
		return ""
	ext = tldextract.extract(host)
	if ext.registered_domain:
		return ext.registered_domain
	return host


def store(url):
	text, links = get_main_text(url)

	tokens = tokenizer.tokenize_all(text)

	if not text:
		print("Failed to retrieve article text.")
		return links

	# tokens: [words, bigrams, trigrams, prefixes]
	words = set(tokens[0]) if tokens and len(tokens) > 0 else set()
	bigrams = set(tokens[1]) if tokens and len(tokens) > 1 else set()
	trigrams = set(tokens[2]) if tokens and len(tokens) > 2 else set()
	prefixes = set(tokens[3]) if tokens and len(tokens) > 3 else set()

	conn = get_conn()
	cur = conn.cursor()

	# Upsert the URL and get its id. Use RETURNING id when inserting; else SELECT.
	cur.execute("INSERT INTO urls (url) VALUES (%s) ON CONFLICT (url) DO NOTHING RETURNING id;", (url,))
	row = cur.fetchone()
	if row:
		url_id = row[0]
	else:
		cur.execute("SELECT id FROM urls WHERE url = %s;", (url,))
		url_id = cur.fetchone()[0]

	# Bulk insert words/bigrams/trigrams/prefixes using execute_values for speed.
	if words:
		extra_vals = [(w,) for w in words]
		extras.execute_values(cur,
			"INSERT INTO words (word) VALUES %s ON CONFLICT (word) DO NOTHING;",
			extra_vals,
			template=None)

	if bigrams:
		extra_vals = [(b,) for b in bigrams]
		extras.execute_values(cur,
			"INSERT INTO bigrams (bigram) VALUES %s ON CONFLICT (bigram) DO NOTHING;",
			extra_vals)

	if trigrams:
		extra_vals = [(t,) for t in trigrams]
		extras.execute_values(cur,
			"INSERT INTO trigrams (trigram) VALUES %s ON CONFLICT (trigram) DO NOTHING;",
			extra_vals)

	if prefixes:
		extra_vals = [(p,) for p in prefixes]
		extras.execute_values(cur,
			"INSERT INTO prefixes (prefix) VALUES %s ON CONFLICT (prefix) DO NOTHING;",
			extra_vals)

	# Fetch ids for all tokens in bulk
	def fetch_id_map(column, table, items):
		if not items:
			return {}
		cur.execute(sql.SQL("SELECT id, {col} FROM {tbl} WHERE {col} = ANY(%s);").format(
			col=sql.Identifier(column), tbl=sql.Identifier(table)
		), (list(items),))
		rows = cur.fetchall()
		return {val: id for (id, val) in rows}

	word_map = fetch_id_map('word', 'words', list(words))
	bigram_map = fetch_id_map('bigram', 'bigrams', list(bigrams))
	trigram_map = fetch_id_map('trigram', 'trigrams', list(trigrams))
	prefix_map = fetch_id_map('prefix', 'prefixes', list(prefixes))

	# Prepare mapping inserts and bulk insert them
	word_url_pairs = [(word_map[w], url_id) for w in words if w in word_map]
	bigram_url_pairs = [(bigram_map[b], url_id) for b in bigrams if b in bigram_map]
	trigram_url_pairs = [(trigram_map[t], url_id) for t in trigrams if t in trigram_map]
	prefix_url_pairs = [(prefix_map[p], url_id) for p in prefixes if p in prefix_map]

	if word_url_pairs:
		extras.execute_values(cur,
			"INSERT INTO word_urls (word_id, url_id) VALUES %s;",
			word_url_pairs)

	if bigram_url_pairs:
		extras.execute_values(cur,
			"INSERT INTO bigram_urls (bigram_id, url_id) VALUES %s;",
			bigram_url_pairs)

	if trigram_url_pairs:
		extras.execute_values(cur,
			"INSERT INTO trigram_urls (trigram_id, url_id) VALUES %s;",
			trigram_url_pairs)

	if prefix_url_pairs:
		extras.execute_values(cur,
			"INSERT INTO prefix_urls (prefix_id, url_id) VALUES %s;",
			prefix_url_pairs)

	conn.commit()
	cur.close()
	conn.close()

	return links


def delete_url(url):
	conn = get_conn()
	cur = conn.cursor()

	cur.execute("SELECT id FROM urls WHERE url = %s", (url,))
	row = cur.fetchone()
	if not row:
		cur.close()
		conn.close()
		return

	url_id = row[0]

	cur.execute("DELETE FROM word_urls WHERE url_id = %s", (url_id,))
	cur.execute("DELETE FROM bigram_urls WHERE url_id = %s", (url_id,))
	cur.execute("DELETE FROM trigram_urls WHERE url_id = %s", (url_id,))
	cur.execute("DELETE FROM prefix_urls WHERE url_id = %s", (url_id,))

	cur.execute("DELETE FROM urls WHERE id = %s", (url_id,))

	# cleanup orphaned entries
	cur.execute("DELETE FROM words WHERE id NOT IN (SELECT DISTINCT word_id FROM word_urls)")
	cur.execute("DELETE FROM bigrams WHERE id NOT IN (SELECT DISTINCT bigram_id FROM bigram_urls)")
	cur.execute("DELETE FROM trigrams WHERE id NOT IN (SELECT DISTINCT trigram_id FROM trigram_urls)")
	cur.execute("DELETE FROM prefixes WHERE id NOT IN (SELECT DISTINCT prefix_id FROM prefix_urls)")

	conn.commit()
	cur.close()
	conn.close()



# Queue and logging helpers (Postgres-backed)
def enqueue_url(url):
	"""Insert a URL into the queue if it's not already present."""
	conn = get_conn()
	cur = conn.cursor()
	cur.execute("INSERT INTO url_queue (url) VALUES (%s) ON CONFLICT (url) DO NOTHING;", (url,))
	conn.commit()
	cur.close()
	conn.close()


def queue_size():
	conn = get_conn()
	cur = conn.cursor()
	cur.execute("SELECT COUNT(*) FROM url_queue;")
	count = cur.fetchone()[0]
	cur.close()
	conn.close()
	return count


def pop_next_url():
	"""Pop the next URL from the queue and return it.
	If the queue's first two URLs are from the same base domain, rotate the first URL to the end and return None.
	If the queue is empty, return None.
	"""
	conn = get_conn()
	cur = conn.cursor()

	cur.execute("SELECT id, url FROM url_queue ORDER BY id LIMIT 2;")
	rows = cur.fetchall()
	if not rows:
		cur.close()
		conn.close()
		return None

	if len(rows) == 1:
		row = rows[0]
		cur.execute("DELETE FROM url_queue WHERE id = %s;", (row[0],))
		conn.commit()
		cur.close()
		conn.close()
		return row[1]

	# Two rows: check domain
	first_id, first_url = rows[0]
	
	second_id, second_url = rows[1]
	if get_base_domain(first_url) == get_base_domain(second_url):
		# rotate: remove first then reinsert it so it goes to the end
		cur.execute("DELETE FROM url_queue WHERE id = %s;", (first_id,))
		cur.execute("INSERT INTO url_queue (url) VALUES (%s) ON CONFLICT (url) DO NOTHING;", (first_url,))
		conn.commit()
		cur.close()
		conn.close()
		return None

	# otherwise pop the first
	cur.execute("DELETE FROM url_queue WHERE id = %s;", (first_id,))
	conn.commit()
	cur.close()
	conn.close()
	return first_url


def get_host_ip():
	"""Return the host machine IP address. Try local hostname first, then fallback to external lookup."""
	try:
		host_ip = socket.gethostbyname(socket.gethostname())
		if host_ip and not host_ip.startswith("127."):
			return host_ip
	except Exception:
		pass
	# fallback to external service
	try:
		resp = requests.get("https://api.ipify.org", timeout=5)
		if resp.status_code == 200:
			return resp.text.strip()
	except Exception:
		pass
	return "unknown"


def log_db(message):
	"""Insert a log message into the `logs` table with timestamp and host IP."""
	conn = get_conn()
	cur = conn.cursor()
	ip = get_host_ip()
	cur.execute("INSERT INTO logs (ip, message) VALUES (%s, %s);", (ip, message))
	conn.commit()
	cur.close()
	conn.close()

"""
def enqueue_urls(urls):
	conn = get_conn()
	cur = conn.cursor()
	for u in urls:
		cur.execute("INSERT INTO url_queue (url) VALUES (%s) ON CONFLICT (url) DO NOTHING;", (u,))
	conn.commit()
	cur.close()
	conn.close()
"""
def enqueue_urls(urls):
    conn = get_conn()
    cur = conn.cursor()

    query = """
        INSERT INTO url_queue (url)
        VALUES %s
        ON CONFLICT (url) DO NOTHING;
    """

    # execute_values handles building the bulk values list efficiently
    execute_values(cur, query, [(u,) for u in urls])

    conn.commit()
    cur.close()
    conn.close()

# Ensure queue and logs tables are created when creating DB
def _extend_create_database_tables(cur):
	cur.execute("""
	CREATE TABLE IF NOT EXISTS url_queue (
		id SERIAL PRIMARY KEY,
		url VARCHAR(2048) UNIQUE NOT NULL,
		enqueued_at TIMESTAMP DEFAULT now()
	);

	CREATE TABLE IF NOT EXISTS logs (
		id SERIAL PRIMARY KEY,
		ts TIMESTAMP DEFAULT now(),
		ip VARCHAR(64),
		message TEXT
	);
	""")
