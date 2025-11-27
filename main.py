# Search Engine

import tokenizer
import scraper
import sqlite3
import time

DATABASE_PATH = "database.db"

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

def search(query):

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    weights = cursor.execute("SELECT * FROM weights;").fetchall()
    

    print(weights)
    word_weight = (weights[0][1])
    bigram_weight = (weights[1][1])
    trigram_weight = (weights[2][1])
    prefix_weight = (weights[3][1])

    tokenized = tokenizer.tokenize_all(query)

    # Build placeholder groups
    word_q = ",".join(["?"] * len(tokenized[0]))
    bigram_q = ",".join(["?"] * len(tokenized[1]))
    trigram_q = ",".join(["?"] * len(tokenized[2]))
    prefix_q = ",".join(["?"] * len(tokenized[3]))

    sql_query = f"""
    WITH scores AS (
        -- Words
        SELECT url_id,
            COUNT(*) * {word_weight} AS score
        FROM word_urls
        WHERE word_id IN (
            SELECT id FROM words
            WHERE word IN ({word_q})
        )
        GROUP BY url_id

        UNION ALL

        -- Bigrams
        SELECT url_id,
            COUNT(*) * {bigram_weight} AS score
        FROM bigram_urls
        WHERE bigram_id IN (
            SELECT id FROM bigrams
            WHERE bigram IN ({bigram_q})
        )
        GROUP BY url_id

        UNION ALL

        -- Trigrams
        SELECT url_id,
            COUNT(*) * {trigram_weight} AS score
        FROM trigram_urls
        WHERE trigram_id IN (
            SELECT id FROM trigrams
            WHERE trigram IN ({trigram_q})
        )
        GROUP BY url_id

        UNION ALL

        -- Prefixes
        SELECT url_id,
            COUNT(*) * {prefix_weight} AS score
        FROM prefix_urls
        WHERE prefix_id IN (
            SELECT id FROM prefixes
            WHERE prefix IN ({prefix_q})
        )
        GROUP BY url_id
    )

    SELECT urls.url,
        SUM(score) AS score
    FROM scores
    JOIN urls ON urls.id = scores.url_id
    GROUP BY urls.id
    ORDER BY score DESC
    LIMIT 10;
    """

    # Same params order as before, just concatenated
    params = (
        tokenized[0] +
        list(tokenized[1]) +
        list(tokenized[2]) +
        list(tokenized[3])
    )

    results = cursor.execute(sql_query, params).fetchall()

    print(results)

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

def store(url):
    text = scraper.get_main_text(url)
    tokens = tokenizer.tokenize_all(text)
    
    if text:
        print("Storing article text")
        
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


#query=input("Search query: ")
#query = "How do I hack a website"
#search(query)

#import os
#os.system("rm database.db")
create_database()

