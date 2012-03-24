"""
Full text search based on tf*idf scores implemented using Redis.
"""

from redis import StrictRedis
from math import log
from collections import defaultdict

redis = StrictRedis()

def index(document):
    counter = int(redis.get("document.counter") or "0")
    counter += 1
    doc_key = "document:%d" % counter
    pipe = redis.pipeline()
    for word in document.split():
        pipe.sadd("word:%s" % word, counter)
        pipe.hincrby(doc_key, word, 1)
    pipe.execute()
    redis.incr("document.counter")

def index_file(path):
    index(open(path).read())

def query(terms):
    data = []
    scores = defaultdict(int)
    for term in terms.split():
        docs = redis.smembers("word:%s" % term)
        tf = lambda id: int(redis.hget("document:%s" % id, term) or "0") # Ignoring constant terms
        idf = 1 / len(docs) if docs else 0
        for doc in docs:
            scores[doc] += tf(doc)*idf
        data.append(set(docs))
    candidates = list(reduce(lambda x, y: x & y, data))
    candidates.sort(key=lambda id: scores[id])
    return candidates[::-1]    
    