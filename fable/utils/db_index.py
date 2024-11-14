"""
Create Index for different collections used by ReorgPageFinder
"""
from pymongo import MongoClient
import pymongo

from fable import config

db = config.DB

db.crawl.create_index([('html', pymongo.HASHED)])
db.crawl.create_index([('site', pymongo.ASCENDING), ('url', pymongo.ASCENDING)], unique=True)


db.searched.create_index([('query', pymongo.ASCENDING), ('engine', pymongo.ASCENDING)])
db.searched.create_index([('query', pymongo.ASCENDING), ('engine', pymongo.ASCENDING), ('site', pymongo.ASCENDING)])

db.wayback_rep.create_index([('site', pymongo.ASCENDING), ('url', pymongo.ASCENDING)], unique=True)

db.wayback_index.create_index([('url', pymongo.ASCENDING)], unique=True)

db.traces.create_index([('url', pymongo.ASCENDING)], unique=True)