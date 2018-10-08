# -*- coding: utf-8 -*-

import pymongo
import pprint

settings = {
	'port': 27017,
	'corpus_id': 'gearnews'
}

from pymongo import MongoClient
client = MongoClient('localhost', settings['port'])
db = client['hyphe_' + settings['corpus_id']]
pages = db.pages
pprint.pprint(pages.find_one())