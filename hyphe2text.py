# -*- coding: utf-8 -*-

import pymongo
import csv
import os

settings = {
	'port': 27017,
	'corpus_id': 'gearnews',
	'output_path': 'data' # Note: a folder named as the corpus id will be created
}

page_metadata = [
	'_id',
	'url',
	'depth',
	'timestamp',
	'size',
	'content_type',
	'encoding',
	'lru',
	'lrulinks',
	'status',
	'error',
	'forgotten',
	# '_job',
]

# FUNCTIONS
def processPage(page_writer, page):
	print('Parsing ' + page['url'])
	elements = [page[k] if k in page else '' for k in page_metadata]
	page_writer.writerow(elements)

def checkPath(filename):
	if not os.path.exists(os.path.dirname(filename)):
	    try:
	        os.makedirs(os.path.dirname(filename))
	    except OSError as exc: # Guard against race condition
	        if exc.errno != errno.EEXIST:
	            raise

# SCRIPT
from pymongo import MongoClient
client = MongoClient('localhost', settings['port'])
db = client['hyphe_' + settings['corpus_id']]
pages = db.pages

pages_csv_filename = settings['output_path']+'/'+settings['corpus_id']+'/pages.csv'
checkPath(pages_csv_filename)

with open(pages_csv_filename, mode='wb') as page_file:
	page_writer = csv.writer(page_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	page_writer.writerow(page_metadata+['path'])
	for page in pages.find():

		processPage(page_writer, page)

