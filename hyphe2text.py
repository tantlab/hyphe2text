# -*- coding: utf-8 -*-

import pymongo
import csv
import os

settings = {
	'port': 27017,
	'corpus_id': 'gearnews',
	'output_path': 'data' # Note: a folder named as the corpus id will be created
}

we_metadata = [
	'_id',
	'name',
	'status',
	'crawled',
	'prefixes',
	'homepage',
	'startpages',
	'lastModificationDate',
	'creationDate',
	'tags',
]

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
def processWE(we_writer, we):
	elements = [we[k] if k in we else '' for k in we_metadata]
	we_writer.writerow(elements)

def processPage(page_writer, page):
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

# Web entities
print('Building Web entities CSV...')
wes = db.webentities
wes_csv_filename = settings['output_path']+'/'+settings['corpus_id']+'/webentities.csv'
checkPath(wes_csv_filename)

with open(wes_csv_filename, mode='wb') as we_file:
	we_writer = csv.writer(we_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	we_writer.writerow(we_metadata+['path'])
	for we in wes.find():
		processWE(we_writer, we)

# Pages
print('Building Pages CSV...')
pages = db.pages
pages_csv_filename = settings['output_path']+'/'+settings['corpus_id']+'/pages.csv'
checkPath(pages_csv_filename)

with open(pages_csv_filename, mode='wb') as page_file:
	page_writer = csv.writer(page_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	page_writer.writerow(page_metadata+['path'])
	for page in pages.find():

		processPage(page_writer, page)

print('Done.')
