# -*- coding: utf-8 -*-

import sys
import jsonrpclib
import pymongo
import csv
import os
import re

settings = {
	'mongodb_port': 27017,
	'hyphe_url': 'localhost/api/',
	'hyphe_port': '80',
	'corpus_id': 'gearnews',
	'webentities_in': True,
	'webentities_out': True,
	'webentities_undecided': True,
	'webentities_discovered': True,
	'output_path': 'data', # Note: a folder named as the corpus id will be created
}

we_metadata = [
	'_id',
	'name',
	'status',
	'crawled',
	'homepage',
	'prefixes',
	'startpages',
	# 'lastModificationDate',
	# 'creationDate',
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
	elements += [we_to_filename(we)]
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

def slugify(value):
	"""
	Normalizes string, converts to lowercase, removes non-alpha characters,
	and converts spaces to hyphens.
	"""
	import unicodedata
	value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
	value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
	value = unicode(re.sub('[-\s]+', '-', value))
	return value

def we_to_filename(we):
	return '%s - '%we['_id'] + slugify(we['name'])

# SCRIPT

# Hyphe connect
try:
	hyphe_api=jsonrpclib.Server('http://%s:%s'%(settings['hyphe_url'], settings['hyphe_port']), version=1)
except Exception as e:
    sys.stderr.write("%s: %s\n" % (type(e), e))
    sys.stderr.write('ERROR: Could not initiate connection to hyphe core\n')
    sys.exit(1)
res = hyphe_api.ping(settings['corpus_id'], 10)
if "message" in res:
	sys.stderr.write("ERROR: please start or create corpus %s before indexing it: %s\n" % (settings['corpus_id'], res['message']))
	sys.exit(1)

# MongoDB connect
from pymongo import MongoClient
client = MongoClient('localhost', settings['mongodb_port'])
db = client['hyphe_' + settings['corpus_id']]

# Web entities
wes_csv_filename = settings['output_path']+'/'+settings['corpus_id']+'/webentities.csv'
checkPath(wes_csv_filename)
page_index = {}
wes_csv_filename = settings['output_path']+'/'+settings['corpus_id']+'/webentities.csv'
we_status = []
we_status +=['IN'] if settings['webentities_in'] else []
we_status +=['OUT'] if settings['webentities_out'] else []
we_status +=['UNDECIDED'] if settings['webentities_undecided'] else []
we_status +=['DISCOVERED'] if settings['webentities_discovered'] else []
wes_all = []
for status in we_status :
	print('')
	print('%s web entities'%status)
	print('----------------------------------------')
	wes = []
	res = hyphe_api.store.get_webentities_by_status(status, None, 500, 0, 'false', 'false', settings['corpus_id'])['result']
	wes += res['webentities']
	while res['next_page']:
	    res = hyphe_api.store.get_webentities_page(res['token'], res['next_page'], settings['corpus_id'])['result']
	    wes += res['webentities']
	we_total = len(wes)
	print('-> %s web entities to process'%we_total)
	we_current = 0
	for we in wes:
		we_current += 1
		we_pages = hyphe_api.store.get_webentity_pages(we['id'], True, settings['corpus_id'])
		if (we_pages['code'] == 'fail'):
			print("ERROR with pages for WE %s: %s" % (we['id'], we_pages['message']))
		else :
			for page in we_pages['result']:
				page_index[page['lru']] = we['_id']
		if we_current%100 == 0 :
			print('%s web entities processed'%we_current)
	print('-> All %s web entities processed.'%status)
	wes_all += wes

print('')
print('Building Web entities CSV...')
with open(wes_csv_filename, mode='wb') as we_file:
	we_writer = csv.writer(we_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	we_writer.writerow(we_metadata+['folder'])
	for we in wes_all:
		processWE(we_writer, we)

# wes = db.webentities
# wes_csv_filename = settings['output_path']+'/'+settings['corpus_id']+'/webentities.csv'
# checkPath(wes_csv_filename)

# with open(wes_csv_filename, mode='wb') as we_file:
# 	we_writer = csv.writer(we_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
# 	we_writer.writerow(we_metadata+['path'])
# 	for we in wes.find():
# 		processWE(we_writer, we)

# Pages
print('')
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
