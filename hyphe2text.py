# -*- coding: utf-8 -*-

import sys
import math
import jsonrpclib
import pymongo
import csv
import os
import re
import goose
from math import floor

settings = {
	'mongodb_port': 27017,
	'hyphe_url': 'localhost/api/',
	'hyphe_port': '80',
	'corpus_id': 'gearnews',
	'webentities_in': True,
	'webentities_out': True,
	'webentities_undecided': True,
	'webentities_discovered': False,
	'output_path': 'data', # Note: a folder named as the corpus id will be created
}

# METADATA SETTINGS
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

def processPage(page_writer, page, page_index, we_index):
	body = page["body"].decode('zip')
	try:
		body = body.decode(page.get("encoding",""))
	except Exception :
		body = body.decode("UTF8","replace")
	elements = [page[k] if k in page else '' for k in page_metadata]
	we_id = page_index[page['lru']]
	we = we_index[we_id]
	filename = settings['output_path']+'/'+we_to_filename(we)+'/'+slugify(page['lru'])
	elements += [we_id, we['name'], we['status'], filename]
	page_writer.writerow(elements)
	writePage(body.encode("utf-8"), filename, page['lru'])

def checkPath(filename):
	if not os.path.exists(os.path.dirname(filename)):
	    try:
	        os.makedirs(os.path.dirname(filename))
	    except OSError as exc: # Guard against race condition
	        if exc.errno != errno.EEXIST:
	            raise

def writePage(html_string, filename, lru):
	from goose import Goose
	checkPath(filename)
	if html_string:
		try:
			extractor = Goose()
			article = extractor.extract(raw_html=html_string)
			text = article.cleaned_text
		except Exception as e:
			print('    Text extraction failed for %s - %s'%(lru, str(e)))
			text = ''
		try:
			with open(filename, 'w') as result:
				result.write(text.encode('UTF8'))
		except Exception as e:
			print('    Writing file failed for %s - %s'%(lru, str(e)))

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
we_index = {}
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
			print('... %s web entities processed'%we_current)
	print('-> All %s web entities processed.'%status)
	wes_all += wes

print('')
print('Building Web entities CSV...')
with open(wes_csv_filename, mode='wb') as we_file:
	we_writer = csv.writer(we_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	we_writer.writerow(we_metadata+['folder'])
	for we in wes_all:
		processWE(we_writer, we)
		we_index[we['_id']] = we

# Pages
print('')
print('Building Pages CSV...')
pages = db.pages
pages_csv_filename = settings['output_path']+'/'+settings['corpus_id']+'/pages.csv'
checkPath(pages_csv_filename)
with open(pages_csv_filename, mode='wb') as page_file:
	page_writer = csv.writer(page_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	page_writer.writerow(page_metadata+['webentity id', 'webentity name', 'webentity status', 'text file path'])
	page_count = pages.count()
	print('-> %s pages to process'%page_count)
	page_current = 0
	for page in pages.find():
		page_current += 1
		processPage(page_writer, page, page_index, we_index)
		if page_current%100 == 0 :
			percent = int(floor(100*page_current/page_count))
			print('... %s pages processed (%s%%)'%(page_current, percent))
	print('-> All pages processed.')

print('')
print('\\O/ IT WORKED!')
