# -*- coding: utf-8 -*-

import sys
import math
import jsonrpclib
import pymongo
import csv
import os
import re
import goose
import elasticsearch

settings = {
	'mongodb_port': 27017,
	'hyphe_host': 'localhost/api/',
	'hyphe_port': '80',
	'corpus_id': 'gearnews',
	'webentities_in': True,
	'webentities_out': True,
	'webentities_undecided': True,
	'webentities_discovered': False,

	'output_to_folder': False,
	'output_folder_path': 'data', # Note: a folder named as the corpus id will be created

	'output_to_elasticsearch': True, # Note: ES index is named as corpus id
	'elasticsearch_host': 'localhost',
	'elasticsearch_port': '9200',
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
def write_WE_in_CSV(we_writer, we):
	elements = [we[k] if k in we else '' for k in we_metadata]
	elements += [we_to_filename(we)]
	we_writer.writerow(elements)

def parse_page_body(page):
	from goose import Goose
	body = page["body"].decode('zip')
	try:
		body = body.decode(page.get("encoding",""))
	except Exception :
		body = body.decode("UTF8","replace")
	html_string = body.encode("utf-8")
	if html_string:
		try:
			extractor = Goose()
			article = extractor.extract(raw_html=html_string)
			text = article.cleaned_text
		except Exception as e:
			print('    Text extraction failed for %s - %s'%(page['lru'], str(e)))
			text = ''
		return text
	else:
		return ''

def write_page_in_CSV(page_writer, page, page_index, we_index, page_current, page_filename):
	elements = [page[k] if k in page else '' for k in page_metadata]
	we_id = page_index[page['lru']]
	we = we_index[we_id]
	elements += [we_id, we['name'], we['status'], page_filename]
	page_writer.writerow(elements)

def checkPath(filename):
	if not os.path.exists(os.path.dirname(filename)):
	    try:
	        os.makedirs(os.path.dirname(filename))
	    except OSError as exc: # Guard against race condition
	        if exc.errno != errno.EEXIST:
	            raise

def write_page_text_file(page, filename):
	text = parse_page_body(page)
	from goose import Goose
	if text:
		try:
			checkPath(filename)
			with open(filename, 'w') as result:
				result.write(text.encode('UTF8'))
		except Exception as e:
			print('    Writing file failed for %s - %s'%(page['lru'], str(e)))

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
	hyphe_api=jsonrpclib.Server('http://%s:%s'%(settings['hyphe_host'], settings['hyphe_port']), version=1)
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

# Web entities: prepare data
page_index = {}
we_index = {}
we_status = []
we_status +=['IN'] if settings['webentities_in'] else []
we_status +=['OUT'] if settings['webentities_out'] else []
we_status +=['UNDECIDED'] if settings['webentities_undecided'] else []
we_status +=['DISCOVERED'] if settings['webentities_discovered'] else []
wes_all = []
if settings['output_to_folder']:
	wes_csv_filename = settings['output_folder_path']+'/'+settings['corpus_id']+'/webentities.csv'
	checkPath(wes_csv_filename)
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
		we_index[we['_id']] = we
	print('-> All %s web entities processed.'%status)
	wes_all += wes

# Web entities: write CSV
if settings['output_to_folder']:
	print('')
	print('Building web entities CSV...')
	with open(wes_csv_filename, mode='wb') as we_file:
		we_writer = csv.writer(we_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
		we_writer.writerow(we_metadata+['folder'])
		for we in wes_all:
			write_WE_in_CSV(we_writer, we)

# Web entitites: store in ES
if settings['output_to_elasticsearch']:
	print('')
	print('Storing web entities in Elastic Search...')
	es = elasticsearch.Elasticsearch([{'host': settings['elasticsearch_host'], 'port': settings['elasticsearch_port']}])
	es.indices.delete(index=settings['corpus_id'], ignore=[400, 404])
	we_current = 0
	for we in wes_all:
		we_es = we.copy()
		we_es['id'] = we_es.pop('_id', None)
		we_es['type'] = 'webentity'
		es.index(index=settings['corpus_id'], doc_type='doc', id=we_es['id'], body=we_es)

# Pages: write CSV and text files
if settings['output_to_folder']:
	print('')
	print('Building pages CSV...')
	pages = db.pages
	pages_csv_filename = settings['output_folder_path']+'/'+settings['corpus_id']+'/pages.csv'
	checkPath(pages_csv_filename)
	with open(pages_csv_filename, mode='wb') as page_file:
		page_writer = csv.writer(page_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
		page_writer.writerow(page_metadata+['webentity id', 'webentity name', 'webentity status', 'text file path'])
		page_count = pages.count()
		print('-> %s pages to process'%page_count)
		page_current = 0
		for page in pages.find():
			page_current += 1
			page_filename = settings['output_folder_path']+'/'+settings['corpus_id']+'/'+we_to_filename(we)+'/'+str(page_current)+' - '+slugify(page['lru'])[:100]+'.txt'
			write_page_in_CSV(page_writer, page, page_index, we_index, page_current, page_filename)
			write_page_text_file(page, page_filename)
			if page_current%100 == 0 :
				percent = int(math.floor(100*page_current/page_count))
				print('... %s pages processed (%s%%)'%(page_current, percent))
		print('-> All pages processed.')

# Pages: store in ES
if settings['output_to_elasticsearch']:
	print('')
	print('Storing pages in Elastic Search...')
	pages = db.pages
	page_count = pages.count()
	print('-> %s pages to process'%page_count)
	page_current = 0
	for page in pages.find():
		page_current += 1
		page_es = page.copy()
		page_es.pop('_id', None)
		page_es.pop('body', None)
		page_es['type'] = 'page'
		page_es['text'] = parse_page_body(page)
		es.index(index=settings['corpus_id'], doc_type='doc', id=page_es['lru'], body=page_es)
		if page_current%100 == 0 :
			percent = int(math.floor(100*page_current/page_count))
			print('... %s pages processed (%s%%)'%(page_current, percent))
	print('-> All pages processed.')

print('')
print('\\O/ IT WORKED!')
