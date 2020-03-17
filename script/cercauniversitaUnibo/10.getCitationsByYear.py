# -*- coding: UTF-8 -*-
import requests
import csv
import sys
import datetime
import time
import os 
import json
from glob import glob
import ast
import sqlite3
from sqlite3 import Error

import urllib.parse

sys.path.append('..')
import conf

apiURL_citations = "https://api.elsevier.com/content/abstract/citations"
pathPublicationsList = "../../data/input/cercauniversita/bologna/publications-list/"
fileProva = "6503879155.json"

def getPublicationPage(eids, max_retry=2, retry_delay=1):
	
	retry = 0
	cont = True
	while retry < max_retry and cont:

		params = {"apikey":apikeys.keys[0], "httpAccept":"application/json", "count"=200, "date": "2010-2020", "scopus_id": eids} #"query": query, "view": "COMPLETE", "start": start}
		r = requests.get(apiURL_citations, params=params)
				
		if r.status_code == 429:
			print ("Quota exceeded for key " + apikeys.keys[0] + " - EXIT.")
			apikeys.keys.pop(0)
		
		elif r.status_code > 200 and r.status_code < 500:
			print(u"{}: errore nella richiesta: {}".format(r.status_code, r.url))
			return None

		if r.status_code != 200:
			retry += 1
			if retry < max_retry:
				time.sleep(retry_delay)
			continue

		cont = False 
			 
	if retry >= max_retry: 
		return None 
 
	j = r.json() 
	j['request-time'] = str(datetime.datetime.now().utcnow())
	# TO DECODE:
	#oDate = datetime.datetime.strptime(json['request-time'], '%Y-%m-%d %H:%M:%S.%f')
	return j

with open(pathPublicationsList + fileProva) as json_file:
		data = json.load(json_file)
		eids = set()
		for entry in data["search-results"]["entry"]:
			eid = entry["eid"]
			eids.add(eid)
		
		eids_string = ",".join(eids)
		res = getPublicationPage(eids_string)
		
		#if not os.path.isdir(pathOutput):
		#	os.makedirs(pathOutput)
		#	
		#completepath = os.path.join(pathOutput, authorId + '.json')
		with open(fileProva.replace(".json","_citations.json"), 'w') as outfile:
			json.dump(j, outfile, indent=3)
		

