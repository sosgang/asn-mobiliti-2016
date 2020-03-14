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

import urllib.parse

sys.path.append('..')
import apikeys

#anno = "2016"

apiURL_Search = "https://api.elsevier.com/content/search/scopus"
pathInput = "../../data/input/cercauniversita/bologna/authors-search/"
pathOutput = "../../data/input/cercauniversita/bologna/publications-list/"

#https://api.elsevier.com/content/search/scopus?apikey=f5f5306cfd6042a38e90dc053d410c56&httpAccept=application/json&query=AU-ID(55303032000)&view=COMPLETE&start=25&count=26
def getPublicationPage(authorId, start, max_retry=2, retry_delay=1):
#def getAbstract(doi, max_retry=2, retry_delay=1):
	
	retry = 0
	cont = True
	while retry < max_retry and cont:

		query = "AU-ID(" + authorId + ")"
		params = {"apikey":apikeys.keys[0], "httpAccept":"application/json", "query": query, "view": "COMPLETE", "start": start}
		r = requests.get(apiURL_Search, params=params)
				
		#if self.raw_output:
		#	self.save_raw_response(r.text)

		# quota exceeded -> http 429 (see https://dev.elsevier.com/api_key_settings.html)
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

def mergeJson(json1, json2):
	try:
		#numRes1 = int(json1["search-results"]["opensearch:totalResults"])
		#numRes2 = int(json2["search-results"]["opensearch:totalResults"])
		#json1["search-results"]["opensearch:totalResults"] = str(numRes1 + numRes2)
		pubs1 = json1["search-results"]["entry"]
		pubs2 = json2["search-results"]["entry"]
		pubs12 = pubs1 + pubs2
		json1["search-results"]["entry"] = pubs12
	except:
		print ("ERROR in mergeJson()")
	return json1
	
def getPublicationList(authorId):
	jFilename = pathOutput + authorId + ".json"
	if os.path.exists(jFilename):
		# json file already downloaded => return None
		return None
		#with open(jFilename) as json_file:
		#	j = json.load(json_file)
		#	return j
	else:
		#print ("NOT FOUND: " + authorId)
		j = getPublicationPage(authorId, 0)
		try:
			numResults = int(j["search-results"]["opensearch:totalResults"])
			numDownloaded = 25
			while numDownloaded < numResults:
				print (authorId)
				jPart = getPublicationPage(authorId, numDownloaded)
				j = mergeJson(j, jPart)
				numDownloaded += 25
		except:
			print ("ERROR in getPublicationList()")
		return j

##### TODO ##### TODO ##### TODO ##### TODO ##### TODO #####
# controlla che json dell'abstract ritornato da api sia ok
##### TODO ##### TODO ##### TODO ##### TODO ##### TODO #####
def checkAbsFormat(j):
	#print (j)
	numRes = int(j["search-results"]["opensearch:totalResults"])
	pubs = j["search-results"]["entry"]
	if numRes == len(pubs):
		return True
	else:
		print (j["search-results"]["opensearch:Query"]["@searchTerms"] + " - ERROR: numRes=" + str(numRes) + ", numPubs in Json=" + str(len(pubs)))
		return False
	
def saveJsonPubs(j, authorId, pathOutput):

	if (checkAbsFormat(j)):
		if not os.path.isdir(pathOutput):
			os.makedirs(pathOutput)
			
		completepath = os.path.join(pathOutput, authorId + '.json')

		with open(completepath, 'w') as outfile:
			json.dump(j, outfile, indent=3)
		
		return True
	else:
		return False







authorIds = set()

contents = glob(pathInput + '*.json')
contents.sort()
for fileAuthorSearch in contents:
	with open(fileAuthorSearch) as json_file:
		jAuthorSearch = json.load(json_file)
		totRes = int(jAuthorSearch["search-results"]["opensearch:totalResults"])
		for entry in jAuthorSearch["search-results"]["entry"]:
			authorId = entry["dc:identifier"].replace("AUTHOR_ID:","")
			authorIds.add(authorId)

print (len(authorIds))

for authorId in authorIds:
	j = getPublicationList(authorId)
	if j is not None and saveJsonPubs(j, authorId, pathOutput):
		print (authorId + ': Saved to file.')
	else:
		print (authorId + ': None -> not saved (i.e. not found or json already downloaded).')
