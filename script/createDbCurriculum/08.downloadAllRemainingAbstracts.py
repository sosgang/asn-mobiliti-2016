# -*- coding: UTF-8 -*-
import requests
import sqlite3
from sqlite3 import Error
from glob import glob
import json
import csv
import sys
import os
import logging
import datetime
import time
import ast
from collections import defaultdict 
import urllib.parse
import pickle

import conf
import mylib
sys.path.append('..')
import apikeys

tsvFN = "../../data/input/candidatesAsn2016.tsv"
pathAbstracts = "../../data/output/abstracts/"

fileAuthorDoisMapping_postStep07 = "../../data/input/04.authorDoisMapping_POST_STEP_07.json"

newAuthorDoisMapping = mylib.loadJson(fileAuthorDoisMapping_postStep07)
print ("CVs still to match = %d" % len(newAuthorDoisMapping.keys()))

eidDoiMap = dict()
doiEidMap = dict()
rows = mylib.select_doiEidMap(conf.dbFilename)
for row in rows:
	eid = row[0]
	doi = row[1]
	eidDoiMap[eid] = doi
	if doi is not None:
		doiEidMap[doi] = eid

doisToDownload = set()
with open(tsvFN, newline='') as tsvFile:
	spamreader = csv.DictReader(tsvFile, delimiter='\t')
	table = list(spamreader)
	for row in table:
		idCv = row["ID_CV"]
		if idCv not in newAuthorDoisMapping.keys():
			continue
		if "NO-CV-" in idCv:
			continue

		doisCandidate = ast.literal_eval(row["DOIS ESISTENTI"])
		for doiCandidate in doisCandidate:
			if doiCandidate not in doiEidMap.keys():
				doisToDownload.add(doiCandidate)

print ("DOIs to download = %d" % len(doisToDownload))

counter = 0
# DOWNLOAD FILTERED DOIs 
for doi in doisToDownload:
	# if the abstract for the DOI has not been downloaded
	if eid not in doiEidMap:
		counter += 1
		conn = mylib.create_connection(conf.dbFilename)
		with conn:
			print ('%d) Processing %s' % (counter,doi))
			jsonAbs = mylib.getAbstract(doi, 'DOI', apikeys.keys)
			if jsonAbs is not None:
				mylib.saveJsonAbstract(jsonAbs,pathAbstracts)
				print ('\tSaved to file.')
				
				try:
					eid = jsonAbs["abstracts-retrieval-response"]["coredata"]["eid"]
				except:
					print ("ERROR: NO EID - EXIT")
					sys.exit()
				create_eidDoi(conn,(eid,doi))
				eidDoiMap[eid] = doi
				doiEidMap[doi] = eid
			else:
				print ('\tNone -> not saved.')

print (counter)
