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
fileAuthorDoisMapping = "../../data/input/04.authorDoisMapping.json"
fileListaDoisToDownload = "../../data/input/04.doisToDownload-list.txt"
pathAbstracts = "../../data/output/abstracts/"

def computeAndSave_authorDoisMapping_listaDoisToDownload(fileMapping,fileLista,numDoisForeachCandidate=5):
	dois = set()
	authorDois = dict()
	doisCounter = defaultdict(int)

	# OUTPUT
	authorDoisMapping = dict()

	with open(tsvFN, newline='') as tsvFile:
		spamreader = csv.DictReader(tsvFile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			idCv = row["ID_CV"]
			doisCandidate = ast.literal_eval(row["DOIS ESISTENTI"])
			
			authorDoisMapping[idCv] = list()
			
			dois.update(doisCandidate)
			authorDois[idCv] = doisCandidate
			
			for doi in doisCandidate:
				#if doi in doisDict:
				#	temp = doisDict[doi]
				doisCounter[doi] += 1

	print ("Numero totale doi nel TSV: %d" % len(dois))

	doisToDownload = set()
	for idCv in authorDois:
		doisCandidate = authorDois[idCv]
		tempDict = dict()
		for doi in doisCandidate:
			tempDict[doi] = doisCounter[doi]
		
		doisCandidate_sorted = sorted(tempDict.items(), key=lambda x: x[1])
		for i in range(1,numDoisForeachCandidate+1):
			if i > len(doisCandidate_sorted):
				break
			curr = doisCandidate_sorted[len(doisCandidate_sorted)-i][0]
			doisToDownload.add(curr)
		
	print ("Numero doi da scaricare (prendendone %d per candidato): %d" % (numDoisForeachCandidate, len(doisToDownload)))

	# INPUT: authorDois, doisToDownload, authorDoisMapping (, fileMapping)
	for idCv in authorDois:
		for doi in authorDois[idCv]:
			if doi in doisToDownload:
				authorDoisMapping[idCv].append(doi)

	with open(fileMapping, 'w') as outfile:
		json.dump(authorDoisMapping, outfile, indent=3)

	# OUTPUT
	listaDoisToDownload = list(doisToDownload)

	with open(fileLista, "wb") as fp:	# pickling
		pickle.dump(listaDoisToDownload, fp)


def load_authorDoisMapping(fileMapping):
	with open(fileMapping, "r") as read_file:
		data = json.load(read_file)
		return data


def load__listaDoisToDownload(fileLista):
	with open(fileLista, "rb") as fp:	# unpickling
		lista = pickle.load(fp)
		return lista




#computeAndSave_authorDoisMapping_listaDoisToDownload(fileAuthorDoisMapping,fileListaDoisToDownload,5)
authorDoisMapping = load_authorDoisMapping(fileAuthorDoisMapping)
listaDoisToDownload = load__listaDoisToDownload(fileListaDoisToDownload)


#filename_regex = os.path.join(pathAuthorsSearch, cognome.replace(" ","-") + '_' + nome.replace(" ","-") + '*') 
contents = glob(pathAbstracts + "*.json")
contents.sort()
for filename_withPath in contents:
	with open(filename_withPath) as json_file:
		data = json.load(json_file)
		try:
			eid = data["abstracts-retrieval-response"]["coredata"]["eid"]
		except:
			print ("ERROR: NO EID - EXIT")
			sys.exit()
		try:
			doi = data["abstracts-retrieval-response"]["coredata"]["prism:doi"]
		except:
			doi = None
		print ("EID: %s - DOI: %s" % (eid,doi))

'''
for i in range(0,10):
	print ("%s - %s" % (listaDoisToDownload[i],lista[i]))

for doi in listaDoisToDownload:
	print ('Processing ' + doi)
	jsonAbs = mylib.getAbstract(doi, 'DOI', apikeys.keys)
	if jsonAbs is not None:
		mylib.saveJsonAbstract(jsonAbs,pathAbstracts)
		print ('\tSaved to file.')
	else:
		print ('\tNone -> not saved.')
'''
