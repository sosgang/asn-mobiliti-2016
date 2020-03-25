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


def create_eidDoi(conn, eidDoi):
	"""
	Create a new eidDoi mapping into the eidDoi table
	:param conn:
	:param dieDoi:
	:return: eidDoi id
	"""
	sql = ''' INSERT INTO eidDoi(eid,doi)
			  VALUES(?,?) '''
	cur = conn.cursor()
	cur.execute(sql, eidDoi)
	return cur.lastrowid


def populateDbEidDoi(dbFilename,path):
	sql_create_eidDoi_table = """ CREATE TABLE IF NOT EXISTS eidDoi (
										eid text NOT NULL,
										doi text
									  ); """
	
	# create a database connection
	conn = mylib.create_connection(dbFilename)
 
	# create tables
	if conn is not None:
		mylib.create_table(conn, sql_create_eidDoi_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	
	# populate table eidDoi
	conn = mylib.create_connection(dbFilename)
	with conn:
		contents = glob(path + "*.json")
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
				#print ("EID: %s - DOI: %s" % (eid,doi))
				
				create_eidDoi(conn,(eid,doi))


def computeAndSave_authorDoisMapping_listaDoisToDownload(tsv,fileMapping,fileLista,numDoisForeachCandidate=5):
	dois = set()
	authorDois = dict()
	doisCounter = defaultdict(int)

	# OUTPUT
	authorDoisMapping = dict()

	with open(tsv, newline='') as tsvFile:
		spamreader = csv.DictReader(tsvFile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			idCv = row["ID_CV"]
			if "NO-CV-" in idCv:
				doisCandidate = []
			else:
				doisCandidate = ast.literal_eval(row["DOIS ESISTENTI"])

			authorDoisMapping[idCv] = list()
			
			dois.update(doisCandidate)
			if idCv in authorDois:
				print ("PROBLEMA: idCv è già stato trovato - %s" % idCv)
				print (row)
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



# COMPUTE
#computeAndSave_authorDoisMapping_listaDoisToDownload(tsvFN,fileAuthorDoisMapping,fileListaDoisToDownload,5)
# LOAD
authorDoisMapping = load_authorDoisMapping(fileAuthorDoisMapping)
listaDoisToDownload = load__listaDoisToDownload(fileListaDoisToDownload)



# COMPUTE
#populateDbEidDoi(conf.dbFilename,pathAbstracts)
# LOAD (REQ: COMPUTE -> db updated)
eidDoiMap = dict()
doiEidMap = dict()
rows = mylib.select_doiEidMap(conf.dbFilename)
for row in rows:
	eid = row[0]
	doi = row[1]
	#print ("EID: %s - DOI: %s" % (eid,doi))
	eidDoiMap[eid] = doi
	if doi is not None:
		doiEidMap[doi] = eid



# FILTER CVs TO MANAGE
#settoriToInclude = ["01","02","03","04","05","06","07","09"]
settoriToInclude = ["10","11","12","13","14"]

idsCvToConsider = set()
with open(tsvFN, newline='') as tsvFile:
	spamreader = csv.DictReader(tsvFile, delimiter='\t')
	table = list(spamreader)
	for row in table:
		#quadrimestre = row["QUADRIMESTRE"]
		#fascia = row["FASCIA"]
		settore = row["SETTORE"]
		for settoreToInclude in settoriToInclude:
			if settoreToInclude not in settore:
				pass
			else:
				idCv = row["ID_CV"]
				idsCvToConsider.add(idCv)
print ("Numero CV da gestire: %d" % len(idsCvToConsider))


# FILTER DOIs TO DOWNLOAD
doisDownloaded_subset = set()
doisToDownload_subset = set()
for idCv in idsCvToConsider:
	dois = authorDoisMapping[idCv]
	for doi in dois:
		if doi in doiEidMap:
			doisDownloaded_subset.add(doi)
		else:
			doisToDownload_subset.add(doi)
print ("Numero DOI già scaricati: %s" % len(doisDownloaded_subset))
print ("Numero DOI da scaricare: %s" % len(doisToDownload_subset))


# DOWNLOAD FILTERED DOIs 
for doi in doisToDownload_subset:
	conn = mylib.create_connection(conf.dbFilename)
	with conn:
		print ('Processing ' + doi)
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

	
'''
i = 1
numDoisDownloaded = 0
doisDownloaded = set()
for authorId in authorDoisMapping:
	authorDois = authorDoisMapping[authorId]
	for authorDoi in authorDois:
		if authorDoi in doiEidMap:
			numDoisDownloaded += 1
			doisDownloaded.add(authorDoi)

print ("Totale DOI da scaricare: %d" % len(listaDoisToDownload))
print ("DOI già scaricati %d" % numDoisDownloaded)

numDoisDownloaded_2 = 0
for doi in listaDoisToDownload:
	if doi in doiEidMap:
		numDoisDownloaded_2 += 1
		
print ("DOI già scaricati (metodo 2) %d" % numDoisDownloaded_2)
#print (len(doisDownloaded))
'''

#def main():
#	...
#
#if __name__ == '__main__':
#	main()



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
