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


def populateDb_scopusData(dbFilename,path):
	sql_create_scopusPublication_table = """CREATE TABLE IF NOT EXISTS scopusPublication (
										eid text NOT NULL PRIMARY KEY,
										doi text,
										publicationDate text,
										publicationYear text NOT NULL,
										title text NOT NULL,
										venueName text,
										numAuthors integer
									  ); """
										
	sql_create_scopusAuthor_table = """CREATE TABLE IF NOT EXISTS scopusAuthor (
										auid text NOT NULL PRIMARY KEY,
										firstname text,
										initials text,
										surname text
									  ); """

	sql_create_wroteRelation_table = """CREATE TABLE IF NOT EXISTS wroteRelation (
										auid text NOT NULL,
										eid text NOT NULL,
										position integer NOT NULL,
										FOREIGN KEY (auid) REFERENCES scopusAuthor(auid),
										FOREIGN KEY (eid) REFERENCES scopusPublication(eid)
									  ); """
	
	# create a database connection
	conn = mylib.create_connection(dbFilename)
 
	# create tables
	if conn is not None:
		mylib.create_table(conn, sql_create_scopusPublication_table)
		mylib.create_table(conn, sql_create_scopusAuthor_table)
		mylib.create_table(conn, sql_create_wroteRelation_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	
	
	eidDoiMap = dict()
	doiEidMap = dict()
	rows = mylib.select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		#print ("EID: %s - DOI: %s" % (eid,doi))
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid

	
	# POPULATE SCOPUSPUBLICATION, SCOPUAUTHOR, WROTERELATION TABLES
	i = 1
	conn = mylib.create_connection(dbFilename)
	with conn:
		contents = glob(path + '*.json')
		contents.sort()
		for filePublications in contents:
			print ("%d/%d) %s" % (i,len(contents),filePublications))
			i += 1
			with open(filePublications) as json_file:
				j = json.load(json_file)
				try:
					eid = j["abstracts-retrieval-response"]["coredata"]["eid"]
				except:
					print ("ERROR: no eid - file: %s" % filePublications)
					sys.exit()
				try:
					doi = j["abstracts-retrieval-response"]["coredata"]["prism:doi"]
				except:
					print ("WARNING: no prism:doi - file: %s" % filePublications)
					doi = None
					if eidDoiMap[eid] != None:
						print ("ERROR: doi in eidDoiMap but missing in json - file %s" % filePublications)
						sys.exit()
				try:
					title = j["abstracts-retrieval-response"]["coredata"]["dc:title"]
				except:
					print ("ERROR: no dc:title - file: %s" % filePublications)
					sys.exit()
				try:
					venueName = j["abstracts-retrieval-response"]["coredata"]["prism:publicationName"]
				except:
					print ("WARNING: no prism:publicationName - file: %s" % filePublications)
					venueName = None
				try:
					coverDate = j["abstracts-retrieval-response"]["coredata"]["prism:coverDate"]
					publicationDate = coverDate
					publicationYear = coverDate.split("-")[0]
				except:
					print ("WARNING: no prism:coverDate - file: %s" % filePublications)
					publicationDate = None
					publicationYear = None
				try:
					numAuthors = len(j["abstracts-retrieval-response"]["authors"]["author"])
				except:
					print ("WARNING: authors - file: %s" % filePublications)
					numAuthors = None
				
				scopusPublicationTuple = (eid,doi,publicationDate,publicationYear,title,venueName,numAuthors)
				try:
					mylib.create_scopusPublication(conn, scopusPublicationTuple)
				except:
					print ("WARNING - PUBLICATION ALREADY IN THE DB - %s, %s, %s, %s, %s, %s, %s" % (eid,doi,publicationDate,publicationYear,title,venueName,numAuthors))
				
				
				
				if j["abstracts-retrieval-response"]["authors"] is None:
					print ("\tWARNING: no authors - file %s" % filePublications)
					continue
				for author in j["abstracts-retrieval-response"]["authors"]["author"]:
					firstname = author["preferred-name"]["ce:given-name"]
					initials = author["preferred-name"]["ce:initials"]
					surname = author["preferred-name"]["ce:surname"]
					auid = author["@auid"]
					seq = author["@seq"]
					
					scopusAuthorTuple = (auid,firstname,initials,surname)
					try:
						mylib.create_scopusAuthor(conn, scopusAuthorTuple)
					except:
						print ("WARNING - AUTHOR ALREADY IN THE DB - %s, %s, %s, %s" % (auid,firstname,initials,surname))
					
					wroteTuple = (auid, eid, seq)
					mylib.create_wroteRelation(conn, wroteTuple)
				



populateDb_scopusData(conf.dbFilename,pathAbstracts)

'''
		contents = glob(pathAbstracts + authorEid + ".json")
		if len(contents) != 1:
			print ("ERROR: cvId = %s, DOI = %s, EID = %s, num abstracts = %d" % (cvId, authorDoi, authorEid, len(contents)))
			sys.exit()
		
		with open(contents[0]) as json_file:
			data = json.load(json_file)
			if data["abstracts-retrieval-response"]["authors"] is None:
				print ("\tNone")
				continue
			tempSet = set()
			for author in data["abstracts-retrieval-response"]["authors"]["author"]:#["0"]:
				name = author["preferred-name"]["ce:given-name"]
				surname = author["preferred-name"]["ce:surname"]
				auid = author["@auid"]
				tempSet.add(auid)
				authorsInfo[auid] = {"nome": name, "cognome": surname}
			if len(authorsSet) == 0:
				authorsSet = set(tempSet)
			else:
				intersectionSet = authorsSet.intersection(tempSet)
				authorsSet = set(intersectionSet)
	res = select_surnameName(conf.dbFilename, cvId)
	cvSurname = res[0][0]
	cvName = res[0][1]
	print ("%s - %s" % (cvSurname,cvName))
	print ("\t%d" % len(authorsSet))
	for authorId in authorsSet:
		matchSurname = authorsInfo[authorId]["cognome"]
		matchName = authorsInfo[authorId]["nome"]
		print ("\t%s - %s" % (matchSurname,matchName))
		if matchSurname is not None and matchName is not None and matchSurname.lower() == cvSurname.lower(): #and matchName.lower() == cvName.lower():
			#counterPerfectMatch += 1
			perfectMatch[cvId] = authorId
			break

	print ("Num perfect mathces = %d" % len(perfectMatch.keys()))
'''


'''
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

# LOAD
authorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
listaDoisToDownload = load__listaDoisToDownload(fileListaDoisToDownload)

#counterPerfectMatch = 0
counter = 1
perfectMatch = dict()
for cvId in authorDoisMapping:
	print ("%d) %s" % (counter,cvId))
	counter += 1
	if "NO" in cvId:
		print ("*** SKIP ***")
		continue
	authorDois = authorDoisMapping[cvId]
	
	authorsSet = set()
	authorsInfo = dict()
	for authorDoi in authorDois:
		try:
			authorEid = doiEidMap[authorDoi]
		except:
			#authorEid = None
			continue
		contents = glob(pathAbstracts + authorEid + ".json")
		if len(contents) != 1:
			print ("ERROR: cvId = %s, DOI = %s, EID = %s, num abstracts = %d" % (cvId, authorDoi, authorEid, len(contents)))
			sys.exit()
		with open(contents[0]) as json_file:
			data = json.load(json_file)
			if data["abstracts-retrieval-response"]["authors"] is None:
				print ("\tNone")
				continue
			tempSet = set()
			for author in data["abstracts-retrieval-response"]["authors"]["author"]:#["0"]:
				name = author["preferred-name"]["ce:given-name"]
				surname = author["preferred-name"]["ce:surname"]
				auid = author["@auid"]
				tempSet.add(auid)
				authorsInfo[auid] = {"nome": name, "cognome": surname}
			if len(authorsSet) == 0:
				authorsSet = set(tempSet)
			else:
				intersectionSet = authorsSet.intersection(tempSet)
				authorsSet = set(intersectionSet)
	res = select_surnameName(conf.dbFilename, cvId)
	cvSurname = res[0][0]
	cvName = res[0][1]
	print ("%s - %s" % (cvSurname,cvName))
	print ("\t%d" % len(authorsSet))
	for authorId in authorsSet:
		matchSurname = authorsInfo[authorId]["cognome"]
		matchName = authorsInfo[authorId]["nome"]
		print ("\t%s - %s" % (matchSurname,matchName))
		if matchSurname is not None and matchName is not None and matchSurname.lower() == cvSurname.lower(): #and matchName.lower() == cvName.lower():
			#counterPerfectMatch += 1
			perfectMatch[cvId] = authorId
			break

	print ("Num perfect mathces = %d" % len(perfectMatch.keys()))
'''
