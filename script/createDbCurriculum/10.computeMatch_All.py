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
#fileAuthorDoisMapping = "../../data/input/04.authorDoisMapping.json"
#fileListaDoisToDownload = "../../data/input/04.doisToDownload-list.txt"
pathAbstracts = "../../data/output/abstracts/"
fileAuthorDoisMapping_postStep07 = "../../data/input/04.authorDoisMapping_POST_STEP_07.json"


def searchAuthorIds_SurnameName(doisCandidate,surname,firstname,conn,doiEidMap):
	auids = set()
	for doiCandidate in doisCandidate:
		try:
			eidCandidate = doiEidMap[doiCandidate]
		except:
			#eidCandidate = None
			continue
		
		rows = mylib.select_match_caseInsensitive(conn,eidCandidate,surname,firstname)
		if len(rows) > 0:
			print ("\tFOUND") # #%d" % counter_match)
				
			for row in rows:
				auids.add(row[0])
			break
	return auids


def	searchAuthorId_intersection(dois,doiEidMap,path,minPapersInIntersection=5):
	eids = list()
	authorsSet = set()
	authorsInfo = dict()
						
	for doi in dois:
		try:
			eid = doiEidMap[doi]
			eids.append(eid)
		except:
			continue
	
	#if len(dois) == 0:
	#	#matchNoDois = (cvId,None)
	#	#mylib.create_matchNoDois(conn,matchNoDois)
	#	#continue
	#	pass
	
	if len(eids) < minPapersInIntersection:
		return None
	
	isFirstEid = True	
	for eid in eids:
		contents = glob(path + eid + ".json")
		if len(contents) != 1:
			print ("ERROR: cvId = %s, DOI = %s, EID = %s, num abstracts = %d" % (cvId, doi, eid, len(contents)))
			sys.exit()
		
		with open(contents[0]) as json_file:
			# CARICO INFO DEL PAPER
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
		
			# FACCIO INTERSEZIONE
			if isFirstEid:
				authorsSet = set(tempSet)
				isFirstEid = False
			else:
				intersectionSet = authorsSet.intersection(tempSet)
				authorsSet = set(intersectionSet)
			#print (authorsSet)
			
			# ESCO SE INTERSEZIONE VUOTA
			if len(authorsSet) == 0:
				return None
	
	# test if 1. only one authorId in intersection, and 2. the number of documents (=eids) > 1
	if len(authorsSet) == 1 and len(eids) >= minPapersInIntersection:
		auid = (list(authorsSet)[0])		
		print ("\tFOUND INTERSECTION!!! %s - %s (%s)" % (authorsInfo[auid]['cognome'],authorsInfo[auid]['nome'],auid))
		return auid
	return None

	

def computeMatch_SurnameName(dbFilename,fileAuthorDoisMapping):
	'''
	sql_create_matchSurnameName_table = """CREATE TABLE IF NOT EXISTS matchSurnameName (
										cvId text NOT NULL,
										auid text NOT NULL,
										FOREIGN KEY (cvId) REFERENCES curriculum(id),
										FOREIGN KEY (auid) REFERENCES scopusAuthor(auid)
									  ); """
	sql_create_matchSurname_table = """CREATE TABLE IF NOT EXISTS matchSurname (
										cvId text NOT NULL,
										auid text NOT NULL,
										FOREIGN KEY (cvId) REFERENCES curriculum(id),
										FOREIGN KEY (auid) REFERENCES scopusAuthor(auid)
									  ); """
	
	
	# create a database connection
	conn = mylib.create_connection(dbFilename)
 
	# create tables
	if conn is not None:
		mylib.create_table(conn, sql_create_matchSurnameName_table)
		mylib.create_table(conn, sql_create_matchSurname_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	'''
	newAuthorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
	print ("CVs still to match = %d" % len(newAuthorDoisMapping.keys()))
	
	eidDoiMap = dict()
	doiEidMap = dict()
	rows = mylib.select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid

	counter = 1
	counter_match = 1 
	conn = mylib.create_connection(dbFilename)
	with conn:
		with open(tsvFN, newline='') as tsvFile:
			spamreader = csv.DictReader(tsvFile, delimiter='\t')
			table = list(spamreader)
			for row in table:
				idCv = row["ID_CV"]
				if idCv not in newAuthorDoisMapping.keys():
					continue
				if "NO-CV-" in idCv:
					continue
				
				#doisToDownload.add(doiCandidate)
				res = mylib.select_surnameName(conn, idCv)
				cvSurname = res[0][0]
				cvFirstname = res[0][1]
				
				doisCandidate = ast.literal_eval(row["DOIS ESISTENTI"])
				
				print ("%d/%d) %s, %s - %s (%d DOIs)" % (counter,len(newAuthorDoisMapping.keys()),idCv,cvSurname,cvFirstname,len(doisCandidate)))
				counter += 1
				
				# SURNAME AND FIRSTNAME
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname,cvFirstname,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),cvFirstname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),conn,doiEidMap)
				
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname.replace("e'","é"),cvFirstname.replace("e'","é"),conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname.replace("'",""),cvFirstname.replace("'",""),conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				
				# ONLY SURNAME
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname,None,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurname(conn,matchSurnameNameTuple)
					continue
				
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),None,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurname(conn,matchSurnameNameTuple)
					continue
				
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname.replace("e'","é"),None,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurname(conn,matchSurnameNameTuple)
					continue
				
				auids = searchAuthorIds_SurnameName(doisCandidate,cvSurname.replace("'",""),None,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurname(conn,matchSurnameNameTuple)
					continue
				
				# INTERSECTION
				auid = searchAuthorId_intersection(doisCandidate,doiEidMap,pathAbstracts)
				if auid is not None:
					counter_match += 1
					matchIntersection = (idCv,auid)
					#mylib.create_matchIntersection(conn,matchIntersection)
					continue

					
computeMatch_SurnameName(conf.dbFilename,fileAuthorDoisMapping_postStep07)

def idCvInMatch(cvId,conn):
	queries = [
		"SELECT * FROM matchSurnameName WHERE cvId = '{cvIdentifier}'",
		"SELECT * FROM matchSurname WHERE cvId = '{cvIdentifier}'",
		"SELECT * FROM matchIntersection WHERE cvId = '{cvIdentifier}'",
		"SELECT * FROM matchNoDois WHERE cvId = '{cvIdentifier}'"
	]
	for query in queries:
		cur = conn.cursor()
		cur.execute(query.format(cvIdentifier=cvId))
		rows = cur.fetchall()
		if len(rows) > 0:
			return True
	return False

found = 0
notFound = 0
newAuthorDoisMapping = mylib.loadJson(fileAuthorDoisMapping_postStep07)
conn = mylib.create_connection(conf.dbFilename)
with conn:
	for cvId in newAuthorDoisMapping:
		if idCvInMatch(cvId,conn):
			found += 1
		else:
			notFound += 1
print ("Totali = %d\nTrovati = %d\nNon trovati = %d" % (found+notFound,found,notFound))
