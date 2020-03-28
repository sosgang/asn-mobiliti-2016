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

fileAuthorDoisMapping_postStep07 = "../../data/input/04.authorDoisMapping_POST_STEP_07.json"


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
	
	
def computeAndSave_newAuthorDoisMapping(dbFilename,fileAuthorDoisMapping_toLoad,fileAuthorDoisMapping_toSave):
	# LOAD
	authorDoisMapping = mylib.loadJson(fileAuthorDoisMapping_toLoad)
	#listaDoisToDownload = load__listaDoisToDownload(fileListaDoisToDownload)

	authorDoisMapping_new = dict()
	counter_match = 1 
	# create a database connection
	conn = mylib.create_connection(dbFilename)
	with conn:
		counter = 0
		perfectMatch = dict()
		for cvId in authorDoisMapping:
			counter += 1
			if "NO" in cvId:
				print ("*** SKIP ***")
				continue
			
			if idCvInMatch(cvId,conn):
				print ("%d) FOUND %s" % (counter,cvId))
				continue
			
			authorDoisMapping_new[cvId] = authorDoisMapping[cvId]
			
			res = mylib.select_surnameName(conn, cvId)
			cvSurname = res[0][0]
			cvFirstname = res[0][1]
			print ("%d) %s, %s - %s" % (counter,cvId,cvSurname,cvFirstname))
	
	with open(fileAuthorDoisMapping_toSave, 'w') as outfile:
		json.dump(authorDoisMapping_new, outfile, indent=3)

def computeMatch_SurnameName_accenti(dbFilename,fileAuthorDoisMapping):
	eidDoiMap = dict()
	doiEidMap = dict()
	rows = mylib.select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid
	
	# LOAD
	newAuthorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
	
	counter_match = 0
	# create a database connection
	conn = mylib.create_connection(dbFilename)
	with conn:
		counter = 0
		perfectMatch = dict()
		for cvId in newAuthorDoisMapping:
			counter += 1
			if "NO" in cvId:
				print ("*** SKIP ***")
				continue
			
			res = mylib.select_surnameName(conn, cvId)
			cvSurname = res[0][0]
			cvFirstname = res[0][1]
			authorDois = newAuthorDoisMapping[cvId]
			#print ("%d) %s, %s - %s (%d)" % (counter,cvId,cvSurname,cvFirstname,len(authorDois)))

			if "'" in cvSurname:
				print ("%d) %s, %s - %s (%d)" % (counter,cvId,cvSurname,cvFirstname,len(authorDois)))
				
				foundSurnameName = False
				for authorDoi in authorDois:
					try:
						authorEid = doiEidMap[authorDoi]
						#print ("../../data/output/abstracts/%s.json" % authorEid)
					except:
						#authorEid = None
						continue
				
					rows = mylib.select_match_caseInsensitive(conn,authorEid,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),cvFirstname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"))
					if len(rows) > 0:
						counter_match += 1
						print ("\tFOUND #%d" % counter_match)
						foundSurnameName = True
						
						for row in rows:
							matchSurnameNameTuple = (cvId,row[0])
							mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
						break
				
				if not foundSurnameName:
					for authorDoi in authorDois:
						try:
							authorEid = doiEidMap[authorDoi]
						except:
							#authorEid = None
							continue
					
						rows = mylib.select_match_caseInsensitive(conn,authorEid,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),None)
						if len(rows) > 0:
							counter_match += 1
							print ("\tFOUND #%d" % counter_match)
							
							for row in rows:
								matchSurnameTuple = (cvId,row[0])
								mylib.create_matchSurname(conn,matchSurnameTuple)
							break

'''
def computeMatch_SurnameName_methodBoh(dbFilename,fileAuthorDoisMapping):
	eidDoiMap = dict()
	doiEidMap = dict()
	rows = mylib.select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid
	
	# LOAD
	newAuthorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
	
	counter_match = 0
	# create a database connection
	conn = mylib.create_connection(dbFilename)
	with conn:
		counter = 0
		perfectMatch = dict()
		for cvId in newAuthorDoisMapping:
			counter += 1
			if "NO" in cvId:
				print ("*** SKIP ***")
				continue
			
			res = mylib.select_surnameName(conn, cvId)
			cvSurname = res[0][0]
			cvFirstname = res[0][1]
			authorDois = newAuthorDoisMapping[cvId]
			authorEids = list()
			for doi in authorDois:
				if doi in doiEidMap:
					authorEids.append(doiEidMap[doi])
			print ("%d) %s, %s - %s (%d) [%s]" % (counter,cvId,cvSurname,cvFirstname,len(authorDois),",".join(authorEids)))
'''

def computeMatch_SurnameName_methodIntersection(dbFilename,fileAuthorDoisMapping):
	
	sql_create_matchIntersection_table = """CREATE TABLE IF NOT EXISTS matchIntersection (
										cvId text NOT NULL,
										auid text NOT NULL,
										FOREIGN KEY (cvId) REFERENCES curriculum(id),
										FOREIGN KEY (auid) REFERENCES scopusAuthor(auid)
									  ); """
	sql_create_matchNoDois_table = """CREATE TABLE IF NOT EXISTS matchNoDois (
										cvId text NOT NULL,
										auid text,
										FOREIGN KEY (cvId) REFERENCES curriculum(id)
									  ); """
	# create a database connection
	conn = mylib.create_connection(dbFilename)
 
	# create tables
	if conn is not None:
		mylib.create_table(conn, sql_create_matchIntersection_table)
		mylib.create_table(conn, sql_create_matchNoDois_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	eidDoiMap = dict()
	doiEidMap = dict()
	rows = mylib.select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid
	
	# LOAD
	newAuthorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
	
	counter_match = 0
	counter_zeroDois = 0
	# create a database connection
	conn = mylib.create_connection(dbFilename)
	with conn:
		counter = 0
		perfectMatch = dict()
		authorsInfo = dict()
		for cvId in newAuthorDoisMapping:
			authorsSet = set()
			counter += 1
			if "NO" in cvId:
				print ("*** SKIP ***")
				matchNoDois = (cvId,None)
				mylib.create_matchNoDois(conn,matchNoDois)
				continue
			
			res = mylib.select_surnameName(conn, cvId)
			cvSurname = res[0][0]
			cvFirstname = res[0][1]
			
			authorDois = newAuthorDoisMapping[cvId]
			authorEids = list()
			for authorDoi in authorDois:
				try:
					authorEid = doiEidMap[authorDoi]
					authorEids.append(authorEid)
				except:
					continue
			print ("%d) %s, %s - %s (%d) [%s]" % (counter,cvId,cvSurname,cvFirstname,len(authorDois),", ".join(authorEids)))
			
			if len(authorDois) == 0:
				counter_zeroDois += 1
				matchNoDois = (cvId,None)
				mylib.create_matchNoDois(conn,matchNoDois)
				continue
			
			for authorEid in authorEids:
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
			
			# test if 1. only one authorId in intersection, and 2. the number of documents (=eids) > 1
			if len(authorsSet) == 1 and len(authorEids) > 1:
				counter_match += 1
				authorIdFound = (list(authorsSet)[0])
				print ("\tFOUND!!! %s - %s (%s)" % (authorsInfo[authorIdFound]['cognome'],authorsInfo[authorIdFound]['nome'],authorIdFound))
				matchIntersection = (cvId,authorIdFound)
				mylib.create_matchIntersection(conn,matchIntersection)
		
		print ("Num CVs left = %d" % len(newAuthorDoisMapping.keys()))
		print ("Num matches = %d" % counter_match)
		print ("Num 0 DOIs = %d" % counter_zeroDois)


def computeMatch_lessThanSixDois(dbFilename,fileAuthorDoisMapping):
	
	#LOAD
	newAuthorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
	
	eidDoiMap = dict()
	doiEidMap = dict()
	rows = mylib.select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid

	# get number of DOIs in the CV
	cvDois = dict()
	with open(tsvFN, newline='') as tsvFile:
		spamreader = csv.DictReader(tsvFile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			idCv = row["ID_CV"]
			if "NO-CV-" in idCv:
				doisCandidate = []
			else:
				doisCandidate = ast.literal_eval(row["DOIS ESISTENTI"])
			cvDois[idCv] = doisCandidate

	counter_zeroDois = 0
	# create a database connection
	conn = mylib.create_connection(dbFilename)
	with conn:
		counter = 0
		perfectMatch = dict()
		authorsInfo = dict()
		for cvId in newAuthorDoisMapping:
			counter += 1
			if "NO" in cvId:
				print ("*** SKIP ***")
				matchNoDois = (cvId,None)
				mylib.create_matchNoDois(conn,matchNoDois)
				continue
			
			res = mylib.select_surnameName(conn, cvId)
			cvSurname = res[0][0]
			cvFirstname = res[0][1]
			
			authorDois = newAuthorDoisMapping[cvId]
			authorEids = list()
			for authorDoi in authorDois:
				try:
					authorEid = doiEidMap[authorDoi]
					authorEids.append(authorEid)
				except:
					continue
			print ("%d) %s, %s - %s (%d) [%s]" % (counter,cvId,cvSurname,cvFirstname,len(authorDois),", ".join(authorEids)))
				
			if len(authorDois) < 5 and len(cvDois[cvId]) < 6:
				counter_zeroDois += 1
				matchNoDois = (cvId,None)
				mylib.create_matchNoDois(conn,matchNoDois)
				continue
					
			print ("\t%d" % len(cvDois[cvId]))

	print ("CVs to process (i.e. no match): %d" % len(newAuthorDoisMapping.keys()))
	print ("CVs with less than 5 DOIs (i.e. to exclude): %d" % counter_zeroDois)


computeAndSave_newAuthorDoisMapping(conf.dbFilename,fileAuthorDoisMapping,fileAuthorDoisMapping_postStep07)
computeMatch_SurnameName_accenti(conf.dbFilename,fileAuthorDoisMapping_postStep07)

computeAndSave_newAuthorDoisMapping(conf.dbFilename,fileAuthorDoisMapping,fileAuthorDoisMapping_postStep07)
computeMatch_SurnameName_methodIntersection(conf.dbFilename,fileAuthorDoisMapping_postStep07)

computeAndSave_newAuthorDoisMapping(conf.dbFilename,fileAuthorDoisMapping,fileAuthorDoisMapping_postStep07)
computeMatch_lessThanSixDois(conf.dbFilename,fileAuthorDoisMapping_postStep07)

computeAndSave_newAuthorDoisMapping(conf.dbFilename,fileAuthorDoisMapping,fileAuthorDoisMapping_postStep07)

	
####computeMatch_SurnameName_methodBoh(conf.dbFilename,fileAuthorDoisMapping_postStep07)
