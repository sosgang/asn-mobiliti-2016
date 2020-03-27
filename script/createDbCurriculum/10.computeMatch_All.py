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
#pathAbstracts = "../../data/output/abstracts/"
fileAuthorDoisMapping_postStep07 = "../../data/input/04.authorDoisMapping_POST_STEP_07.json"

def select_surnameName(conn,cvId):
	q = """
		SELECT DISTINCT cognome,nome
		FROM curriculum
		WHERE id = {idCurriculum}
		"""
	
	# create a database connection
	cur = conn.cursor()
	cur.execute(q.format(idCurriculum=cvId))
	rows = cur.fetchall()
	return rows



def searchAuthorIds(doisCandidate,surname,firstname,conn,doiEidMap):
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
	print (auids)
	return auids

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
				res = select_surnameName(conn, idCv)
				cvSurname = res[0][0]
				cvFirstname = res[0][1]
				
				doisCandidate = ast.literal_eval(row["DOIS ESISTENTI"])
				
				print ("%d/%d) %s, %s - %s (%d DOIs)" % (counter,len(newAuthorDoisMapping.keys()),idCv,cvSurname,cvFirstname,len(doisCandidate)))
				counter += 1
				
				
				found = False
				auids = searchAuthorIds(doisCandidate,cvSurname,cvFirstname,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					found = True
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				'''
				found = False
				for doiCandidate in doisCandidate:
					try:
						eidCandidate = doiEidMap[doiCandidate]
					except:
						#eidCandidate = None
						continue
					
					rows = mylib.select_match_caseInsensitive(conn,eidCandidate,cvSurname,cvFirstname)
					if len(rows) > 0:
						print ("\tFOUND SURNAME-NAME #%d" % counter_match)
						counter_match += 1
						found = True
							
						for row in rows:
							matchSurnameNameTuple = (idCv,row[0])
							#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
						break
				'''
				auids = searchAuthorIds(doisCandidate,cvSurname,None,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					found = True
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				'''
				if not found:
					for doiCandidate in doisCandidate:
						try:
							eidCandidate = doiEidMap[doiCandidate]
						except:
							#eidCandidate = None
							continue
					
						rows = mylib.select_match_caseInsensitive(conn,eidCandidate,cvSurname,None)
						if len(rows) > 0:
							print ("\tFOUND NAME #%d" % counter_match)
							counter_match += 1
							
							for row in rows:
								matchSurnameTuple = (idCv,row[0])
								#mylib.create_matchSurname(conn,matchSurnameTuple)
							break
				'''			
				auids = searchAuthorIds(doisCandidate,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),cvFirstname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					found = True
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				'''
				if not found:
					for doiCandidate in doisCandidate:
						try:
							eidCandidate = doiEidMap[doiCandidate]
						except:
							#eidCandidate = None
							continue
						
						#rows = mylib.select_match_caseInsensitive(conn,eidCandidate,cvSurname,cvFirstname)
						rows = mylib.select_match_caseInsensitive(conn,authorEid,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),cvFirstname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"))
						if len(rows) > 0:
							print ("\tFOUND SURNAME-NAME accenti#%d" % counter_match)
							counter_match += 1
							found = True
								
							for row in rows:
								matchSurnameNameTuple = (idCv,row[0])
								#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
							break
				'''
				auids = searchAuthorIds(doisCandidate,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),None,conn,doiEidMap)
				if len(auids) != 0:
					counter_match += 1
					found = True
					for auid in auids:
						matchSurnameNameTuple = (idCv,auid)
						#mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					continue
				'''	
				if not found:
					for doiCandidate in doisCandidate:
						try:
							eidCandidate = doiEidMap[doiCandidate]
						except:
							#eidCandidate = None
							continue
					
						#rows = mylib.select_match_caseInsensitive(conn,eidCandidate,cvSurname,None)
						rows = mylib.select_match_caseInsensitive(conn,authorEid,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),None)
						if len(rows) > 0:
							print ("\tFOUND NAME #%d" % counter_match)
							counter_match += 1
							
							for row in rows:
								matchSurnameTuple = (idCv,row[0])
								#mylib.create_matchSurname(conn,matchSurnameTuple)
							break
				'''
				
				
				'''
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
						
						res = select_surnameName(conn, cvId)
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
					'''
	sys.exit()					
									
	
				
	
	# LOAD
	#authorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
	
	counter_match = 1 
	# create a database connection
	conn = mylib.create_connection(dbFilename)
	with conn:
		counter = 1
		perfectMatch = dict()
		for cvId in authorDoisMapping:
			if "NO" in cvId:
				print ("*** SKIP ***")
				counter += 1
				continue
			
			res = select_surnameName(conn, cvId)
			cvSurname = res[0][0]
			cvFirstname = res[0][1]
			print ("%d) %s, %s - %s" % (counter,cvId,cvSurname,cvFirstname))
			counter += 1
			authorDois = authorDoisMapping[cvId]
			
			found = False
			for authorDoi in authorDois:
				try:
					authorEid = doiEidMap[authorDoi]
				except:
					#authorEid = None
					continue
			
				rows = mylib.select_match_caseInsensitive(conn,authorEid,cvSurname,cvFirstname)
				if len(rows) > 0:
					print ("\tFOUND #%d" % counter_match)
					counter_match += 1
					found = True
					
					for row in rows:
						matchSurnameNameTuple = (cvId,row[0])
						mylib.create_matchSurnameName(conn,matchSurnameNameTuple)
					break
			
			if not found:
				for authorDoi in authorDois:
					try:
						authorEid = doiEidMap[authorDoi]
					except:
						#authorEid = None
						continue
				
					rows = mylib.select_match_caseInsensitive(conn,authorEid,cvSurname,None)
					if len(rows) > 0:
						print ("\tFOUND #%d" % counter_match)
						counter_match += 1
						
						for row in rows:
							matchSurnameTuple = (cvId,row[0])
							mylib.create_matchSurname(conn,matchSurnameTuple)
						break


computeMatch_SurnameName(conf.dbFilename,fileAuthorDoisMapping_postStep07)
