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


def myReplace(word,replaceType):
	if word is None:
		return None
	elif replaceType == "accentiGravi":
		return word.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù")
	elif replaceType == "accentiAcuti":
		return word.replace("e'","é")
	elif replaceType == "apostrofi":
		return word.replace("'","")
	elif replaceType == "trattino":
		return word.replace("-"," ")
	else:
		print ("ERROR in myReplace(). Exit.")
		sys.exit()


def searchAuthorIds_SurnameName_v3(eids,surname,firstname,conn,doiEidMap):
	#auids = searchAuthorIds_SurnameName(dois,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),cvFirstname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),conn,doiEidMap)
	#auids = searchAuthorIds_SurnameName(dois,cvSurname.replace("e'","é"),cvFirstname.replace("e'","é"),conn,doiEidMap)
	#auids = searchAuthorIds_SurnameName(dois,cvSurname.replace("'",""),cvFirstname.replace("'",""),conn,doiEidMap)
	if firstname is not None:
		msgType = "surname+name"
	else:
		msgType = "surname"
		
	found = False
	auids = set()
	for eid in eids:
		rows = mylib.select_match_caseInsensitive(conn,eid,surname,firstname)
		if len(rows) > 0:
			print ("\tFOUND AS-IS (%s)" % msgType) # #%d" % counter_match)
			for row in rows:
				auids.add(row[0])
			#break
			found = True
	
	if not found:
		for eid in eids:
			rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"accentiGravi"),myReplace(firstname,"accentiGravi"))
			if len(rows) > 0:
				print ("\tFOUND ACCENTIGRAVI (%s)" % msgType) # #%d" % counter_match)
				for row in rows:
					auids.add(row[0])
				#break
				found = True
			
	if not found:
		for eid in eids:
			rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"accentiAcuti"),myReplace(firstname,"accentiAcuti"))
			if len(rows) > 0:
				print ("\tFOUND ACCENTIACUTI (%s)" % msgType) # #%d" % counter_match)
				for row in rows:
					auids.add(row[0])
				#break
				found = True
		
	if not found:
		for eid in eids:
			rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"apostrofi"),myReplace(firstname,"apostrofi"))
			if len(rows) > 0:
				print ("\tFOUND APOSTROFI (%s)" % msgType) # #%d" % counter_match)
				for row in rows:
					auids.add(row[0])
				#break
				found = True
		
	if not found:
		for eid in eids:
			rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"trattino"),myReplace(firstname,"apostrofi"))
			if len(rows) > 0:
				print ("\tFOUND TRATTINO (%s)" % msgType) # #%d" % counter_match)
				for row in rows:
					auids.add(row[0])
				#break
				found = True
			
	return auids
		

def searchAuthorIds_SurnameName_v2(dois,surname,firstname,conn,doiEidMap):
	#auids = searchAuthorIds_SurnameName(dois,cvSurname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),cvFirstname.replace("a'","à").replace("e'","è").replace("i'","ì").replace("o'","ò").replace("u'","ù"),conn,doiEidMap)
	#auids = searchAuthorIds_SurnameName(dois,cvSurname.replace("e'","é"),cvFirstname.replace("e'","é"),conn,doiEidMap)
	#auids = searchAuthorIds_SurnameName(dois,cvSurname.replace("'",""),cvFirstname.replace("'",""),conn,doiEidMap)
	if firstname is not None:
		msgType = "surname+name"
	else:
		msgType = "surname"
		
	auids = set()
	for doi in dois:
		try:
			eid = doiEidMap[doi]
		except:
			#eid = None
			continue
		
		rows = mylib.select_match_caseInsensitive(conn,eid,surname,firstname)
		if len(rows) > 0:
			print ("\tFOUND AS-IS (%s)" % msgType) # #%d" % counter_match)
			for row in rows:
				auids.add(row[0])
			break
		
		rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"accentiGravi"),myReplace(firstname,"accentiGravi"))
		if len(rows) > 0:
			print ("\tFOUND ACCENTIGRAVI (%s)" % msgType) # #%d" % counter_match)
			for row in rows:
				auids.add(row[0])
			break
		
		rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"accentiAcuti"),myReplace(firstname,"accentiAcuti"))
		if len(rows) > 0:
			print ("\tFOUND ACCENTIACUTI (%s)" % msgType) # #%d" % counter_match)
			for row in rows:
				auids.add(row[0])
			break
		
		rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"apostrofi"),myReplace(firstname,"apostrofi"))
		if len(rows) > 0:
			print ("\tFOUND APOSTROFI (%s)" % msgType) # #%d" % counter_match)
			for row in rows:
				auids.add(row[0])
			break
		
		rows = mylib.select_match_caseInsensitive(conn,eid,myReplace(surname,"trattino"),myReplace(firstname,"apostrofi"))
		if len(rows) > 0:
			print ("\tFOUND TRATTINO (%s)" % msgType) # #%d" % counter_match)
			for row in rows:
				auids.add(row[0])
			break
			
	return auids


def	searchAuthorId_intersection(dois,doiEidMap,path,minPapersInIntersection=2):
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

	

def computeMatch(dbFilename,fileAuthorDoisMapping):
	
	sql_create_matchCvidAuid_table = """CREATE TABLE IF NOT EXISTS matchCvidAuid (
										cvId text NOT NULL,
										auid text,
										matchSurnameName integer,
										matchSurname integer,
										matchIntersection integer,
										numDois integer,
										numEids integer,
										FOREIGN KEY (cvId) REFERENCES curriculum(id),
										FOREIGN KEY (auid) REFERENCES scopusAuthor(auid),
										PRIMARY KEY(cvId,auid)
									  ); """
	
	
	# create a database connection
	conn = mylib.create_connection(dbFilename)
 
	# create tables
	if conn is not None:
		mylib.create_table(conn, sql_create_matchCvidAuid_table)
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

	counter = 0
	counter_match = 1 
	
	with open(tsvFN, newline='') as tsvFile:
		spamreader = csv.DictReader(tsvFile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			conn = mylib.create_connection(dbFilename)
			with conn:
				counter += 1
				idCv = row["ID_CV"]
				
				if len(mylib.select_cvidInMatchCvidAuid(conn,idCv)) > 0:
					print ("%d) cvId %s already in table matchCvidAuid -> Skip." % (counter,idCv))
					continue
				
				#if idCv not in newAuthorDoisMapping.keys():
				#	continue
				if "NO-CV-" in idCv:
					print ("%d) NO-CV (%s) -> Skip." % (counter,idCv))
					matchTuple = (idCv,None,False,False,False,0,0)
					#print (matchTuple)
					mylib.create_match(conn,matchTuple)
					continue
				
				res = mylib.select_surnameName(conn, idCv)
				cvSurname = res[0][0]
				cvFirstname = res[0][1]
				
				doisCandidate = ast.literal_eval(row["DOIS ESISTENTI"])
				eidsCandidate = set()
				for doiCandidate in doisCandidate:
					try:
						#print ("\t" + doiEidMap[doiCandidate])
						eidsCandidate.add(doiEidMap[doiCandidate])
					except:
						continue
				
				print ("%d/%d) %s, %s - %s (%d DOIs)" % (counter,len(table),idCv,cvSurname,cvFirstname,len(doisCandidate)))
				
				
				# se non ho nessun EID (= nessun paper) => inserisco match con auid NULL ed esco
				if len(eidsCandidate) == 0:
					matchTuple = (idCv,None,False,False,False,len(doisCandidate),len(eidsCandidate))
					#print (matchTuple)
					mylib.create_match(conn,matchTuple)
					#sys.exit()
					continue
				
				#auids_surnameName = searchAuthorIds_SurnameName_v2(doisCandidate,cvSurname,cvFirstname,conn,doiEidMap)
				#auids_surname = searchAuthorIds_SurnameName_v2(doisCandidate,cvSurname,None,conn,doiEidMap)
				auids_surnameName = searchAuthorIds_SurnameName_v3(eidsCandidate,cvSurname,cvFirstname,conn,doiEidMap)
				auids_surname = searchAuthorIds_SurnameName_v3(eidsCandidate,cvSurname,None,conn,doiEidMap)
				auid_intersection = searchAuthorId_intersection(doisCandidate,doiEidMap,pathAbstracts)
				
				# se non ho trovato match => inserisco match con auid NULL ed esco
				if len(auids_surnameName) == 0 and len(auids_surname) == 0 and auid_intersection is None:
					matchTuple = (idCv,None,False,False,False,len(doisCandidate),len(eidsCandidate))
					#print (matchTuple)
					mylib.create_match(conn,matchTuple)
					continue
					
				for auid_surnameName in auids_surnameName:
					inSurname = False
					if auid_surnameName in auids_surname:
						inSurname = True
						auids_surname.remove(auid_surnameName)
					
					inIntersection = False
					if auid_surnameName == auid_intersection:
						inIntersection = True
						auid_intersection = None
					
					matchTuple = (idCv,auid_surnameName,True,inSurname,inIntersection,len(doisCandidate),len(eidsCandidate))
					#print (matchTuple)
					mylib.create_match(conn,matchTuple)
				
				for auid_surname in auids_surname:
					inSurnameName = False
					if auid_surname in auids_surnameName:
						#inSurnameName = True
						#auids_surnameName.remove(auid_surname)
						print ("ERROR (auid_surname): this should not happen. Exit.")
						sys.exit()
						
					inIntersection = False
					if auid_surname == auid_intersection:
						inIntersection = True
						auid_intersection = None
					
					matchTuple = (idCv,auid_surname,inSurnameName,True,inIntersection,len(doisCandidate),len(eidsCandidate))
					#print (matchTuple)
					mylib.create_match(conn,matchTuple)
				
				if auid_intersection is not None:
					if auid_intersection in auids_surnameName or auid_intersection in auids_surname:
						print ("ERROR (auid_intersection): this should not happen. Exit.")
						sys.exit()
					matchTuple = (idCv,auid_intersection,False,False,True,len(doisCandidate),len(eidsCandidate))
					#print (matchTuple)
					mylib.create_match(conn,matchTuple)
				'''	
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
				'''
					
computeMatch(conf.dbFilename,fileAuthorDoisMapping_postStep07)
'''
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
'''
