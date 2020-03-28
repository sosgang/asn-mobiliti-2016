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


def computeMatch_SurnameName(dbFilename,fileAuthorDoisMapping):
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

	# LOAD
	authorDoisMapping = mylib.loadJson(fileAuthorDoisMapping)
	#listaDoisToDownload = load__listaDoisToDownload(fileListaDoisToDownload)

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
			
			res = mylib.select_surnameName(conn, cvId)
			cvSurname = res[0][0]
			cvFirstname = res[0][1]
			print ("%d) %s, %s - %s" % (counter,cvId,cvSurname,cvFirstname))
			counter += 1
			authorDois = authorDoisMapping[cvId]
			
			foundSurnameName = False
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
				
					rows = mylib.select_match_caseInsensitive(conn,authorEid,cvSurname,None)
					if len(rows) > 0:
						print ("\tFOUND #%d" % counter_match)
						counter_match += 1
						
						for row in rows:
							matchSurnameTuple = (cvId,row[0])
							mylib.create_matchSurname(conn,matchSurnameTuple)
						break


computeMatch_SurnameName(conf.dbFilename,fileAuthorDoisMapping)
