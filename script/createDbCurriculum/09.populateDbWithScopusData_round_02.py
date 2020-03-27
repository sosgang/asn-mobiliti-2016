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

'''
# FPOGGI
def create_eidDoi(conn, eidDoi):
	"""
	Create a new eidDoi mapping into the eidDoi table
	:param conn:
	:param dieDoi:
	:return: eidDoi id
	"""
	sql = """ INSERT INTO eidDoi(eid,doi)
			  VALUES(?,?) """
	cur = conn.cursor()
	cur.execute(sql, eidDoi)
	return cur.lastrowid
'''

def populateDb_scopusData(dbFilename,path):
	'''
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
	'''
	# get ***ALL*** EID/DOI couples (including those of the newly downloaded abstracts)
	eidDoiMap = dict()
	doiEidMap = dict()
	# FPOGGI
	#rows = mylib.select_doiEidMap("../../data/output/candidatesAsn2016_mobiliti.db")
	rows = mylib.select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		#print ("EID: %s - DOI: %s" % (eid,doi))
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid
	
	# get ***OLD*** EID/DOI couples from scopusPublication table
	oldEids = set()
	rows = mylib.select_scopusPublication(dbFilename)
	for row in rows:
		eid = row[0]
		#doi = row[1]
		oldEids.add(eid)

	newEids = set()
	# POPULATE SCOPUSPUBLICATION, SCOPUAUTHOR, WROTERELATION TABLES
	i = 1
	conn = mylib.create_connection(dbFilename)
	with conn:
		contents = glob(path + '*.json')
		contents.sort()
		for filePublications in contents:
			eid = os.path.basename(filePublications).replace(".json","")
			doi = eidDoiMap[eid]
			if eid not in oldEids:
				newEids.add(eid)
				
				#print ("%s - %s" % (eid,doi))
				#create_eidDoi(conn,(eid,doi))
				
				
				#print ("%d) %s" % (i,filePublications))
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
						print ("WARNING: num. authors - file: %s" % filePublications)
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
						
				
	print ("new abstracts managed = %d" % len(newEids))

populateDb_scopusData(conf.dbFilename,pathAbstracts)
