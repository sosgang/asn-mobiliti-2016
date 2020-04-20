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

def createTableCvidDoi(dbFilename):
	
	sql_create_cvidDoiEid_table = """CREATE TABLE IF NOT EXISTS cvidDoiEid (
										cvId text NOT NULL,
										doi text NOT NULL,
										eid text,
										FOREIGN KEY (cvId) REFERENCES curriculum(id),
										FOREIGN KEY (doi) REFERENCES eidDoi(doi),
										FOREIGN KEY (eid) REFERENCES eidDoi(eid),
										PRIMARY KEY(cvId,doi)
									  ); """
	
	# create a database connection
	conn = mylib.create_connection(dbFilename)
 
	# create tables
	if conn is not None:
		mylib.create_table(conn, sql_create_cvidDoiEid_table)
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
	
	
	conn = mylib.create_connection(dbFilename)
	with conn:
		with open(tsvFN, newline='') as tsvFile:
			spamreader = csv.DictReader(tsvFile, delimiter='\t')
			table = list(spamreader)
			i=0
			for row in table:
				i+=1
				cvId = row["ID_CV"]
				if "NO-CV" in cvId:
					continue
				print ("%d/%d) %s" % (i,len(table),cvId))
				dois = ast.literal_eval(row["DOIS ESISTENTI"])
				for doi in dois:
					try:
						eid = doiEidMap[doi]
					except:
						eid = None
					myTuple = (cvId,doi,eid)
					mylib.create_cvidDoiEid(conn,myTuple)
				#if i == 10:
				#	break
	

createTableCvidDoi(conf.dbFilename)
