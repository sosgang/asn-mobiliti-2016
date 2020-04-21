# -*- coding: UTF-8 -*-

'''
ALTER TABLE matchCvidAuid
ADD COLUMN matchManual integer NOT NULL DEFAULT 0;
'''

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

tsvManualMatch = "../../data/output/candidatesAsn2016_missingMatches_gnu.tsv"
outputFolder = "../../data/output/publications-list/"

def select_match(conn,idCv):
	q = """
		SELECT DISTINCT *
		FROM matchCvidAuid
		WHERE cvId = '{idCurriculum}'
		"""
	cur = conn.cursor()
	cur.execute(q.format(idCurriculum=idCv))
	rows = cur.fetchall()
	return rows 


def select_AuidInMatch(conn):
	q = """
		SELECT DISTINCT auid
		FROM matchCvidAuid
		WHERE auid NOT NULL
		"""
	cur = conn.cursor()
	cur.execute(q)
	rows = cur.fetchall()
	return rows 


def update_manualMatch(conn,idCv,auid):
	q = """
		UPDATE matchCvidAuid
		SET matchManual = 1, 
		  auid = '{authorId}'
		WHERE cvId = '{idCurriculum}'
	"""
	cur = conn.cursor()
	cur.execute(q.format(idCurriculum=idCv,authorId=auid))
	rows = cur.fetchall()
	return rows 


def addManualMatches(tsvMatches,dbFilename):
	
	conn = mylib.create_connection(dbFilename)
	
	with conn:
		with open(tsvMatches, newline='') as tsvFile:
			spamreader = csv.DictReader(tsvFile, delimiter='\t')
			table = list(spamreader)
			i = 2
			for row in table:
				cvId = row["id"]
				if "NO-CV" in cvId:
					continue
				#print ("%d/%d) %s" % (i,len(table),cvId))
				#dois = ast.literal_eval(row["DOIS ESISTENTI"])
				auid = row["AUID"]
				if auid != "":
					print ("%d) %s: %s" % (i,cvId,auid))
					res = select_match(conn,cvId)
					if (len(res)) == 1:
						auidDb = res[0][1]
						if auidDb != None:
							print ("ERRORE: match presente nel DB per cv cob id %s -> exit" % auidDb)
							break
						else:
							update_manualMatch(conn,cvId,auid)
					else:
						print ("Presente più di una riga di match nel DB per cv con id %s -> exit" % cvId)
						break
				i += 1
				#myTuple = (cvId,doi,eid)
				#mylib.create_cvidDoiEid(conn,myTuple)


def downloadPublications(dbFilename, folder):
	conn = mylib.create_connection(dbFilename)
	
	i = 0
	with conn:
		rows = select_AuidInMatch(conn)
		for row in rows:
			if i == 10:
				break
			authorId = row[0]
			print (auid)
			j = mylib.getPublicationList(authorId)
			if j is not None and mylib.saveJsonPubs(j, authorId, folder):
				print (authorId + ': Saved to file.')
			else:
				print (authorId + ': None -> not saved (i.e. not found or json already downloaded).')
			i += 1

#addManualMatches(tsvManualMatch,conf.dbFilename)

downloadPublications(conf.dbFilename, outputFolder)
