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



def create_connection(db_file):
	""" create a database connection to the SQLite database
		specified by db_file
	:param db_file: database file
	:return: Connection object or None
	"""
	conn = None
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Error as e:
		print(e)
 
	return conn


def create_table(conn, create_table_sql):
	""" create a table from the create_table_sql statement
	:param conn: Connection object
	:param create_table_sql: a CREATE TABLE statement
	:return:
	"""
	try:
		c = conn.cursor()
		c.execute(create_table_sql)
	except Error as e:
		print(e)



def select_doiEidMap(dbFile):
	q = """
		SELECT DISTINCT eid,doi
		FROM eidDoi
		ORDER BY eid
		"""
	
	# create a database connection
	conn = create_connection(dbFile)
	with conn:
		cur = conn.cursor()
		cur.execute(q)
		rows = cur.fetchall()
	return rows


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


def select_match_caseInsensitive(conn,eid,surname,firstname):
	q = """
		SELECT scopusAuthor.auid AS auid
		FROM scopusPublication
		INNER JOIN wroteRelation
		ON
		  scopusPublication.eid = wroteRelation.eid
		INNER JOIN scopusAuthor
		ON
		  wroteRelation.auid = scopusAuthor.auid
		WHERE scopusPublication.eid = '{electronicId}'
		"""
	if surname is not None:
		q += ' AND lower(scopusAuthor.surname) = "' + surname.lower() + '"'
	if firstname is not None:
		q += ' AND lower(scopusAuthor.firstname) = "' + firstname.lower() + '"'
	
	cur = conn.cursor()
	cur.execute(q.format(electronicId=eid))
	rows = cur.fetchall()
	return rows


def create_matchSurname(conn, matchSurname):
	"""
	Create a new record into the matchSurname table
	:param conn:
	:param record:
	:return: matchSurname id
	"""
	sql = ''' INSERT INTO matchSurname(cvId,auid)
			  VALUES(?,?) '''
	cur = conn.cursor()
	cur.execute(sql, matchSurname)
	return cur.lastrowid


def load_authorDoisMapping(fileMapping):
	with open(fileMapping, "r") as read_file:
		data = json.load(read_file)
		return data


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
	conn = create_connection(dbFilename)
 
	# create tables
	if conn is not None:
		create_table(conn, sql_create_matchSurnameName_table)
		create_table(conn, sql_create_matchSurname_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")

	eidDoiMap = dict()
	doiEidMap = dict()
	rows = select_doiEidMap(dbFilename)
	for row in rows:
		eid = row[0]
		doi = row[1]
		#print ("EID: %s - DOI: %s" % (eid,doi))
		eidDoiMap[eid] = doi
		if doi is not None:
			doiEidMap[doi] = eid

	# LOAD
	authorDoisMapping = load_authorDoisMapping(fileAuthorDoisMapping)
	#listaDoisToDownload = load__listaDoisToDownload(fileListaDoisToDownload)

	counter_match = 1 
	# create a database connection
	conn = create_connection(dbFilename)
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
			
			foundSurnameName = False
			for authorDoi in authorDois:
				try:
					authorEid = doiEidMap[authorDoi]
				except:
					#authorEid = None
					continue
			
				rows = select_match_caseInsensitive(conn,authorEid,cvSurname,cvFirstname)
				if len(rows) > 0:
					print ("\tFOUND #%d" % counter_match)
					counter_match += 1
					foundSurnameName = True
					
					for row in rows:
						matchSurnameNameTuple = (cvId,row[0])
						create_matchSurnameName(conn,matchSurnameNameTuple)
					break
			
			if not foundSurnameName:
				for authorDoi in authorDois:
					try:
						authorEid = doiEidMap[authorDoi]
					except:
						#authorEid = None
						continue
				
					rows = select_match_caseInsensitive(conn,authorEid,cvSurname,None)
					if len(rows) > 0:
						print ("\tFOUND #%d" % counter_match)
						counter_match += 1
						
						for row in rows:
							matchSurnameTuple = (cvId,row[0])
							create_matchSurname(conn,matchSurnameTuple)
						break


computeMatch_SurnameName(conf.dbFilename,fileAuthorDoisMapping)
