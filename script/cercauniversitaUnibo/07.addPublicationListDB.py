# -*- coding: UTF-8 -*-
import requests
import csv
import sys
import datetime
import time
import os 
import json
from glob import glob
import ast
import sqlite3
from sqlite3 import Error

import urllib.parse

sys.path.append('..')
import conf

pathAuthorsSearch = "../../data/input/cercauniversita/bologna/authors-search/"
pathPublications = "../../data/input/cercauniversita/bologna/publications-list/"
		 
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
		
def create_author(conn, author):
	"""
	Create a new author into the authorScopus table
	:param conn:
	:param author:
	:return: author id
	"""
	sql = ''' INSERT INTO authorScopus(id,givenname,surname,initials,orcid)
			  VALUES(?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, author)
	return cur.lastrowid
	
def create_wroteRelation(conn, wroteRelation):
	"""
	Create a new wroteRelation into the wroteRelation table
	:param conn:
	:param wroteRelation:
	:return: wroteRelation id
	"""
	sql = ''' INSERT INTO wroteRelation(authorId,eid)
			  VALUES(?,?) '''
	cur = conn.cursor()
	cur.execute(sql, wroteRelation)
	return cur.lastrowid

def create_publication(conn, publication):
	"""
	Create a new publication into the publication table
	:param conn:
	:param publication:
	:return: publication id
	"""
	sql = ''' INSERT INTO publication(eid, doi, publicationDate,publicationYear,title,venueName)
			  VALUES(?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, publication)
	return cur.lastrowid


def main():
	sql_create_authorScopus_table = """CREATE TABLE IF NOT EXISTS authorScopus (
									id integer PRIMARY KEY,
									givenname text,
									surname text,
									initials text,
									orcid text
								);"""

	sql_create_wroteRelation_table = """ CREATE TABLE IF NOT EXISTS wroteRelation (
										authorId integer NOT NULL,
										eid string,
										FOREIGN KEY (eid) REFERENCES authorScopus(id),
										FOREIGN KEY (eid) REFERENCES publication(eid)
									); """

	sql_create_publication_table = """ CREATE TABLE IF NOT EXISTS publication (
										eid string PRIMARY KEY,
										doi string,
										publicationDate string,
										publicationYear string NOT NULL,
										title string NOT NULL,
										venueName string NOT NULL
									); """

	# create a database connection
	conn = create_connection(conf.dbFilename)
 
	# create tables
	if conn is not None:
		create_table(conn, sql_create_authorScopus_table)
		create_table(conn, sql_create_publication_table)
		create_table(conn, sql_create_wroteRelation_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")

	conn = create_connection(conf.dbFilename)
	with conn:
		
		authorIds = set()
		
		# POPULATE AUTHORSCOPUS
		contents = glob(pathAuthorsSearch + '*.json')
		contents.sort()
		for fileAuthorSearch in contents:
			with open(fileAuthorSearch) as json_file:
				j = json.load(json_file)
				for entry in j["search-results"]["entry"]:
					authorId = entry["dc:identifier"].replace("AUTHOR_ID:","")
					authorIds.add(authorId)
					try:
						surname = entry["preferred-name"]["surname"]
						givenname = entry["preferred-name"]["given-name"]
						initials = entry["preferred-name"]["initials"]
					except:
						print ("ERROR: name info not found - " + fileAuthorSearch)
						sys.exit()
					try:
						orcid = entry["orcid"]
					except:
						orcid = None
					
					try:
						authorTuple = (int(authorId),givenname,surname,initials,orcid)
						author_id = create_author(conn, authorTuple)
					except:
						print ("WARNING: authorId %s already present." % authorId)
		
		
		# POPULATE PUBLICATION
		contents = glob(pathPublications + '*.json')
		contents.sort()
		for filePublications in contents:
			with open(filePublications) as json_file:
				j = json.load(json_file)
				authorId = j["search-results"]["opensearch:Query"]["@searchTerms"].replace("AU-ID(","").replace(")","")
				#print (authorId)
				for entry in j["search-results"]["entry"]:
					eid = entry["eid"]
					try:
						doi = entry["prism:doi"]
					except:
						doi = None
					try:
						title = entry["dc:title"]
					except:
						title = None
					try:
						coverDate = entry["prism:coverDate"]
						publicationDate = coverDate
						publicationYear = coverDate.split("-")[0]
					except:
						publicationDate = None
						publicationYear = None
						# Togliere? Mettere Warning (invece di Error)?
						print ("ERROR: no date in " + filePublications)
						sys.exit()
					try:
						venueName = entry["prism:publicationName"]
					except:
						venueName = None

					publicationTuple = (eid,doi,publicationDate,publicationYear,title,venueName)
					try:
						create_publication(conn, publicationTuple)
					except:
						print ("WARNING - PUBLICATION ALREADY IN THE DB - %s, %s, %s, %s, %s, %s" % (eid,doi,publicationDate,publicationYear,title,venueName))


					# POPULATE WROTERELATION TABLE
					try:
						authors = entry["author"]
						for author in authors:
							authorId = author["authid"]

							# OCCHIO # OCCHIO # OCCHIO # 
							# keep only authors in authorIds
							if authorId in authorIds:
								wroteRelation = (authorId,eid)
								create_wroteRelation(conn,wroteRelation)
							# OCCHIO # OCCHIO # OCCHIO # 
					except:
						print ("WARNING - problem in wroteRelation(): " + eid)
						
'''
				# POPULATE PUBLICATION TABLE
				eid = j["abstracts-retrieval-response"]["coredata"]["eid"]
				try:
					doi = j["abstracts-retrieval-response"]["coredata"]["prism:doi"]
				except:
					#print ("Missing doi: " + eid)
					doi = ""
				try:
					publicationDate = j["abstracts-retrieval-response"]["coredata"]["prism:coverDate"]
				except:
					#print ("Missing publicationDate: " +eid)
					publicationDate = ""
				try:
					publicationYear = j["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["source"]["publicationyear"]["@first"]
				except:
					#print ("Missing publication year: " + eid)
					publicationYear = ""
				#title = j["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["citation-title"]
				try:
					title = j["abstracts-retrieval-response"]["coredata"]["dc:title"]
				except:
					title = ""
				#venueName = j["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["source"]["sourcetitle"]
				try:
					venueName = j["abstracts-retrieval-response"]["coredata"]["prism:publicationName"]
				except:
					venueName = ""
				publication = (eid,doi,publicationDate,publicationYear,title,venueName)
				try:
					create_publication(conn, publication)
				except:
					print ("%s, %s, %s, %s, %s, %s" % (eid,doi,publicationDate,publicationYear,title,venueName))
					sys.exit()
				#print (str(counter) + ") insert done!")
				counter += 1
				
				# POPULATE WROTERELATION TABLE
				try:
					authors = j["abstracts-retrieval-response"]["authors"]["author"]
					for author in authors:
						authorId = author["@auid"]

						# OCCHIO # OCCHIO # OCCHIO # 
						# keep only authors in cercauniversita or curriculum
						if authorId in authorIds:
							wroteRelation = (authorId,eid)
							create_wroteRelation(conn,wroteRelation)
						# OCCHIO # OCCHIO # OCCHIO # 

				except:
					print (eid + ": wroteRelation()")
				
				# POPULATE CITESRELATION TABLE
				try:
					references = j["abstracts-retrieval-response"]["item"]["bibrecord"]["tail"]["bibliography"]["reference"]
					if type(references) is not list:
						references = [references]
					for reference in references:
						try:
							#if "$" in reference["ref-info"]["refd-itemidlist"]["itemid"]:
							eidCited = "2-s2.0-" + reference["ref-info"]["refd-itemidlist"]["itemid"]["$"]
						except:
							eidCited = ""
						# Skip 1. papers without citations; 2. papers with no authors in the network
						if eidCited == "" or eidCited not in eids:
							continue
						else:
							citesRelation = (eid,eidCited,publicationDate,publicationYear)
							create_citesRelation(conn,citesRelation)
					#else:
					#	print (filename)
				except:
					#print ("No reference for " + eid + ": skipping.")
					pass
'''				
if __name__ == '__main__':
	main()


