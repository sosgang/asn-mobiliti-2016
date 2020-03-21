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
 

def select_distinctCognomeNome(conn):
	q = """
		SELECT DISTINCT id,cognome,nome
		FROM cercauniversitaFromExcel
		WHERE anno=2019
		ORDER BY cognome, nome
		"""
		#'Perez Vazquez'
		# %Albanes
		# WHERE anno=2019 AND cognome LIKE 'A%' 
		# WHERE anno=2019 AND cognome LIKE '%Angelin%' AND nome='Giovanni'
	AND cognome <> 'Perez Vazquez' AND cognome <> 'Martin' AND nome <> 'Sanna Maria'
	cur = conn.cursor()
	cur.execute(q)
	rows = cur.fetchall()
	return rows


def select_publicationTitle(conn,authorId):
	q = """
		SELECT DISTINCT title
		FROM wroteRelation
		INNER JOIN publication
		ON
		  wroteRelation.eid=publication.eid
		INNER JOIN pubblicazioneUniBo
		ON
		  publication.title=pubblicazioneUniBo.titolo
		WHERE wroteRelation.authorId='{auid}'
		"""
	
	cur = conn.cursor()
	cur.execute(q.format(auid=authorId))
	rows = cur.fetchall()
	return rows


def create_table(conn, create_table_sql):
	""" create a table from the create_table_sql statement
	:param conn: Connection object
	:param create_table_sql: a CREATE TABLE statement
	:return:
	"""
	try:
		c = conn.cursor()
		c.execute("DROP TABLE IF EXISTS mapping;")
		c = conn.cursor()
		c.execute(create_table_sql)
	except Error as e:
		print(e)
		
def create_mapping(conn, mapping):
	"""
	Create a new author into the mapping table
	:param conn:
	:param mapping:
	:return: mapping id
	"""
	sql = """ INSERT INTO mapping(idCercauniversita,authorId,cognome,nome,numAuthorIdMatches,numPublicationsMatches,numFoundAUthorSeachScopus)
			  VALUES(?,?,?,?,?,?,?) """
	cur = conn.cursor()
	cur.execute(sql, mapping)
	return cur.lastrowid


def getInfoFromJson(filename):
	res = list()
	with open(filename) as json_file:
		data = json.load(json_file)
		totRes = int(data["search-results"]["opensearch:totalResults"])
		query = data["search-results"]["opensearch:Query"]["@searchTerms"]
		if totRes == 0:
			print ("ERROR: no results for file " + filename)
			sys.exit()
		else:
			for entry in data["search-results"]["entry"]:
				authorId = int(entry["dc:identifier"].replace("AUTHOR_ID:",""))
				#print (authorId)
				try:
					orcid = entry["orcid"]
				except:
					orcid = None
				try:
					documentCount = int(entry["document-count"])
				except:
					documentCount = None
				try:
					subjectAreasList = list()
					for sa in entry["subject-area"]:
						subjectAreasList.append(sa["@abbrev"] + "(" + sa["@frequency"] + ")")
					subjectAreas = ", ".join(subjectAreasList)
				except:
					subjectAreas = None
				res.append({"authorId": authorId, "orcid": orcid, "documentCount": documentCount, "subjectAreas": subjectAreas,"query":query})
	return res

# Escludo persone che hanno tantissime (1000+) risultati in authorSearch Scopus -> Vanno gestiti a mano
def isToExclude(cognome,nome):
	if cognome == 'Perez Vazquez' and nome == 'Maria Enriqueta':
		return True
	elif cognome == 'Martin' and nome == 'Sanna Maria':
		return True
	else:
		return False

def main():
	sql_create_mapping_table = """ CREATE TABLE IF NOT EXISTS mapping (
										idCercauniversita integer NOT NULL,
										authorId integer,
										cognome string NOT NULL,
										nome string NOT NULL,
										numAuthorIdMatches integer NOT NULL,
										numPublicationsMatches integer NOT NULL,
										numFoundAUthorSeachScopus integer NOT NULL,
										FOREIGN KEY (idCercauniversita) REFERENCES cercauniversitaFromExcel(id),
										FOREIGN KEY (authorId) REFERENCES authorScopus(id)
									); """

	# create a database connection
	conn = create_connection(conf.dbFilename)
 
	# create tables
	if conn is not None:
		create_table(conn, sql_create_mapping_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	
	# POPULATE MAPPING
	conn = create_connection(conf.dbFilename)
	with conn:
		rows = select_distinctCognomeNome(conn)
		counterOneMatch = 0
		for row in rows:
			idCercauni = int(row[0])
			cognome = row[1]
			nome = row[2]
			#print ("%s) %s - %s" % (idCercauni, cognome, nome))
			
			authorIds = set()
			filename_regex = os.path.join(pathAuthorsSearch, cognome.replace(" ","-") + '_' + nome.replace(" ","-") + '*') 
			contents = glob(filename_regex)
			
			contents.sort()
			for filename_withPath in contents:
				#path = os.path.dirname(os.path.abspath(filename_withPath))
				#filename = os.path.basename(filename_withPath)
				jInfos = getInfoFromJson(filename_withPath)	
				for jInfo in jInfos:
					authorIds.add(jInfo["authorId"])
			
			#print ("len(authorIds) = %s" % len(authorIds))
			
			# Escludo persone che hanno tantissime (1000+) risultati in authorSearch Scopus -> Vanno gestiti a mano
			if isToExclude(cognome,nome):
				mappingTuple = (idCercauni,None,cognome,nome,0,0,len(authorIds)) #idCercauniversita,authorId,cognome,nome,numMatches
				create_mapping(conn, mappingTuple)
				continue
			
			if len(authorIds) == 0:
				mappingTuple = (idCercauni,None,cognome,nome,0,0,len(authorIds)) #idCercauniversita,authorId,cognome,nome,numMatches
				create_mapping(conn, mappingTuple)
				pass
			else:
				authorIdsFound = list()
				numRowsFound = list()
				for authorId in authorIds:
					#print ("Managing authorId %s" % authorId)
					rowsPubs = select_publicationTitle(conn,authorId)
					if len(rowsPubs) > 0:
						authorIdsFound.append(authorId)
						numRowsFound.append(len(rowsPubs))
			
				# POPULATE DB
				if len(authorIdsFound) != 0:
					for i in range(0,len(authorIdsFound)):
						mappingTuple = (idCercauni,authorIdsFound[i],cognome,nome,len(authorIdsFound),numRowsFound[i],len(authorIds)) #idCercauniversita,authorId,cognome,nome,numMatches
						create_mapping(conn, mappingTuple)
				else:
					mappingTuple = (idCercauni,None,cognome,nome,len(authorIdsFound),0,len(authorIds)) #idCercauniversita,authorId,cognome,nome,numMatches
					create_mapping(conn, mappingTuple)
				
				# PRINT
				if len(authorIdsFound) == 1:
					print ("OK: ONE MATCH: %s - %s -> %s" % (cognome, nome,authorId))
					counterOneMatch += 1
				elif len(authorIdsFound) > 1:
					print ("ERROR: 2+ matches: %s - %s -> %s" % (cognome, nome,authorId))
			
			'''
			if len(contents) == 0:
				#create_mappingCercauniversitaAuthorId(conn, (cognome,nome,None,0,None,None,None,None,None,",".join(urls),len(urls)))
				#print ("QUI")
				mappingTuple = (idCercauni,None,cognome,nome,0,0,len(contents)) #idCercauniversita,authorId,cognome,nome,numMatches
				create_mapping(conn, mappingTuple)
				pass
			else:
				#print ("")
				contents.sort()
				for filename_withPath in contents:
					path = os.path.dirname(os.path.abspath(filename_withPath))
					filename = os.path.basename(filename_withPath)
				
					jInfos = getInfoFromJson(filename_withPath)
					authorIdsFound = list()
					numRowsFound = list()
					for jInfo in jInfos:
						rowsPubs = select_publicationTitle(conn,jInfo["authorId"])
						#for rowPubs in rowsPubs:
						#	print (rowPubs[0])
						#print ("len RowsPubs: %d" % len(rowsPubs))
						if len(rowsPubs) > 0:
							authorIdsFound.append(jInfo["authorId"])
							numRowsFound.append(len(rowsPubs))
							
					#print ("len authorIdsFound: %d" % len(authorIdsFound))
					# POPULATE DB
					if len(authorIdsFound) != 0:
						for i in range(0,len(authorIdsFound)):
							#print ("QUO")
							mappingTuple = (idCercauni,authorIdsFound[i],cognome,nome,len(authorIdsFound),numRowsFound[i],len(contents)) #idCercauniversita,authorId,cognome,nome,numMatches
							create_mapping(conn, mappingTuple)
						#for authorIdFound in authorIdsFound:
						#	mappingTuple = (idCercauni,authorIdFound["authorId"],cognome,nome,len(authorIdsFound),authorIdFound["numRows"]) #idCercauniversita,authorId,cognome,nome,numMatches
						#	create_mapping(conn, mappingTuple)
					else:
						#print ("QUA")
						mappingTuple = (idCercauni,None,cognome,nome,len(authorIdsFound),0,len(contents)) #idCercauniversita,authorId,cognome,nome,numMatches
						create_mapping(conn, mappingTuple)
					
					# PRINT
					if len(authorIdsFound) == 1:
						print ("OK: ONE MATCH: %s - %s -> %s" % (cognome, nome,jInfo["authorId"]))
						counterOneMatch += 1
					elif len(authorIdsFound) > 1:
						print ("ERROR: 2+ matches: %s - %s -> %s" % (cognome, nome,jInfo["authorId"]))
			'''		
					
									
		print (len(rows))
		print (counterOneMatch)
		'''			
		SCHEMA: 
			prendo nome,cognome in cercauniversita 2019
				per ognuno, 
					prendo lista authorId scopus da json in pathAuthorsSearch
					found = 0
					per ogni authorId, 
						#prendo relative pubblicazioni considerando eid,titolo,autori e li metto in listaPubs[authorId]
						faccio query di sue pubblicazioni scopus join pupplicazioniUnibo WHERE pubScopus.titolo=pubUnibo.titolo
						conto res: se > 0 => inserisco match idCercauniversita/authorId
						found += 1
					se found > 1:
						ERROR!
		'''
if __name__ == '__main__':
	main()

		
qTest1 = """
SELECT DISTINCT cercauniversitaFromExcel.id
FROM cercauniversitaFromExcel
INNER JOIN rubricaUniBo
ON
  cercauniversitaFromExcel.cognome=rubricaUniBo.cognome AND
  cercauniversitaFromExcel.nome=rubricaUniBo.nome
INNER JOIN pubblicazioneUniBo
ON
  rubricaUniBo.id=pubblicazioneUniBo.idRubricaUniBo
INNER JOIN publication
ON
  pubblicazioneUniBo.titolo=publication.title
WHERE cercauniversitaFromExcel.anno=2020
"""
