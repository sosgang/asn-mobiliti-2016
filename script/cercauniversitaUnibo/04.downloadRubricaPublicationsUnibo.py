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
from lxml import html

sys.path.append('..')
import apikeys
import conf

pathInput = "../../data/input/cercauniversita/bologna/authors-search/"
pathHtml = "../../data/input/cercauniversita/bologna/html-rubrica/"
pathPublications = "../../data/input/cercauniversita/bologna/html-publications/"
waitTime = 0.5

def create_connection(db_file):
	""" create a database connection to the SQLite database
		specified by the db_file
	:param db_file: database file
	:return: Connection objecfrom lxml import htmlt or None
	"""
	conn = None
	try:
		conn = sqlite3.connect(db_file)
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

def select_distinctCognomeNome(conn):
	q = """
		SELECT DISTINCT cognome, nome
		FROM cercauniversitaFromExcel
		"""
		#WHERE cognome LIKE 'A%'
		#LIMIT 10
		#OFFSET 0"""
	cur = conn.cursor()
	cur.execute(q)
	rows = cur.fetchall()
	return rows

def create_mappingCercauniversitaAuthorId(conn, mappingCercauniversitaAuthorId):
	"""
	Create a new record into the cercauniversitaFromExcel table
	:param conn:
	:param record:
	:return: mappingCercauniversitaAuthorId id
	"""
	
	sql = ''' INSERT INTO mappingCercauniversitaAuthorId(cognome,nome,authorId,numMappings,orcid,documentCount,subjectAreas,filename,query,homepage,numLinks)
			  VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
	#print (sql)
	cur = conn.cursor()
	cur.execute(sql, mappingCercauniversitaAuthorId)
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

def getUrlHomepage(cognome,nome,max_retry=2, retry_delay=1):
	urlRubricaUnibo = "https://www.unibo.it/uniboweb/unibosearch/rubrica.aspx?mode=people&query=%2bnome%3a" + nome.replace(" ", "+%2bnome%3a") + "+%2bcognome%3a" + cognome.replace(" ", "+%2bcognome%3a") + "&tab=PersonePanel"
	return getUrl_toString(urlRubricaUnibo)

def getUrl_toString(url,max_retry=2, retry_delay=1):
	time.sleep(waitTime)
	retry = 0
	cont = True
	while retry < max_retry and cont:
		#https://www.unibo.it/uniboweb/unibosearch/rubrica.aspx?mode=people&query=%2bnome%3afabio+%2bcognome%3avitali&tab=PersonePanel
		#print (url)
		
		#queryEncoded = urllib.parse.quote(query)
		r = requests.get(url)
		
		#header = r.headers
		#content_type = header.get('content-type')
		#print (content_type)
		
		#if self.raw_output:
		#	self.save_raw_response(r.text)

		#print (r.status_code)

		if r.status_code > 200 and r.status_code < 500:
			print(u"{}: errore nella richiesta: {}".format(r.status_code, r.url))
			return None

		if r.status_code != 200:
			retry += 1
			if retry < max_retry:
				time.sleep(retry_delay)
			continue

		cont = False 
			 
	if retry >= max_retry: 
		return None 
		
	#return r.content
	return r.text

def saveToFile(filename,text):
	
	path = os.path.dirname(os.path.abspath(filename))
	if not os.path.isdir(path):
		os.makedirs(path)
		
	with open(filename, 'w') as outfile:
		outfile.write(text)
		#outfile.close()

def main():
	'''
	sql_create_mappingCercauniversitaAuthorId_table = """ CREATE TABLE IF NOT EXISTS mappingCercauniversitaAuthorId (
										id integer PRIMARY KEY AUTOINCREMENT,
										cognome text NOT NULL,
										nome text NOT NULL,
										authorId integer,
										numMappings integer,
										orcid string,
										documentCount int,
										subjectAreas string,
										filename string,
										query string,
										homepage string,
										numLInks integer
									); """
	
	# create tables
	conn = create_connection(conf.dbFilename)
	if conn is not None:
		create_table(conn, sql_create_mappingCercauniversitaAuthorId_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	'''
	
	conn = create_connection(conf.dbFilename)
	with conn:
	
		# DOWNLOAD HOMEPAGE	
		rows = select_distinctCognomeNome(conn)
		for row in rows:
			cognome = row[0]
			nome = row[1]
			
			print ("%s - %s" % (cognome, nome))
			
			# DOWNLOAD FILE
			#if nome == "Giuseppe" and cognome == "Di Modica":
			#	print (cognome + " - " + nome)
			
			htmlFile = os.path.join(pathHtml, cognome.replace(" ","-") + '_' + nome.replace(" ","-") + '.html')
			
			# Se non ho ancora scaricato il file
			if not os.path.isfile(htmlFile):
				# HOMEPAGE: DOWNLOAD AND SAVE TO FILE
				htmlText = getUrlHomepage(cognome,nome)
				urls = list()
				if "non ha prodotto risultati" in htmlText:
					print ("\tNessuna homepage trovata per %s - %s" % (cognome,nome))
					continue
					#sys.exit()
				else:
					# save HTML to file
					saveToFile(htmlFile,htmlText)
					print ("\tHomepage scaricata e salvata su file per %s - %s" % (cognome,nome))
					'''
					tree = html.document_fromstring(htmlText)
					tables = tree.xpath("//table[@class='contact vcard']")
					for table in tables:
						if len(table.xpath(".//tr[th[text()='web']]")) == 1:
							vcardEl = table.xpath(".//tr[th[text()='web']]/td[1]/a/text()")
							urls.append(vcardEl[0])
						else:
							print ("PROBLEMA: numero di link web = %d (%s - %s)" % (len(table.xpath(".//tr[th[text()='web']]")),cognome,nome))
				
				if len(urls) > 1:
					print ("Numero di link > 1: %s - %s" % (cognome,nome))
				
				filename_regex = os.path.join(pathInput, cognome.replace(" ","-") + '_' + nome.replace(" ","-") + '*') 
				contents = glob(filename_regex)
				if len(contents) == 0:
					create_mappingCercauniversitaAuthorId(conn, (cognome,nome,None,0,None,None,None,None,None,",".join(urls),len(urls)))
				else:
					contents.sort()
					for filename_withPath in contents:
						path = os.path.dirname(os.path.abspath(filename_withPath))
						filename = os.path.basename(filename_withPath)
					
						jInfos = getInfoFromJson(filename_withPath)
						for jInfo in jInfos:
							#if len(urls) > 0:
							create_mappingCercauniversitaAuthorId(conn, (cognome,nome,jInfo["authorId"],len(contents),jInfo["orcid"],jInfo["documentCount"],jInfo["subjectAreas"],filename,jInfo["query"],",".join(urls),len(urls)))
					'''

				
				
				
					# PUBLICATION PAGE: DOWNLOAD AND SAVE TO FILE
					treeHomepage = html.parse(htmlFile)
					tables = treeHomepage.xpath("//table[@class='contact vcard'][.//tr[th[text()='web']]]")
					if len(tables) != 1:
						print ("\tNumero link homepage trovati = %d (!= 1) - %s" % (len(tables),htmlFile))
			
					# solo per tabelle in cui c'è link a homepage
					mapHomepageIdAutore = dict()
					i = 0
					for table in tables:
						i+=1
						if len(table.xpath(".//tr[th[text()='web']]")) == 1:
							homepage = table.xpath(".//tr[th[text()='web']]/td[1]/a/text()")[0]
						else:
							print ("\tERROR: homepage not found for %s - %s" % (cognome,nome))
							sys.exit()
						if len(table.xpath(".//tr[th[@class='uid']]")) == 1:
							idAutore = int(table.xpath(".//tr/th[@class='uid']/text()")[0])
						else:
							print ("\tERROR: idAutore not found for %s - %s" % (cognome,nome))
							sys.exit()
						mapHomepageIdAutore[homepage] = {"idAutore": idAutore}
					#print (mapHomepageIdAutore)
			
			
					for homepage in mapHomepageIdAutore:
						idAutore = mapHomepageIdAutore[homepage]["idAutore"]
						publicationsUrl = homepage + "/pubblicazioni"
						publicationsText = getUrl_toString(publicationsUrl)
						if publicationsText is None:
							print ("\tNo publications - skip")
							continue
						publicationsFile = os.path.join(pathPublications, str(idAutore) + '_1.html')
						saveToFile(publicationsFile,publicationsText)
						
						treePublications = html.document_fromstring(publicationsText)
						hrefs = treePublications.xpath("//div[@class='pagination']//a/@href[contains(.,'page=')]")
						
						#print ()
						#print (len(hrefs)) 
						#print ()
						if len(hrefs) == 0:
							continue
							
						indexLastPage = int(hrefs[len(hrefs)-1].split("=")[1])
						for index in range(2,indexLastPage+1):
							#print (index)
							publicationsText = getUrl_toString(publicationsUrl + "?page=" + str(index))
							publicationsFile = os.path.join(pathPublications, str(idAutore) + "_" + str(index) + '.html')
							saveToFile(publicationsFile,publicationsText)
						print ("\tPublications page(s) scaricata/e e salvata/e.")
	
	'''
	# DOWNLOAD PUBLICATIONS PAGE
	contents = glob(pathHtml + '*.html')
	contents.sort()
	for htmlFile in contents:
		#with open(htmlFile,"r") as f:
		#	htmlText = f.read()
		#	if "Docente dell’Università di Bologna fino al" in htmlText:
		#		print (htmlFile)
		#	else:
		#		treeHomepage = html.document_fromstring(htmlText)
		
		treeHomepage = html.parse(htmlFile)
		tables = treeHomepage.xpath("//table[@class='contact vcard'][.//tr[th[text()='web']]]")
		if len(tables) != 1:
			print (htmlFile + ": " + str(len(tables)))
		
		# solo per tabelle in cui c'è link a homepage
		mapHomepageIdAutore = dict()
		i = 0
		for table in tables:
			i+=1
			if len(table.xpath(".//tr[th[text()='web']]")) == 1:
				homepage = table.xpath(".//tr[th[text()='web']]/td[1]/a/text()")[0]
			else:
				print ("ERROR: homepage not found for %s - %s" % (cognome,nome))
				sys.exit()
			if len(table.xpath(".//tr[th[@class='uid']]")) == 1:
				idAutore = int(table.xpath(".//tr/th[@class='uid']/text()")[0])
			else:
				print ("ERROR: idAutore not found for %s - %s" % (cognome,nome))
				sys.exit()
			mapHomepageIdAutore[homepage] = {"idAutore": idAutore}
		print (mapHomepageIdAutore)
		
		
		for homepage in mapHomepageIdAutore:
			idAutore = mapHomepageIdAutore[homepage]["idAutore"]
			publicationsUrl = homepage + "/pubblicazioni"
			publicationsText = getUrl_toString(publicationsUrl)
			if publicationsText is None:
				print ("No publications - skip")
				continue
			publicationsFile = os.path.join(pathPublications, str(idAutore) + '_1.html')
			saveToFile(publicationsFile,publicationsText)
			
			treePublications = html.document_fromstring(publicationsText)
			hrefs = treePublications.xpath("//div[@class='pagination']//a/@href[contains(.,'page=')]")
			
			print ()
			print (len(hrefs)) 
			print ()
			if len(hrefs) == 0:
				continue
				
			indexLastPage = int(hrefs[len(hrefs)-1].split("=")[1])
			for index in range(2,indexLastPage+1):
				#print (index)
				publicationsText = getUrl_toString(publicationsUrl + "?page=" + str(index))
				publicationsFile = os.path.join(pathPublications, str(idAutore) + "_" + str(index) + '.html')
				saveToFile(publicationsFile,publicationsText)
	'''	
if __name__ == '__main__':
	main()

'''
counterSingleFiles_MultipleRes = 0
counterSingleFiles = 0
counterMultipleFiles = 0

contents = glob(pathInput + '*.json')
contents.sort()
for filename_withPath in contents:
	path = os.path.dirname(os.path.abspath(filename_withPath))
	filename = os.path.basename(filename_withPath)
	# Multiple files
	if len(filename.split("_")) == 3:
		#print (filename)
		counterMultipleFiles += 1
	else:
		counterSingleFiles += 1
		with open(filename_withPath) as json_file:
			data = json.load(json_file)
			totRes = int(data["search-results"]["opensearch:totalResults"])
			if totRes > 1:
				counterSingleFiles_MultipleRes += 1
		
print ("Num. single files: " + str(counterSingleFiles))
print ("\tof which, multiple res: " + str(counterSingleFiles_MultipleRes))
print ("Num. multiple files: " + str(counterMultipleFiles))
'''
