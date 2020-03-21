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

pathHtml = "../../data/input/cercauniversita/bologna/html-rubrica/"
pathPublications = "../../data/input/cercauniversita/bologna/html-publications/"

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
	cur = conn.cursor()
	cur.execute(q)
	rows = cur.fetchall()
	return rows

"""
def create_mappingCercauniversitaAuthorId(conn, mappingCercauniversitaAuthorId):
	'''
	Create a new record into the cercauniversitaFromExcel table
	:param conn:
	:param record:
	:return: mappingCercauniversitaAuthorId id
	'''
	
	sql = ''' INSERT INTO mappingCercauniversitaAuthorId(cognome,nome,authorId,numMappings,orcid,documentCount,subjectAreas,filename,query,homepage,numLinks)
			  VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
	#print (sql)
	cur = conn.cursor()
	cur.execute(sql, mappingCercauniversitaAuthorId)
	return cur.lastrowid
"""


def create_rubricaUniBo(conn, rubricaRecord):
	'''
	Create a new record into the cercauniversitaFromExcel table
	:param conn:
	:param rubricaRecord:
	:return: rubricaUniBo id
	'''
	
	sql = ''' INSERT INTO rubricaUniBo(id,cognome,nome,idStruttura,nomeStruttura,homepage)
			  VALUES(?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, rubricaRecord)
	return cur.lastrowid

def create_pubblicazioneUniBo(conn, pubblicazioneRecord):
	'''
	Create a new record into the cercauniversitaFromExcel table
	:param conn:
	:param pubblicazioneRecord:
	:return: pubblicazioneUniBo id
	'''
	
	sql = ''' INSERT INTO pubblicazioneUniBo(idRubricaUniBo,testo,autori,titolo)
			  VALUES(?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, pubblicazioneRecord)
	return cur.lastrowid


def main():
	
	sql_create_rubricaUniBo_table = """ CREATE TABLE IF NOT EXISTS rubricaUniBo (
										id integer PRIMARY KEY,
										cognome text NOT NULL,
										nome text NOT NULL,
										idStruttura integer,
										nomeStruttura string,
										homepage string
									); """
	
	sql_create_pubblicazioneUniBo_table = """ CREATE TABLE IF NOT EXISTS pubblicazioneUniBo (
										idRubricaUniBo integer,
										testo string NOT NULL,
										autori string,
										titolo string,
										FOREIGN KEY (idRubricaUnibo) REFERENCES rubricaUniBo(id)
									); """
	
	# create tables
	conn = create_connection(conf.dbFilename)
	if conn is not None:
		create_table(conn, sql_create_rubricaUniBo_table)
		create_table(conn, sql_create_pubblicazioneUniBo_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	
	counterHtmlFiles = 0
	conn = create_connection(conf.dbFilename)
	with conn:
		
		rows = select_distinctCognomeNome(conn)
		for row in rows:
			cognome = row[0]
			nome = row[1]
			
			# POPULATE RUBRICAUNIBO
			htmlFile = os.path.join(pathHtml, cognome.replace(" ","-") + '_' + nome.replace(" ","-") + '.html')
			if not os.path.isfile(htmlFile):
				continue
			
			counterHtmlFiles += 1
			treeHomepage = html.parse(htmlFile)
			tables = treeHomepage.xpath("//table[@class='contact vcard'][.//tr[th[text()='web']]]")
			mapHomepageIdAutore = dict()
			i = 0
			for table in tables:
				i+=1
				if len(table.xpath(".//tr[th[text()='web']]")) == 1:
					homepage = table.xpath(".//tr[th[text()='web']]/td[1]/a/text()")[0]
				else:
					print ("ERROR: homepage not found for %s - %s" % (cognome,nome))
					sys.exit()
				if len(table.xpath(".//tr[td[@class='fn name']]")) == 1:
					cognomeNomeAutore = table.xpath(".//tr/td[@class='fn name']/text()")[0]
					cognomeRubrica = cognomeNomeAutore.split(", ")[0]
					nomeRubrica = cognomeNomeAutore.split(", ")[1]
				else:
					print ("ERROR: cognomeNomeAutore not found for %s - %s" % (cognome,nome))
					sys.exit()
				if len(table.xpath(".//tr[th[@class='uid']]")) == 1:
					idAutore = int(table.xpath(".//tr/th[@class='uid']/text()")[0])
				else:
					print ("ERROR: idAutore not found for %s - %s" % (cognome,nome))
					sys.exit()
				if len(table.xpath(".//tr[3]")) == 1:
					try:
						idStruttura = int(table.xpath(".//tr[3]/th/text()")[0])
						nomeStruttura = table.xpath(".//tr[3]/td/text()")[0]
					except:
						# rettore Francesco Ubertini non ha idStruttura
						idStruttura = 0
						print ("Rettore: %s - %s" % (cognome,nome))
				else:
					print ("ERROR: idStruttura e nomeStruttura not found for %s - %s" % (cognome,nome))
					sys.exit()
				mapHomepageIdAutore[homepage] = {"idAutore": idAutore, "idStruttura": idStruttura, "nomeStruttura": nomeStruttura, "nome": nomeRubrica, "cognome": cognomeRubrica}
				
				for homepage in mapHomepageIdAutore:
					data = mapHomepageIdAutore[homepage]
					
					#print (mapHomepageIdAutore)
					if cognome != data["cognome"] and nome != data["nome"]:
						print ("Cercauniversita: %s - %s, Rubrica: %s - %s" % (cognome,nome,data["cognome"],data["nome"])) 
					
					try:
						rubricaTuple = (data["idAutore"],cognome,nome,data["idStruttura"],data["nomeStruttura"],homepage)
						create_rubricaUniBo(conn,rubricaTuple)
					except:
						print ("WARNING: %s - %s already in the DB." % (cognome, nome))
			
					
		# POPULATE PUBBLICAZIONEUNIBO
		contents = glob(pathPublications + "*.html")
		contents.sort()
		for filePublications in contents:
			#path = os.path.dirname(os.path.abspath(filename_withPath))
			filename = os.path.basename(filePublications)
			idAutore = filename.split("_")[0]
				
			treePublications = html.parse(filePublications)
			try:
				pars = treePublications.xpath("//div[@class='report-list']/p")
			except:
				print ("WARNING: unable to parse file " + filename)
				continue
			for par in pars:
				try:
					authors = par.xpath("span[@class='author']/text()")[0]
					title = par.xpath("em/text()")[0]
					text = ""
					for t in par.xpath(".//text()"):
						text += t
					#print (text)
				except:
					print ("WARNING: unable to extract authors and title - %s" % (filename))
					authors = None
					#print ("%s - %s" % (authors, title))
				if authors is not None:
					# FPOGGI: ho messo titolo minuscolo
					pubblicazioneTuple = (idAutore,text,authors,title.lower())
					create_pubblicazioneUniBo(conn,pubblicazioneTuple)

	print (counterHtmlFiles)

if __name__ == '__main__':
	main()
