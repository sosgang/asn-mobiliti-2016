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
import apikeys

import conf

apiURL_search = 'https://api.elsevier.com/content/search/author'
# scopus author search settings, see https://dev.elsevier.com/api_key_settings.html 
authorsPerPage = 200
numItemLimit = 5000

#pathInput = "../data/input/cercauniversita/"
#pathOutput = "../data/input/authors-search/"
#anno = "2016"
pathOutput = "../../data/input/cercauniversita/bologna/authors-search/"

def create_connection(db_file):
	""" create a database connection to the SQLite database
		specified by the db_file
	:param db_file: database file
	:return: Connection object or None
	"""
	conn = None
	try:
		conn = sqlite3.connect(db_file)
	except Error as e:
		print(e)
 
	return conn
	
def select_distinctCognomeNome(conn):
	q = """
		SELECT DISTINCT cognome, nome
		FROM cercauniversitaFromExcel
		LIMIT 200
		OFFSET 0"""
	cur = conn.cursor()
	cur.execute(q)
	rows = cur.fetchall()
	return rows
	

def searchAuthor(firstnames, lastnames, ateneo='', area=''):
	start = 0
	#filepath = os.path.join(pathOutput + sector.replace("/","") + "/", idCercauni + '.json')
	data = searchAuthorScopus(firstnames, lastnames, ateneo, area, start)
	numRes = int(data['search-results']['opensearch:totalResults'])
	numPerPage = int(data['search-results']['opensearch:itemsPerPage'])
	numStart = int(data['search-results']['opensearch:startIndex'])
	if numRes == 1:
		return [data]
	elif numRes == 0:
		temp = list()
		for firstname in firstnames:
			if len(firstnames) > 1:
				temp.append({'fn': [firstname], 'sn': lastnames})
		#for firstname in firstnames:	
			if len(lastnames) > 1:
				for lastname in lastnames:
					temp.append({'fn': [firstname], 'sn': [lastname]})
		for curr in temp:
			data = searchAuthorScopus(curr['fn'], curr['sn'], ateneo, area, 0) #, start)
			numRes = int(data['search-results']['opensearch:totalResults'])
			numPerPage = int(data['search-results']['opensearch:itemsPerPage'])
			numStart = int(data['search-results']['opensearch:startIndex'])
			if numRes == 1:
				return [data]
			elif numRes == 0:
				continue
			else:
				if numRes < numPerPage:
					return [data]
				else:
					res = [data]
					while (numStart + numPerPage) < numRes: # and start < numItemLimit:
						data = searchAuthorScopus(curr['fn'], curr['sn'], ateneo, area, numStart+numPerPage)
						res.append(data)
						numRes = int(data['search-results']['opensearch:totalResults'])
						numPerPage = int(data['search-results']['opensearch:itemsPerPage'])
						numStart = int(data['search-results']['opensearch:startIndex'])
						print ("numRes: %d, numStart: %d, numPerPage: %d" % (numRes, numStart, numPerPage))
					# TODO JOIN RES
					return res
	else:
		res = [data]
		while (numStart + numPerPage) < numRes: #and start < numItemLimit:
			data = searchAuthorScopus(firstnames, lastnames, ateneo, area, numStart+numPerPage)
			res.append(data)
			numRes = int(data['search-results']['opensearch:totalResults'])
			numPerPage = int(data['search-results']['opensearch:itemsPerPage'])
			numStart = int(data['search-results']['opensearch:startIndex'])
			print ("numRes: %d, numStart: %d, numPerPage: %d" % (numRes, numStart, numPerPage))
		return res
	return None

def searchAuthorScopus(firstname, lastname, ateneo, area, start=0, max_retry=3, retry_delay=1):
	
	retry = 0
	cont = True
	#SUBJAREA(XX)
	#AFFIL()
	query = 'AUTHFIRST(' + " ".join(firstname) + ') and AUTHLAST(' + " ".join(lastname) + ')'
	if ateneo != '':
		query += " AND AFFIL(" + ateneo + ")"
	if area != '':
		query += " AND SUBJAREA(" + area + ")"
	
	while retry < max_retry and cont:
		#queryEncoded = urllib.parse.quote(query)
		params = {
			'apikey':apikeys.keys[0],
			'httpAccept':'application/json',
			'query': query,
			'count': str(authorsPerPage),
			'start': str(start)
		}
		
		r = requests.get(apiURL_search, params=params)
			
		#if self.raw_output:
		#	self.save_raw_response(r.text)

		# quota exceeded -> http 429 (see https://dev.elsevier.com/api_key_settings.html)
		if r.status_code == 429:
			print ("Quota exceeded for key " + apikeys.keys[0] + " - EXIT.")
			apikeys.keys.pop(0)
		
		elif r.status_code > 200 and r.status_code < 500:
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
		
	json = r.json()
	numRes = int(json['search-results']['opensearch:totalResults'])
	print(str(numRes) + ": " + query)
	return json
	
	
	


def main():


	if not os.path.isdir(pathOutput):
		os.makedirs(pathOutput)
	
	conn = create_connection(conf.dbFilename)
	
	missing = list()
	with conn:
		rows = select_distinctCognomeNome(conn)
		for row in rows:
			cognome = row[0]
			nome = row[1]
			print ("Cognome: %s, Nome: %s" % (cognome, nome))

			data = searchAuthor(nome.split(), cognome.split(), "Bologna")
			if data is None:
				print ("Res = None -> searching without 'Bologna'")
				data = searchAuthor(nome.split(), cognome.split())
			if data is not None:
				if type(data) is list and len(data) > 1:
					for i in range(0,len(data)):
						completepath = os.path.join(pathOutput, cognome.replace(" ","-") + '_' + nome.replace(" ","-") + '.json')
						with open(completepath, 'w') as outfile:
							json.dump(data[i], outfile, indent=3)
				else:
					completepath = os.path.join(pathOutput, cognome.replace(" ","-") + '_' + nome.replace(" ","-") + '.json')
					with open(completepath, 'w') as outfile:
						json.dump(data[0], outfile, indent=3)
			else:
				print ("%s, %s: not found" % (cognome, nome))
				missing.append([cognome,nome])
			
			#sys.exit()

	print ("Missing (i.e. no match found):")
	for el in missing:
		print (el)
	
	
	

if __name__ == '__main__':
	main()
	
	
	
	'''
	missing = list()
	with open(fileCercauniversita, newline='') as csvfile:
		if not os.path.isdir(pathOutput + sector.replace("/","") + "/"):
			os.makedirs(pathOutput + sector.replace("/","") + "/")
		counter = 0
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			sn = (row['Cognome e Nome']).split()
			idCercauni = row['Id']
		
			lastname = list()
			firstname = list()
			for part in sn:
				if part.isupper():
					lastname.append(part)
				else:
					firstname.append(part)
			data = searchAuthor(firstname, lastname, idCercauni, sector)
			if data is not None:
				if type(data) is list and len(data) > 1:
					for i in range(0,len(data)):
						completepath = os.path.join(pathOutput + sector.replace("/","") + "/", idCercauni + '_' + str(i+1) + '.json')
						with open(completepath, 'w') as outfile:
							json.dump(data[i], outfile, indent=3)
				else:
					completepath = os.path.join(pathOutput + sector.replace("/","") + "/", idCercauni + '.json')
					with open(completepath, 'w') as outfile:
						json.dump(data[0], outfile, indent=3)
			else:
				missing.append(idCercauni)
	
	if len(missing) > 0:
		res = "Id	Fascia	Cognome e Nome	Genere	Ateneo	Facoltà	S.S.D.	S.C.	Struttura di afferenza\n"
		for row in table:
			currId = row["Id"]
			if currId in missing:
				#print (missing)
				res += row["Id"] + "\t" + row["Fascia"] + "\t" + row["Cognome e Nome"] + "\t" + row["Genere"] + "\t" + row["Ateneo"] + "\t" + row["Facoltà"] + "\t" + row["S.S.D."] + "\t" + row["S.C."] + "\t" + row["Struttura di afferenza"] + "\n"   
		text_file = open(pathInput + "notFoundCercauniversita_" + sector.replace("/","")  + ".csv", "w")
		text_file.write(res)
		text_file.close()
	'''
