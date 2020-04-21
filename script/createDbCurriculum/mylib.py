# -*- coding: UTF-8 -*-
import datetime
import time
import requests
import csv
import ast
from glob import glob
import json
import sys
import re
from lxml import html
import os
import json
import urllib.parse
import sqlite3
from sqlite3 import Error

sys.path.append('..')
import apikeys

apiURL_Abstract = {
	'doi': 'https://api.elsevier.com/content/abstract/doi/',
	'eid': 'https://api.elsevier.com/content/abstract/eid/'
}

apiURL_Search = "https://api.elsevier.com/content/search/scopus"



##### TODO ##### TODO ##### TODO ##### TODO ##### TODO #####
# controlla che json dell'abstract ritornato da api sia ok
##### TODO ##### TODO ##### TODO ##### TODO ##### TODO #####
def checkAbsFormat(j):
	#print (j)
	numRes = int(j["search-results"]["opensearch:totalResults"])
	pubs = j["search-results"]["entry"]
	if numRes == len(pubs):
		return True
	else:
		print (j["search-results"]["opensearch:Query"]["@searchTerms"] + " - ERROR: numRes=" + str(numRes) + ", numPubs in Json=" + str(len(pubs)))
		return False


def saveJsonPubs(j, authorId, pathOutput):

	if (checkAbsFormat(j)):
		if not os.path.isdir(pathOutput):
			os.makedirs(pathOutput)
			
		completepath = os.path.join(pathOutput, authorId + '.json')

		with open(completepath, 'w') as outfile:
			json.dump(j, outfile, indent=3)
		
		return True
	else:
		return False


#https://api.elsevier.com/content/search/scopus?apikey=f5f5306cfd6042a38e90dc053d410c56&httpAccept=application/json&query=AU-ID(55303032000)&view=COMPLETE&start=25&count=26
def getPublicationPage(authorId, start, max_retry=2, retry_delay=1):
	
	retry = 0
	cont = True
	while retry < max_retry and cont:

		query = "AU-ID(" + authorId + ")"
		params = {"apikey":apikeys.keys[0], "httpAccept":"application/json", "query": query, "view": "COMPLETE", "start": start}
		r = requests.get(apiURL_Search, params=params)
				
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
 
	j = r.json() 
	j['request-time'] = str(datetime.datetime.now().utcnow())
	# TO DECODE:
	#oDate = datetime.datetime.strptime(json['request-time'], '%Y-%m-%d %H:%M:%S.%f')
	return j	


def mergeJson(json1, json2):
	try:
		pubs1 = json1["search-results"]["entry"]
		pubs2 = json2["search-results"]["entry"]
		pubs12 = pubs1 + pubs2
		json1["search-results"]["entry"] = pubs12
	except:
		print ("ERROR in mergeJson()")
	return json1


def getPublicationList(authorId, pathPublicationList):
	jFilename = pathPublicationList + authorId + ".json"
	if os.path.exists(jFilename):
		# json file already downloaded => return None
		print ("Author %s: publication list already downloaded -> skip" % authorId)
		return None
	else:
		#print ("NOT FOUND: " + authorId)
		j = getPublicationPage(authorId, 0)
		try:
			numResults = int(j["search-results"]["opensearch:totalResults"])
			numDownloaded = 25
			while numDownloaded < numResults:
				print (authorId)
				jPart = getPublicationPage(authorId, numDownloaded)
				j = mergeJson(j, jPart)
				numDownloaded += 25
		except:
			print ("ERROR in getPublicationList()")
		return j


#'https://api.elsevier.com/content/abstract/scopus_id/0032717048?apikey=5953888c807d52ee017df48501d3e598&httpAccept=application/json&view=FULL'
def getAbstract(doi, doiOrEid, apikeys, max_retry=2, retry_delay=1):
	
	retry = 0
	cont = True
	while retry < max_retry and cont:

		if doiOrEid.lower() not in ['doi','eid']:
			print ("ERROR in mylib.getAbstract(): allowed type of search are 'DOI' and 'EID'.")

		params = {'apikey':apikeys[0], 'httpAccept':'application/json'} #, 'view':'FULL'}
		doiEncoded = urllib.parse.quote(doi)
		#print(apiURL_AbstractDoi + urllib.parse.quote(doi))
		r = requests.get(apiURL_Abstract[doiOrEid.lower()] + doiEncoded, params=params)
				
		#if self.raw_output:
		#	self.save_raw_response(r.text)

		# quota exceeded -> http 429 (see https://dev.elsevier.com/api_key_settings.html)
		if r.status_code == 429:
			print ("Quota exceeded for key " + apikeys[0] + " - EXIT.")
			apikeys.pop(0)
			time.sleep(0.5)
			continue
		
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
	json['request-time'] = str(datetime.datetime.now().utcnow())
	# TO DECODE:
	#oDate = datetime.datetime.strptime(json['request-time'], '%Y-%m-%d %H:%M:%S.%f')
	return json


##### TODO ##### TODO ##### TODO ##### TODO ##### TODO #####
# controlla che json dell'abstract ritornato da api sia ok
##### TODO ##### TODO ##### TODO ##### TODO ##### TODO #####
def checkAbsFormat(j):
	return True

# Salvo usando eid come nome
def saveJsonAbstract(j, pathOutput):

	if (checkAbsFormat(j)):
		eid = j['abstracts-retrieval-response']['coredata']['eid']
		
		if not os.path.isdir(pathOutput):
			os.makedirs(pathOutput)
			
		counter = 1
		completepath = os.path.join(pathOutput, eid + '.json')

		with open(completepath, 'w') as outfile:
			json.dump(j, outfile, indent=3)
		
		return True

	else:
		return False


# Get authors' names and surnames from the cv PDFs
def addAuthorsNamesToTsv(tsvIn, tsvOut, pathPdf):
	res = "QUADRIMESTRE	FASCIA	SETTORE	BIBL?	ID_CV	COGNOME	NOME	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3\n"
	# Load TSV
	with open(tsvIn, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		for row in spamreader:
			#if row['SETTORE'].replace('-','/') in sectors:
			idCv = row['ID CV']
			sessione = row['SESSIONE']
			fascia = row['FASCIA']
			settore = row['SETTORE']
			pdfBasePath = pathPdf + "quadrimestre-" + sessione + "/fascia-" + fascia + "/" + settore + "/CV/"
			pdfFullPath = pdfBasePath + idCv + "_*.pdf"
			contents = glob(pdfFullPath)
			contents.sort()
			if len(contents) != 1:
				print ("ERROR - NOT FOUND - sessione: %s, fascia: %s, settore: %s, idCv: %s" % (sessione, fascia, settore, idCv))
				#sys.exit()
			
			if idCv == "17819":
				name = "M Hammed"
				surname = "Aguennouz"
			elif idCv == "36015":
				name = "Francesco G. A."
				surname = "Quarati"
			elif idCv == "86924":
				name = "Andrea L. P."
				surname = "Pirro"
			else:
				filename = (contents[0].replace(pdfBasePath, ""))
				filenameSPlit = filename.split("_")
				idPdf = filenameSPlit[0]
				surnameList = list()
				nameList = list()
				for i in range(1,len(filenameSPlit)):
					if filenameSPlit[i].isupper():
						surnameList.append(filenameSPlit[i].title())
					else:
						nameList.append(filenameSPlit[i].replace(".pdf", "").title())
				name = " ".join(nameList)
				surname = " ".join(surnameList)
			res += "\t".join([row["SESSIONE"], row["FASCIA"], row["SETTORE"], row["BIBL?"], row["ID CV"], surname, name, row["NUMERO DOI ESISTENTI"], row["DOIS ESISTENTI"], row["DOIS NON ESISTENTI"], row["I1"], row["I2"], row["I3"], row["SETTORE CONCORSUALE"], row["SSD"], row["S1"], row["S2"], row["S3"]]) + "\n"
			
	text_file = open(tsvOut, "w")
	text_file.write(res)
	text_file.close()
			

class AutoVivification(dict):
	"""Implementation of perl's autovivification feature."""
	def __getitem__(self, item):
		try:
			return dict.__getitem__(self, item)
		except KeyError:
			value = self[item] = type(self)()
			return value

def readFile(f):
	f = open(f,"r")
	string = f.read()
	f.close
	return string
		


def manageDuplicates(quadrimestre, fascia, settore, cognome, nome):

	duplicates = {
		"1": {
			"1": {
				"06-N1": {
					"cognome": "Guida",
					"nome": "Maurizio",
					"infos": [
						#{"idCv": "38349", "abilitato": "No", "validitaDal": "", "validitaAl": ""},
						{"idCv": "28220", "abilitato": "Si", "validitaDal": "31/03/2017", "validitaAl": "31/03/2023"}
					]
				}
			}
		},
		"5": {
			"2": {
				"10-F3": {
					"cognome": "Lorenzi",
					"nome": "Cristiano",
					"infos": [
								{"idCv": "80835", "abilitato": "Si", "validitaDal": "20/08/2018", "validitaAl": "20/08/2024"},
								{"idCv": "95372", "abilitato": "Si", "validitaDal": "20/08/2018", "validitaAl": "20/08/2024"}
					]
				},
				"09-A1": {
					"cognome": "Battista", 
					"nome": "Francesco", 
					"infos": [
						#{"idCv": "100258", "abilitato": "No", "validitaDal": "", "validitaAl": ""},
						{"idCv": "87110", "abilitato": "Si", "validitaDal": "08/08/2018", "validitaAl": "08/08/2024"}
					]
				}
			}
		}
	}
	
	try:
		el = duplicates[quadrimestre][fascia][settore]
		if el["cognome"] == cognome and el["nome"] == nome:
			return el["infos"]
		else:
			return None
	except:
		return None

def searchCvId(persona, tsvDict, quadrimestre, fascia, settore, senzaCvCounterGlobal):

	res = list()
	
	# cerco corrispondenza nome e cognome (dato quadrimestre, settore e fascia) nel TSV
	for row in tsvDict[quadrimestre][fascia][settore]:
		idCvTsv = row['ID_CV']
		cognomeTsv = row['COGNOME']
		nomeTsv = row['NOME']
		if nomeTsv == persona["nome"] and cognomeTsv == persona["cognome"]:
			res.append(idCvTsv)
	if len(res) == 1:
		return res[0]
	elif len(res) == 0:
		print ("NESSUNA CORRISPONDENZA IDCV - Q: %s, F: %s, S: %s, Cognome: %s, Nome: %s" % (quadrimestre, fascia, settore, persona["cognome"], persona["nome"]))
		return "NO-CV-" + str(senzaCvCounterGlobal)
	elif len(res) > 1:
		print ("ERRORE: più di un individuo per lo stesso idCv: Q: %s, F: %s , S: %s- %s %s" % (quadrimestre, fascia, settore, persona["cognome"], persona["nome"]))
		sys.exit()



def addAsnOutcomesToTsv_soloAbilitati(tsvIn, tsvOut, pathAsnDownload):
	
	sectors = set()
	tsvDictIdCv = dict()
	
	tsvDict = AutoVivification()
	esitiMap = AutoVivification()
	
	senzaCvCounterGlobal = 1
	
	#missing_soloAbilitati = set()
	#esitiMap_soloAbilitati = AutoVivification()
	#esitiMapCounter = 0
	#esitiMap_soloAbilitatiCounter
	
	with open(tsvIn, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			quadrimestre = row['QUADRIMESTRE']
			fascia = row['FASCIA']
			settore = row['SETTORE'].replace("/","-")
			idCv = row['ID_CV']
				
			sectors.add("%s__%s__%s" % (quadrimestre, fascia, settore))
			
			tsvDictIdCv[idCv] = dict(row)
			
			if type(tsvDict[quadrimestre][fascia][settore]) is AutoVivification: 
				tsvDict[quadrimestre][fascia][settore] = list()
			tsvDict[quadrimestre][fascia][settore].append(dict(row))
	
	print ("Numero idCV nei CV: " + str(len(tsvDictIdCv.keys())))
	
	# metto in esitiMap info dove posso risalire ad idCV (i.e. tutto tranne quelli con _soloAbilitati, es. Q:1, F:1, S:08-E1)
	for datum in sectors:
		quadrimestre = datum.split("__")[0]
		fascia = datum.split("__")[1]
		settore = datum.split("__")[2]

		#print ("%s - %s - %s" % (quadrimestre, fascia, settore))
		htmlResFile = pathAsnDownload + "quadrimestre-" + quadrimestre + "/fascia-" + fascia + "/" + settore + "/" + settore + "_risultati_soloAbilitati.html"
		
		# non si verifica mai: abbiamo 1890/1900 dir dei settori, e per tutte abbiamo file [settore]_soloAbilitati.html
		if not os.path.isfile(htmlResFile):
			print ("1. NO RESULTS FOR " + htmlResFile)
			#missing_soloAbilitati.add({"quadrimestre": quadrimestre, "fascia": fascia, "settore": settore})
			sys.exit()
			#continue
		
		tree = html.parse(htmlResFile)
		els = tree.xpath('//table[position()=last()]/tbody/tr')
		
		# Caso: la tabella dei risultati in [settore]_soloAbilitati.html esiste ma è vuota -> nessuno è passato
		if "Validità Abilitazione" in readFile(htmlResFile) and len(els) == 0:
			#print ("TABELLA IN _soloAbilitati.html mancante vuota: nessuno è passato - Q: %s, F: %s, S: %s" % (quadrimestre, fascia, settore))
			for row in table:
				idCvTsv = row['ID_CV']
				quadrimestreTsv = row['QUADRIMESTRE']
				fasciaTsv = row['FASCIA']
				settoreTsv = row['SETTORE'].replace("/","-")
				cognomeTsv = row['COGNOME']
				nomeTsv = row['NOME']
				if settoreTsv == settore and fasciaTsv == fascia and quadrimestreTsv == quadrimestre:
					#esitiMapCounter += 1
					temp = tsvDictIdCv[idCvTsv]
					temp["abilitato"] = "No"
					temp["validitaDal"] = ""
					temp["validitaAl"] = ""
					temp["note"] = ""
					del tsvDictIdCv[idCvTsv]
					
					esitiMap[quadrimestre][fascia][settore][idCvTsv] = temp
			continue
		
		# 2 settori (Q: 5, F: 1 e F: 2, S: 12-D2): nel file _soloAbilitati.html non c'è tabella -> metto risultato senza esito in  esitiMap
		elif (len(els) == 0):
			print ("ESISTE FILE _SOLOABILITATI.HTML MA NON C'E' TABELLA - Q: %s, F: %s, S: %s" % (quadrimestre, fascia, settore))
			for row in tsvDict[quadrimestre][fascia][settore]:
				idCv = row["ID_CV"]
				temp = tsvDictIdCv[idCv]
				temp["abilitato"] = ""
				temp["validitaDal"] = ""
				temp["validitaAl"] = ""
				temp["note"] = ""
				del tsvDictIdCv[idCv]
				esitiMap[quadrimestre][fascia][settore][idCv] = temp
		
		else:
			posizione = 0
			# Gestisco gli abilitati
			for el in els:
				cognome = (el.xpath('td[1]')[0].text).title()
				nome = (el.xpath('td[2]')[0].text).title()
				abilitato = re.sub(r'\s+', '', (el.xpath('td[3]')[0].text).title())
				validitaDal = re.sub(r'\s+', '', (el.xpath('td[4]')[0].text).split("\n")[1].replace("Dal","") )
				validitaAl = re.sub(r'\s+', '', (el.xpath('td[4]')[0].text).split("\n")[2].replace("al","") )
				note = (el.xpath('td[5]')[0].text).strip()
				
				# Ci sono casi di ominimia (stesso quadrimestre, sessione, fascia e settore: li gestisco a manona.
				# Ottengo idCv degli abilitati
				duplicates = manageDuplicates(quadrimestre, fascia, settore, cognome, nome)
				if duplicates is None:
					idCv = searchCvId({"cognome": cognome, "nome": nome}, tsvDict, quadrimestre, fascia, settore, senzaCvCounterGlobal)
				else:
					if len(duplicates) != 1:
						print (duplicates)
						print ("Prendo posizione %d" % posizione)
						idCv = duplicates[posizione]["idCv"]
						posizione += 1
					else:
						print (duplicates)
						print ("Solo uno abilitato -> lo prendo")
						idCv = duplicates[0]["idCv"]
						
				if "NO-CV" in idCv:
					temp = {"cognome": cognome, "nome": nome, "abilitato": abilitato, "validitaDal": validitaDal, "validitaAl": validitaAl, "note": note}
					senzaCvCounterGlobal += 1
				else:
					temp = tsvDictIdCv[idCv]
					temp["abilitato"] = abilitato
					temp["validitaDal"] = validitaDal
					temp["validitaAl"] = validitaAl
					temp["note"] = note
					# tsvDictIdCv contiene idCv da cercare -> cancello tsvDictIdCv[idCv] perché l'ho già trovato 
					del tsvDictIdCv[idCv]
				
				esitiMap[quadrimestre][fascia][settore][idCv] = temp

			# Gestisco i NON abilitati
			for row in tsvDict[quadrimestre][fascia][settore]:
				idCv = row["ID_CV"]
				if idCv not in esitiMap[quadrimestre][fascia][settore]:
					temp = tsvDictIdCv[idCv]
					temp["abilitato"] = "No"
					temp["validitaDal"] = ""
					temp["validitaAl"] = ""
					temp["note"] = ""
					del tsvDictIdCv[idCv]
					esitiMap[quadrimestre][fascia][settore][idCv] = temp
	
	print ("Numero idCV rimasti da cercare: " + str(len(tsvDictIdCv.keys())))
	
	# SALVO RISULTATI NEL TSV
	res = "QUADRIMESTRE	FASCIA	SETTORE	BIBL?	ID_CV	COGNOME	NOME	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3	ABILITATO_SOLOABILITATI	VALIDITA_DAL_SOLOABILITATI	VALIDITA_AL_SOLOABILITATI	NOTE_SOLOABILITATI\n"
	'''
	for settore in esitiMap:
		for quadrimestre in esitiMap[settore]:
			for fascia in esitiMap[settore][quadrimestre]:
				for idCv in esitiMap[settore][quadrimestre][fascia]:
					mapPersona = esitiMap[settore][quadrimestre][fascia][idCv]
	'''
	for quadrimestre in esitiMap:
		for fascia in esitiMap[quadrimestre]:
			for settore in esitiMap[quadrimestre][fascia]:
				for idCv in esitiMap[quadrimestre][fascia][settore]:
					mapPersona = esitiMap[quadrimestre][fascia][settore][idCv]
					if "NO-CV" not in idCv:
						res += ("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (mapPersona['QUADRIMESTRE'],mapPersona['FASCIA'],mapPersona['SETTORE'],mapPersona['BIBL?'],mapPersona['ID_CV'],mapPersona['COGNOME'],mapPersona['NOME'],mapPersona['NUMERO DOI ESISTENTI'],mapPersona['DOIS ESISTENTI'],mapPersona['DOIS NON ESISTENTI'],mapPersona['I1'],mapPersona['I2'],mapPersona['I3'],mapPersona['SETTORE CONCORSUALE'],mapPersona['SSD'],mapPersona['S1'],mapPersona['S2'],mapPersona['S3'],mapPersona['abilitato'],mapPersona['validitaDal'],mapPersona['validitaAl'], mapPersona["note"]))
					else:
						#print (mapPersona)
						print ("Q: %s, F: %s, S: %s, idCv: %s" % (str(quadrimestre), str(fascia), str(settore), str(idCv)))
						res += ("%s\t%s\t%s\t\t%s\t%s\t%s\t\t\t\t\t\t\t%s\t\t\t\t\t%s\t%s\t%s\t%s\n" % (quadrimestre,fascia,settore,idCv,mapPersona['cognome'],mapPersona['nome'],settore,mapPersona['abilitato'],mapPersona['validitaDal'],mapPersona['validitaAl'],mapPersona["note"]))
						
	text_file = open(tsvOut, "w")
	text_file.write(res)
	text_file.close()



def addAsnOutcomesToTsv_risultati(tsvIn, tsvOut, pathAsnDownload):
	
	sectors = set()
	
	tsvDictIdCv = dict()
	tsvDict = AutoVivification()
	
	esitiMap = AutoVivification()
	
	with open(tsvIn, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			quadrimestre = row['QUADRIMESTRE']
			fascia = row['FASCIA']
			settore = row['SETTORE'].replace("/","-")
			idCv = row['ID_CV']
			
			sectors.add("%s__%s__%s" % (quadrimestre, fascia, settore))
			
			tsvDictIdCv[idCv] = dict(row)
			
			#if type(tsvDict[quadrimestre][fascia][settore]) is AutoVivification: 
			#	tsvDict[quadrimestre][fascia][settore] = list()
			#tsvDict[quadrimestre][fascia][settore].append(dict(row))
			tsvDict[quadrimestre][fascia][settore][idCv] = dict(row)
	
	print ("Numero idCV nei CV: " + str(len(tsvDictIdCv.keys())))
	
	# metto in esitiMap info dove posso risalire ad idCV (i.e. tutto tranne quelli con _soloAbilitati, es. Q:1, F:1, S:08-E1)
	for datum in sectors:
		quadrimestre = datum.split("__")[0]
		fascia = datum.split("__")[1]
		settore = datum.split("__")[2]
		
		# NON C'E' FILE _RISULTATI.HTML -> SKIP
		htmlResFile = pathAsnDownload + "quadrimestre-" + quadrimestre + "/fascia-" + fascia + "/" + settore + "/" + settore + "_risultati.html"
		if not os.path.isfile(htmlResFile):
			#print ("Q: %s, F: %s, S: %s" % (quadrimestre, fascia, settore))
			continue
		
		tree = html.parse(htmlResFile)
		els = tree.xpath('//table[position()=last()]/tbody/tr')
	
		# NON CI SONO RIGHE NELLA TABELLA -> SKIP
		#if "Validità Abilitazione" in readFile(htmlResFile) and len(els) == 0:
		if len(els) == 0:
			#print ("TABELLA C'E' MA NON CONTIENE NESSUNA RIGA - Q: %s, F: %s, S: %s" % (quadrimestre, fascia, settore))
			continue
		for el in els:
			elEsito = el.xpath('td[7]')
			if len(elEsito) != 1:
				#print ("TABELLA C'E' MA NON C'E' COLONNA 7 - Q: %s, F: %s, S: %s" % (quadrimestre, fascia, settore))
				break #sys.exit()
			esito = re.sub(r'\s+', '', elEsito[0].text)
				
			validitaDal = ""
			validitaAl = ""
			if esito == "Si":
				elValidita = el.xpath('td[8]')
				if len(elEsito) != 1:
					print ("XPATH ERROR VALIDITA: " + htmlResFile)
					sys.exit()
				validitaDal = re.sub(r'\s+', '', (elValidita[0].text).split("\n")[1].replace("Dal","") )
				validitaAl = re.sub(r'\s+', '', (elValidita[0].text).split("\n")[2].replace("al","") )
			
			linkPdfCv = el.xpath('td[3]/a/@href')
			if len(linkPdfCv) != 1:
				print ("XPATH ERROR: " + htmlResFile)
				sys.exit()
			idCvEsito = linkPdfCv[0].split("/")[7]
			
			elCognome = el.xpath('td[1]')
			if len(elCognome) != 1:
				print ("XPATH ERROR ESITO (NO COGNOME): " + htmlResFile)
				sys.exit()
			cognome = re.sub(r'\s+', '', elCognome[0].text).title()
			
			elNome = el.xpath('td[2]')
			if len(elNome) != 1:
				print ("XPATH ERROR ESITO (NO NOME): " + htmlResFile)
				sys.exit()
			nome = re.sub(r'\s+', '', elNome[0].text).title()

			elNote = el.xpath('td[9]')
			if len(elNote) != 1:
				print ("XPATH ERROR ESITO (NO NOTE): " + htmlResFile)
				sys.exit()
			note = (elNote[0].text).strip()
			
			if idCvEsito in tsvDictIdCv:
				temp = tsvDictIdCv[idCvEsito]
				temp["abilitato"] = esito
				temp["validitaDal"] = validitaDal
				temp["validitaAl"] = validitaAl
				temp["note"] = note
				esitiMap[quadrimestre][fascia][settore][idCvEsito] = temp #{"abilitato": esito, "validitaDal": validitaDal, "validitaAl": validitaAl, "cognome": cognome, "nome": nome}
			else:
				print ("PERSONA in HTML RISULTATI ma MANCA CV - idCv: %s - Q: %s, F: %s, S: %s" % (idCvEsito, quadrimestre, fascia, settore))
	
	#print ("Numero idCV rimasti da cercare: " + str(len(tsvDictIdCv.keys())))
	
	# SALVO RISULTATI NEL TSV
	res = "QUADRIMESTRE	FASCIA	SETTORE	BIBL?	ID_CV	COGNOME	NOME	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3	ABILITATO_SOLOABILITATI	VALIDITA_DAL_SOLOABILITATI	VALIDITA_AL_SOLOABILITATI	NOTE_SOLOABILITATI	ABILITATO_RISULTATI	VALIDITA_DAL_RISULTATI	VALIDITA_AL_RISULTATI	NOTE_RISULTATI\n"
	for quadrimestre in tsvDict:
		for fascia in tsvDict[quadrimestre]:
			for settore in tsvDict[quadrimestre][fascia]:
				for idCv in tsvDict[quadrimestre][fascia][settore]:
					if idCv == "48147":
						print (tsvDict[quadrimestre][fascia][settore][idCv])
					if idCv in esitiMap[quadrimestre][fascia][settore]:
						mapPersona = esitiMap[quadrimestre][fascia][settore][idCv]
						#print ("TROVATO: " + idCv)
						#print (mapPersona)
						res += ("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (mapPersona['QUADRIMESTRE'],mapPersona['FASCIA'],mapPersona['SETTORE'],mapPersona['BIBL?'],mapPersona['ID_CV'],mapPersona['COGNOME'],mapPersona['NOME'],mapPersona['NUMERO DOI ESISTENTI'],mapPersona['DOIS ESISTENTI'],mapPersona['DOIS NON ESISTENTI'],mapPersona['I1'],mapPersona['I2'],mapPersona['I3'],mapPersona['SETTORE CONCORSUALE'],mapPersona['SSD'],mapPersona['S1'],mapPersona['S2'],mapPersona['S3'],mapPersona['ABILITATO_SOLOABILITATI'],mapPersona['VALIDITA_DAL_SOLOABILITATI'],mapPersona['VALIDITA_AL_SOLOABILITATI'],mapPersona['NOTE_SOLOABILITATI'],mapPersona['abilitato'],mapPersona['validitaDal'],mapPersona['validitaAl'],mapPersona['note']))
					else:
						mapPersona = tsvDict[quadrimestre][fascia][settore][idCv]
						#print ("NON TROVATO: " + idCv)
						res += ("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\t\t\t\n" % (mapPersona['QUADRIMESTRE'],mapPersona['FASCIA'],mapPersona['SETTORE'],mapPersona['BIBL?'],mapPersona['ID_CV'],mapPersona['COGNOME'],mapPersona['NOME'],mapPersona['NUMERO DOI ESISTENTI'],mapPersona['DOIS ESISTENTI'],mapPersona['DOIS NON ESISTENTI'],mapPersona['I1'],mapPersona['I2'],mapPersona['I3'],mapPersona['SETTORE CONCORSUALE'],mapPersona['SSD'],mapPersona['S1'],mapPersona['S2'],mapPersona['S3'],mapPersona['ABILITATO_SOLOABILITATI'],mapPersona['VALIDITA_DAL_SOLOABILITATI'],mapPersona['VALIDITA_AL_SOLOABILITATI'],mapPersona['NOTE_SOLOABILITATI']))
	
	text_file = open(tsvOut, "w")
	text_file.write(res)
	text_file.close()


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


def create_scopusPublication(conn, scopusPublication):
	"""
	Create a new publication into the scopusPublication table
	:param conn:
	:param scopusPublication:
	:return: scopusPublication id
	"""
	sql = ''' INSERT INTO scopusPublication(eid, doi, publicationDate,publicationYear,title,venueName,numAuthors)
			  VALUES(?,?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, scopusPublication)
	return cur.lastrowid

def create_scopusAuthor(conn, scopusAuthor):
	"""
	Create a new author into the scopusAuthor table
	:param conn:
	:param author:
	:return: scopusAuthor id
	"""
	sql = ''' INSERT INTO scopusAuthor(auid, firstname,initials,surname)
			  VALUES(?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, scopusAuthor)
	return cur.lastrowid


def create_wroteRelation(conn, wroteRelation):
	"""
	Create a new record into the wroteRelation table
	:param conn:
	:param wroteRelation record:
	:return: wroteRelation id
	"""
	sql = ''' INSERT INTO wroteRelation(auid, eid, position)
			  VALUES(?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, wroteRelation)
	return cur.lastrowid
	

def create_matchSurnameName(conn, matchSurnameName):
	"""
	Create a new record into the matchSurnameName table
	:param conn:
	:param record:
	:return: matchSurnameName id
	"""
	sql = ''' INSERT INTO matchSurnameName(cvId,auid)
			  VALUES(?,?) '''
	cur = conn.cursor()
	cur.execute(sql, matchSurnameName)
	return cur.lastrowid


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


def create_matchIntersection(conn, matchIntersection):
	"""
	Create a new record into the matchIntersection table
	:param conn:
	:param record:
	:return: matchIntersection id
	"""
	sql = ''' INSERT INTO matchIntersection(cvId,auid)
			  VALUES(?,?) '''
	cur = conn.cursor()
	cur.execute(sql, matchIntersection)
	return cur.lastrowid


def create_matchNoDois(conn, matchNoDois):
	"""
	Create a new record into the matchNoDois table
	:param conn:
	:param record:
	:return: matchNoDois id
	"""
	sql = ''' INSERT INTO matchNoDois(cvId,auid)
			  VALUES(?,?) '''
	cur = conn.cursor()
	cur.execute(sql, matchNoDois)
	return cur.lastrowid


def create_match(conn, match):
	"""
	Create a new record into the matchCvidAuid table
	:param conn:
	:param record:
	:return: match id
	"""
	sql = ''' INSERT INTO matchCvidAuid(cvId,auid,matchSurnameName,matchSurname,matchIntersection,numDois,numEids)
			  VALUES(?,?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, match)
	return cur.lastrowid


def create_cvidDoiEid(conn, cvidDoiEid):
	"""
	Create a new record into the cvidDoiEid table
	:param conn:
	:param cvidDoiEid:
	:return: cvidDoiEid id
	"""
	sql = ''' INSERT INTO cvidDoiEid(cvId,doi,eid)
			  VALUES(?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, cvidDoiEid)
	return cur.lastrowid


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


def select_cvidInMatchCvidAuid(conn,idCv):
	q = """
		SELECT *
		FROM matchCvidAuid
		WHERE cvId = '{idCurriculum}'
		"""
	cur = conn.cursor()
	cur.execute(q.format(idCurriculum=idCv))
	rows = cur.fetchall()
	return rows 
	

def select_scopusPublication(dbFile):
	q = """
		SELECT DISTINCT eid,doi
		FROM scopusPublication
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


#def load_authorDoisMapping(fileMapping):
def loadJson(fileMapping):
	with open(fileMapping, "r") as read_file:
		data = json.load(read_file)
		return data


def load__listaDoisToDownload(fileLista):
	with open(fileLista, "rb") as fp:	# unpickling
		lista = pickle.load(fp)
		return lista


'''
def addAsnOutcomesToTsv(tsvIn, tsvOut, pathAsnDownload):
	tsvDict[quadrimestre][fascia][settore][idCv] = dict(row)
	esitiMap_soloAbilitatiCounter = 0
	esitiMapCounter = 0

	sectors = set()
	sectorsSoloEsiti = set()
	
	esitiMap = AutoVivification()
	esitiMap_soloAbilitati = AutoVivification()
	
	tsvDict = dict()
	
	tsvCounter = 0
	with open(tsvIn, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		table = list(spamreader)
		for row in table:
			quadrimestre = row['QUADRIMESTRE']
			fascia = row['FASCIA']
			settore = row['SETTORE'].replace("/","-")
			#sectors.add({"quadrimestre":quadrimestre, "fascia": fascia, "settore": settore})
			sectors.add("%s__%s__%s" % (quadrimestre, fascia, settore))

			idCv = row['ID_CV']
			tsvDict[idCv] = dict(row)
			tsvCounter += 1

	# metto in esitiMap info dove posso risalire ad idCV (i.e. tutto tranne quelli con _soloAbilitati, es. Q:1, F:1, S:08-E1)
	for datum in sectors:
		quadrimestre = datum.split("__")[0]
		fascia = datum.split("__")[1]
		settore = datum.split("__")[2]
		
		htmlResFile = pathAsnDownload + "quadrimestre-" + quadrimestre + "/fascia-" + fascia + "/" + settore + "/" + settore + "_risultati.html"
		if not os.path.isfile(htmlResFile):
			#print ("1. NO RESULTS FOR " + htmlResFile)
			#htmlResFile = htmlResFile.replace(".html", "_soloAbilitati.html")
			sectorsSoloEsiti.add("%s__%s__%s" % (quadrimestre, fascia, settore))
			continue
		
		tree = html.parse(htmlResFile)
		els = tree.xpath('//table[position()=last()]/tbody/tr')
		if len(els) == 0:
			#print ("XPATH ERROR ESITO (LEN=0): " + htmlResFile)
			sectorsSoloEsiti.add("%s__%s__%s" % (quadrimestre, fascia, settore))
			continue
		for el in els:
			elEsito = el.xpath('td[7]')
			if len(elEsito) != 1:
				#print ("XPATH ERROR ESITO (= NUM TABLE CELLS): " + htmlResFile)
				sectorsSoloEsiti.add("%s__%s__%s" % (quadrimestre, fascia, settore))
				break #sys.exit()
			esito = re.sub(r'\s+', '', elEsito[0].text)
				
			validitaDal = ""
			validitaAl = ""
			if esito == "Si":
				elValidita = el.xpath('td[8]')
				if len(elEsito) != 1:
					print ("XPATH ERROR VALIDITA: " + htmlResFile)
					sys.exit()
				validitaDal = re.sub(r'\s+', '', (elValidita[0].text).split("\n")[1].replace("Dal","") )
				validitaAl = re.sub(r'\s+', '', (elValidita[0].text).split("\n")[2].replace("al","") )
			
			linkPdfCv = el.xpath('td[3]/a/@href')
			if len(linkPdfCv) != 1:
				print ("XPATH ERROR: " + htmlResFile)
				sys.exit()
			idCvEsito = linkPdfCv[0].split("/")[7]
			#if idCvEsito == "19573":
			#	print (idCvEsito)
			#	sys.exit()

			elCognome = el.xpath('td[1]')
			if len(elCognome) != 1:
				print ("XPATH ERROR ESITO (NO COGNOME): " + htmlResFile)
				#sectorsSoloEsiti.add("%s__%s__%s" % (quadrimestre, fascia, settore))
				sys.exit()
			cognome = re.sub(r'\s+', '', elCognome[0].text).title()
			
			elNome = el.xpath('td[2]')
			if len(elNome) != 1:
				print ("XPATH ERROR ESITO (NO NOME): " + htmlResFile)
				#sectorsSoloEsiti.add("%s__%s__%s" % (quadrimestre, fascia, settore))
				sys.exit()
			nome = re.sub(r'\s+', '', elNome[0].text).title()

			esitiMapCounter += 1
			esitiMap[settore][quadrimestre][fascia][idCvEsito] = {"abilitato": esito, "validitaDal": validitaDal, "validitaAl": validitaAl, "cognome": cognome, "nome": nome}
		
	# cerco esiti in _soloAbilitati (mi manca corrispondenza con idCv)
	for datum in sectorsSoloEsiti:
		quadrimestre = datum.split("__")[0]
		fascia = datum.split("__")[1]
		settore = datum.split("__")[2]
		htmlResFile = pathAsnDownload + "quadrimestre-" + quadrimestre + "/fascia-" + fascia + "/" + settore + "/" + settore + "_risultati_soloAbilitati.html"
		if not os.path.isfile(htmlResFile):
			print ("2. NO RESULTS FOR " + htmlResFile)
			continue
			#sys.exit()
		
		tree = html.parse(htmlResFile)
		els = tree.xpath('//table[position()=last()]/tbody/tr')
		
		# Caso: la tabella dei risultati _soloAbilitati esiste ma è vuota -> nessuno è passato
		if "Validità Abilitazione" in readFile(htmlResFile) and len(els) == 0:
			print ("Q: %s, F: %s, S: %s" % (quadrimestre, fascia, settore))
			#sys.exit()
			for row in table:
				idCvTsv = row['ID_CV']
				quadrimestreTsv = row['QUADRIMESTRE']
				fasciaTsv = row['FASCIA']
				settoreTsv = row['SETTORE'].replace("/","-")
				cognomeTsv = row['COGNOME']
				nomeTsv = row['NOME']
				if settoreTsv == settore and fasciaTsv == fascia and quadrimestreTsv == quadrimestre:
					esitiMap[settore][quadrimestre][fascia][idCvTsv] = {"abilitato": "No", "validitaDal": "", "validitaAl": "", "cognome": cognomeTsv, "nome": nomeTsv}
		
		for el in els:
			cognome = (el.xpath('td[1]')[0].text).title()
			nome = (el.xpath('td[2]')[0].text).title()
			abilitato = re.sub(r'\s+', '', (el.xpath('td[3]')[0].text).title())
			validitaDal = re.sub(r'\s+', '', (el.xpath('td[4]')[0].text).split("\n")[1].replace("Dal","") )
			validitaAl = re.sub(r'\s+', '', (el.xpath('td[4]')[0].text).split("\n")[2].replace("al","") )
			# first time -> create list
			if type(esitiMap_soloAbilitati[settore][quadrimestre][fascia]) is AutoVivification: # = {"cognome": cognome, "nome": nome, "abilitato": abilitato, "validita": validita}
				esitiMap_soloAbilitati[settore][quadrimestre][fascia] = list()
			esitiMap_soloAbilitatiCounter += 1
			esitiMap_soloAbilitati[settore][quadrimestre][fascia].append({"cognome": cognome, "nome": nome, "abilitato": abilitato, "validitaDal": validitaDal, "validitaAl": validitaAl})

	# risalgo ad idCv
	senzaCvCounterGlobal = 0
	esitoNoCounter = 0
	conCvCounterGlobal = 0
	for settore in esitiMap_soloAbilitati:
		for quadrimestre in esitiMap_soloAbilitati[settore]:
			for fascia in esitiMap_soloAbilitati[settore][quadrimestre]:
				print ("%s - %s - %s" % (quadrimestre, fascia, settore))
				for persona in esitiMap_soloAbilitati[settore][quadrimestre][fascia]:
					
					found = 0
					# cerco corrispondenza nome e cognome (dato quadrimestre, settore e fascia) nel TSV
					for row in table:
						idCvTsv = row['ID_CV']
						quadrimestreTsv = row['QUADRIMESTRE']
						fasciaTsv = row['FASCIA']
						settoreTsv = row['SETTORE'].replace("/","-")
						cognomeTsv = row['COGNOME']
						nomeTsv = row['NOME']
						if settoreTsv == settore and fasciaTsv == fascia and quadrimestreTsv == quadrimestre and nomeTsv == persona['nome'] and cognomeTsv == persona['cognome']:
							if type(esitiMap[settore][quadrimestre][fascia][idCvTsv]) is not AutoVivification:
								print ("ERRORE: più di un individuo per lo stesso idCv")
								sys.exit()
							found += 1
							# FPOGGI - ERRORE - OCCHIO!!!
							#esitiMap[settore][quadrimestre][fascia][idCvEsito] = persona #{"abilitato": persona['abilitato'], "validitaDal": persona["validitaDal"], "validitaAl": persona["validitaAl"]}
							esitiMap[settore][quadrimestre][fascia][idCvTsv] = persona #{"abilitato": persona['abilitato'], "validitaDal": persona["validitaDal"], "validitaAl": persona["validitaAl"]}
							conCvCounterGlobal += 1
					if found != 1:
						senzaCvCounterGlobal += 1
						print ("found: %d" % found)
						print ("'%s' - '%s' - '%s' - '%s' - '%s'" % (settore, fascia, quadrimestre, persona['nome'], persona['cognome']))
						esitiMap[settore][quadrimestre][fascia]["NO-CV-" + str(senzaCvCounterGlobal)] = persona #{"abilitato": persona['abilitato'], "validitaDal": persona["validitaDal"], "validitaAl": persona["validitaAl"]}
				# Cerco gli esiti negativi (che non sono stati presi sopra, perché c'erano solo gli abilitati)		
				for row in table:
					idCvTsv = row['ID_CV']
					quadrimestreTsv = row['QUADRIMESTRE']
					fasciaTsv = row['FASCIA']
					settoreTsv = row['SETTORE'].replace("/","-")
					if quadrimestreTsv == quadrimestre and fasciaTsv == fascia and settoreTsv == settore:
						#if idCvEsito not in esitiMap[settore][quadrimestre][fascia]:
						if type(esitiMap[settore][quadrimestre][fascia][idCvTsv]) is AutoVivification:
							esitoNoCounter += 1
							esitiMap[settore][quadrimestre][fascia][idCvTsv] = {"abilitato": 'No', "validitaDal": '', "validitaAl": ''}
	
	for row in table:
		idCvTsv = row['ID_CV']
		quadrimestreTsv = row['QUADRIMESTRE']
		fasciaTsv = row['FASCIA']
		settoreTsv = row['SETTORE'].replace("/","-")
		if idCvTsv not in esitiMap[settoreTsv][quadrimestreTsv][fasciaTsv]:
			print ("S: %s, Q: %s, F: %s, idCv: %s" % (settoreTsv,quadrimestreTsv,fasciaTsv,idCvTsv))
			esitiMap[settore][quadrimestre][fascia][idCvTsv] = {"abilitato": '', "validitaDal": '', "validitaAl": ''}
	
	res = "QUADRIMESTRE	FASCIA	SETTORE	BIBL?	ID_CV	COGNOME	NOME	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3	ESITO	VALIDITA_DAL	VALIDITA_AL\n"
	for settore in esitiMap:
		for quadrimestre in esitiMap[settore]:
			for fascia in esitiMap[settore][quadrimestre]:
				#print ("%s - %s - %s" % (quadrimestre, fascia, settore))
				for idCv in esitiMap[settore][quadrimestre][fascia]:
					mapPersona = esitiMap[settore][quadrimestre][fascia][idCv]
					#print (mapPersona)
					if idCv in tsvDict:
						mapTsv = tsvDict[idCv]
						res += ("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (mapTsv['QUADRIMESTRE'],mapTsv['FASCIA'],mapTsv['SETTORE'],mapTsv['BIBL?'],mapTsv['ID_CV'],mapTsv['COGNOME'],mapTsv['NOME'],mapTsv['NUMERO DOI ESISTENTI'],mapTsv['DOIS ESISTENTI'],mapTsv['DOIS NON ESISTENTI'],mapTsv['I1'],mapTsv['I2'],mapTsv['I3'],mapTsv['SETTORE CONCORSUALE'],mapTsv['SSD'],mapTsv['S1'],mapTsv['S2'],mapTsv['S3'],mapPersona['abilitato'],mapPersona['validitaDal'],mapPersona['validitaAl']))
					else:
						res += ("%s\t%s\t%s\t\t%s\t%s\t%s\t\t\t\t\t\t\t%s\t\t\t\t\t%s\t%s\t%s\n" % (quadrimestre,fascia,settore,idCv,esitiMap[settore][quadrimestre][fascia][idCv]['cognome'],esitiMap[settore][quadrimestre][fascia][idCv]['nome'],settore,esitiMap[settore][quadrimestre][fascia][idCv]['abilitato'],esitiMap[settore][quadrimestre][fascia][idCv]['validitaDal'],esitiMap[settore][quadrimestre][fascia][idCv]['validitaAl']))
						print ("Q: %s, F: %s, S: %s, idCv: %s" % (str(quadrimestre), str(fascia), str(settore), str(idCv)))
						print (esitiMap[settore][quadrimestre][fascia][idCv])
	
	text_file = open(tsvOut, "w")
	text_file.write(res)
	text_file.close()

	print ("Numero righe in TSV input: %d" % tsvCounter)
	
	print ("Trovati in esito: %d" % esitiMapCounter)
	
	print ("Solo abilitati con CV: %d" % conCvCounterGlobal)
	print ("In TSV ma non in esiti abilitati (= non abilitati): %d" % esitoNoCounter)
	print ("In risultati ma senza CV: %d" % senzaCvCounterGlobal)


	print (esitiMap_soloAbilitatiCounter)
'''
