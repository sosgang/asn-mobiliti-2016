# -*- coding: UTF-8 -*-
import datetime
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



def addAsnOutcomesToTsv(tsvIn, tsvOut, pathAsnDownload):
	esitiMap_soloAbilitatiCounter = 0
	esitiMapCounter = 0

	sectors = set()
	sectorsSoloEsiti = set()
	
	esitiMap = AutoVivification()
	esitiMap_soloAbilitati = AutoVivification()
	
	tsvDict = dict()

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

	# metto in esitiMap info dove posso risalire ad idCV (i.e. tutto tranne quelli con _soloAbilitati, es. Q:1, F:1, S:08-E1)
	for datum in sectors:
		quadrimestre = datum.split("__")[0]
		fascia = datum.split("__")[1]
		settore = datum.split("__")[2]
		
		htmlResFile = pathAsnDownload + "quadrimestre-" + quadrimestre + "/fascia-" + fascia + "/" + settore + "/" + settore + "_risultati.html"
		if not os.path.isfile(htmlResFile):
			print ("1. NO RESULTS FOR " + htmlResFile)
			htmlResFile = htmlResFile.replace(".html", "_soloAbilitati.html")

		tree = html.parse(htmlResFile)
		els = tree.xpath('//table[position()=last()]/tbody/tr')
		if len(els) == 0:
			print ("XPATH ERROR ESITO (LEN=0): " + htmlResFile)
			sectorsSoloEsiti.add("%s__%s__%s" % (quadrimestre, fascia, settore))
			continue
		for el in els:
			elEsito = el.xpath('td[7]')
			if len(elEsito) != 1:
				print ("XPATH ERROR ESITO (= NUM TABLE CELLS): " + htmlResFile)
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

			esitiMapCounter += 1
			esitiMap[settore][quadrimestre][fascia][idCvEsito] = {"abilitato": esito, "validitaDal": validitaDal, "validitaAl": validitaAl}
		
	# cerco esiti in _soloAbilitati (mi manca corrispondenza con idCv)
	for datum in sectorsSoloEsiti:
		#print (datum)
		quadrimestre = datum.split("__")[0]
		fascia = datum.split("__")[1]
		settore = datum.split("__")[2]
		htmlResFile = pathAsnDownload + "quadrimestre-" + quadrimestre + "/fascia-" + fascia + "/" + settore + "/" + settore + "_risultati_soloAbilitati.html"
		if not os.path.isfile(htmlResFile):
			print ("2. NO RESULTS FOR " + htmlResFile)
			#sys.exit()
		tree = html.parse(htmlResFile)
		els = tree.xpath('//table[position()=last()]/tbody/tr')
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

	print (esitiMapCounter)
	print (esitiMap_soloAbilitatiCounter)
	sys.exit()
	
	# risalgo ad idCv
	senzaCvCounter = 1
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
							esitiMap[settore][quadrimestre][fascia][idCvEsito] = {"abilitato": persona['abilitato'], "validitaDal": persona["validitaDal"], "validitaAl": persona["validitaAl"]}
					if found != 1:
						print ("found: %d" % found)
						print ("'%s' - '%s' - '%s' - '%s' - '%s'" % (settore, fascia, quadrimestre, persona['nome'], persona['cognome']))
						esitiMap[settore][quadrimestre][fascia]["NO-CV-" + str(senzaCvCounter)] = {"abilitato": persona['abilitato'], "validitaDal": persona["validitaDal"], "validitaAl": persona["validitaAl"]}
						senzaCvCounter += 1

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
	
	text_file = open(tsvOut, "w")
	text_file.write(res)
	text_file.close()


	'''
def addAsnOutcomesToTsv(sectors, inputTsv, outputTsv, pathAsnDownload):
	esitiMap = AutoVivification()
	for sector in sectors:
		for quadrimestre in range(1,6):
			for fascia in range(1,3):
				htmlFile = pathAsnDownload + "quadrimestre-" + str(quadrimestre) + "/fascia-" + str(fascia) + "/" + sector.replace("/", "-") + "/" + sector.replace("/", "-") + "_risultati.html"
				#print (htmlFile)
				tree = html.parse(htmlFile)
				els = tree.xpath('//table[position()=last()]/tbody/tr')
				for el in els:
					linkPdfCv = el.xpath('td[3]/a/@href')
					if len(linkPdfCv) != 1:
						print ("XPATH ERROR")
						sys.exit()
					idCvEsito = linkPdfCv[0].split("/")[7]
					elEsito = el.xpath('td[7]')
					if len(elEsito) != 1:
						print ("XPATH ERROR")
						sys.exit()
					esito = re.sub(r'\s+', '', elEsito[0].text)
					#print ("\t%s: %s" % (idCvEsito, esito))
					esitiMap[sector.replace("/","-")][str(quadrimestre)][str(fascia)][idCvEsito] = esito
	#print (esitiMap)			
	#sys.exit()
	
	res = "QUADRIMESTRE	FASCIA	SETTORE	BIBL?	ID_CV	COGNOME	NOME	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3	ESITO\n"
	with open(inputTsv, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		for row in spamreader:
			idCv = row["ID CV"]
			fascia = row["FASCIA"]
			quadrimestre = row["SESSIONE"]
			settore = row["SETTORE"]
			esito = esitiMap[settore][quadrimestre][fascia][idCv]
			#print (esito)
			res += "\t".join([row["SESSIONE"], row["FASCIA"], row["SETTORE"], row["BIBL?"], row["ID CV"], row["COGNOME"], row["NOME"], row["NUMERO DOI ESISTENTI"], row["DOIS ESISTENTI"], row["DOIS NON ESISTENTI"], row["I1"], row["I2"], row["I3"], row["SETTORE CONCORSUALE"], row["SSD"], row["S1"], row["S2"], row["S3"], esito]) + "\n"
			
	text_file = open(outputTsv, "w")
	text_file.write(res)
	text_file.close()
	'''
