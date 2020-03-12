import sqlite3
from sqlite3 import Error
from glob import glob
import json
import csv
import sys
import os
import logging

import conf

tsvFN = "../../data/input/candidatesAsn2016.tsv"

tsvSoglieBilbio = "../../data/input/soglie_2016_bibliometrici.tsv"
tsvSoglieNonBiblio = "../../data/input/soglie_2016_non-bibliometrici.tsv"
		

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
	
def create_curriculum(conn, curriculum):
	"""
	Create a new cv into the curriculum table
	:param conn:
	:param curriculum:
	:return: curriculum id
	"""
	
	sql = ''' INSERT INTO curriculum(id,annoAsn,settore,ssd,quadrimestre,fascia,orcid,cognome,nome,bibl,I1,I2,I3,idSoglia,abilitato,idRisultato)
			  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, curriculum)
	return cur.lastrowid

def create_risultatoAsn(conn, risultato):
	"""
	Create a new risultato into the risultatoAsn table
	:param conn:
	:param risultato:
	:return: risultato id
	"""
	
	sql = ''' INSERT INTO risultatoAsn(abilitatoSoloAbilitati,validitaDalSoloAbilitati,validitaAlSoloAbilitati,noteSoloAbilitati,abilitatoSoloRisultati,validitaDalSoloRisultati,validitaAlSoloRisultati,noteSoloRisultati)
			  VALUES(?,?,?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, risultato)
	return cur.lastrowid
									
def create_sogliaAsn(conn, sogliaAsn):
	"""
	Create a new soglia into the sogliaAsn table
	:param conn:
	:param sogliaAsn:
	:return: sogliaAsn id
	"""
	sql = ''' INSERT INTO sogliaAsn(annoAsn,settore,descrSettore,ssd,fascia,bibliometrico,S1,S2,S3,descrS1,descrS2,descrS3,bibl)
			  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, sogliaAsn)
	return cur.lastrowid



def select_sogliaAsn_conSoglia(conn,fasc,sett,soglia1,soglia2,soglia3,year):
	cur = conn.cursor()
	q = """
	SELECT id
	FROM sogliaAsn
	WHERE
	  fascia = {fascia} AND
	  settore = '{settore}' AND
	  s1 = {s1} AND
	  s2 = {s2} AND
	  s3 = {s3} AND
	  annoAsn = {anno}"""

	#print (q.format(fascia=fasc,settore=sett,s1=soglia1,s2=soglia2,s3=soglia3,anno=year))
	cur.execute(q.format(fascia=fasc,settore=sett,s1=soglia1,s2=soglia2,s3=soglia3,anno=year))
 
	rows = cur.fetchall()

	return rows

def select_sogliaAsn(conn,fasc,sett,year):
	cur = conn.cursor()
	q = """
	SELECT id,S1,S2,S3
	FROM sogliaAsn
	WHERE
	  fascia = {fascia} AND
	  settore = '{settore}' AND
	  annoAsn = {anno}"""

	#print (q.format(fascia=fasc,settore=sett,anno=year))
	cur.execute(q.format(fascia=fasc,settore=sett,anno=year))
 
	rows = cur.fetchall()

	return rows
	
def main():
	# create a database connection
	conn = create_connection(conf.dbFilename)
	with conn:
		
		# POPULATE TABLE soglia
		# BIBLIOMETRICI
		with open(tsvSoglieBilbio, newline='') as tsvFile:
			spamreader = csv.DictReader(tsvFile, delimiter='\t')
			table = list(spamreader)
			for row in table:
				temp = row["SC/SSD"]
				if len(temp) == 5:
					settore = temp
					ssd = ""
				else:
					settore = temp[:5]
					ssd = temp[6:]

				descrSettore = row["SETTORE CONCORSUALE"]
				s1_l1 = row["Numero articoli 10 anni"]
				s2_l1 = row["Numero citazioni 15 anni"]
				s3_l1 = row["Indice H 15 anni"]
				soglia = ("2016",settore,descrSettore,ssd,"1","Si",s1_l1,s2_l1,s3_l1,"Numero articoli 10 anni","Numero citazioni 15 anni","Indice H 15 anni",1)
				create_sogliaAsn(conn,soglia)

				s1_l2 = row["Numero articoli 5 anni"]
				s2_l2 = row["Numero citazioni 10 anni"]
				s3_l2 = row["Indice H 10 anni"]
				soglia = ("2016",settore,descrSettore,ssd,"2","Si",s1_l2,s2_l2,s3_l2,"Numero articoli 5 anni","Numero citazioni 10 anni","Indice H 10 anni",1)
				create_sogliaAsn(conn,soglia)

		# NON BIBLIOMETRICI
		with open(tsvSoglieNonBiblio, newline='') as tsvFile:
			spamreader = csv.DictReader(tsvFile, delimiter='\t')
			table = list(spamreader)
			for row in table:
				temp = row["SC/SSD"]
				if len(temp) == 5:
					settore = temp
					ssd = ""
				else:
					settore = temp[:5]
					ssd = temp[6:]

				descrSettore = row["SETTORE CONCORSUALE"]
				s1_l1 = row["Numero articoli e contributi 10 anni"]
				s2_l1 = row["Numero articoli classe A 15 anni"]
				s3_l1 = row["Numero Libri 15 anni"]
				soglia = ("2016",settore,descrSettore,ssd,"1","No",s1_l1,s2_l1,s3_l1,"Numero articoli e contributi 10 anni","Numero articoli classe A 15 anni","Numero Libri 15 anni",0)
				create_sogliaAsn(conn,soglia)

				s1_l2 = row["Numero articoli e contributi 5 anni"]
				s2_l2 = row["Numero articoli classe A 10 anni"]
				s3_l2 = row["Numero Libri 10 anni"]
				soglia = ("2016",settore,descrSettore,ssd,"2","No",s1_l2,s2_l2,s3_l2,"Numero articoli e contributi 5 anni","Numero articoli classe A 10 anni","Numero Libri 10 anni",0)
				create_sogliaAsn(conn,soglia)
		
		
		with open(tsvFN, newline='') as tsvFile:
			spamreader = csv.DictReader(tsvFile, delimiter='\t')
			table = list(spamreader)
			for row in table:
				# POPULATE TABLE RISULTATOASN
				risultatoTuple = (row["ABILITATO_SOLOABILITATI"],row["VALIDITA_DAL_SOLOABILITATI"],row["VALIDITA_AL_SOLOABILITATI"],row["NOTE_SOLOABILITATI"],row["ABILITATO_RISULTATI"],row["VALIDITA_DAL_RISULTATI"],row["VALIDITA_AL_RISULTATI"],row["NOTE_RISULTATI"])
				risultatoId = create_risultatoAsn(conn, risultatoTuple)
				#print (risultatoId)
				
				# SEARCH FOR IDSOGLIA 
				if row["S1"] != "":
					res = select_sogliaAsn_conSoglia(conn,row["FASCIA"],row["SETTORE"].replace("-","/"),row["S1"],row["S2"],row["S3"],conf.anno)
					if (len(res)) == 1:
						idSoglia = (res[0][0])
					else:
						print ("conSoglia")
						print (row["ID_CV"])
						print (res[0][0])
						for r in res:
							print (r)
							print (r[0])
						sys.exit()
				else:
					res = select_sogliaAsn(conn,row["FASCIA"],row["SETTORE"].replace("-","/"),conf.anno)
					if (len(res)) != 1:
						counterNotNull = 0
						# ci sono settori che hanno 2+ ssd, ma tutti tranne uno hanno soglie "--" -> in questi casi, prendo ssd != "--"
						for r in res:
							if r[1] != '--':
								idSoglia = r[0]
								counterNotNull += 1
						if counterNotNull > 1:
							idSoglia = ""
							print ("MORE THAN ONE RESULT NOT NULL")
							print ("%s - %s - %s" % (row["ID_CV"],row["SETTORE"],row["FASCIA"]))
							print (res)
						
				curriculumTuple = (row["ID_CV"],conf.anno,row["SETTORE"],row["SSD"],row["QUADRIMESTRE"],row["FASCIA"],"",row["COGNOME"],row["NOME"],row["BIBL?"],row["I1"],row["I2"],row["I3"],idSoglia,row["ABILITATO_SOLOABILITATI"],risultatoId)
				curriculum_id = create_curriculum(conn, curriculumTuple)
				

if __name__ == '__main__':
	main()
