import sqlite3
from sqlite3 import Error
from glob import glob
import json
import csv
import sys
import os

import conf

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


def create_cercauniversitaFromExcel(conn, cercauniversitaFromExcel):
	"""
	Create a new record into the cercauniversitaFromExcel table
	:param conn:
	:param record:
	:return: cercauniversitaFromExcel id
	"""
	
	sql = ''' INSERT INTO cercauniversitaFromExcel(ateneo,anno,cognome,nome,genere,settore,ssd,fascia,strutturaDiAfferenza,facolta)
			  VALUES(?,?,?,?,?,?,?,?,?,?) '''
	#print (sql)
	cur = conn.cursor()
	cur.execute(sql, cercauniversitaFromExcel)
	return cur.lastrowid


def main():
	
	# create a database connection
	conn = create_connection(conf.dbFilename)
 
	# populate tables
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
		

		# POPULATE TABLE CERCAUNIVERSITAFROMEXCEL
		contents = glob(conf.tsvPath + '*.tsv')
		contents.sort()
		# get ...
		for filename_withPath in contents:
			anno = int(filename_withPath.split("bologna_")[1].replace(".tsv",""))
			with open(filename_withPath, newline='') as csvfile:
				spamreader = csv.DictReader(csvfile, delimiter='\t')
				for row in spamreader:
					sn = (row['Cognome e Nome']).split()
					lastname = list()
					firstname = list()
					for part in sn:
						if part.isupper():
							lastname.append(part)
						else:
							firstname.append(part)
					cognome = (" ".join(lastname)).title()
					nome = (" ".join(firstname)).title()
					
					cercauniTuple = ("Bologna",anno,cognome,nome,row["Genere"],row["S.C."],row["S.S.D."],row["Fascia"],row["Struttura di afferenza"],row["Facoltà"])
					#print (cercauniTuple)
					a = create_cercauniversitaFromExcel(conn, cercauniTuple)
					print (a)
					
		#conn.close()
	
if __name__ == '__main__':
	main()
