import sqlite3
from sqlite3 import Error
from glob import glob
import json
import csv
import sys
import os

import conf
 
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


def create_cercauniversitaFromExcel(conn, cercauniversitaFromExcel):
	"""
	Create a new record into the cercauniversitaFromExcel table
	:param conn:
	:param record:
	:return: cercauniversitaFromExcel id
	"""
	
	sql = ''' INSERT INTO cercauniversitaFromExcel(ateneo,anno,cognome,nome,genere,settore,ssd,fascia,strutturaDiAfferenza,facolta)
			  VALUES(?,?,?,?,?,?,?,?,?,?) '''
	print (sql)
	cur = conn.cursor()
	cur.execute(sql, cercauniversitaFromExcel)
	return cur.lastrowid


def main():
	
	# create a database connection
	conn = create_connection(conf.dbFilename)
 
	# create tables
	with conn:
		
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
