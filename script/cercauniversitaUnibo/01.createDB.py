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

def main():
	
	sql_create_cercauniversitaFromExcel_table = """ CREATE TABLE IF NOT EXISTS cercauniversitaFromExcel (
										id integer PRIMARY KEY AUTOINCREMENT,
										ateneo text NOT NULL,
										anno integer NOT NULL,
										cognome text NOT NULL,
										nome text NOT NULL,
										genere text NOT NULL,
										settore text NOT NULL,
										ssd text,
										fascia text NOT NULL,
										strutturaDiAfferenza text,
										facolta text
									); """
										#authorId integer,
										#FOREIGN KEY (authorId) REFERENCES authorScopus(id)
	
	sql_create_sogliaAsn_table = """ CREATE TABLE IF NOT EXISTS sogliaAsn (
										id integer PRIMARY KEY AUTOINCREMENT,
										annoAsn text NOT NULL,
										settore text NOT NULL,
										descrSettore string,
										ssd text,
										fascia integer NOT NULL,
										bibliometrico string NOT NULL,
										S1 integer,
										S2 integer,
										S3 integer,
										descrS1 string NOT NULL,
										descrS2 string NOT NULL,
										descrS3 string NOT NULL,
										bibl integer
									); """
	# create a database connection
	conn = create_connection(conf.dbFilename)
 
	# create tables
	if conn is not None:
		create_table(conn, sql_create_cercauniversitaFromExcel_table)
		create_table(conn, sql_create_sogliaAsn_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")

if __name__ == '__main__':
	main()
