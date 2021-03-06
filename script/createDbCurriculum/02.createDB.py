import sqlite3
from sqlite3 import Error

import conf
import mylib


def main():
	
	sql_create_curriculum_table = """ CREATE TABLE IF NOT EXISTS curriculum (
										id string PRIMARY KEY,
										annoAsn text NOT NULL,
										settore text NOT NULL,
										ssd text,
										quadrimestre integer NOT NULL,
										fascia integer NOT NULL,
										orcid text,
										cognome text,
										nome text,
										bibl integer,
										I1 integer NOT NULL,
										I2 integer NOT NULL,
										I3 integer NOT NULL,
										idSoglia integer NOT NULL,
										
										abilitato text NOT NULL,
										idRisultato integer NOT NULL,
										
										FOREIGN KEY (idSoglia) REFERENCES sogliaAsn(id)
										FOREIGN KEY (idRisultato) REFERENCES risultatoAsn(id)
									); """
										#authorId integer,
										#FOREIGN KEY (authorId) REFERENCES authorScopus(id)
										
	sql_create_risultatoAsn_table = """ CREATE TABLE IF NOT EXISTS risultatoAsn (
										id integer PRIMARY KEY AUTOINCREMENT,
										abilitatoSoloAbilitati text,
										validitaDalSoloAbilitati text,
										validitaAlSoloAbilitati text,
										noteSoloAbilitati text,
										abilitatoSoloRisultati text,
										validitaDalSoloRisultati text,
										validitaAlSoloRisultati text,
										noteSoloRisultati text
									); """

	
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

	
	'''
	sql_create_authorScopus_table = """CREATE TABLE IF NOT EXISTS authorScopus (
									id integer PRIMARY KEY,
									givenname text,
									surname text,
									initials text,
									orcid text
								);"""
	
	sql_create_cercauniversita_table = """CREATE TABLE IF NOT EXISTS cercauniversita (
									id string PRIMARY KEY,
									authorId integer,
									anno text NOT NULL,
									settore text NOT NULL,
									ssd text,
									fascia integer NOT NULL,
									orcid text,
									cognome text,
									nome text,
									genere text,
									ateneo text,
									facolta text,
									strutturaAfferenza,
									FOREIGN KEY (authorId) REFERENCES authorScopus(id)
								);"""

	sql_create_wroteRelation_table = """ CREATE TABLE IF NOT EXISTS wroteRelation (
										authorId integer NOT NULL,
										eid string,
										FOREIGN KEY (eid) REFERENCES authorScopus(id),
										FOREIGN KEY (eid) REFERENCES publication(eid)
									); """

	sql_create_publication_table = """ CREATE TABLE IF NOT EXISTS publication (
										eid string PRIMARY KEY,
										doi string,
										publicationDate string,
										publicationYear string NOT NULL,
										title string NOT NULL,
										venueName string NOT NULL
									); """

	sql_create_citesRelation_table = """ CREATE TABLE IF NOT EXISTS citesRelation (
										eidCiting string,
										eidCited string,
										citationDate string,
										citationYear string NOT NULL
									); """
	'''
	# create a database connection
	conn = mylib.create_connection(conf.dbFilename)
 
	# create tables
	if conn is not None:
		mylib.create_table(conn, sql_create_curriculum_table)
		mylib.create_table(conn, sql_create_risultatoAsn_table)
		mylib.create_table(conn, sql_create_sogliaAsn_table)
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	
if __name__ == '__main__':
	main()
