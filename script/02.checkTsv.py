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

tsvTemp = "../data/input/09.dois-candidati-2016-ordered_withNames.tsv"
tsvOut = "../data/input/candidatesAsn2016.tsv"

tempDict = dict()
outDict = dict()

with open(tsvOut, newline='') as csvfile:
	spamreader = csv.DictReader(csvfile, delimiter='\t')
	table = list(spamreader)
	for row in table:
		quadrimestre = row['QUADRIMESTRE']
		fascia = row['FASCIA']
		settore = row['SETTORE'].replace("/","-")
		idCv = row['ID_CV']
		outDict[idCv] = dict(row)

with open(tsvTemp, newline='') as csvfile:
	spamreader = csv.DictReader(csvfile, delimiter='\t')
	table = list(spamreader)
	for row in table:
		quadrimestre = row['QUADRIMESTRE']
		fascia = row['FASCIA']
		settore = row['SETTORE'].replace("/","-")
		idCv = row['ID_CV']
		tempDict[idCv] = dict(row)

print (len(outDict.keys()))
print (len(tempDict.keys()))

for idCvOut in tempDict:
	if idCvOut not in outDict:
		print ("%s\t%s\t%s\t%s\t%s\t%s" % (tempDict[idCvOut]["QUADRIMESTRE"],tempDict[idCvOut]["FASCIA"],tempDict[idCvOut]["SETTORE"],tempDict[idCvOut]["ID_CV"],tempDict[idCvOut]["COGNOME"],tempDict[idCvOut]["NOME"]))
		#pass
