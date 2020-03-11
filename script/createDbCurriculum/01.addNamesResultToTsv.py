# -*- coding: UTF-8 -*-

import mylib


tsvIn = "../../data/input/09.dois-candidati-2016-ordered.tsv"
tsvOut_temp = "../../data/input/09.dois-candidati-2016-ordered_withNames.tsv"
tsvOut_soloAbilitati = "../../data/input/candidatesAsn2016_soloAbilitati.tsv"
tsvOut = "../../data/input/candidatesAsn2016.tsv"

pathPdf = "/media/fpoggi/wd-pico/backup/ANVUR/2016-mobiliti/"
#pathPdf = "../data/input/2016-mobiliti/"

#####mylib.addAsnOutcomesToTsv(tsvOut_temp, tsvOut, pathPdf)
#mylib.addAuthorsNamesToTsv(tsvIn, tsvOut_temp, pathPdf)

#mylib.addAsnOutcomesToTsv_soloAbilitati(tsvOut_temp, tsvOut_soloAbilitati, pathPdf)

mylib.addAsnOutcomesToTsv_risultati(tsvOut_soloAbilitati, tsvOut, pathPdf)
