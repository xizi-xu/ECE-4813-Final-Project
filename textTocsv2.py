import csv
import itertools
import re

txt_file = r"resultPaul.txt"
in_txt = open(txt_file, "r")

everything = []

for line in in_txt:
	one_phrase = []
	# remove punctuation 
	line = line.strip('[|\n|]')
	line = re.split(', ',line)

	for part in line:
		part = part.strip('\"|[|\'')
		one_phrase.append(part)
	# let each phrase's info be a row
	everything.append(one_phrase)

with open('ResultPaul.csv', 'wb') as csvfile:
	mywriter = csv.writer(csvfile)
	mywriter.writerow(('count', 'phrase', 'average score', 'links'))
	for line in everything:
		mywriter.writerow(line)


in_txt.close()

