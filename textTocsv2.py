import csv
import itertools
import re

txt_file = r"resultPaul.txt"
in_txt = open(txt_file, "r")

everything = []
# one_hr = []

# line = in_txt.readline()
# line = line.strip('[|\n|]')
# line = re.split(', ',line)
# for part in line:
# 	part = part.strip('\"|[|\'')
# 	one_hr.append(part)


for line in in_txt:
	one_phrase = []
	line = line.strip('[|\n|]')
	line = re.split(', ',line)
	for part in line:
		part = part.strip('\"|[|\'')
		one_phrase.append(part)
	# print one_hr
	everything.append(one_phrase)

with open('ResultPaul.csv', 'wb') as csvfile:
	mywriter = csv.writer(csvfile)
	mywriter.writerow(('count', 'phrase', 'average score', 'links'))
	for line in everything:
		mywriter.writerow(line)


in_txt.close()

