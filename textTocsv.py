import csv
import itertools
import re

txt_file = r"result.txt"
in_txt = open(txt_file, "r")

everything = []

for line in in_txt:
	one_hr = []
	# remove punctuation 
	line = line.strip('[|\n|]')
	line = re.split(']	|, ',line)

	for part in line:
		part = part.strip('""|[')
		one_hr.append(part)
	# let each hr's info be a row
	everything.append(one_hr)

with open('Result.csv', 'wb') as csvfile:
	mywriter = csv.writer(csvfile)
	mywriter.writerow(('time', 'total news', 'total score', 'highest score', 'h_title', 'h_link', 'lowest score', 'l_title', 'h_link'))
	for line in everything:
		mywriter.writerow(line)


in_txt.close()

