"""
Map Reduce that removes duplicate headlines
"""

from mrjob.job import MRJob
import sys

input_file_name = 'Live_RSS_Headlines_Output_Big.csv'
csv_file = open("Live_RSS_Headlines_Output_Big_No_Duplicates.csv","a")

class MyMRJob(MRJob):
    def mapper(self, _, line):
        try:
        	data=line.split(',')
        	headline = data[1].strip()
        except:
        	return
        yield headline, line
	
    def reducer(self, key, list_of_values):
		global csv_file
		try:
			csv_file.write(list(list_of_values)[0]+"\n")
		except:
			pass

    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer)]
        
def remove_duplicate_headlines():
		global csv_file
		sys.argv = ['Remove Duplicates.py', input_file_name, '-q']
		MyMRJob.run()
		csv_file.close()

if __name__ == '__main__':
	remove_duplicate_headlines()

