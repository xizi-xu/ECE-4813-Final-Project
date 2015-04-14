from mrjob.job import MRJob

class MyMRJob(MRJob):
    def mapper(self, _, line):
        data=line.split('; ')
        score = data[0].strip()
        title = data[1].strip()
        site = data[3].strip()
        yield title, (float(score), site)

    def reducer(self, key, list_of_values):
        for temp in list_of_values:
            if temp[0] > 0.0:
                yield key, temp
		
        
if __name__ == '__main__':
    MyMRJob.run()