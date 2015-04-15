from mrjob.job import MRJob

class MyMRJob(MRJob):
    def mapper(self, _, line):
        data=line.split(', ')
        score = int(float(data[0].strip()))
        title = data[1].strip()
        link = data[2].strip()
        site = data[3].strip()
        timestamp = data[4].strip()[0:13]
        yield timestamp, score

    def combiner(self, key, list_of_values):
        # for temp in list_of_values:
        #     yield 
        yield key, sum(list_of_values)

    def reducer(self, key, list_of_values):
        current_time = ""
        for temp in list_of_values:
                yield key, temp
        # yield key, list_of_values
		
        
if __name__ == '__main__':
    MyMRJob.run()