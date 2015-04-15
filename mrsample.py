from mrjob.job import MRJob

class MyMRJob(MRJob):
    def mapper(self, _, line):
        data=line.split(', ')
        score = int(float(data[0].strip()))
        title = data[1].strip()
        link = data[2].strip()
        site = data[3].strip()
        timestamp = data[4].strip()[0:13]
        yield timestamp, (score, title, link)


    # def combiner(self, key, list_of_values):


    # def init_reducer(self):


    def reducer(self, key, list_of_values):
        temp_count = 0
        temp_tot_score = 0
        temp_happiest_score = 0
        temp_happiest_title = ""
        temp_happiest_link = ""
        temp_saddest_score = 0
        temp_saddest_title = ""
        temp_saddest_link = ""

        for temp in list_of_values:
            temp_count += 1
            temp_tot_score += temp[0]
            #find the highest score news
            if temp_happiest_score < temp[0]:
                temp_happiest_score = temp[0]
                temp_happiest_title = temp[1]
                temp_happiest_link = temp[2]
            #find the lowest score news
            if temp_saddest_score > temp[0]:
                temp_saddest_score = temp[0]
                temp_saddest_title = temp[1]
                temp_saddest_link = temp[2]

        yield (key, temp_count, temp_tot_score) , (temp_happiest_score, temp_happiest_title, temp_happiest_link, temp_saddest_score, temp_saddest_title, temp_saddest_link)


    # def steps(self):
    #     return [self.mr(mapper=self.mapper, combiner=self.combiner, reducer_init=self.init_reducer, reducer=self.reducer)]

        
if __name__ == '__main__':
    MyMRJob.run()