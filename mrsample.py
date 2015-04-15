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
    #     # count = 0
    #     # tot_sc = 0
    #     # for temp in list_of_values:
    #     #     count = count + 1;
    #     # yield key, (sum(list_of_values), count)
    #     yield key, sum(list_of_values)


    # def init_reducer(self):
        # self.count = 0
        # self.tot_score = 0
        # self.happiest_score = 0
        # self.happiest_title = ""
        # self.happiest_link = ""

    def reducer(self, key, list_of_values):
        temp_count = 0
        temp_tot_score = 0
        temp_happiest_score = 0
        temp_happiest_title = ""
        temp_happiest_link = ""

        for temp in list_of_values:
            temp_count += 1
            temp_tot_score += temp[0]
            if temp_happiest_score < temp[0]:
                temp_happiest_score = temp[0]
                temp_happiest_title = temp[1]
                temp_happiest_link = temp[2]
        yield (key, temp_count, temp_tot_score) , (temp_happiest_score, temp_happiest_title, temp_happiest_link)


    # def steps(self):
    #     return [self.mr(mapper=self.mapper, combiner=self.combiner, reducer_init=self.init_reducer, reducer=self.reducer)]

        
if __name__ == '__main__':
    MyMRJob.run()

# "2015-04-13 19" 574  -54
# "2015-04-13 20" 131  -93
# "2015-04-13 21" 132  -61
# "2015-04-13 22" 12     6