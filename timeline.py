"""
    Define the total sentiment analysis score of news per hour
    Find the news with highest and lowest score as well
"""
from mrjob.job import MRJob

class MyMRJob(MRJob):
    def init_mapper(self):
        self.healines = []

    def mapper(self, _, line):
        data=line.split(', ')
        score = int(float(data[0].strip()))
        headline = data[1].strip()
        link = data[2].strip()
        site = data[3].strip()
        # only need the timestamp upto hr
        timestamp = data[4].strip()[0:13]

        if not headline in self.healines:
            # remove duplicated headlines
            self.healines.append(headline)
            yield timestamp, (score, headline, link)


    # def combiner(self, key, list_of_values):


    def reducer(self, key, list_of_values):
        temp_count = 0
        temp_tot_score = 0
        temp_happiest_score = 0
        temp_happiest_headline = ""
        temp_happiest_link = ""
        temp_saddest_score = 0
        temp_saddest_headline = ""
        temp_saddest_link = ""

        for temp in list_of_values:
            temp_count += 1
            temp_tot_score += temp[0]
            #find the highest score news
            if temp_happiest_score < temp[0]:
                temp_happiest_score = temp[0]
                temp_happiest_headline = temp[1]
                temp_happiest_link = temp[2]
            #find the lowest score news
            if temp_saddest_score > temp[0]:
                temp_saddest_score = temp[0]
                temp_saddest_headline = temp[1]
                temp_saddest_link = temp[2]

        yield (key, temp_count, temp_tot_score) , (temp_happiest_score, temp_happiest_headline, temp_happiest_link, temp_saddest_score, temp_saddest_headline, temp_saddest_link)


    def steps(self):
        return [self.mr(mapper_init=self.init_mapper, mapper=self.mapper, reducer=self.reducer)]

        
if __name__ == '__main__':
    MyMRJob.run()