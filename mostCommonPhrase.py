"""
    Find most common phrases
"""

from mrjob.job import MRJob
import string

words_to_ignore = ["for","of","the","in","a","to","is","news", "breaking", "and", "you", "by",
                   "your", "on", "at", "as", "this", "it", "with", "from"]

exclude = set(string.punctuation)

#-------- Load Ignore Words Dict ----
stopFile = open('StopWords.txt')
lines = stopFile.readlines()
for line in lines:
    s = line.split("\t")
    words_to_ignore.append(s)

stopFile.close()

class MyMRJob(MRJob):
    def mapper(self, _, line):
        #Get data
        data=line.split(',')
        sentiment = data[0].strip()
        headline = data[1].strip()
        headline = headline.lower()

        #Remove punctuation
        headline = ''.join(ch for ch in headline if ch not in exclude) #Remove punctuation
        words = headline.split()

        good_words = []
        for word in words:
            # if word in words_to_ignore:
            #     continue
            good_words.append(word)

        # Create phrases
        phrases = []
        for i in range(0, len(good_words) - 1):
            word1 = good_words[i]
            word2 = good_words[i+1]
            if word1 in words_to_ignore:
                continue
            if word2 in words_to_ignore:
                continue

            phrase = '%s %s' %(word1, word2)
            phrases.append(phrase)


        link = data[2].strip()
        for phrase in phrases:
            yield phrase, (1, sentiment, link)

    def reducer(self, key, list_of_values):
        # totalCount = sum(list_of_values)
        # link = list_of_values[1]
        # yield None, (totalCount, key)

        totalCount = 0.0
        totalSentiment = 0.0
        list_of_links = []
        for item in list_of_values:
            #Add count
            totalCount = totalCount + item[0]
            #Add sentiment
            totalSentiment = totalSentiment + float(item[1])
            #Append links
            list_of_links.append(item[2])

        avgSentiment = totalSentiment / totalCount
        yield None, (totalCount, key, avgSentiment, list_of_links)

    def reducer2(self, _, list_of_values):
        #Print top 25
        count = 0
        for item in sorted(list_of_values, reverse = True):
            if count > 25:
                break
            print item
            count = count + 1

    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer), self.mr(reducer=self.reducer2)]


if __name__ == '__main__':
    MyMRJob.run()
