"""
    Find most common words
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
        data=line.split(',')
        sentiment = data[0].strip()
        headline = data[1].strip()
        headline = headline.lower()
        headline = ''.join(ch for ch in headline if ch not in exclude) #Remove punctuation
        words = headline.split()

        good_words = []
        for word in words:
            if word in words_to_ignore:
                continue
            good_words.append(word)

        for word in good_words:
            yield word, 1


    def reducer(self, key, list_of_values):
        totalCount = sum(list_of_values)
        # link = list_of_values[1]
        yield None, (totalCount, key)

    def reducer2(self, _, list_of_values):
        count = 0
        for item in sorted(list_of_values, reverse = True):
            if count > 25:
                break
            output = '\"%s\" occurs %d times.' %(item[1], item[0])
            print output
            count = count + 1


    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer), self.mr(reducer=self.reducer2)]


if __name__ == '__main__':
    MyMRJob.run()
