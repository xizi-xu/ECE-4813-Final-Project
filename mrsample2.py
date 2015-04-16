from mrjob.job import MRJob
from mrjob.job import MRStep

dumb_words = ['news', 'a', 'about', 'above', 'above', 'across', 'after', 'afterwards', 'again', 'against', 'all', 'almost', 'alone', 'along', 'already', 'also','although','always','am','among', 'amongst', 'amoungst', 'amount',  'an', 'and', 'another', 'any','anyhow','anyone','anything','anyway', 'anywhere', 'are', 'around', 'as',  'at', 'back','be','became', 'because','become','becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'below', 'beside', 'besides', 'between', 'beyond', 'bill', 'both', 'bottom','but', 'by', 'call', 'can', 'cannot', 'cant', 'co', 'con', 'could', 'couldnt', 'cry', 'de', 'describe', 'detail', 'do', 'done', 'down', 'due', 'during', 'each', 'eg', 'eight', 'either', 'eleven','else', 'elsewhere', 'empty', 'enough', 'etc', 'even', 'ever', 'every', 'everyone', 'everything', 'everywhere', 'except', 'few', 'fifteen', 'fify', 'fill', 'find', 'fire', 'first', 'five', 'for', 'former', 'formerly', 'forty', 'found', 'four', 'from', 'front', 'full', 'further', 'get', 'give', 'go', 'had', 'has', 'hasnt', 'have', 'he', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'however', 'hundred', 'ie', 'if', 'in', 'inc', 'indeed', 'interest', 'into', 'is', 'it', 'its', 'itself', 'keep', 'last', 'latter', 'latterly', 'least', 'less', 'ltd', 'made', 'many', 'may', 'me', 'meanwhile', 'might', 'mill', 'mine', 'more', 'moreover', 'most', 'mostly', 'move', 'much', 'must', 'my', 'myself', 'name', 'namely', 'neither', 'never', 'nevertheless', 'next', 'nine', 'no', 'nobody', 'none', 'noone', 'nor', 'not', 'nothing', 'now', 'nowhere', 'of', 'off', 'often', 'on', 'once', 'one', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'our', 'ours', 'ourselves', 'out', 'over', 'own','part', 'per', 'perhaps', 'please', 'put', 'rather', 're', 'same', 'see', 'seem', 'seemed', 'seeming', 'seems', 'serious', 'several', 'she', 'should', 'show', 'side', 'since', 'sincere', 'six', 'sixty', 'so', 'some', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhere', 'still', 'such', 'system', 'take', 'ten', 'than', 'that', 'the', 'their', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'therefore', 'therein', 'thereupon', 'these', 'they', 'thickv', 'thin', 'third', 'this', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'top', 'toward', 'towards', 'twelve', 'twenty', 'two', 'un', 'under', 'until', 'up', 'upon', 'us', 'very', 'via', 'was', 'we', 'well', 'were', 'what', 'whatever', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'with', 'within', 'without', 'would', 'yet', 'you', 'your', 'yours', 'yourself', 'yourselves', 'the']

two_word_phrases = []

class MyMRJob(MRJob):
    def mapper1(self, _, line):
        data=line.split(',')
        score = int(float(data[0].strip()))
        headline = data[1].strip()
        for char in '\'\"[!@#$)(*<>=+/:;&^%#|\{},.?~`]-':
            headline = headline.replace(char,' ')
        # link = data[2].strip()
        # site = data[3].strip()
        # timestamp = data[4].strip()

        clean_words = headline.split()
    #     clean_words = []

    #     for word in words:
    #     	for char in '\'\"[!@#$)(*<>=+/:;&^%#|\{},.?~`]-':
				# word = word.replace(char,'')
    #     	clean_words.append(word)
        for i in range(0,len(clean_words)-1):
        	phrase = clean_words[i] + ' ' + clean_words[i+1]
        	for dumb_word in dumb_words:
				if clean_words[i] == dumb_word or clean_words[i+1] == dumb_word:
					return
        	yield phrase, 1
	
    # def combiner1(self, key, list_of_values):
    #     yield key, sum(list_of_values)

    # def reducer1(self, key, values):
    #     for temp in values:
    #         if temp > 20:
    #             # yield key[0], (key[1], temp)
    #             yield key, 
    def reducer1(self, key, values):
        yield key, sum(values)


    def steps(self):
        return [self.mr(mapper=self.mapper1, reducer=self.reducer1)]

    # def mapper2(self, )

    # def steps(self):
    #     return [
    #         MRStep(mapper=self.mapper1,
    #                combiner=self.combiner1,
    #                reducer=self.reducer1),
    #         MRStep(mapper=self.mapper2)
    #     ]

if __name__ == '__main__':
    MyMRJob.run()

  #   def reducer(self, key, list_of_values):
		# phrase = key
		# total_count = sum(list_of_values)
		# two_word_phrases.append((total_count,phrase))

    # def steps(self):
    #     return [self.mr(mapper=self.mapper, reducer=self.reducer)]
        
# if __name__ == '__main__':
#     MyMRJob.run()
#     two_word_phrases = sorted(two_word_phrases, reverse = True)
#     top_10 = two_word_phrases[0:10]
#     for tup in top_10:
# 		print('\'' + tup[1] + '\' occurs ' + str(tup[0]) + ' times.')

