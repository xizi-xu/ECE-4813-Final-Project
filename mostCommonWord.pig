/*
mostCommonWord.pig
Finds the most common words from the csv file
*/

/*
Load in the csv file as data
sentiment: float
headline: string
link: string
*/

data = LOAD 'test1.csv' USING PigStorage(',') as (sentiment:float, headline:chararray, link:chararray);

/*words_to_ignore = LOAD 'StopWords.txt' USING PigStorage('\n') as (word:chararray);*/

/* Remove any duplicates */
data = DISTINCT data;

/* Group by each headline*/
headlines = group data BY headline;


words = FOREACH data GENERATE flatten(TOKENIZE(headline)) as wordTuple;
C = group words by wordTuple;

/* Get count, organize by count, limit to the top 50  */
wordCount = foreach C generate COUNT(words) as count, group as word;
OUT = ORDER wordCount by count DESC;
OUT = LIMIT OUT 50;
STORE OUT into 'mostCommonWordPig.txt';

/*
filteredOut = FILTER OUT by word != words_to_ignore.$0;
DUMP filteredOut;
*/
