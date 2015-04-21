/*
NegativeSentiment.pig
Finds the negative articles from the csv file
*/

/*
Load in the csv file as data
sentiment: float
headline: string
link: string
*/

data = LOAD 'test1.csv' USING PigStorage(',') as (sentiment:float, headline:chararray, link:chararray);

-- Remove any duplicates
data = DISTINCT data;

--- Filter out negative articles
FilteredData = FILTER data BY sentiment < 0;

-- Sort by sentiment
SortedData = ORDER FilteredData BY sentiment DESC;

-- Store output
STORE SortedData into 'NegativeSentiment';
