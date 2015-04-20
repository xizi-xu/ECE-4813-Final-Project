--RSS feed's duplicates have to be removed prior 

data = LOAD 'test1.csv' using PigStorage(',') AS (score:int, headline:chararray, link: chararray, site: chararray, timestamp: chararray);

--group by hr
grouped = GROUP data BY SUBSTRING(timestamp, 1, 14);

--compute total count and score, max and min
totStats = FOREACH grouped GENERATE group, COUNT(data.score) as tot_count, SUM(data.score) as tot_score, MAX(data.score) as max_score, MIN(data.score) as min_score;

--merge result and remove duplicated fields
jointMax = JOIN totStats by max_score, data by score;
jointMin = JOIN totStats by min_score, data by score;
joint = JOIN jointMax by $0, jointMin by $0;
pre_result = FOREACH joint GENERATE $0, $1, $2, $5, $6, $7, $15, $16, $17;

--only take one max and min from each hour
result_group = GROUP pre_result BY $0;
result = FOREACH result_group {
	single = LIMIT pre_result 1;
	GENERATE FLATTEN(single);
};

STORE result INTO 'timelineResult';