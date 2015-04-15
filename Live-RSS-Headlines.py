#install feedparser from https://pypi.python.org/pypi/feedparser
import feedparser
import threading
import sys
import time
import subprocess
from datetime import datetime

refresh_time = 300 #in seconds

rss_feeds = [
('http://rss.cnn.com/rss/cnn_topstories.rss', 'CNN'),
('http://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml', 'The New York Times'),
('http://www.wsj.com/xml/rss/3_7085.xml', 'Wall Street Journal'),
('http://www.cbsnews.com/latest/rss/main', 'CBS'),
('http://www.news.gatech.edu/rss/all', 'Georgia Tech News'),
('http://www.ajc.com/list/rss/news/breaking-news-center/aFSL/', 'Atlanta Journal Constitution'),
('http://archive.11alive.com/rss/news/40/10.xml', '11 Alive Atlanta'),
('http://www.huffingtonpost.com/feeds/verticals/good-news/index.xml', 'Huffington Post'),
('http://feeds.feedburner.com/SunnySkyz?format=xml', 'SunnySkyz'),
('http://www.goodnewsnetwork.org/feed', 'Good News Network'),
('http://news.yahoo.com/rss', 'Yahoo! News'),
('http://feeds.bbci.co.uk/news/rss.xml', 'BBC'),
('http://rssfeeds.usatoday.com/usatoday-NewsTopStories', 'USA Today'),
('http://www.npr.org/rss/rss.php?id=1001', 'NPR'),
('http://rss.newser.com/rss/section/13.rss', 'Newser'),
('http://www.forbes.com/feeds/popstories.xml','Forbes'),
('http://www.economist.com/sections/science-technology/rss.xml','The Economist'),
('http://www.tmz.com/rss.xml','TMZ'),
('http://gizmodo.com/excerpts.xml','Gizmodo'),
('http://www.tampabay.com/feeds/rss.page?collatedTag=news&section=staffArticle&feedType=rss','Tampa Bay Times'),
('http://www.baynews9.com/content/news/baynews9/feeds/rss.html/local-news.html','Bay News 9'),
('http://www.nbclosangeles.com/news/top-stories/?rss=y&embedThumb=y&summary=y','Southern California NBC'),
('http://www.ksat.com/content/pns/ksat.topstories.news.rss','Texas KSAT'),
("http://feeds.washingtonpost.com/rss/rss_election-2012",'The Washington Post'),
('feed://www.buzzfeed.com/index.xml',"BuzzFeed")]

csv_file = open("Live_RSS_Headlines_Output.csv","a")

sentiments={}
file = open('AFINN-111.txt')
lines = file.readlines()
for line in lines:
    s = line.split("\t")
    sentiments[s[0]] = s[1].strip()
file.close()

def get_sentiment_score(phrase):
	phrase = phrase.strip()
	total = 0.0
	for word in phrase.split():
		word = word.lower()
		for char in '[!@#$)(*<>=+/:;&^%#|\{},.?~`]-\"':
			word = word.replace(char,'')
		if word in list(sentiments):
			total = total + float(sentiments[word])
	return total

	
last_round_headlines = []
this_round_headlines = []

def get_headlines(rss_feed):
	feed = feedparser.parse(rss_feed[0])
	for entry in feed.entries:
		title = entry.title.encode('utf-8')
		this_round_headlines.append(title)
		if title not in last_round_headlines:
			score = get_sentiment_score(title)
			title = title.replace(',','')
			to_print = "" + str(score) + ", " + title + ", " + entry.link.encode('utf-8') + ", " + rss_feed[1] + ", " + str(datetime.now()) # <score>, <title>, <link>, <source>, <date/time downloaded>
			csv_file.write(to_print+"\n")
			sys.stdout.write(to_print + '\n')
		

while True:
	#print("Checking for new headlines..")
	#print("after: length of last_round_headlines: " + str(len(last_round_headlines)))
	#print("after: length of this_round_headlines: " + str(len(this_round_headlines)))
	for rss_feed in rss_feeds:
		t = threading.Thread(target=get_headlines, args = (rss_feed,))
		t.start()
	time.sleep(refresh_time)
	last_round_headlines = list(this_round_headlines)
	this_round_headlines = list()



