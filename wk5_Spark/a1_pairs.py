from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))

# parse data and create raw RDD
import re

def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []

wiki = sc.textFile("/data/wiki/en_articles_part/articles-part", 16).map(parse_article)

# build word pairs
wiki_pairs = wiki.flatMap(lambda r: [r[i].lower()+'_'+r[i+1].lower() for i in range(len(r)-1)])

# count each pair
wiki_pairs_counts = wiki_pairs.map(lambda pair: (pair, 1)).reduceByKey(lambda x, y: x + y)

# find word pairs starting with required word 'narodnaya'
results = wiki_pairs_counts.filter(lambda count: count[0][:len('narodnaya')] == 'narodnaya').collect()

# output
for pair in results:
    print '%s\t%s' % pair