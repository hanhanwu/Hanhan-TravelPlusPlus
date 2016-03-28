# cell 1
# allow foreign languages in the posts
import sys  

reload(sys)  
sys.setdefaultencoding('utf8')


# cell 2
import nltk
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('maxent_treebank_pos_tagger')
nltk.download('averaged_perceptron_tagger')


# cell 3
# Tokens did not pass threshold in reddit posts matching
q4_tokens = [u'urban', u'backpacking', u'advic']
q5_tokens = [u'camera', u'travel', u'free']


# cell 4
# Crawl inbound pages, images using the seed pages
from BeautifulSoup import *
import urllib2

def edit_url(l):
  url_prefix = "https://en.wikipedia.org"
  return url_prefix+l

# using BFS, depth define the search depth
def travel_crawler(pages, depth=2):
  all_text_links = []
  all_image_links = set()
  all_text_links.extend(pages)
  connections = {}
  
  for d in range(depth):
    crawled_links = set()
    for p in pages:
      try:
        page = urllib2.urlopen(p)
      except:
        continue
      connections.setdefault(p, [])
      soup = BeautifulSoup(page.read())
      links = [l["href"] for l in soup("a") if "href" in dict(l.attrs)]
      filtered_links = [l for l in links if l.startswith("/wiki/")]
      image_links = set([edit_url(l) for l in filtered_links if l.startswith("/wiki/File:") and "jpg" in l])
      text_links = set([edit_url(l) for l in filtered_links if ":" not in l])
      text_links = [l for l in text_links if l not in all_text_links]
      all_image_links.update(image_links)
      all_text_links.extend(text_links)
      connections[p] = text_links
      crawled_links.update(text_links)
      
    pages = crawled_links
  return all_text_links, list(all_image_links), connections

seed_pages = ["https://en.wikipedia.org/wiki/Travel", "https://en.wikipedia.org/wiki/Backpacking_(travel)", "https://en.wikipedia.org/wiki/Traveler"]

all_text_links, all_image_links, connections = travel_crawler(seed_pages, 1)
print len(all_image_links)
print len(all_text_links)
print len(connections)
print
print all_image_links[0]
print all_text_links[0]
print connections.items()[0]


# cell 5
# save image name, image url into df
# comapre each token in the query with each image name, using Levenshtein Distance
# for each image name, compare with all the query tokens, find the min distance, then for all the min disatnce, find the top small values
from pyspark.sql import Row
from Levenshtein import distance
from pyspark.sql.types import *
from pyspark.sql.functions import udf

def get_image_name(u):
  img_name = u.split("https://en.wikipedia.org/wiki/File:")[1].split(".")[0]
  return Row(name=img_name, url=u)

def get_min_score(tokens, img_name):
  min_dist = distance(tokens[0], img_name)
  for i in range(1,len(tokens)):
    dist = distance(tokens[i], img_name)
    if min_dist > dist:
      min_dist = dist
  return min_dist

img_df = sc.parallelize(all_image_links).map(get_image_name).toDF()
tokens = [str(t.encode("utf-8")) for t in q4_tokens]

levenshtein_udf = udf(lambda name: get_min_score(tokens, str(name)), IntegerType())
img_score_df = img_df.withColumn("levenshtein_dist", levenshtein_udf(img_df.name)).coalesce(1)
sorted_img_df = img_score_df.sort(img_score_df.levenshtein_dist).cache()
sorted_img_df.show(truncate=False)


# cell 6
# display the most matched image
closest_image = sorted_img_df.first()[1]
displayHTML(closest_image)


# cell 7
# build GraphFrame by using connections dict, sort the links on pagerank score
vertices = sc.parallelize(all_text_links).zipWithIndex().map(lambda (u, uid): Row(id=uid, url=u)).toDF()
vertices.registerTempTable("VerticeTable")

from_to_lst = []
for k,v in connections.items():
  for t in v:
    from_to_lst.append(Row(from_url=k, to_url=t, relationship="contains"))
tmp_df = sc.parallelize(from_to_lst).toDF()
tmp_df.registerTempTable("ConnectTable")

edges = sqlContext.sql("""
select from_id as src, t.id as dst, relationship from
(select id as from_id, to_url, relationship
from VerticeTable t1, ConnectTable t2
where t1.url=t2.from_url) t0
join
VerticeTable t
on t.url=t0.to_url
""")


# cell 8
# The pagerank score shows how important each page is
from graphframes import *

g = GraphFrame(vertices, edges)
results = g.pageRank(resetProbability=0.15, tol=0.01)
result_vertices = results.vertices
sorted_vertices = result_vertices.sort(result_vertices.pagerank.desc())
sorted_vertices .registerTempTable("SortedLinks")

display(sorted_vertices)


# cell 9
# Only choose top 200 wiki pages for the following score calculation, 
# otherwise Spark clsuter will become terribly slow even when using DataFrame. 

chosen_links = sqlContext.sql("""
select id as uid, url 
from SortedLinks 
limit 200
""").cache()
chosen_links.show(n= 5,truncate=False)


# cell 10
# get tokens from each url
from nltk.stem.porter import *
from pyspark.ml.feature import StringIndexer

stemmer = PorterStemmer()
stopwords = nltk.corpus.stopwords.words('english')

def get_cleaned_tokens(t):
  uid = t[0]
  url = t[1]
  try:
    page = urllib2.urlopen(url)
  except:
    return None
  sp = BeautifulSoup(page.read())
  utext = sp.text
  sentences = [s.lower() for s in nltk.tokenize.sent_tokenize(utext)]
  tokens = [w for s in sentences for w in s.split() if w not in stopwords and len(w)<20]
  cleaned_tokens = [re.sub(r'[\W]+', r'', w) for w in tokens]
  return [Row(word=stemmer.stem(cleaned_tokens[i]), uid=uid, location=i) for i in range(len(cleaned_tokens)) if cleaned_tokens[i]!=""]
  
  
# cell 11
word_map_rdd = chosen_links.rdd.map(get_cleaned_tokens).filter(lambda lst:lst!=None)
word_map_df = word_map_rdd.flatMap(lambda r:r).toDF()
word_map_df.registerTempTable("word_map")
word_map_df.show(n=10)


# cell 12
fields = "t0.uid"
tables = ""
where_clause = ""
count = 0
tokens = q5_tokens

for w in tokens:
  fields += ", t%d.location as location%d" % (count, count)
  if count==0:
    tables = "word_map t0"
    where_clause = "t0.word='%s'" % w
  else:
    tables += ", word_map t%d" % count
    where_clause += " and t%d.word='%s'" % (count, w)
  count += 1
  
for i in range(count-1):
  where_clause += " and t%d.uid=t%d.uid" % (i, i+1)
sql_query1 = "select %s from %s where %s" % (fields, tables, where_clause)

rows_df = sqlContext.sql(sql_query1).cache()
rows_df.registerTempTable("RowsTable")
rows_df.show(n=3)


# cell 13
def rescale_score(score, mins, maxs, small_is_better=0):
  v = 0.00001  # avoid to be divided by 0
  if small_is_better:
    new_score = float(mins)/max(score, v)
  else:
    if maxs == 0: maxs = v
    new_score = float(score)/maxs
  return new_score
  
# Approach 1: Calculate scores based on words locations
location_lists = "location0"

for i in range(1, count):
  location_lists += "+location%d" % i
  
sql_query2 = "select uid, min("+location_lists+") as location_score from RowsTable group by uid"
loc_df1 = sqlContext.sql(sql_query2).cache()

mins1 = loc_df1.agg({"location_score": "min"}).collect()[0][0]
maxs1 = loc_df1.agg({"location_score": "max"}).collect()[0][0]

udf1 = udf(lambda s: rescale_score(s, mins1, maxs1, 1))
loc_df2 = loc_df1.withColumn("rescaled_location_score", udf1(loc_df1.location_score)).cache()
loc_df3 = loc_df2.orderBy(loc_df2.rescaled_location_score.desc())

cond1 = [chosen_links.uid==loc_df3.uid]
loc_df4 = loc_df3.join(chosen_links, cond1).select(loc_df3.uid, loc_df3.rescaled_location_score, chosen_links.url).cache()
loc_df4.show(n=5, truncate=False)


# Approach 2: Calculate scores based on words distance
location_lists = "abs(location0-location1)"

if count>2:
  for i in range(1,count-1):
    location_lists += "+abs(location%d-location%d)" % (i,i+1)
if count<2:
  sql_query3 = "select uid, 1.0 as dist_score from RowsTable"
else:
  sql_query3 = "select uid, min("+location_lists+") as dist_score from RowsTable group by uid"
  
dist_df1 = sqlContext.sql(sql_query3).cache()

mins2 = dist_df1.agg({"dist_score": "min"}).collect()[0][0]
maxs2 = dist_df1.agg({"dist_score": "max"}).collect()[0][0]

udf2 = udf(lambda s: rescale_score(s, mins2, maxs2, 1))
dist_df2 = dist_df1.withColumn("rescaled_dist_score", udf2(dist_df1.dist_score)).cache()
dist_df3 = dist_df2.orderBy(dist_df2.rescaled_dist_score.desc())

cond2 = [chosen_links.uid==dist_df3.uid]
dist_df4 = dist_df3.join(chosen_links, cond2).select(dist_df3.uid, dist_df3.rescaled_dist_score, chosen_links.url).cache()
dist_df4.show(n=5, truncate=False)


# Approach 3: tokens frequency
sql_query4 = "select uid, count(uid) as tokens_count from RowsTable group by uid"

tc_df1 = sqlContext.sql(sql_query4).cache()

mins3 = tc_df1.agg({"tokens_count": "min"}).collect()[0][0]
maxs3 = tc_df1.agg({"tokens_count": "max"}).collect()[0][0]

udf3 = udf(lambda s: rescale_score(s, mins3, maxs3))
tc_df2 = tc_df1.withColumn("rescaled_tokens_count", udf3(tc_df1.tokens_count)).cache()
tc_df3 = tc_df2.orderBy(tc_df2.rescaled_tokens_count.desc())

cond3 = [chosen_links.uid==tc_df3.uid]
tc_df4 = tc_df3.join(chosen_links, cond3).select(tc_df3.uid, tc_df3.rescaled_tokens_count, chosen_links.url).cache()
tc_df4.show(n=5, truncate=False)


# Approach 4: add weights to each score calculation method and use them together

cond1 = [loc_df4.uid == dist_df4.uid]
tmp_df1 = loc_df4.join(dist_df4, cond1).select(loc_df4.uid, loc_df4.url, loc_df4.rescaled_location_score, dist_df4.rescaled_dist_score)

cond2 = [tmp_df1.uid == tc_df4.uid]
tmp_df2 = tmp_df1.join(tc_df4, cond2)
.select(tmp_df1.uid, tmp_df1.url, tmp_df1.rescaled_location_score, tmp_df1.rescaled_dist_score,
        tc_df4.rescaled_tokens_count).cache()
        
tmp_df2.show(n=5)

w1 = 2
w2 = 3.5
w3 = 4
tmp_df3 = tmp_df2.select(tmp_df2.uid, tmp_df2.url, 
(tmp_df2.rescaled_location_score*w1+tmp_df2.rescaled_dist_score*w2+tmp_df2.rescaled_tokens_count*w3)
.alias("ranking")).cache()

result_df = tmp_df3.sort(tmp_df3.ranking.desc())
result_df.show(n=5, truncate=False)
