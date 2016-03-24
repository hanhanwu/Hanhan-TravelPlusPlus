'''
Once a user has given a search query, I am using NLP techniques and 3 level methods to find close Reddit posts 
    1. Level 1: Using 
'''


# cell 1: allow foreign languages in the query
import sys  

reload(sys)  
sys.setdefaultencoding('utf8')


# cell 2
import nltk
from nltk.stem.porter import *
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('maxent_treebank_pos_tagger')
nltk.download('averaged_perceptron_tagger')

stemmer = PorterStemmer()
stopwords = nltk.corpus.stopwords.words('english')


# cell 3: process user query, get cleaned unique tokens and entities (the query may contain typo for testing purpose)
q1 = "Could you give me some advice for post college trip? Like a trip to Europe."
q2 = "advice for post colleague trip, any suggestions, or advice, haha?"   
q3 = "Advice for Europe trip?"

def get_cleaned_tokens(query):
  sentences = [s.lower() for s in nltk.tokenize.sent_tokenize(query)]
  tokens = [w for s in sentences for w in s.split() if w not in stopwords]
  cleaned_tokens = [re.sub(r'[\W]+', r'', w) for w in tokens]
  return list(set([(stemmer.stem(w)) for w in cleaned_tokens if w!=""]))


def get_NN_entities(query):
    sentences = nltk.tokenize.sent_tokenize(query)
    token_sets = [nltk.tokenize.word_tokenize(s) for s in sentences]
    pos_tagged_token_sets = [nltk.pos_tag(t) for t in token_sets]
    pos_tagged_tokens = [t for v in pos_tagged_token_sets for t in v]
    
    all_entities = []
    previous_pos = None
    current_entities = []
    for (entity, pos) in pos_tagged_tokens:
        if previous_pos == pos and pos.startswith('NN'):
            current_entities.append(entity.lower())
        elif pos.startswith('NN'):
            if current_entities != []:
                all_entities.append(' '.join(current_entities))
            current_entities = [entity.lower()]
        previous_pos = pos
    return list(set([entity for entity in all_entities]))
  
q1_tokens = get_cleaned_tokens(q1)
q2_tokens = get_cleaned_tokens(q2)
q3_tokens = get_cleaned_tokens(q3)
q1_entities = get_NN_entities(q1)
q2_entities = get_NN_entities(q2)
q3_entities = get_NN_entities(q3)

print q1_tokens
print q2_tokens
print q3_tokens
print q1_entities
print q2_entities
print q3_entities


# cell 4: Level 1 Method - find matched posts using Levenshtein Distance to compare the query and the Reddit post title

from Levenshtein import distance
from pyspark.sql.types import *
from pyspark.sql.functions import udf, concat_ws

post_all_df = sqlContext.sql("""
select * from postall
""").cache()

q = q1

levenshtein_udf = udf(lambda t: distance(str(t.encode("utf-8")), str(q)), IntegerType())
levenshtein_df = post_all_df.withColumn("levenshtein_score", levenshtein_udf(post_all_df.title)).coalesce(1)
levenshtein_df = levenshtein_df.sort(levenshtein_df.levenshtein_score.desc())

levenshtein_df.show(n=5)


# cell 5: Level 2 Method - calculate scores using entities, the count of each post is the score, then rescale the score into [0,1]

q_entities = q1_entities

field_lst = "entity='%s'" % q_entities[0]
for i in range(1,len(q_entities)):
  field_lst += " or entity='%s'" % q_entities[i]
  
sql_query1 = "select entity_id from entitylist where " + field_lst
entity_ids = [e[0] for e in sqlContext.sql(sql_query1).collect()]

sql_query2 = "select count(t0.post_id) as count, t0.post_id from "
count = 0
tables = ""
where_clause = ""
for eid in entity_ids:
  if count==0:
    tables = "entitylocation t0"
    where_clause = "t0.entity_id=%d" % eid
  else:
    tables += ", entitylocation t%d" % count
    where_clause += " and t%d.entity_id=%d" % (count, eid)
  count += 1
  
for i in range(count-1):
  where_clause += " and t%d.post_id=t%d.post_id" % (i, i+1)
sql_query2 += tables+" where "+where_clause+" group by t0.post_id order by count desc limit 5"

def add_prefix(up, pid):
  return up+pid

matched_entities_df = sqlContext.sql(sql_query2)

# even if the url prefix is wrong, reddit will direst to the right url based on post_id, so random choose a url prefix here is fine
url_prefix = "https://www.reddit.com/r/travel/"
add_prefix_udf = udf(lambda pid: add_prefix(url_prefix, pid))
results = matched_entities_df.withColumn("url", add_prefix_udf(matched_entities_df.post_id))
results.show(truncate=False)


# cell 6: Level 3 Method - Calculate scores using words (tokens)
# calculate different scores, find top 5 matched posts with scores, set score threshold

# Get all the posts that contain all the query tokens
q_tokens = q3_tokens

field_lst = "word='%s'" % q_tokens[0]
for i in range(1,len(q_tokens)):
  field_lst += " or word='%s'" % q_tokens[i]
  
sql_query1 = "select word_id from wordlist where " + field_lst
word_ids = [w[0] for w in sqlContext.sql(sql_query1).collect()]


fields = "t0.post_id"
tables = ""
where_clause = ""
count = 0

for wid in word_ids:
  fields += ", t%d.location as location%d" % (count, count)
  if count==0:
    tables = "wordlocation t0"
    where_clause = "t0.word_id=%d" % wid
  else:
    tables += ", wordlocation t%d" % count
    where_clause += " and t%d.word_id=%d" % (count, wid)
  count += 1
  
for i in range(count-1):
  where_clause += " and t%d.post_id=t%d.post_id" % (i, i+1)
sql_query2 = "select %s from %s where %s" % (fields, tables, where_clause)

rows_df = sqlContext.sql(sql_query2).cache()

# rescale score, so that all the score in [0,1] and all have "higher score means better"
def rescale_score(score, mins, maxs, small_is_better=0):
  v = 0.00001  # avoid to be divided by 0
  if small_is_better:
    new_score = float(mins)/max(score, v)
  else:
    if maxs == 0: maxs = v
    new_score = float(score)/maxs
  return new_score


# Approach 1: Calculate scores based on words locations
rows_df.registerTempTable("RowsTable")
location_lists = "location0"

for i in range(1, count):
  location_lists += "+location%d" % i
  
sql_query3 = "select post_id, min("+location_lists+") as location_score from RowsTable group by post_id"
loc_df1 = sqlContext.sql(sql_query3).cache()

mins1 = loc_df1.agg({"location_score": "min"}).collect()[0][0]
maxs1 = loc_df1.agg({"location_score": "max"}).collect()[0][0]

udf1 = udf(lambda s: rescale_score(s, mins1, maxs1, 1))
loc_df2 = loc_df1.withColumn("rescaled_location_score", udf1(loc_df1.location_score)).withColumn("url", add_prefix_udf(loc_df1.post_id)).cache()
loc_df3 = loc_df2.orderBy(loc_df2.rescaled_location_score.desc()).cache()

loc_df3.show(n=5)
loc_df3.select(loc_df3.url).show(n=5, truncate=False)

# Approach 2: Calculate scores based on words distance
location_lists = "abs(location0-location1)"

if count>2:
  for i in range(1,count-1):
    location_lists += "+abs(location%d-location%d)" % (i,i+1)
if count<2:
  sql_query4 = "select post_id, 1.0 as dist_score from RowsTable"
else:
  sql_query4 = "select post_id, min("+location_lists+") as dist_score from RowsTable group by post_id"
  
dist_df1 = sqlContext.sql(sql_query4).cache()

mins2 = dist_df1.agg({"dist_score": "min"}).collect()[0][0]
maxs2 = dist_df1.agg({"dist_score": "max"}).collect()[0][0]

udf2 = udf(lambda s: rescale_score(s, mins2, maxs2, 1))
dist_df2 = dist_df1.withColumn("rescaled_dist_score", udf2(dist_df1.dist_score)).withColumn("url", add_prefix_udf(dist_df1.post_id)).cache()
dist_df3 = dist_df2.orderBy(dist_df2.rescaled_dist_score.desc()).cache()

dist_df3.show(n=5)
dist_df3.select(dist_df3.url).show(n=5, truncate=False)

# Approach 3: tokens frequency
sql_query5 = "select post_id, count(post_id) as tokens_count from RowsTable group by post_id"

tc_df1 = sqlContext.sql(sql_query5).cache()

mins3 = tc_df1.agg({"tokens_count": "min"}).collect()[0][0]
maxs3 = tc_df1.agg({"tokens_count": "max"}).collect()[0][0]

udf3 = udf(lambda s: rescale_score(s, mins3, maxs3))
tc_df2 = tc_df1.withColumn("rescaled_tokens_count", udf3(tc_df1.tokens_count)).withColumn("url", add_prefix_udf(tc_df1.post_id)).cache()
tc_df3 = tc_df2.orderBy(tc_df2.rescaled_tokens_count.desc()).cache()

tc_df3.show(n=5)
tc_df3.select(tc_df3.url).show(n=5, truncate=False)
