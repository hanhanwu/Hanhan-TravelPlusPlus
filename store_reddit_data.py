# cell 1
import nltk
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('maxent_treebank_pos_tagger')
nltk.download('averaged_perceptron_tagger')


# cell 2
import praw
from nltk.stem.porter import *
import re

user_agent = ("travel++ 1.0")

r = praw.Reddit(user_agent = user_agent)
subreddit_lst = ["backpacking", "travel", "shoestring", "travelers"]
reddit_prefix = "https://www.reddit.com/r/"
limit_num = 1000
stemmer = PorterStemmer()
stopwords = nltk.corpus.stopwords.words('english')

posts = {}

for st in subreddit_lst:
  subreddit = r.get_subreddit(st)
  url_prefix = reddit_prefix+st+"/"
  for s in subreddit.get_hot(limit = limit_num):
    sid = s.id
    posts[sid] = {}
    posts[sid]["title"] = s.title
    posts[sid]["text"] = s.title+' '+s.selftext
    posts[sid]["score"] = s.score
    posts[sid]["comments_num"] = s.num_comments
    posts[sid]["url"] = url_prefix+sid
print len(posts.keys())


# cell 3
'''
create tables: 
PostList(post_id, url), EntityList(entity_id, entity), WordList(word_id, word), 
WordLocation(post_id, word_id, location), EntityLocation (post_id, entity_id)
''' 
from pyspark.sql import Row


def get_cleaned_tokens(all_text, post_id):
  sentences = [s.lower() for s in nltk.tokenize.sent_tokenize(all_text)]
  tokens = [w for s in sentences for w in s.split() if w not in stopwords]
  cleaned_tokens = [re.sub(r'[\W]+', r'', w) for w in tokens]
  return [(stemmer.stem(cleaned_tokens[i]), post_id, i) for i in range(len(cleaned_tokens)) if cleaned_tokens[i]!=""]


def get_NN_entities(all_text, post_id):
    sentences = nltk.tokenize.sent_tokenize(all_text)
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
    return [(entity, post_id) for entity in all_entities]
  
  
entity_lst = set()
entity_map = []
word_lst = set()
word_map = []
post_all = []

for k,v in posts.items():
  post_all.append(Row(post_id=k, title=v["title"], text=v["text"], score=v["score"], comments_num=v["comments_num"], url=v["url"]))
  entity_map.extend(get_NN_entities(v["text"], k))
  for t in entity_map:
    entity_lst.add(t[0])
  word_map.extend(get_cleaned_tokens(v["text"], k))
  for t in word_map:
    word_lst.add(t[0])
    
word_lst = list(word_lst)
entity_lst = list(entity_lst)


# cell 4
# Test: check lists
print len(entity_lst)
print entity_lst[0], entity_lst[10]
print 
print len(entity_map)
print entity_map[0], entity_map[1]
print
print len(word_lst)
print word_lst[0]
print
print len(word_map)
print word_map[0], word_map[1]
print
print len(post_all)
print post_all[0]


# cell 5
# generate dfs using different methods

post_all_df = sc.parallelize(post_all).toDF()
post_all_df.show(n=3)
print post_all_df.schema
print post_all_df.count()

entity_map_df = sc.parallelize(entity_map).map(lambda (e, pid): Row(entity=e, post_id=pid)).toDF()
entity_map_df.show(n=3)
print entity_map_df.count()

entity_lst_df = sc.parallelize(entity_lst).zipWithIndex().map(lambda (e, idx): Row(entity=e, entity_id=idx)).toDF()
entity_lst_df.show(n=3)
print entity_lst_df.count()

cond1 = [entity_map_df.entity == entity_lst_df.entity]
entity_location_df = entity_map_df.join(entity_lst_df, cond1).select(entity_map_df.post_id, entity_lst_df.entity_id)
entity_location_df.show(n=5)
print entity_location_df.count()

word_lst_df = sc.parallelize(word_lst).zipWithIndex().map(lambda (w, idx): Row(word=w, word_id=idx)).toDF()
word_lst_df.show(n=3)
print word_lst_df.count()

word_map_df = sc.parallelize(word_map).map(lambda (w, pid, l): Row(word=w, post_id=pid, location=l)).toDF()
word_map_df.show(n=3)
print word_map_df.count()

cond2 = [word_map_df.word == word_lst_df.word]
word_location_df = word_map_df.join(word_lst_df, cond2).select(word_lst_df.word_id, word_map_df.post_id, word_map_df.location)
word_location_df.show(n=5)
print word_location_df.count()

# generate parquet to create tables, so that the data will be stored there. Uisng word_map as an example here
print word_map_df.schema
filename = '/FileStore/reddit/word_mapx'
word_map_df.coalesce(1).write.parquet(filename)

# cell 6
%sql
CREATE TABLE if not exists WordMap
USING org.apache.spark.sql.parquet
OPTIONS (
  path '/FileStore/reddit/word_map1'
);

select count(*) as count from WordMap;
select * from WordMap limit 3;
