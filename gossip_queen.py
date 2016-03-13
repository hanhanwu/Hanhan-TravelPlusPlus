import twitter
import json
import oauth2 as oauth
import flickrapi
from datetime import date
from instagram.client import InstagramAPI
from nltk.stem.porter import *
from sets import Set
import operator
import flickrapi
from pyspark.sql import Row
from pyspark.sql.functions import udf, concat_ws
from pyspark.sql.types import *
from urllib2 import urlopen
import re


Twitter_CONSUMER_KEY = "[your twitter customer key]"
Twitter_CONSUMER_SECRET = [your twitter customer secret]"

Twitter_ACCESS_KEY = "[your twitter access key]"
Twitter_ACCESS_SECRET = "[your twitter access secret]"

auth = twitter.oauth.OAuth(Twitter_ACCESS_KEY, Twitter_ACCESS_SECRET, Twitter_CONSUMER_KEY, Twitter_CONSUMER_SECRET)
twitter_api = twitter.Twitter(auth=auth)


# Part 1: world wide trends, tags and urls
WORLD_WOE_ID = 1
world_trends = twitter_api.trends.place(_id=WORLD_WOE_ID)
world_trends_set = set([(trend['name'], trend['url'])
                        for trend in world_trends[0]['trends']])
                        
country_woeids = dict([('US', 23424977), ('India', 2295420), 
                      ('Canada', 23424775), ('Australia', 23424748)])
country_popular_trends = {}

for k,v in country_woeids.items():
  country_trends = twitter_api.trends.place(_id=v)
  country_trends_set = set([(trend['name'], trend['url']) for trend in country_trends[0]['trends']])
  popular_trends = list(world_trends_set.intersection(country_trends_set))
  country_popular_trends[k] = {}
  country_popular_trends[k]['woeid'] = v
  country_popular_trends[k]['tag'] = [trend[0] for trend in popular_trends]
  country_popular_trends[k]['url'] = [trend[1] for trend in popular_trends]

for k,v in country_popular_trends.items():
  print k
  print v
  print
  

# Part 2: real time trends
q = 'travel' 
count = 200

search_results = twitter_api.search.tweets(q=q, count=count)
statuses = search_results['statuses']

for _ in range(5):
    print "Length of statuses", len(statuses)
    try:
        next_results = search_results['search_metadata']['next_results']
    except KeyError, e: 
        break
        
    kwargs = dict([ kv.split('=') for kv in next_results[1:].split("&") ])
    
    search_results = twitter_api.search.tweets(**kwargs)
    statuses += search_results['statuses']
    
              
def clean_tweet(status):
  count=int(status['retweet_count'])
  screen_name=status['retweeted_status']['user']['screen_name']
  cleaned_text = re.split("RT\s@[\w\W]+:\s",status['text'])[1]
  first_idx = cleaned_text.find('https://')
  if first_idx != -1:
    if first_idx == 0:
      tweet_text = screen_name
    else:
      tweet_text = cleaned_text[0:first_idx]
    urls = ''
    us = [u for u in cleaned_text.split() if u.startswith('https://')]
    for u in us:
      urls+=u+',  '
  else:
    tweet_text = cleaned_text
    urls = ''
  location=status['user']['location']
  return (count, screen_name, tweet_text, urls, location)
  
retweets = [clean_tweet(status) for status in statuses if status.has_key('retweeted_status')]

retweets_rows = [Row(count=retweet[0], 
             tweet_title=retweet[1],
             text=retweet[2],
             urls=retweet[3],
             location=retweet[4]) 
            for retweet in retweets]

ww_df = sc.parallelize(retweets_rows).toDF()
ww_df.groupBy('text').agg({'count': 'sum'}).alias('count')
ww_df = ww_df.sort("count", ascending=False)

today = date.today()
today_str = str(today.year)+'_'+str(today.month)+'_'+str(today.day)
print today_str
filename = '/FileStore/ww_trends/ww_trends_'+today_str+'.csv'
ww_df.coalesce(1).write.format("com.databricks.spark.csv").save(filename)


# Part 3: hot tourism spots

Instagram_CLIENT_ID = "[your Instagram client ID]"
Instagram_CLIENT_SECRET = "[your Instagram client secret]"
Instagram_ACCESS_TOKEN = "[your Instagram access token]"

Instagram_api = InstagramAPI(access_token=Instagram_ACCESS_TOKEN,
                      client_id=Instagram_CLIENT_ID,
                      client_secret=Instagram_CLIENT_SECRET)
q = 'travel'
stemmer = PorterStemmer()
count=100

related_tags = [stemmer.stem(t.name) for t in Instagram_api.tag_search(q, count)[0]]
tags_count = dict((i, related_tags.count(i)) for i in related_tags)
sorted_tags = sorted(tags_count.items(), key=operator.itemgetter(1), reverse=True)[:5]
print sorted_tags

tags = ''
for tg in sorted_tags:
  tags += tg[0]+','
  
print tags


def get_location(pid):
  try:
    t = flickr.photos.geo.getLocation(photo_id=pid)
    return t
  except:
    return None

flickr_API_KEY = '[your flickr api key]'
flickr_API_SECRET = '[your flickr api secret]'
flickr = flickrapi.FlickrAPI(flickr_API_KEY, flickr_API_SECRET)

today = date.today()
photo_list= flickr.photos_search(tags=tags, format='json')
photo_list_json = json.loads(photo_list)
pids = [e['id'] for e in photo_list_json['photos']['photo']]

rs = []
for pid in pids:
  t = get_location(pid)
  if t != None:
    lat = t[0][0].get('latitude')
    lng = t[0][0].get('longitude')
    rs.append(Row(id=pid, latitude=lat, longitude=lng, post_date=today))
print len(rs)
    
flickr_location_df = sc.parallelize(rs).toDF().cache()
flickr_location_df.show()


def get_level1_locations(lat_lng):
    elems = lat_lng.split(',')
    url = "http://maps.googleapis.com/maps/api/geocode/json?"
    url += "latlng=%s,%s&sensor=false" % (float(elems[0]), float(elems[1]))
    v = urlopen(url).read()
    j = json.loads(v)
    if len(j['results'])>0:
      components = j['results'][0]['address_components']
      country = town = city = None
      for c in components:
          if "country" in c['types']:
            country = c['long_name']
          if "locality" in c['types']:
            city = c['long_name']
          if "administrative_area_level_1" in c['types']:
            state = c['long_name']
      return (city, state, country)
    return [None]
    
get_level1_locations_udf = udf(lambda lat_lng: get_level1_locations(lat_lng), ArrayType(StringType()))
new_df1 = flickr_location_df.withColumn("level1_locations", get_level1_locations_udf(concat_ws(',', flickr_location_df.latitude, flickr_location_df.longitude).alias('lat_lng'))).cache()
new_df1.show(truncate=False)


def get_level2_locations(lat_lng):
  elems = lat_lng.split(',')
  search_results = Instagram_api.location_search(q, count, float(elems[0]), float(elems[1]))
  locations = Set([los.name.split(',')[0] for los in search_results])
  return list(locations)

get_level2_locations_udf = udf(lambda lat_lng: get_level2_locations(lat_lng), ArrayType(StringType()))
new_df2 = new_df1.withColumn("level2_locations", get_level2_locations_udf(concat_ws(',', new_df1.latitude, new_df1.longitude).alias('lat_lng'))).cache()
new_df2.show(truncate=False)
