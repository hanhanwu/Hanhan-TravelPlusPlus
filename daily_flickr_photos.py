'''
How to save df as .csv and save it in Spark Cluster, 
so that the .csv can be imported into Spark Cluster Tables
'''

# cell 1
import flickrapi
from datetime import date
from instagram.client import InstagramAPI
from nltk.stem.porter import *
from sets import Set
import operator

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


# cell 2
import flickrapi
import json
from datetime import date
from pyspark.sql import Row

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
    
    
# cell 3, this will save time, since Flickr API is slow, generate df in another cell no need to call the API again
flickr_location_df = sc.parallelize(rs).toDF().cache()
flickr_location_df.show()


# cell 4, save df as .csv in Spark Cluster /FileStore folder
today_str = str(today.year)+'_'+str(today.month)+'_'+str(today.day)
filename = '/FileStore/flickr_photo_'+today_str+'.csv'
flickr_location_df.coalesce(1).write.format("com.databricks.spark.csv").save(filename)
