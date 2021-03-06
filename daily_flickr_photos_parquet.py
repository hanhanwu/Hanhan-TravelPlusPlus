import flickrapi
from datetime import date
from instagram.client import InstagramAPI
from nltk.stem.porter import *
import operator

Instagram_CLIENT_ID = "[your clicne ID]"
Instagram_CLIENT_SECRET = "[your client secret]"
Instagram_ACCESS_TOKEN = "[your access token]"

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


import flickrapi
import json
from datetime import date, datetime
from pyspark.sql import Row

def get_location(pid):
  try:
    t = flickr.photos.geo.getLocation(photo_id=pid)
    return t
  except:
    return None

flickr_API_KEY = u'[your API key]'
flickr_API_SECRET = u'[your API secret]'
flickr = flickrapi.FlickrAPI(flickr_API_KEY, flickr_API_SECRET)

today = date.today()
weekday = datetime.now().strftime("%A")
pd = str(today)+" ("+weekday+")"
photo_list= flickr.photos_search(tags=tags, format='json')
photo_list_json = json.loads(photo_list)
pids = [e['id'] for e in photo_list_json['photos']['photo']]

rs = []
for pid in pids:
  t = get_location(pid)
  if t != None:
    lat = t[0][0].get('latitude')
    lng = t[0][0].get('longitude')
    rs.append(Row(id=pid, latitude=lat, longitude=lng, post_date=pd))
    
print len(rs)


flickr_location_df = sc.parallelize(rs).toDF().cache()
flickr_location_df.show()


today_str = str(today.year)+'_'+str(today.month)+'_'+str(today.day)
print today_str
filename = '/FileStore/flickr_photos/flickr_photo_'+today_str
flickr_location_df.coalesce(1).write.parquet(filename)


%sql
CREATE TABLE if not exists daily_table
USING org.apache.spark.sql.parquet
OPTIONS (
  path "/FileStore/flickr_photos/flickr_photo_[today date]"
);


%sql -- Spark tables only support INSERT OVERWRITE for now
drop table if exists flickr_photo0;
create table flickr_photo0 as 
select * from flickr_photo


%sql
insert overwrite table flickr_photo
select * from flickr_photo0
 union all
select * from daily_table


%sql
select * from flickr_photo 
order by post_date desc


%sql
drop table daily_table;
show tables;


%sql
select count(id) as count, post_date from all_flickr_photo
group by post_date
order by post_date
