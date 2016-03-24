# Hanhan-TravelPlusPlus
Using machine learning, data mining, data visualization techniques

* Project Presentation Slides - travel++ slides.pdf

* Using Spark Cloud
  * The code is written and tested in Spark Cluster (Cloud). Spark Cluster is super awesome! It has Scala, Python, R, Spark SQL Notebook and each Notebook will work for all these languages in a very convenient and fast way! Each day I just need to open my browser and work on the cluster. The Notebook just functions like IPython Notebook, each cell will show output. It is also very fast to attach libraries to the cluster. There are much more advantages about using Spark Cluster, I will write them and what to note in the following implemenation detials.

* Main Features
  * Gossip Queen - Real time world wide trends, hot tourism spots recommendation absed on travelers movements
  * Dr.Q - Automatic answers for traveling questions
  * May Attentive - Personalized Visualized Map
  
* Gossip Queen - Real Time Hottest Trends world wide
  1. World Wide Hottest Trends in different countries - The hot trends in this part may not be real time, the origional data generated from Twitter trends API, I extracted the hashtag and the url from the data. The trends got here may not be real time but still the newest within 2 days! But you may notice that, here I am using limited amount of countries to extract the data and I am not using Spark RDD or DataFrame to do parallized requests. Because of the limitation of Twitter trends api. If I am using parallized requests or do more country trend request, Trwitter will send 429 HTTP error which means too many requests. But by doing the intersection of world-wide trends with each country trends, the trends showing here are newest popular country trends world-wide! This is the just the appetizer I am showing :)
  2. Real Time Trends - The data source is twitter search API with "travel" as the search key words. I have cleaned the data. The tweets are sorted by retweet count in descending order. Origionally, I was using Python pretty table, but it bacame pretty slow when there were more tweets to deal with, especially when there is sorting. However, using Spark DataFrame became much faster, it will easier to do sorting, or other Spark sql query in the future.

    Sometimes, DataFrame show() cannot really show the generated df on Spark Clsuter UI, so here I have to save the output as .csv file. When using these .csv file to create csv tables, foreign languages (not English) will create error in Spark sql queries. There is a way to use filter out all the foreign languages using PyEnchant, but it need enchant C library installed in the OS, this is not possible for me to do in Spark Cluster, so even though I will save the data in Spark Tables, I will still use df to do the queries.
      
    As you will see, in the code, I am saving the DataFrame as parquet instead of .csv. This is because saving as .csv file will create some error, for example some columns in a row will be removed while other columns in the row will be move left and when creating the tables, this will generate errors because of the inconsistent data type in a column. Saving DataFrame as Parquet will avoid this problem, and it is easier to write the code to create tables (check parquet_to_table.sql), no longer need to manually create tables. But because of this, parquet needs you to define the table schema in the code otherwise it cannot convert the data type to the right one.
      
  3. Hot tourism sports - This is my favorite part in Gossip Queen. It is using a new way to recommend hot tourism spots. Instead of using existing tourist spot lists, this feature presents the real trends at current time by counting and sorting different posts in the same latitude+longitude. It also includes level_1 and level_2 locations, which means it recomends different spots in the same small area. For example, several people has visited the same town in California, but that maybe the Ghost Town, or China Town, or Vingine Lake. These 3 palces all share similar latitude and longtitude. People travel to the same town for different purpose, this is very interesting. By doing frequency analysis, I can do tourism spots recommendation. The detailed implementation looks tricky but interesting, sicne it overcomes the shortage of different APIs and in fact takes advantage of these shortages :)

    Here's how am I handling this intresting problem: The Flickr photo API will return photo ids by giving a search key word like "travel", by using the ids, the latitude and the longtide of the photo can also be found. But you cannot get any relative tags similar to "travel". The Instagram API will return relative tages by just giving "travel" as the key word, but the API is funnny that it needs you to give latitude and the longtitude when search for photos... However, once you give it the latitude and the longtitude, it returns the photos posted in that area with detailed location info. Just like my example, Gost Town or China Town or Vingine Lake in that area. But most of the location names do not indicate the city/State/country, so I am using Google api here to get the city, State and the country. But Google API has request limitation, once I have reached to the rate limit, it retuns me null.

Note: I am using DataFame and udf for the 2nd and the 3rd parts of Gossip Queen, since it makes the calculation mush faster because of parallized calculation and Spark awesome architecture. However, it is better to cache() the generated dataframe if you need to use it later, otherwise running the code in Spark python Notebook using different cells, you may get "lost task" error.
And in many cases, it is really good to run code in different cells, since it stores some previous generated output which will be used later. You don't need to re-run previous code again, which saves much time. But if you have dettached your Cluster, you have to re-run the code from the begining. And click "Run All" is not a good choice since Spark will run all the cells together in stead of running from the top to the bottom.


 * Dr.Q
  * This feature will answer user traveling questions automatically.
  * It will firstly find relative posts from stored Reddit posts and recommend to the user - Part1
  * If Reddit data cannot provide results that are strong enough, it will search wiki links to provide references - Part2
  * store_reddit_data.py - get data from Reddit travel subreddits, store them into different tables. It took me longer time to design the database structure, thinking which database is better. In this case, I choose Spark Table since using Spark Notebook on its cluster still efficient and writing Saprk sql query is convenient. Just need to think how to use DataFrame, RDD, Sparl Tables and how can I apply my later algorithms on these tables.
  * Find matched Reddit posts based on user query - match_reddit_posts.py
   1. 3 levels matchong methods.
   2. The Reddit data comes from those stored tables 
   3. Level 1 Method: find matched posts using Levenshtein Distance to compare the query and the Reddit post title. This method works better when the length of user query and the length of reddit post are similar.
   4. Level 2 Method: get NN entities from usey query and find reddit posts that share more same entities. 
   5. Level 3 Method: use tokens from user query and reddit posts, calculate scores based on token frequency, token location, token distances. In the code, using Spark DataFrame udf has made the code simpler and more efficient.


* Create tables in Spark Cluster - daily_flickr_photos.py
  * Since Flickr API is slow in searching photos, and daily returns may have very few photos with latitude and longitude info. In case on the presebtation day, there is not too much data, I am saving daily data into tables.
  * The code convert the generated DataFrame into .csv and save it on Spark Cluster. Then just create Table by choosing "DBFS" as the data source, choosing the data from Spark FileStore folder, changing table schema, table name, column names if needed.
  * So far, I am using Spark beta environment, ti shows cannot insert new data into an existing table, so I have to create daily table.

* Merger tables on Spark cluster - merge_spark_tables.sql
 * The tables generated from .csv file in the previous step are called csv tables.
 * So far, Spark only supports INSERT OVERWRITE for csv tables, therefore the code is working on merging tables into 1.

* Generate table through parquet file - parquet_to_table.sql
 * Convert DataFrame to parquet will avoid errors that may be created in .csv.
 * It is easier to write the code to create tables using parquet.
 * Parquet tables cannot recognize all the data types right, you may need to define the schema in the DataFrame is necessary.
