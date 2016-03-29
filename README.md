# Hanhan-TravelPlusPlus
Using machine learning, data mining, data visualization techniques

* Tools - Spark Cluster (Databricks Cloud), Python, Spark sql
* travel++ slides.pdf - Project Proposal Presentation Slides


* Main Features
  * Gossip Queen - Real time world wide trends, hot tourism spots recommendation based on travelers movements
  * Dr.Q - Automatic answers for traveling questions
  * May Attentive - Personalized Visualized Map
  
* gossip_queen.py
 * part 1 - longer term world popular topics
 * part 2 - real time world popular topics
 * part 3 - recommend current hottest tourism spots based on social media posts
      
* Dr.Q
 * DrQ_store_reddit_data.py - store reddit data into tables, easy to do query match
 * DrQ_match_reddit_posts.py
   1. Level 1 matching method - find matched posts using Levenshtein Distance
   2. Level 2 Method - calculate matching scores using NN entities
   3. Level 3 Method - Calculate scores using words (tokens)
   4. Level 3 Method - Approach 1: Calculate scores based on words locations
   5. Level 3 Method - Approach 2: Calculate scores based on words distance
   6. Level 3 Method - Approach 3: tokens frequency
   7. Level 3 Method - Approach 4: combine all the above 3 approaches and set weight to each of them
   8. Accurate Output sample: 
   
     User Query = "Advice for Europe trip?"
     
     Top 5 returned Reddit posts: (https://www.reddit.com/r/travel/4b46hd, https://www.reddit.com/r/travel/4arqzu, https://www.reddit.com/r/travel/48uu2h, https://www.reddit.com/r/travel/4atds4, https://www.reddit.com/r/travel/2ltqv3)
   
 * DrQ_search_engine.py
   1. When the user query cannot get a high matching score in DrQ_match_reddit_posts.py, this code will find relative wiki pages, images to the user for reference
   2. Approach 1: Calculate scores based on words locations
   3. Approach 2: Calculate scores based on words distance
   4. Approach 3: tokens frequency
   5. Approach 4: combine all the above 3 approaches and set weight to each of them
   6. Accurate Output sample: 
   
     Query Tokens = [u'camera', u'travel', u'free']
     
     Top 5 returned Reddit posts: (https://en.wikipedia.org/wiki/Digital_cameras, https://en.wikipedia.org/wiki/The_Traveler_(novel), https://en.wikipedia.org/wiki/The_Traveler_(1974_film), https://en.wikipedia.org/wiki/Elevator, https://en.wikipedia.org/wiki/Guided_bus)


* daily_flickr_photos_csv.py - Create csv tables in Spark Cluster
* daily_flickr_photos_parquet.py - Create parquet tables in Spark Cluster, added bar chart to show daily photo posting trend
* dataframe_visualization.png - sample bar chart which shows daily photo posting trend, using Spark Clsuter is very convenient to create simple chart like this. Just write Saprk sql, then click the chart button.
* merge_spark_tables.sql - Merger tables on Spark cluster
* parquet_to_table.sql - Generate table through parquet file
* DetailedReadMe.txt - Detailed technical notes
