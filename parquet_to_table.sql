--  "/FileStore/current_hot_spots/hot_spots_2016_3_18" is my parquet file path in Spark Cluster
-- This code is Spark sql, it will create the table from this parquet

%sql
CREATE TABLE if not exists hot_spots_2016_3_18
USING org.apache.spark.sql.parquet
OPTIONS (
  path "/FileStore/current_hot_spots/hot_spots_2016_3_18"
);
select * from hot_spots_2016_3_18;
