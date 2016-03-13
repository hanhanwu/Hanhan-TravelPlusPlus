-- So far tables generated from .csv files (csv tables) only support INSERT OVERWRITE on Spark Cluster (cloud)
-- I am using code here to merge 2 tables into a new table
-- Using Spark Notebook, just need to start with "%sql" in the cell, so that it will know here's sql query


-- cell 1: create a new table

%sql -- CSV tables only support INSERT OVERWRITE for now
drop table if exists t;
create table t as 
select * from t0


-- cell 2

%sql show tables


-- cell 3: merge tables into the new table

%sql
insert overwrite table t0
select * from t
 union all
select * from daily_table


-- cell 4: 

%sql -- t0 is the table saves all the data
select count(*) as row_count from t0


