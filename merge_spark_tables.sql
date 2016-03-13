-- So far tables generated from .csv files (csv tables) only support INSERT OVERWRITE on Spark Cluster (cloud)
-- I am using code here to merge 2 tables into a new table
-- Using Spark Notebook, just need to start with "%sql" in the cell, so that it will know here's sql query


-- cell 1: create a new table

%sql -- CSV tables only support INSERT OVERWRITE for now
drop table if exists t;
create table t as 
select * from t1


-- cell 2

%sql show tables


-- cell 3: merge tables into the new table

%sql
insert overwrite table t
select * from t1
 union all
select * from t2


-- cell 4: 

%sql
select count(*) as row_count from t


