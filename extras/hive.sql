CREATE SCHEMA|DATABASE [IF NOT EXISTS]  userdb; 
SHOW DATABASES; 
DROP DATABASE IF EXISTS userdb; 


CREATE TABLE Sonoo(foo INT, bar STRING); 
CREATE TABLE HIVE_TABLE (foo INT, bar STRING) PARTITIONED BY (ds STRING);
CREATE [TEMPORARY ] [EXTERNAL] TABLE [IF NOT EXISTS] db_name table_name;
Show tables;
DESCRIBE (FORMATTED|EXTENDED) table;


ALTER TABLE Sonoo RENAME TO Kafka;  
ALTER TABLE Kafka ADD COLUMNS (col INT); 
ALTER TABLE HIVE_TABLE REPLACE COLUMNS (col2 INT, weight STRING, baz INT COMMENT 'baz replaces new_col1');
DROP TABLE test1;

SELECT [ALL | DISTINCT ] select_col, select_col 
FROM table 
WHERE where_condition 
[GROUP BY col_list] 
[HAVING having_con] 
[ORDER BY col_list][LIMIT number];


LOAD DATA [LOCAL] INPATH ‘filepath’ [OVERWRITE] INTO TABLE table_name;

CREATE VIEW [IF NOT EXISTS] view_name [(column_name [COMMENT column_comment], …) ]
[COMMENT table_comment]
AS SELECT …

Create INDEX < INDEX_NAME> ON TABLE < TABLE_NAME(column names)>

CREATE INDEX index_name
ON TABLE base_table_name (col_name, ...)
AS 'index.handler.class.name'
[WITH DEFERRED REBUILD]
[IDXPROPERTIES (property_name=property_value, ...)]
[IN TABLE index_table_name]
[PARTITIONED BY (col_name, ...)]
[
  [ ROW FORMAT ...] STORED AS ...
  | STORED BY ...
]
[LOCATION hdfs_path]
[TBLPROPERTIES (...)]


CREATE TABLE table_name 
PARTITIONED BY (partition1 data_type, partition2 data_type,….) 
CLUSTERED BY (column_name1, column_name2, …) 
SORTED BY (column_name [ASC|DESC], …)] 
INTO num_buckets BUCKETS;


partitoning example : 
CREATE TABLE table_tab1 (id INT, name STRING, dept STRING, yoj INT) PARTITIONED BY (year STRING);
LOAD DATA LOCAL INPATH tab1’/clientdata/2009/file2’OVERWRITE INTO TABLE studentTab PARTITION (year='2009');
LOAD DATA LOCAL INPATH tab1’/clientdata/2010/file3’OVERWRITE INTO TABLE studentTab PARTITION (year='2010');



INSERT OVERWRITE TABLE bucketed_user PARTITION (country)
      SELECT firstname,
               lastname,
               address,
              city,
              state,
               post,
               phone1,
               phone2,
               email,
               webweb,
              country   
        FROM temp_user;



SELECT c.ID, c.NAME, c.AGE, o.AMOUNT 
FROM CUSTOMERS c [LEFT|RIGHT|FULL OUTER]JOIN ORDERS o 
ON (c.ID = o.CUSTOMER_ID);

map join ((using distributed cache)

SELECT /*+ MAPJOIN(Product)*/ Product.*, Sales.*
FROM Sales
INNER JOIN Product ON Sales.ProductId = Product.ProductId;
/*+ MAPJOIN(Product)*/  used as hint for map join 

bucket map join 
 it is very important that the tables are created bucketed on the same join columns
set hive.optimize.bucketmapjoin=true;
select /*+ MAPJOIN(b2) */ b1.* from b1,b2 where b1.col0=b2.col0;


skew joins : https://medium.com/hotels-com-technology/skew-join-optimization-in-hive-b66a1f4cc6ba
-- need to make sure the skewed data is not directed to a single reducer 
-- done usign declaring a table skewed  : https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-SkewedTables
-- Skewed Table is a table which has skewed information.
-- List Bucketing Table is a skewed table. In addition, it tells Hive to use the list bucketing feature on the skewed table: create sub-directories for skewed values

sort merge bucket join


Using python mapper code in hive insert query :
add FILE weekday_mapper.py;

INSERT OVERWRITE TABLE u_data_new
SELECT
  TRANSFORM (userid, movieid, rating, unixtime)
  USING 'python weekday_mapper.py'
  AS (userid, movieid, rating, weekday)
FROM u_data;



Transactional Tables: Hive supports single-table transactions. Tables must be marked as transactional in order to support UPDATE and DELETE operations.
CREATE TRANSACTIONAL TABLE



