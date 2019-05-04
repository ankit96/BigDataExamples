

val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")


val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

peopleDF.write.parquet("people.parquet")


val peopleDFCsv = spark.read.format("csv")
  .option("sep", ";")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("examples/src/main/resources/people.csv")


 usersDF.write.format("orc")
  .option("orc.bloom.filter.columns", "favorite_color")
  .option("orc.dictionary.key.threshold", "1.0")
  .save("users_with_options.orc")


 val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")


 peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
// Bucketing and sorting are applicable only to persistent tables

usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")


usersDF
  .write
  .partitionBy("favorite_color")
  .bucketBy(42, "name")
  .saveAsTable("users_partitioned_bucketed")



// Encoders for most common types are automatically provided by importing spark.implicits._
import spark.implicits._

parquetFileDF.createOrReplaceTempView("parquetFile")
val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
namesDF.map(attributes => "Name: " + attributes(0)).show()



// for hive 
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
case class Record(key: Int, value: String)

// warehouseLocation points to the default location for managed databases and tables
val warehouseLocation = new File("spark-warehouse").getAbsolutePath
val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._
import spark.sql

sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

sql("SELECT * FROM src").show()

sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
// Save DataFrame to the Hive managed table
val df = spark.table("src")
df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")



//jdbc reading : more then 1 methods

val jdbcDF = spark.read/write
  .format("jdbc")
  .option("url", "jdbc:postgresql:dbserver")
  .option("dbtable", "schema.tablename")
  .option("user", "username")
  .option("password", "password")
  .load()


//avro:
val usersDF = spark.read.format("avro").load("examples/src/main/resources/users.avro")
usersDF.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")


//broadcast join :
import org.apache.spark.sql.functions.broadcast
broadcast(spark.table("src")).join(spark.table("records"), "key").show()


//adding additional column for cumulative frequency (used in finding median)
val csumwindow = Window.orderBy("rating").rowsBetween(Long.MinValue, 0)

ratings_frequency.withColumn( "cum_frequency", sum(ratings_frequency("frequency")).over(csumwindow)  )
  .toDF("rating","frequency","cum_frequency")
  .createOrReplaceTempView("ratings_frequency")



 println(q.queryExecution.logical.numberedTreeString)