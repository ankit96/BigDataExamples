import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("StructuredNetworkWordCount")
  .getOrCreate()
  
import spark.implicits._

val linesDF = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()


linesDF.isStreaming    // Returns True for DataFrames that have streaming sources

linesDF.printSchema

// Split the lines into words
val words = linesDF.as[String].flatMap(_.split(" "))

// Generate running word count
val wordCounts = words.groupBy("value").count()


df.createOrReplaceTempView("updates")
spark.sql("select count(*) from updates")  // returns another streaming DF


// Start running the query that prints the running counts to the console
val query = wordCounts.writeStream
  .outputMode("complete")
  .format("console")
  .start()

query.awaitTermination()




data obtained from input is DataFrame ,to convert it into dataset : 
case class DeviceData(device: String, deviceType: String, signal: Double, time: DateTime)
val ds: Dataset[DeviceData] = df.as[DeviceData]


val words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

// Group the data by window and word and compute the count of each group
val windowedCounts = words
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"),
        //10 min window with 5 min sliding window interval
        $"word")
    .count()




val staticDf = spark.read. ...
val streamingDf = spark.readStream. ...

streamingDf.join(staticDf, "type")          // inner equi-join with a static DF
streamingDf.join(staticDf, "type", "right_join")  // right outer join with a static DF  




val impressions = spark.readStream. ...
val clicks = spark.readStream. ...

// Apply watermarks on event-time columns //optional for inner joins
val impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours") 
val clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")

// Join with event-time constraints
impressionsWithWatermark.join(
  clicksWithWatermark,
  expr("""
    clickAdId = impressionAdId AND
    clickTime >= impressionTime AND
    clickTime <= impressionTime + interval 1 hour
    """)
)


// Without watermark using guid column
streamingDf.dropDuplicates("guid")



writeStream
    .format("parquet")        // can be "orc", "json", "csv", etc.
    .option("path", "path/to/destination/dir")
    .start() //you have to call start() to actually start the execution of the query


writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("topic", "updates")
    .start()


writeStream
    .foreach(...)
    .start()


import org.apache.spark.sql.streaming.Trigger
// ProcessingTime trigger with two-seconds micro-batch interval
df.writeStream
  .format("console")
  .trigger(Trigger.ProcessingTime("2 seconds"))
  .start()



