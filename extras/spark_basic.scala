import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


val conf = new SparkConf().setAppName(appName).setMaster(master)
new SparkContext(conf)


val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)

val distFile = sc.textFile("data.txt")


val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a, b) => a + b)
val counts = pairs.reduceByKey((a, b) => a + b)



val results = teenagers.collect() //fetching entire data into single machine (can cause memory issue)



transformations : 

Map(func)
Flatmap(func)
Filter(func)
mapPartitions(func) : 
mapPartitionsWithIndex(func) : 
Sample() 
union(otherDataset)
intersection(otherDataset)
distinct([numPartitions]))
groupByKey([numPartitions])
reduceByKey(func, [numPartitions])
aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])
sortByKey([ascending], [numPartitions])
join(otherDataset, [numPartitions])
cogroup(otherDataset, [numPartitions])
cartesian(otherDataset)
pipe(command, [envVars])
coalesce(numPartitions)
repartition(numPartitions)
repartitionAndSortWithinPartitions(partitioner)


Actions :
reduce(func) :  
collect() : 
count(),first(),take(n)
takeSample(withReplacement, num, [seed])
takeOrdered(n, [ordering])
saveAsTextFile(path)
saveAsSequenceFile(path) 
saveAsObjectFile(path) 
countByKey() : 
foreach(func) : 


val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar.value

val accum = sc.longAccumulator("My Accumulator")
sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
accum.value



