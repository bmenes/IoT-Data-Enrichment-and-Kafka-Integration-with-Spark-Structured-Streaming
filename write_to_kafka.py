from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, split, to_timestamp, year, month, dayofweek, concat_ws

spark = (SparkSession.builder
.appName("Read From Kafka")
.getOrCreate())

spark.sparkContext.setLogLevel('ERROR')


# Read data from kafka source
lines = (spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "kafka:9092")
.option("subscribe", "iot-temp-raw")
.load())

# Operation
lines2=lines.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","topic","partition","offset","timestamp")

split_col=split(col("value"),",")

lines_with_date=lines2.withColumn("noted_date",split_col.getItem(2))

lines_with_timestamp=lines_with_date.withColumn("noted_date_timestamp",to_timestamp(col("noted_date"),"dd-MM-yyyy HH:mm"))


lines_with_extra_columns = lines_with_timestamp.withColumn("year", year(col("noted_date_timestamp"))) \
    .withColumn("month", month(col("noted_date_timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("noted_date_timestamp"))) 



lines_final = lines_with_extra_columns.withColumn(
    "value",
    concat_ws(",", col("key"), col("value"), col("year"), col("month"), col("day_of_week"))
).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")


checkpoint_dir = "file:///tmp/streaming/write_to_kafka"

streamingQuery = (lines_final
.writeStream
.format("kafka")
.outputMode("append")
.trigger(processingTime="2 second")
.option("checkpointLocation", checkpoint_dir)
.option("kafka.bootstrap.servers", "kafka:9092")
.option("topic", "iot-temp-enriched")
.start())

# start streaming
streamingQuery.awaitTermination()