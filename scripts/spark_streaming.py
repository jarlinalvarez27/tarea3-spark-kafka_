import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, expr, timestamp_millis
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

KAFKA_BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC           = os.getenv("TOPIC", "eventos_sensores")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR", "../output/chk")
OUTPUT_DIR      = os.getenv("OUTPUT_DIR", "../output/stream")

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("ts",        LongType()),   # epoch millis
    StructField("temp",      DoubleType()),
    StructField("hum",       DoubleType()),
    StructField("ok",        BooleanType())
])

spark = SparkSession.builder.appName("T3-Streaming-Kafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Leer de Kafka
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load())

# 2) Parsear JSON
events = (raw.selectExpr("CAST(value AS STRING) AS json")
              .select(from_json(col("json"), schema).alias("e"))
              .select("e.*")
              .withColumn("tstamp", timestamp_millis(col("ts"))))

# 3) Agregación por ventana 10s y sensor
agg = (events
    .withWatermark("tstamp", "1 minute")
    .groupBy(window(col("tstamp"), "10 seconds"), col("sensor_id"))
    .agg(
        expr("count(*) as eventos"),
        expr("avg(temp) as avg_temp"),
        expr("avg(hum)  as avg_hum"),
        expr("sum(CASE WHEN ok THEN 1 ELSE 0 END) as ok_count")
    )
    .select(
        col("window.start").alias("win_start"),
        col("window.end").alias("win_end"),
        "sensor_id","eventos","avg_temp","avg_hum","ok_count"
    )
)

# 4) Escribir a consola (debug) y a parquet (evidencias)
q_console = (agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start())

q_parquet = (agg.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .option("path", OUTPUT_DIR)
    .start())

q_console.awaitTermination()
q_parquet.awaitTermination()
