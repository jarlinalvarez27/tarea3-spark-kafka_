from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

spark = SparkSession.builder.appName("T3-Batch-from-Stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Lee las salidas del streaming
df = spark.read.parquet("../output/stream")

# df tiene columnas: win_start, win_end, sensor_id, eventos, avg_temp, avg_hum, ok_count
# Agregamos a nivel de sensor sobre todas las ventanas
agg = (df.groupBy("sensor_id")
         .agg(
            _sum("eventos").alias("total_eventos"),
            _avg("avg_temp").alias("avg_temp_global"),
            _avg("avg_hum").alias("avg_hum_global"),
            _sum("ok_count").alias("ok_total"),
            _count("*").alias("ventanas")
         )
      )

agg.coalesce(1).write.mode("overwrite").parquet("../output/batch/agg_desde_stream")

spark.stop()
