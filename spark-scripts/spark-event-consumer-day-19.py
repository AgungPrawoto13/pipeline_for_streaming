import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import sum as pyspark_sum
from pyspark.sql.functions import from_json, window, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, TimestampType


dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

json_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_address", StringType()),
    StructField("customer_country", StringType()),
    StructField("furniture", StringType()),
    StructField("color", StringType()),
    StructField("price", IntegerType()),
    StructField("timestamp", TimestampType())
])

stream_data = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

stream_df_json = stream_data.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
)

stream_df = stream_df_json.select("data.*")

(
    stream_df.writeStream.format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
)

# (
#     stream_data.selectExpr("CAST(value AS STRING)")
#     .writeStream.format("console")
#     .outputMode("append")
#     .start()
#     .awaitTermination()
# )

#aggregated_df = stream_df.groupBy(window(col('timestamp'), "10 minutes")).agg({
#["order_id","customer_id","customer_name","customer_address","customer_country","furniture","color","price"]: "sum"})

# aggregated_df = (
#     stream_df.groupBy(window(col('timestamp'), "10 minutes"))
#     .agg(
#         pyspark_sum("order_id"),
#         pyspark_sum("customer_id"),
#         pyspark_sum("customer_name"),
#         pyspark_sum("customer_address"),
#         pyspark_sum("customer_country"),
#         pyspark_sum("furniture"),
#         pyspark_sum("color"),
#         pyspark_sum("price")
#     )
# )

# (
#     aggregated_df.selectExpr("CAST(value AS STRING)")
#     .writeStream.format("console")
#     .outputMode("append")
#     .start()
#     .awaitTermination()
# )


# query = (
#     aggregated_df.writeStream.format("console")
#     .outputMode("complete")  # OutputMode sesuai kebutuhan kamu (complete, append, atau update)
#     .start()
#     .awaitTermination()
# )