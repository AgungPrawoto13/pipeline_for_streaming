import pyspark
import os
#import psycopg2

from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import sum as pyspark_sum
from pyspark.sql.functions import from_json, window, col, count
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, TimestampType

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

postgre_user = os.getenv("POSTGRES_USER")
postgre_pass = os.getenv("POSTGRES_PASSWORD")
postgre_db = os.getenv("POSTGRES_DB")

#connection database postgre
# conn = psycopg2.connect(
#    database = postgre_db, user = postgre_user, password = postgre_pass, host = postgre_host, port= '5432'
# )

# cursor = conn.cursor()

#Creating Table Station_Status
# cursor.execute("DROP TABLE IF EXISTS report")
# sql ='''CREATE TABLE report(
#     order_id VARCHAR NOT NULL,
#     customer_id INT,
#     customer_name VARCHAR,
#     customer_address VARCHAR,
#     customer_country VARCHAR,
#     furniture VARCHAR,
#     color VARCHAR,
#     price INT, 
#     ts TIMESTAMP
# )'''
# cursor.execute(sql)
# conn.commit()

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark
    .SparkConf()
    .setAppName('DibimbingStreaming')
    .setMaster(spark_host)
    .set('spark.jars','../../bitnami/spark/jars/postgresql-42.5.2.jar'))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://localhost/{postgre_db}'
jdbc_properties = {
    'user': postgre_user,
    'password': postgre_pass,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

retail_df = spark.read.jdbc(
    jdbc_url,
    'public.report',
    properties=jdbc_properties
)

json_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_address", StringType()),
    StructField("customer_country", StringType()),
    StructField("furniture", StringType()),
    StructField("color", StringType()),
    StructField("price", IntegerType()),
    StructField("ts", TimestampType())
])

stream_data = (
    spark.readStream.format("kafka") 
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092") 
    .option("subscribe", "rioAJG") 
    .option("startingOffsets", "earliest") 
    .option("failOnDataLoss", "false") 
    .load() 
    .selectExpr("CAST(value AS STRING) as string").select(from_json("string", schema=json_schema).alias("data")).select("data.*") 
    # .withWatermark('ts','3 minute') 
    # .groupBy(window('ts', '3 minute')) 
    # .agg(count('price').alias('Total Harga'))
)

# stream_data.writeStream.foreachBatch(lambda batch_df, batch_id: 
#     batch_df.write.format("complete")
# ).start().awaitTermination()

(
    stream_data.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)  
    .option("numRows", 500)    
    .start()
    .awaitTermination()
)