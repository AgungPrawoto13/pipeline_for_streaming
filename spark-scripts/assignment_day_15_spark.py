import pyspark, os

from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp,col,when

dotenv_path = Path('../.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_db = os.getenv('POSTGRES_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster('local')
        .set("spark.jars", "/opt/postgresql-42.2.18.jar")
        #.set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

retail_df = spark.read.jdbc(
    jdbc_url,
    'public.retail',
    properties=jdbc_properties
)

retail_df.show(5)