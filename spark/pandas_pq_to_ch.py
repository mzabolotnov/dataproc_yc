import fastparquet
import pandas as pd
from clickhouse_driver import Client
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParquetClickhouse").getOrCreate()

# Чтение данных из Parquet-файла
parquetFile = spark.read.parquet("s3a://dprocoutlog/parquet/*.parquet")
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")
parquetFile = parquetFile.select(parquetFile["SCN"])
pd_df = parquetFile.toPandas()
# pd_df.show()
# parquetFile.write.mode('overwrite').parquet("dprocoutlog/parquet/")

# Connect to your ClickHouse cluster
CH_Client = Client(
   host='c-c9qca1ohvsmc8kgrnvd4.rw.mdb.yandexcloud.net', 
   port=9440, 
   user='user1',
   password='Tratatam;3434', 
   secure=True)

# # Read the Parquet file into a pandas DataFrame
# df = pd.read_parquet('parquet.file')
# df.show()

# # Insert the DataFrame into ClickHouse
CH_Client.execute(
   'INSERT INTO db1.parquet_test_data FORMAT Parquet', pd_df.to_dict(orient='records')
)
