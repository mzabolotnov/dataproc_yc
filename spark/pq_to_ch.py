from pyspark.sql import SparkSession
import sys

# Создание Spark-сессии
spark = SparkSession.builder.appName("ParquetClickhouse").getOrCreate()

# Чтение данных из Parquet-файла
parquetFile = spark.read.parquet("s3a://dprocoutlog/parquet/*.parquet")
parquetFile.show()

# Указание порта и параметров кластера ClickHouse
jdbcPort = 8443
# jdbcHostname = sys.argv[1]
jdbcHostname = ""
jdbcDatabase = "db1"
jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"

# Перенос таблицы из Parquet-файла в ClickHouse-таблицу с именем measurements
parquetFile.write.format("jdbc") \
.mode("error") \
.option("url", jdbcUrl) \
.option("dbtable", "t_oralog") \
.option("createTableOptions", "ENGINE = MergeTree() ORDER BY TimeStamp") \
.option("user","user1") \
.option("password","Tratatam;3434") \
.save()