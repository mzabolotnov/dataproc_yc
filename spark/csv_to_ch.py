from pyspark.sql import SparkSession
import sys

# Создание Spark-сессии
spark = SparkSession.builder.appName("ParquetClickhouse").getOrCreate()

# Чтение данных из CSV-файла
parquetFile = spark.read.options(header='True').csv("s3a://dprocoutlog/csv/")
parquetFile = parquetFile.select(parquetFile["SCN"])
parquetFile.show()

# Указание порта и параметров кластера ClickHouse
jdbcPort = 8443
# jdbcHostname = sys.argv[1]
jdbcHostname = "c-c9qca1ohvsmc8kgrnvd4.rw.mdb.yandexcloud.net"
jdbcDatabase = "db1"
jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"

# Перенос таблицы из Parquet-файла в ClickHouse-таблицу с именем measurements
parquetFile.write.format("jdbc") \
.mode("overwrite") \
.option("url", jdbcUrl) \
.option("dbtable", "t_oralog") \
.option("createTableOptions", "ENGINE = MergeTree() ORDER BY SCN") \
.option("user","user1") \
.option("password","Tratatam;3434") \
.save()
#  ORDER BY TimeStamp