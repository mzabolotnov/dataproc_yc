from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F

# Создание Spark-сессии
spark = SparkSession.builder.appName("ParquetClickhouse").getOrCreate()

# Чтение данных из csv-файла
OraLogDF = spark.read.options(header='True').csv("s3a://dprocoutlog/csv/")
# parquetFile = spark.read.options(header='True').csv("csv/")

OraLogDF.show()

# Указание порта и параметров кластера ClickHouse
jdbcPort = 8443
# jdbcHostname = sys.argv[1]
jdbcHostname = "c-c9q1dg4adc02478u5omv.rw.mdb.yandexcloud.net"
jdbcDatabase = "db1"
jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"

# Перенос таблицы из csv-файла в ClickHouse-таблицу с именем csv_data_oralog
OraLogDF.write.format("jdbc") \
.mode("overwrite") \
.option("url", jdbcUrl) \
.option("dbtable", "csv_data_oralog") \
.option("createTableOptions", "ENGINE = MergeTree() ORDER BY SCN") \
.option("user","user1") \
.option("password","Tratatam;3434") \
.save()
