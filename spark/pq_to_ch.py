from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F
# Создание Spark-сессии
spark = SparkSession.builder.appName("ParquetClickhouse").getOrCreate()

# Чтение данных из Parquet-файла
OraLogDF = spark.read.parquet("s3a://dprocoutlog/parquet/*.parquet")
# parquetFile = parquetFile.filter(~F.col("SCN").rlike("<"))
OraLogDF.show()

# Указание порта и параметров кластера ClickHouse
jdbcPort = 8443
# jdbcHostname = sys.argv[1]
file_ch_host = open("./ch_host", 'r')
jdbcHostname = file_ch_host.readline().splitlines()[0]
file_ch_host.close()
# jdbcHostname = "c-c9q1dg4adc02478u5omv.rw.mdb.yandexcloud.net"
jdbcDatabase = "db1"
jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"

# Перенос таблицы из Parquet-файла в ClickHouse-таблицу с именем measurements
OraLogDF.write.format("jdbc") \
.mode("overwrite") \
.option("url", jdbcUrl) \
.option("dbtable", "pq_data_oralog") \
.option("createTableOptions", "ENGINE = MergeTree() ORDER BY SCN") \
.option("user","user1") \
.option("password","Tratatam;3434") \
.save()