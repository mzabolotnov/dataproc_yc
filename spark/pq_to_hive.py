from pyspark.sql import SparkSession
import sys


# Создание Spark-сессии
spark = SparkSession.builder.master("local").appName("PQ_to_Hive_table").enableHiveSupport().getOrCreate()
# Чтение данных из Parquet-файла
parquetFile = spark.read.parquet("s3a://dprocoutlog/parquet/*.parquet")

spark.sql('DROP TABLE IF EXISTS oralog_table_pq')
parquetFile.write.saveAsTable("oralog_table_pq")
df1=spark.sql("select count(*) from oralog_table_pq")
df1.show()


# # Указание порта и параметров кластера ClickHouse
# jdbcPort = 8443
# # jdbcHostname = sys.argv[1]
# jdbcHostname = "c-c9qca1ohvsmc8kgrnvd4.rw.mdb.yandexcloud.net"
# jdbcDatabase = "db1"
# jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"

# # Перенос таблицы из Parquet-файла в ClickHouse-таблицу с именем measurements
# parquetFile.write.format("jdbc") \
# .mode("overwrite") \
# .option("url", jdbcUrl) \
# .option("dbtable", "t_oralog") \
# .option("createTableOptions", "ENGINE = MergeTree() ORDER BY SCN") \
# .option("user","user1") \
# .option("password","Tratatam;3434") \
# .save()
# #  ORDER BY TimeStamp