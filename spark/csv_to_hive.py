from pyspark.sql import SparkSession
# from pyspark.sql import HiveContext
import sys


# Создание Spark-сессии
spark = SparkSession.builder.master("local").appName("CSV_to_Hive_table").enableHiveSupport().getOrCreate()
# spark = SparkSession.builder.appName("ParquetClickhouse").getOrCreate()
# Чтение данных из CSV-файла
CSVFile = spark.read.options(header='True').csv("s3a://dprocoutlog/csv/")
CSVFile.show()
# sqlContext = HiveContext(sc)
spark.sql('DROP TABLE IF EXISTS oralog_table_csv')
CSVFile.write.saveAsTable("oralog_table_csv")
# rows = df1.count()
# print(f"DataFrame Rows count : {rows}")
df1=spark.sql("select count(*) from oralog_table_csv")
df1.show()
