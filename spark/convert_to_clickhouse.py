import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Row, Window
import pyspark.sql.functions as F
import time
import subprocess
import os
# import pydoop.hdfs as hdfs


class file_proc:
  # import pydoop.hdfs as hdfs
  def __init__(self,backet_name,file_in,spark_app,bucket_name_out):
      self.file_in = file_in
      self.file_out = self.file_in+"_t"
      self.spark_app = spark_app
      self.bucket_name = backet_name
      self.bucket_name_out = bucket_name_out 

  def read_file_from_bucket(self):
      log_ora = self.spark_app.read.text('s3a://'+self.bucket_name+'/'+self.file_in)
      # cmd = "/bin/hdfs dfs -rm -f -r \'./"+self.file_in+"\'"
      # returned_value = os.system(cmd)
      # print ("returned_code_cmd_file: ", returned_value)
      log_ora.write.text(self.file_in)
  
  def delete_file_from_local(self,file):
      cmd = "rm -fr \'./"+file+"\'"
      returned_value = os.system(cmd)
      print ("returned_code_cmd_file: ", returned_value)
  
  def delete_file_from_hdfs(self,file):
      cmd = "hdfs dfs -rm -f -r \'./"+file+"\'"
      returned_value = os.system(cmd)
      print ("returned_code_cmd_file: ", returned_value)
  
  def transform_file(self):
      myfile = open(self.file_in, "r")
      file_write = open(self.file_out, 'w')
      myline = ''.join(myfile.readline().splitlines())
      
      while myline:
        if myline.endswith(":"):
                list_string = []
                list_string.append(myline)

                for i in range(1,100,1):
                  myline = ''.join(myfile.readline().splitlines())
                  if ',NULL,NULL);' not in myline:
                    list_string.append(myline)
                  if ',NULL,NULL);' in myline:
                    list_string.append(myline)
                    myline = '><'.join(list_string)
                    break
        print('.',end="")
        file_write.write(myline+"\n")
        myline = ''.join(myfile.readline().splitlines())

      myfile.close()
      file_write.close()
  
  def move_file_from_hdfs_to_local(self):
      cmd = "/bin/hdfs dfs -cat \'./"+self.file_in+"/part*\' > \'" + self.file_in + "\'"
      returned_value = os.system(cmd)
      print ("returned_code_cmd_file: ", returned_value)
  
  def put_file_to_hdfs(self):
        cmd = "/bin/hdfs dfs -put -f \'" + self.file_out + "\'"
        returned_value = os.system(cmd)
        print ("returned_code_cmd_file: ", returned_value)
        
  def dowload_file_from_bucket(self,url):
        cmd = "curl \'" + url +"\' > \'" + self.file_in + "\'"
        returned_value = os.system(cmd)
        print ("returned_code_cmd_file: ", returned_value)

  def transform_oralog(self):
          log_ora = self.spark_app.read.text(self.file_out)
          header = log_ora.first()[0]
          header_list = header.split(',')
          log_ora = log_ora.filter(~(F.col("value").contains(header)))
          log_ora.show()

          log_ora_transform = log_ora
          df_add_col = lambda df,i,spl_str,col_s,header: df.withColumn(str(header[i]),F.split(log_ora_transform[col_s],spl_str,len(header)).getItem(i))
          for i in range(0,len(header_list),1):
            log_ora_transform = df_add_col(log_ora_transform,i,',','value',header_list)
          log_ora_transform = log_ora_transform.drop("value")
          header_list = ['Redo_SQL','ERROR']
          for i in range(0,len(header_list),1):
            log_ora_transform = df_add_col(log_ora_transform,i,'><','SQL Redo',header_list)
          log_ora_transform = log_ora_transform.drop('SQL Redo')
          log_ora_transform = log_ora_transform.withColumnRenamed('Session #','Session')
          log_ora_transform = log_ora_transform.withColumnRenamed('Time Stamp','TS_IN_STRING')
          log_ora_transform = log_ora_transform.withColumn('TimeStamp',F.regexp_replace('TS_IN_STRING', '\t', ' '))
          log_ora_transform = log_ora_transform.withColumn('TimeStamp',F.regexp_replace('TimeStamp', "\.", '-'))
          log_ora_transform = log_ora_transform.withColumn('TimeStamp',F.to_timestamp(F.col('TimeStamp'),"dd-MM-yyyy H:mm:ss"))
          log_ora_transform = log_ora_transform.drop('TS_IN_STRING')
          log_ora_transform = log_ora_transform.filter(~((F.col("SCN").rlike("\'")) | (F.col("SCN"
                                                       ).rlike("<")) | (F.col("TimeStamp").cast(
                                                       'string').rlike("1970"))))
          #log_ora_transform = log_ora_transform.filter(~F.col("SCN").rlike("<"))
          #log_ora_transform = log_ora_transform.filter(~F.col("TimeStamp").cast('string').rlike("1970"))


          # log_ora_transform.write.mode('overwrite').parquet(self.bucket_name_out)
          # log_ora_transform = self.spark_app.read.parquet(self.bucket_name_out)
          
          jdbcPort = 8443
          # jdbcHostname = sys.argv[1]
          file_ch_host = open("./ch_host", 'r')
          jdbcHostname = file_ch_host.readline().splitlines()[0]
          file_ch_host.close()
          print(jdbcHostname)
          # jdbcHostname = "c-c9q1dg4adc02478u5omv.rw.mdb.yandexcloud.net"
          jdbcDatabase = "db1"
          jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"
          
          # Запись в ClickHouse-таблицу с именем csv_data_oralog
          log_ora_transform.write.format("jdbc") \
          .mode("overwrite") \
          .option("url", jdbcUrl) \
          .option("dbtable", "data_oralog") \
          .option("createTableOptions", "ENGINE = MergeTree() ORDER BY SCN") \
          .option("user","user1") \
          .option("password","Tratatam;3434") \
          .save()
          log_ora_transform.show()
          log_ora_transform.printSchema()
          rows = log_ora_transform.count()
          print(f"DataFrame Rows count : {rows}")

          # log_ora_transform.filter(F.col("SCN") == '12934735784').show(truncate=False)
    

def main():
          time1 =  time.time() 
          spark = SparkSession.builder.appName("log_stat").getOrCreate()
          file_transform = file_proc("projectdata1","V_$LOGMNR_CONTENTS_utf8.csv",
                                     spark,"s3a://dprocoutlog/parquet/")
          # file_transform.dowload_file_from_bucket(url)
          print(f"Удаление файла {file_transform.file_in} из HDFS")
          file_transform.delete_file_from_hdfs(file_transform.file_in)
          print(f"Чтение файла {file_transform.file_in} из бакета и запись его в HDFS в каталог {file_transform.file_in}")
          file_transform.read_file_from_bucket()
          print(f"Перемещение файла {file_transform.file_in} из HDFS в local-fs")
          file_transform.move_file_from_hdfs_to_local()
          #преодразование файла построчно и запись результата в файл <имя файла>_t 
          file_transform.transform_file()
          print(f"Удаление исходного файла {file_transform.file_in} из local-fs")
          file_transform.delete_file_from_local(file_transform.file_in)
          print(f"Копирование преобразованного файла {file_transform.file_in}_t из local-fs в HDFS")
          file_transform.put_file_to_hdfs()
          print(f"Удаление преобразованного файла {file_transform.file_in}_t из local-fs")
          file_transform.delete_file_from_local(file_transform.file_out)
          # # time2 = time.time()
          # # delta_time = time2-time1
          # # print(delta_time)
          
          # Обработка файла
          file_transform.transform_oralog()
          print(f"Удаление преобразованного файла {file_transform.file_in}_t из HDFS")
          file_transform.delete_file_from_hdfs(file_transform.file_out)
          
          time2 = time.time()
          delta_time = time2-time1
          print(f"Время выполнения скрипта - {delta_time}сек.")


if __name__ == "__main__":
     main()
