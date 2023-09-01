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
  def __init__(self,backet_name,file_in,file_out,spark_app,bucket_name_out):
      self.file_in = file_in
      self.file_out = file_out
      self.spark_app = spark_app
      self.bucket_name = backet_name
      self.bucket_name_out = bucket_name_out 

  def read_file_from_bucket(self):
      log_ora = self.spark_app.read.text('s3a://'+self.bucket_name+'/'+self.file_in)
      cmd = "/bin/hdfs dfs -rm -f -r \'./"+self.file_in+"\'"
      returned_value = os.system(cmd)
      print ("returned_code_put_file: ", returned_value)

      log_ora.write.text(self.file_in)
      
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
  
  def move_file_from_hdfs(self):
      cmd = "/bin/hdfs dfs -cat \'./"+self.file_in+"/part*\' > \'" + self.file_in + "\'"
      returned_value = os.system(cmd)
      print ("returned_code_put_file: ", returned_value)
  
  def put_file_to_hdfs(self):
        cmd = "/bin/hdfs dfs -put -f \'" + self.file_out + "\'"
        returned_value = os.system(cmd)
        print ("returned_code_put_file: ", returned_value)
        
  def dowload_file_from_bucket(self,url):
        cmd = "curl \'" + url +"\' > \'" + self.file_in + "\'"
        returned_value = os.system(cmd)
        print ("returned_code_put_file: ", returned_value)
  
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
          log_ora_transform = log_ora_transform.filter(~F.col("SCN").rlike("\'"))
          log_ora_transform = log_ora_transform.filter(~F.col("SCN").rlike("<"))


          log_ora_transform.write.mode('overwrite').parquet(self.bucket_name_out)
          # log_ora_transform = spark.read.parquet("s3a://dprocoutlog/parquet/")
          log_ora_transform.show()
          log_ora_transform.printSchema()
          rows = log_ora_transform.count()
          print(f"DataFrame Rows count : {rows}")
          # log_ora_transform.filter(F.col("SCN") == '12934735784').show(truncate=False)
          

        

def main():
          time1 =  time.time() 
          spark = SparkSession.builder.appName("log_stat").getOrCreate()
          file_transform = file_proc("projectdata1","V_$LOGMNR_CONTENTS_utf8.csv","V_$LOGMNR_CONTENTS_utf8.csv_t",spark,"s3a://dprocoutlog/parquet/")
          # url =  ''.join(("https://storage.yandexcloud.net/projectdata1/V_$LOGMNR1_CONTENT_utf8.csv?X-Amz",
          #                 "-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=YghjhjhEl-fX5dfggdfgdfgertertert%2Fru"))
          # file_transform.dowload_file_from_bucket(url)
          file_transform.read_file_from_bucket()
          file_transform.move_file_from_hdfs()
          file_transform.transform_file()
          file_transform.put_file_to_hdfs()
          # time2 = time.time()
          # delta_time = time2-time1
          # print(delta_time)
          file_transform.transform_oralog()    
         
          time2 = time.time()
          delta_time = time2-time1
          print(delta_time)


if __name__ == "__main__":
     main()