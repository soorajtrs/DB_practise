# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC 1.download a random data 
# MAGIC

# COMMAND ----------

# Accessing S3 bucket using dbutils
ACCESS_KEY = "AKIAZQ3DRZUWMDR4LOFM"
SECRET_KEY = "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"
print(ACCESS_KEY)
print(SECRET_KEY)



aws_bucket_name = "db-tesing-data"
dbutils.fs.ls(f"s3a://{ACCESS_KEY}:{SECRET_KEY}@{aws_bucket_name}/")



# COMMAND ----------

# MAGIC %md
# MAGIC 2.load it in s3 
# MAGIC
# MAGIC

# COMMAND ----------

SOURCE_NAME= f"s3a://{ACCESS_KEY}:{SECRET_KEY}@{aws_bucket_name}/"
print(SOURCE_NAME)
MOUNT_NAME=f"/mnt/{aws_bucket_name}"
print(MOUNT_NAME)
dbutils.fs.unmount(MOUNT_NAME)
dbutils.fs.mount(SOURCE_NAME,MOUNT_NAME)

# COMMAND ----------

# MAGIC %fs ls /mnt/db-tesing-data

# COMMAND ----------

aws_df = spark.read.format("csv").option('header','true').option('inferschema','true').load('/mnt/db-tesing-data/ElectronicsData.csv')
display(aws_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`s3://db-tesing-data/ElectronicsData.csv`;

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("s3://db-tesing-data/ElectronicsData.csv")
 
df.show(1)


# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists Demo_lake;
# MAGIC show databases;
# MAGIC use demo_lake;
# MAGIC show tables;

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('demo_lake.delta_1')

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;
# MAGIC use demo_lake;
# MAGIC show tables;
# MAGIC show create table delta_1; 
# MAGIC select * from delta_1 limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;
# MAGIC use demo_lake;
# MAGIC  
# MAGIC update delta_1 set Discount = '50% discount' where subcategory='Batteries'  ;
# MAGIC select * from delta_1 limit 10;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


