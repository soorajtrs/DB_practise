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



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


