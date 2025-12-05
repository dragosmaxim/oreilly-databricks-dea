# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader in Action

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/oreilly-databricks-dea/main/Includes/Images/school_schema.png" alt="School Schema">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/School-Setup

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_school}/enrollments-json-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setting up Auto Loader

# COMMAND ----------

(spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.inferColumnTypes","true")
           .option("cloudFiles.schemaLocation", "s3://dalhussein-books/DEA-Book/checkpoints/enrollments")
           .load(f"{dataset_school}/enrollments-json-raw")
     .writeStream
           .option("checkpointLocation", "s3://dalhussein-books/DEA-Book/checkpoints/enrollments")
           .table("enrollments_updates")
)

/Volumes/workspace/school/checkpoints

# COMMAND ----------

volume_base = "/Volumes/workspace/school/checkpoints"

schema_location     = f"{volume_base}/enrollments_schemas"
checkpoint_location = f"{volume_base}/enrollments"

(
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .load("s3://dalhussein-books/DEA-Book/datasets/school/v1/enrollments-json-raw")
    .writeStream
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .table("enrollments_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM enrollments_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM enrollments_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Observing Auto Loader

# COMMAND ----------

raw_path = "/Volumes/workspace/school/checkpoints/enrollments_raw"

(
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.inferColumnTypes", "true")
       .option("cloudFiles.schemaLocation", schema_location)
       .load(raw_path)              # 👈 now watching your Volume!
       .writeStream
       .option("checkpointLocation", checkpoint_location)
       .trigger(availableNow=True)
       .table("enrollments_updates")
)


# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_school}/enrollments-json-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM enrollments_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY enrollments_updates

# COMMAND ----------

def load_new_data():
    src = "dbfs:/databricks-datasets/some/example/file.json"
    dst = "/Volumes/workspace/school/checkpoints/enrollments_raw/file.json"
    dbutils.fs.cp(src, dst)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE enrollments_updates

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/DEA-Book/checkpoints/enrollments", True)

# COMMAND ----------


