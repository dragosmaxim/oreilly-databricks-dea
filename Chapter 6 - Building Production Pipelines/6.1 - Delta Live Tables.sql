-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Implementing DLT Pipelines

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/oreilly-databricks-dea/main/Includes/Images/school_schema.png" alt="School Schema">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_path="/Volumes/workspace/school/checkpoints/enrollments_dlt_raw"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = (
-- MAGIC     spark.readStream
-- MAGIC          .format("cloudFiles")
-- MAGIC          .option("cloudFiles.format", "json")
-- MAGIC          .option("cloudFiles.inferColumnTypes", "true")
-- MAGIC          .load(dataset_path)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating a streaming table

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE enrollments_raw
COMMENT "The raw courses enrollments, ingested from enrollments-dlt-raw folder"
AS SELECT * FROM cloud_files('/Volumes/workspace/school/checkpoints/enrollments_dlt_raw',
                            "json",
                            map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "dataset_path",
-- MAGIC     "/Volumes/workspace/school/checkpoints/enrollments_dlt_raw"
-- MAGIC )
-- MAGIC

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA school;

CREATE OR REFRESH STREAMING TABLE enrollments_raw
COMMENT "The raw courses enrollments, ingested from enrollments_dlt-raw folder"
AS SELECT * FROM cloud_files(
  '/Volumes/workspace/school/checkpoints/enrollments_dlt_raw',
  'json',
  map('cloudFiles.inferColumnTypes','true')
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating a materialized view

-- COMMAND ----------

SET dataset_path = '/Volumes/workspace/school/checkpoints/enrollments_dlt_raw';

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA school;

CREATE OR REPLACE TABLE students_raw
USING json
LOCATION '/Volumes/workspace/school/checkpoints/students-json';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Path to your students JSON inside the checkpoints volume
-- MAGIC # Path to the original students JSON in the course S3 bucket (read-only is fine)
-- MAGIC students_path = "s3://dalhussein-books/DEA-Book/datasets/school/v1/students-json"
-- MAGIC
-- MAGIC df_students = (
-- MAGIC     spark.read
-- MAGIC          .format("json")
-- MAGIC          .load(students_path)
-- MAGIC )
-- MAGIC
-- MAGIC # Write as a managed Delta table in workspace.school
-- MAGIC df_students.write.mode("overwrite").saveAsTable("workspace.school.students_raw")
-- MAGIC

-- COMMAND ----------

use catalog `workspace`; select * from `school`.`students_raw` limit 100;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW students
COMMENT "The students lookup table, ingested from students-json"
AS SELECT * FROM json.`${dataset_path}/students-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Silver layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE enrollments_cleaned (
 CONSTRAINT valid_order_number EXPECT (enroll_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned courses enrollments with valid enroll_id"
AS
 SELECT enroll_id, quantity, o.student_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
        cast(from_unixtime(enroll_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) formatted_timestamp,
        o.courses, c.profile:address:country as country
 FROM STREAM(LIVE.enrollments_raw) o
 LEFT JOIN LIVE.students c
   ON o.student_id = c.student_id

-- COMMAND ----------

dataset_path = "/Volumes/workspace/school/checkpoints/enrollments_dlt_raw"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold layer

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW uk_daily_student_courses
COMMENT "Daily number of courses per student in United Kingdom"
AS
 SELECT student_id, f_name, l_name, date_trunc("DD", formatted_timestamp) order_date, sum(quantity) courses_counts
 FROM LIVE.enrollments_cleaned
 WHERE country = "United Kingdom"
 GROUP BY student_id, f_name, l_name, date_trunc("DD", formatted_timestamp)

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW students
COMMENT "The students lookup table, ingested from students-json"
AS
SELECT *
FROM students_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Configuring DLT Pipelines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Modifying DLT pipelines

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW fr_daily_student_courses
COMMENT "Daily number of courses per student in France"
AS
 SELECT student_id, f_name, l_name, date_trunc("DD", formatted_timestamp) order_date, sum(quantity) courses_counts
 -- FROM enrollments_cleaned
 FROM LIVE.enrollments_cleaned
 WHERE country = "France"
 GROUP BY student_id, f_name, l_name, date_trunc("DD", formatted_timestamp)
