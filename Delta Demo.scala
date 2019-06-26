// Databricks notebook source
// MAGIC %md 
// MAGIC ## How does Delta Lake do it?

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1. Load historical mobile events into Delta Lake
// MAGIC #### 2. Load streaming mobile events into Delta Lake
// MAGIC #### 3. Query delta table via SQL while streaming data 
// MAGIC #### 4. Time travel and Vacuum
// MAGIC #### 5. Optimization
// MAGIC #### 6. Upserts
// MAGIC #### 7. Delete

// COMMAND ----------

// MAGIC %run ./Enable_Delta

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 1. Load Historical Mobile Events into Delta Lake:

// COMMAND ----------

val deltaDF = spark.sql("""select * from testDeltaTableEvents""")
display(deltaDF)

// COMMAND ----------

createEvents(10, "2019-02-28", "batch-insert")
  .toDF()
  .write
  .format("delta")
  .save("/mnt/prasad_delta/hg")

// COMMAND ----------

spark.sql("Drop TABLE if exists mobileEvents")
%sql CREATE TABLE mobileEvents USING DELTA LOCATION '/mnt/prasad_delta/hg

// COMMAND ----------

// MAGIC %md
// MAGIC ### Query historical data

// COMMAND ----------

// MAGIC %sql SELECT count(*), date, eventType FROM mobileEvents GROUP BY date, eventType ORDER BY date ASC

// COMMAND ----------

createEvents(5000, "2019-03-01", "batch-append")
  .toDF()
  .write
  .format("delta")
  .mode("append")
  .save("/mnt/prasad_delta/hg")

createEvents(1000, "2019-03-03", "batch-append")
  .toDF()
  .write
  .format("delta")
  .mode("append")
  .save("/mnt/prasad_delta/hg")

// COMMAND ----------

// MAGIC %sql SELECT count(*), date, eventType FROM mobileEvents GROUP BY date, eventType ORDER BY date ASC

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 2. Load streaming mobile data into Delta Lake

// COMMAND ----------

val df = createEvents(10000, "2019-03-04", "stream").toDF()
val schema = df.schema
df.write
  .format("json")
  .mode("overwrite")
  .save("/mnt/prasad_delta/eventsStream")

val streamDF = spark
  .readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .json("/mnt/prasad_delta/eventsStream")

// COMMAND ----------

streamDF
  .writeStream
  .format("delta")
  .option("path", "/mnt/prasad_delta/hg")
  .option("checkpointLocation", "/tmp/prasad_delta/hg/checkpoint/")
  .start()

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 3. Query data via `SQL` while streaming data

// COMMAND ----------

// MAGIC %md
// MAGIC ### Using SQL to check how many active users we have per day from Delta Lake Table

// COMMAND ----------

// MAGIC %sql SELECT count(*), date, eventType FROM mobileEvents GROUP BY date, eventType ORDER BY date ASC

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 4. Time travel and Vacuum

// COMMAND ----------

// MAGIC %sql select * from mobileEvents
// MAGIC version as of 0
// MAGIC 
// MAGIC -- you can use timestamp to do timetravel as well - %sql SELECT * FROM mobileEvents TIMESTAMP AS OF '2019-03-19 16:53:35'

// COMMAND ----------

// MAGIC %sql vacuum mobileEvents

// COMMAND ----------

// MAGIC %md vacuum mobileEvents RETAIN 24 HOURS

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 5. Optimization 

// COMMAND ----------

// MAGIC %md # OPTIMIZE and ZORDER
// MAGIC 
// MAGIC Improve your query performance with `OPTIMIZE` and `ZORDER` using file compaction and a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
// MAGIC 
// MAGIC Legend:
// MAGIC * Gray dot = data point e.g., chessboard square coordinates
// MAGIC * Gray box = data file; in this example, we aim for files of 4 points each
// MAGIC * Yellow box = data file that’s read for the given query
// MAGIC * Green dot = data point that passes the query’s filter and answers the query
// MAGIC * Red dot = data point that’s read, but doesn’t satisfy the filter; “false positive”
// MAGIC 
// MAGIC ![](https://databricks.com/wp-content/uploads/2018/07/Screen-Shot-2018-07-30-at-2.03.55-PM.png)
// MAGIC 
// MAGIC Reference: [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE mobileEvents
// MAGIC ZORDER by EventType, Date;
// MAGIC select * from mobileEvents

// COMMAND ----------

// MAGIC %sql SELECT * FROM mobileEvents

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 6. Upserts

// COMMAND ----------

(1 to 10)
  .toList.map(id => Event(java.sql.Date.valueOf("2019-02-28"), "upsert", "CA", id))
  .toDF
  .createOrReplaceTempView("temp_table")

// COMMAND ----------

// MAGIC %sql SELECT * FROM temp_table

// COMMAND ----------

// MAGIC %sql SELECT  e.location, count(*) FROM mobileEvents e GROUP BY e.location

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO mobileEvents AS target
// MAGIC USING temp_table
// MAGIC ON temp_table.deviceId == target.deviceId AND temp_table.date == target.date
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET target.date = temp_table.date, target.eventType = temp_table.eventType, target.location = temp_table.location, target.deviceId = temp_table.deviceId
// MAGIC WHEN NOT MATCHED 
// MAGIC   THEN INSERT (target.date, target.eventType, target.location, target.deviceId) VALUES (temp_table.date, temp_table.eventType, temp_table.location, temp_table.deviceId)

// COMMAND ----------

// MAGIC %sql SELECT e.location, count(*) FROM mobileEvents e GROUP BY e.location

// COMMAND ----------

// MAGIC %fs ls /mnt/prasad_delta/hg

// COMMAND ----------

// MAGIC %sql select * from maggie.delta_scratchpad where service != 'null' and resp_h != '192.168.22.252' and orig_p = 4444

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7. Delete

// COMMAND ----------

// MAGIC %sql delete from lei.delta_scratchpad where uid = 'CUPmW415nQIhlQXZQe'