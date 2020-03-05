# Databricks notebook source
# MAGIC %md
# MAGIC ## Finding damaged IOT devices with Random Forest Classification and MLFlow

# COMMAND ----------

# MAGIC %md ##1. Data Load

# COMMAND ----------

# MAGIC %run ../notebookname

# COMMAND ----------

from pyspark.ml import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Define Schema
schema = StructType([ \
    StructField("AN3", DoubleType(), False), \
    StructField("AN4", DoubleType(), False), \
    StructField("AN5", DoubleType(), False), \
    StructField("AN6", DoubleType(), False), \
    StructField("AN7", DoubleType(), False), \
    StructField("AN8", DoubleType(), False), \
    StructField("AN9", DoubleType(), False), \
    StructField("AN10", DoubleType(), False)  # , \
                     
])

# COMMAND ----------

# DBTITLE 1,Load CSV Data From S3 Bucket
damagedSensorReadings = spark.read.schema(schema).csv("/windturbines/csv/D*gz").drop("TORQUE").drop("SPEED")
healthySensorReadings = spark.read.schema(schema).csv("/windturbines/csv/H*gz").drop("TORQUE").drop("SPEED")

damagedSensorReadings.cache()


# COMMAND ----------

display(damagedSensorReadings)

# COMMAND ----------

print(damagedSensorReadings.count())
print(healthySensorReadings.count())

# COMMAND ----------

# DBTITLE 1,Create SQL Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS windturbines.turbine_healthy (
# MAGIC   AN3 DOUBLE,
# MAGIC   AN4 DOUBLE,
# MAGIC   AN5 DOUBLE,
# MAGIC   AN6 DOUBLE,
# MAGIC   AN7 DOUBLE,
# MAGIC   AN8 DOUBLE,
# MAGIC   AN9 DOUBLE,
# MAGIC   AN10 DOUBLE
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION '/tobyb/windturbines/healthy/parquet'

# COMMAND ----------

# DBTITLE 1,Create SQL Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS windturbines.turbine_damaged (
# MAGIC   AN3 DOUBLE,
# MAGIC   AN4 DOUBLE,
# MAGIC   AN5 DOUBLE,
# MAGIC   AN6 DOUBLE,
# MAGIC   AN7 DOUBLE,
# MAGIC   AN8 DOUBLE,
# MAGIC   AN9 DOUBLE,
# MAGIC   AN10 DOUBLE
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION '/tobyb/windturbines/damaged/parquet'

# COMMAND ----------

turbine_healthy = table("turbine_healthy")
turbine_damaged = table("turbine_damaged")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Transformation

# COMMAND ----------

# DBTITLE 1,Join Both Complete Tables
df = turbine_healthy.withColumn("ReadingType", lit("HEALTHY")) \
    .union(turbine_damaged.withColumn("ReadingType", lit("DAMAGED")))

# COMMAND ----------

train, test = df.randomSplit([0.7, 0.3], 42)
train = train.repartition(32)
train.cache()
test.cache()
print(train.count())

# COMMAND ----------

# MAGIC %md ##3. Model Creation and Tracking with MLFlow

# COMMAND ----------

# MAGIC %md ### Workflows with Pyspark.ML Pipeline
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/pub-tc/ML-workflow.png" width="800">

# COMMAND ----------

# DBTITLE 1,Set MLFlow Experiment ID
import mlflow
mlflow.set_experiment("/Users/lei.pan@databricks.com/MLFlow/Topic-Model")

# COMMAND ----------

# DBTITLE 1,Features Pipeline & Model Pipeline Build
from pyspark.ml.feature import StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier

with mlflow.start_run(experiment_id = 2525470):
  #------- Feature Pipeline -------------------
  # create feature vector for model building
  featureCols = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]
  va = VectorAssembler(inputCols=featureCols, outputCol="va")

  # scale features
  scaler = StandardScaler(inputCol="va", outputCol="features", withStd=True, withMean=True)

  # create binary class variable named 'label'
  indexer = StringIndexer(inputCol="ReadingType", outputCol="label")

  # set stages in model building pipeline
  stages = [va, scaler, indexer]
  pipeline = Pipeline(stages=stages)

  # cross-validated model
  featuresPipeline = pipeline.fit(train)
  featuresDF = featuresPipeline.transform(train)
  
  
  #------- Model Pipeline ----------------------
  # specify Random Forest Classifier Algorithm
  rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
 

  # create a parameter grid against which to optimise model
  grid = ParamGridBuilder().addGrid(\
      rf.maxDepth, [4, 5, 6]).build()


  # modelevaluator
  ev = BinaryClassificationEvaluator()

  # 3-fold cross validation
  numFolds=3
  cv = CrossValidator(estimator=rf, \
                      estimatorParamMaps=grid, \
                      evaluator=ev, \
                      numFolds=numFolds)

  cvModel = cv.fit(featuresDF)
  
  #--MLFlow--
  mlflow.log_param("numFolds", numFolds)
  mlflow.log_param("Feature Pipeline Stages", stages)
  mlflow.log_metric("Ave Metrics", cvModel.avgMetrics[0])
  mlflow.pyfunc.log_model(model_path,loader_module='mlflow.spark')
  mlflow.spark.log_model(cvModel, "spark-model")
