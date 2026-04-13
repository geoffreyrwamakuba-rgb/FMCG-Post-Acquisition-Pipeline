# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/geoffreyrwamakuba@gmail.com/consolidated_pipeline/1_Setup/Utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

# create widget to take in variables and names of data
dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data Source")

# COMMAND ----------

# create variables from widget value
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(catalog,data_source)

# COMMAND ----------

# create a dynamic file path
base_path = f"s3://sportsbar-gr/{data_source}/*.csv"
print(base_path)

# COMMAND ----------

# Read data into dataframe -->Add new columns for time of ingestion and metadata 
df = (spark.read\
    .option("header",True)
    .option("inferSchema",True)
    .format("csv")\
    .load(base_path)\
    .withColumn("read_timestamp",current_timestamp())\
    .select("*","_metadata.file_name","_metadata.file_size"))

display(df.limit(10))

# COMMAND ----------



# COMMAND ----------

# Check Schema output -- customer_id is integer we will updae this later to match the gold
df.printSchema()

# COMMAND ----------

# Ingest into our bronze table -- See CDF is being enabled

df.write.format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")
    


# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
display(df_bronze)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_bronze.groupBy("customer_id").agg(count("customer_id").alias("count")).filter(col("count")>1).display()


# COMMAND ----------

# Drop duplicates here -- this method is not deterministic (shouls use row_number and filter but not necessary here - no date) 
df_silver = df_bronze.drop_duplicates(["customer_id"])
df_silver.groupBy("customer_id").agg(count("customer_id").alias("count")).filter(col("count")>1).display()
print(f"rows before {df_bronze.count()} + rows after {df_silver.count()}")

# COMMAND ----------

# Get rid of Leading spaces
#df_bronze.filter(col("customer_name")!= trim(col("customer_name"))).display()
df_silver = df_silver.withColumn("customer_name",trim(col("customer_name")))
df_silver.filter(col("customer_name")!= trim(col("customer_name"))).display()

# COMMAND ----------

df_silver.select("city").distinct().display()
# There are missspelling of cities and old city names
# We will fix this with mapping

# COMMAND ----------

city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}


allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

# COMMAND ----------

# Using replace on the data frame
df_silver = df_silver.replace(city_mapping,subset=["city"])\
    .withColumn("city",when(col("city").isNull(),None).when(col("city").isin(allowed),col("city")).otherwise(None))
df_silver.select("city").distinct().display()

# COMMAND ----------

# Title case issues
# df_silver.select("customer_name").distinct().display()
df_silver = df_silver.withColumn("customer_name",when(col("customer_name").isNull(),None).otherwise(initcap(col("customer_name"))))
df_silver.select("customer_name").distinct().display()

# COMMAND ----------


null_customer_names = ["Sprintx Nutrition","Zenathlete Foods","Primefuel Nutrition","Recovery Lane"]
df_silver.filter(col("customer_name").isin(null_customer_names)).display()

# COMMAND ----------

# df_silver.filter(col("city").isNull()).display()
# The managers have told us these customers belong to the below cities

# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",
    # Zenathlete Foods
    789420: "Bengaluru",
    # Primefuel Nutrition
    789521: "Hyderabad",
    # Recovery Lane
    789603: "Hyderabad"
}

schema = ["customer_id","fixed_city"]
df_fix = spark.createDataFrame(list(customer_city_fix.items()),schema)
df_fix.display()
#data

# COMMAND ----------

# DBTITLE 1,Cell 22
df_silver = df_silver.join(df_fix,"customer_id","left")\
    .withColumn("city",coalesce("city","fixed_city"))\
    .drop(df_fix.fixed_city)

# COMMAND ----------

df_silver.filter(col("customer_name").isin(null_customer_names)).display()


# COMMAND ----------

df_silver = df_silver.withColumn("customer_id",col("customer_id").cast("string"))
df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn("customer",concat_ws("-","customer_name",coalesce(col("city"),lit("Unknown"))))
df_silver = df_silver.withColumnRenamed("customer_id","customer_code")
df_silver = df_silver.withColumn("market",lit("India"))
df_silver = df_silver.withColumn("platform",lit("Sports Bar"))
df_silver = df_silver.withColumn("channel",lit("Acquisition"))

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.write.format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .option("mergeSchema","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source}")
df_gold = df_silver.select("customer_code","customer_name","city","customer","market","platform","channel")
df_gold.limit(10).display()

# COMMAND ----------

df_gold.write.format("delta")\
.option("delta.enableChangeDataFeed","true")\
.mode("overwrite")\
.saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")


# COMMAND ----------

 df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select("customer_code","customer_name","city","customer","market","platform","channel")
 

# COMMAND ----------

# DBTITLE 1,a
delta_table = DeltaTable.forName(spark,"fmcg.gold.dim_customers")
delta_table.alias("target").merge(
    source = df_child_customers.alias("source"),
    condition = "target.customer_code = source.customer_code")\
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

