# Databricks notebook source

# DBTITLE 1,Define common functions
def load_file_as_dataframe(file_path):
  from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

  schema = StructType([
      StructField("customer_id", IntegerType(), True),
      StructField("address", StringType(), True),
      StructField("effective_date", TimestampType(), True)
  ])
  
  return spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(file_path)

def merge(source):
  from delta.tables import DeltaTable
  target = DeltaTable.forName(spark, "customer_address")

  # Rows to INSERT new records for existing and new customers
  to_insert = source.alias("source") \
      .join(target.toDF().alias("target"), "customer_id") \
      .where("target.is_current = true AND NOT(target.address <=> source.address)") \
      .select("source.customer_id", "source.address", "source.effective_date")
    
  # Stage the update by unioning two sets of rows
  # 1. Rows that will be inserted in the whenNotMatched clause
  # 2. Rows that will either update the address of an existing customer (only if it is not the same address as before) or insert the new address records of before unseen customers 
  staged_updates = (
      to_insert
      .selectExpr("NULL as merge_key", "*")   # Rows for 1
      .union(source.selectExpr("customer_id as merge_key", "*"))  # Rows for 2.
  )

  # Apply SCD Type 2 operation using merge
  target.alias("target").merge(
    staged_updates.alias("staged_updates"),
    "target.customer_id = staged_updates.merge_key AND target.is_current = true") \
  .whenMatchedUpdate(
    condition = "target.address != staged_updates.address",
    set = {
      "is_current": "false",
      "end_date": "staged_updates.effective_date"
    }
  ).whenNotMatchedInsert(
    values = {
      "customer_id": "staged_updates.customer_id",
      "address": "staged_updates.address",
      "start_date": "staged_updates.effective_date",
      "end_date": "null",
      "is_current": "true",
    }
  ).whenNotMatchedBySourceUpdate(
    condition = "target.is_current = true",
    set = {
      "is_current": "false",
      "end_date": "now()"
    }
  ).execute()

def show_table():
  spark.sql("SELECT * FROM customer_address ORDER BY customer_id").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Initial table is empty
show_table()

# COMMAND ----------

# DBTITLE 1,Scenario 1 – Initial load of customer addresses
source = load_file_as_dataframe("file:/Workspace/Users/[your_user]/data/customers1.csv")
merge(source)
show_table()

# COMMAND ----------

# DBTITLE 1,Scenario 2 – Adding new customer with customer_id = 6
source = load_file_as_dataframe("file:/Workspace/Users/[your_user]/data/customers2.csv")
merge(source)
show_table()

# COMMAND ----------

# DBTITLE 1,Scenario 3 – Update the address of customer 3 and remove customer 6
source = load_file_as_dataframe("file:/Workspace/Users/[your_user]/data/customers3.csv")
merge(source)
show_table()


# COMMAND ----------

# DBTITLE 1,Scenario 4 – Restore customer 6
source = load_file_as_dataframe("file:/Workspace/Users/[your_user]/data/customers4.csv")
merge(source)
show_table()