from pyspark.sql.functions import *
from pyspark.sql.types import *

dataPath = "/Volumes/databricks_aws_lakehouse/bronze/bronze_volume/raw_data/"
checkpoint = "/Volumes/databricks_aws_lakehouse/bronze/bronze_volume/destination/checkpoint/" # store which all files processed
schema_location = "/Volumes/databricks_aws_lakehouse/bronze/bronze_volume/destination/scheam_location/" #remember what data looks like

schema = StructType([
    StructField("orders", ArrayType(
        StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("items", ArrayType(
                StructType([
                    StructField("product_id", IntegerType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("price_per_unit", DoubleType(), True)
                ])
            ), True)
        ])
    ), True)
])

def view_csv_file():
    sample_df = spark.read.format("csv").option("header","True").csv(dataPath)
    sample_df.printSchema()

#view_csv_file()

csv_schema = StructType([
    StructField("ship_mode",StringType(),True),
    StructField("segment",StringType(),True),
    StructField("country",StringType(),True),
    StructField("city",StringType(),True),
    StructField("state",StringType(),True),
    StructField("postal_code",StringType(),True),
    StructField("region",StringType(),True),
    StructField("category",StringType(),True),
    StructField("sub_category",StringType(),True),
    StructField("sales",StringType(),True),
    StructField("qualtity",StringType(),True),
    StructField("discount",StringType(),True),
    StructField("profit",StringType(),True),
    StructField("_corrupted_data",StringType(),True)
])
### Streaming Code
# df = spark.readStream.format("cloudFiles")\
#         .option("cloudFiles.format","csv")\
#         .option("cloudFiles.schemaLocation",schema_location)\
#         .option("cloudFiles.schemaEvolutionMode","rescue")\
#         .schema(csv_schema)\
#         .load(dataPath)

# df.writeStream.format("delta")\
#         .option("checkpointLocation",checkpoint)\
#         .outputMode("append")\
#         .trigger(once=True)\
#         .start("/Volumes/databricks_aws_lakehouse/bronze/bronze_volume/table/")


df = spark.read.format("csv")\
    .option("header","True")\
    .option("mode","PERMISSIVE")\
    .option("columnNameOfCorruptRecord", "_corrupt_record")\
    .schema(csv_schema)\
    .load(dataPath)

df = df.withColumn("ingest_ts",current_timestamp())


df.write.format("delta")\
    .option("mergeSchema","True")\
    .mode("overwrite")\
    .saveAsTable("databricks_aws_lakehouse.bronze.bz_superstore")



