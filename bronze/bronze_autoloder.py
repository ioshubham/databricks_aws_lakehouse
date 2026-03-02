from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType

# ── Paths ──
dataPath = "/Volumes/databricks_aws_lakehouse/bronze/bronze_volume/raw_data/SampleSuperstore.csv"
#                                                                             ^^^^^^^^^^^^^^^^^^^^^^^^^
#                                                                             exact file, not folder!

# ── Schema ──
csv_schema = StructType([
    StructField("ship_mode",      StringType(), True),
    StructField("segment",        StringType(), True),
    StructField("country",        StringType(), True),
    StructField("city",           StringType(), True),
    StructField("state",          StringType(), True),
    StructField("postal_code",    StringType(), True),
    StructField("region",         StringType(), True),
    StructField("category",       StringType(), True),
    StructField("sub_category",   StringType(), True),
    StructField("sales",          StringType(), True),
    StructField("quantity",       StringType(), True),  # ✅ fixed typo
    StructField("discount",       StringType(), True),
    StructField("profit",         StringType(), True),
    StructField("_corrupt_record",StringType(), True),  # ✅ standard name
])

# ── Read specific file ──
df = (spark.read
    .format("csv")
    .option("header",                    "True")
    .option("mode",                      "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(csv_schema)
    .load(dataPath)
)

# ── Verify count BEFORE writing ──
print(f"CSV row count: {df.count()}")   # Must print 9994

# ── Split good and bad rows ──
df_good = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
df_bad  = df.filter(df["_corrupt_record"].isNotNull())

# ── Add audit columns ──
df_bronze = df_good \
    .withColumn("ingest_ts",      current_timestamp()) \
    .withColumn("_source_format", lit("csv")) \
    .withColumn("_layer",         lit("bronze"))

# ── Write ──
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("databricks_aws_lakehouse.bronze.bz_superstore")
)

# ── Verify count AFTER writing ──
final = spark.table("databricks_aws_lakehouse.bronze.bz_superstore").count()
print(f"✅ Bronze table: {final} rows")   # Should be 9994

# ── Quarantine bad rows if any ──
bad_count = df_bad.count()
if bad_count > 0:
    print(f"⚠️ {bad_count} corrupt rows found")
    df_bad.write.format("delta").mode("append") \
        .saveAsTable("databricks_aws_lakehouse.bronze.bz_superstore_quarantine")
else:
    print("✅ No corrupt rows found")
