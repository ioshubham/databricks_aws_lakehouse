from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_silver_dataFrame():
    df_silver = spark.sql("select * from databricks_aws_lakehouse.bronze.bz_superstore")
    df_silver = df_silver.drop(col("_rescued_data"))
    return df_silver

def get_data_type_correct(df_silver):
    df_silver = df_silver.withColumn("sales",col("sales").cast("double"))\
                .withColumn("qualtity",col("qualtity").cast("double"))\
                .withColumn("discount",col("discount").cast("double"))\
                .withColumn("profit",col("profit").cast("double"))
    return df_silver

def count_of_nulls(df_silver):
    df_silver = df_silver.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df_silver.columns
        ]).show()
    return df_silver

def get_pk():
    # Check if any combination of columns is unique
    df = spark.table("databricks_aws_lakehouse.silver.silver_enr")

    # Check total rows vs distinct rows on suspected key columns
    total = df.count()

    # Try single columns first
    for col_name in df.columns:
        distinct = df.select(col_name).distinct().count()
        if distinct == total:
            print(f"✅ {col_name} is unique — can be PK")
        else:
            print(f"❌ {col_name} has {total - distinct} duplicates")


df_silver = get_silver_dataFrame()
df_silver = get_data_type_correct(df_silver)
df_silver = count_of_nulls(df_silver)

get_pk()







