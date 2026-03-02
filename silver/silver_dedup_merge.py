from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window

spark.sql("USE CATALOG databricks_aws_lakehouse")
spark.sql("USE SCHEMA silver")

def deduplication_silver():
    bronze_df = spark.table("databricks_aws_lakehouse.bronze.bz_superstore")

    window_spec = (
        Window
        .partitionBy("ship_mode", "segment", "country", "city", "state", "postal_code", "region", "category")
        .orderBy(desc("ingest_ts"))
    )

    df_silver = (bronze_df.withColumn
                    (
                        "rn",row_number().over(window_spec)     
                    )
                    .filter(col("rn")==1)
                    .drop(col("rn"),col("_layer"),col("_source_format"))
                )
    return df_silver



def row_level_idempotency(df_silver):
    if not spark.catalog.tableExists("databricks_aws_lakehouse.silver.silver_enr"):
        print("silver_enr table not present droping duplicates & creating table")
        df_silver = df_silver.dropDuplicates()\
                            .write.mode("overwrite")\
                            .saveAsTable("databricks_aws_lakehouse.silver.silver_enr")
    else:
        print("Table silver_enr already present, Performing merge to table...")
        silver = DeltaTable.forName(spark, "databricks_aws_lakehouse.silver.silver_enr")  
        (
            silver.alias("t")
                  .merge(
                      df_silver.alias("s"),       
                        condition="""
                            t.ship_mode   = s.ship_mode AND
                            t.segment     = s.segment AND
                            t.country     = s.country AND
                            t.city        = s.city AND
                            t.state       = s.state AND
                            t.postal_code = s.postal_code AND
                            t.region      = s.region AND
                            t.category    = s.category
                        """
                      )
                  .whenMatchedUpdateAll
                  (
                      condition = "s.ingest_ts > t.ingest_ts"
                  )\
                  .whenNotMatchedInsertAll()\
                  .execute()
        )


df_silver = deduplication_silver()   
row_level_idempotency(df_silver)



    