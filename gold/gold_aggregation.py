from pyspark.sql.functions import *
from delta.tables import DeltaTable

df_silver = spark.table("databricks_aws_lakehouse.silver.silver_enr")

df_gold = (
    df_silver
    .withColumn("sales_num", col("sales").cast("double"))
    .withColumn("quantity_num", col("quantity").cast("int"))
    .withColumn("discount_num", col("discount").cast("double"))
    .withColumn(
        "total_amount",
        col("sales_num") * col("quantity_num") * (1 - col("discount_num"))
    )
)

def upsert_gold_tables(df_gold , table_name , merge_key):

    spark.sql("USE CATALOG databricks_aws_lakehouse")
    spark.sql("USE SCHEMA gold")

    condition = " AND ".join([f"t.{key} = s.{key}" for key in merge_key])

    if not spark.catalog.tableExists(table_name):
        print(f"Table {table_name} does not exist. Creating a new table.")
        df_gold.write.mode("overwrite").saveAsTable(table_name)
    else:
        print(f"Table {table_name} exists. Updating the tabel.") 
        gold_table = DeltaTable.forName(spark, table_name)

        (
            gold_table.alias("t")
            .merge
            (
                df_gold.alias("s"),
                condition = condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

df_sales = df_gold.select("postal_code","city","ship_mode","total_amount").groupBy("postal_code","city","ship_mode").agg(sum("total_amount").alias("Total_state_sales"))

df_city = df_gold.select("ship_mode","segment","country","city","total_amount")\
            .groupBy("country","city")\
            .agg(sum("total_amount").alias("Total_city_sales"))

df_category = df_gold.select("category","total_amount").groupBy("category").agg(sum ("total_amount").alias("Total_category_sales"))

df_region = df_gold.select("region","total_amount").groupBy("region").agg(sum("total_amount").alias("Total_region_sales"))

upsert_gold_tables(df_sales,"gold_sales_aggregate",["postal_code","city","ship_mode"])
upsert_gold_tables(df_city,"gold_city_sales",["country","city"])
upsert_gold_tables(df_category,"gold_category_sales",["category"])
upsert_gold_tables(df_region,"gold_region_sales",["region"])
