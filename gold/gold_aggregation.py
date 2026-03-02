from pyspark.sql.functions import *

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


df_gold = df_gold.select("postal_code","city","ship_mode","total_amount").groupBy("postal_code","city","ship_mode").agg(sum("total_amount").alias("Total_state_sales"))

df_gold.write.mode("append").saveAsTable("databricks_aws_lakehouse.gold.gold_Sales_aggregate")

