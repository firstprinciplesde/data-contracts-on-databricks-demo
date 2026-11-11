"""Silver to gold: one row per day with the order count and the revenue."""
from pyspark.sql import DataFrame, functions as F


def build_revenue_daily(silver: DataFrame) -> DataFrame:
    return (silver
            .groupBy(F.col("created_at").cast("date").alias("order_date"))
            .agg(F.count("*").alias("orders"),
                 F.sum("amount").cast("decimal(18,2)").alias("revenue")))
