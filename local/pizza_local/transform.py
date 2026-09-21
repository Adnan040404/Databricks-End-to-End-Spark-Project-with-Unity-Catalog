"""TRANSFORM: the seven business questions, implemented as the Databricks notebooks define them.

Definitions carried over from the notebooks (kept on purpose, so results are comparable):
  * "sales" for questions 1-4 is a COUNT OF ORDER LINES, not units. Questions 5-7 are money:
    price x quantity, summed exactly as decimals.
  * rank() keeps ties, so a tie for first place returns every tied row.
  * Question 1 ranks sizes within each chicken pizza and month (which SIZE of each pizza
    sold most that month), not pizzas against each other.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F


def enrich(frames):
    """Join the four tables into one row per order line, with order month and hour."""
    joined = (frames["order_details"]
              .join(F.broadcast(frames["pizzas"]), "pizza_id")          # 96 rows: broadcast, no shuffle
              .join(frames["pizza_types"], "pizza_type_id")
              .join(frames["orders"], "order_id"))
    return (joined
            .withColumn("Order_Month", F.month("date"))
            .withColumn("Order_Hour", F.substring("time", 1, 2).cast("int"))
            .withColumn("line_amount", F.col("price") * F.col("quantity")))


def q1_chicken_best_size_by_month(df):
    w = Window.partitionBy("Order_Month", "name").orderBy(F.desc("total_sales"), "size")
    return (df.groupBy("name", "size", "Order_Month", "category")
            .agg(F.count("order_id").alias("total_sales"))
            .withColumn("Rank", F.rank().over(w))
            .filter((F.col("category") == "Chicken") & (F.col("Rank") == 1))
            .select(F.col("name").alias("Pizza_Name"), F.col("size").alias("Pizza_Size"),
                    "total_sales", "Order_Month")
            .orderBy("Order_Month", F.desc("total_sales"), "Pizza_Name"))


def q2_top_pizza_per_category(df):
    w = Window.partitionBy("category").orderBy(F.desc("Total_Sales"))
    return (df.groupBy("category", "size", "name")
            .agg(F.count("order_id").alias("Total_Sales"))
            .withColumn("Rank", F.rank().over(w))
            .filter(F.col("Rank") == 1)
            .select(F.col("name").alias("Pizza_Name"), F.col("size").alias("Pizza_Size"),
                    "Total_Sales", F.col("category").alias("Category"))
            .orderBy("Category", "Pizza_Name"))


def q3_top_pizza_5pm_to_10pm(df):
    w = Window.partitionBy("Order_Hour").orderBy(F.desc("Total_Sales"))
    return (df.groupBy("Order_Hour", "name", "size")
            .agg(F.count("order_id").alias("Total_Sales"))
            .withColumn("Rank", F.rank().over(w))
            .filter((F.col("Rank") == 1) & (F.col("Order_Hour") > 16) & (F.col("Order_Hour") < 23))
            .select(F.col("name").alias("Pizza_Name"), F.col("size").alias("Size"),
                    "Total_Sales", "Order_Hour")
            .orderBy("Order_Hour", "Pizza_Name"))


def q4_top_large_pizza_by_month(df):
    w = Window.partitionBy("Order_Month", "size").orderBy(F.desc("Total_Sales"))
    return (df.groupBy("Order_Month", "name", "size")
            .agg(F.count("order_id").alias("Total_Sales"))
            .withColumn("Rank", F.rank().over(w))
            .filter((F.col("Rank") == 1) & (F.col("size") == "L"))
            .select(F.col("name").alias("Pizza_Name"), "Order_Month", "Total_Sales")
            .orderBy("Order_Month", "Pizza_Name"))


def q5_small_pizza_revenue_by_category(df):
    return (df.filter(F.col("size") == "S")
            .groupBy("category", "size", "pizza_id")
            .agg(F.round(F.sum("line_amount"), 2).alias("Total_Amount"))
            .select(F.col("pizza_id").alias("Pizza_Code"), "category", "Total_Amount")
            .orderBy("category", F.desc("Total_Amount"), "Pizza_Code"))


def q6_chicken_revenue_in_may(df):
    return (df.filter((F.col("Order_Month") == 5) & (F.col("category") == "Chicken"))
            .groupBy("name")
            .agg(F.round(F.sum("line_amount"), 2).alias("Total_Sales"))
            .select(F.col("name").alias("Pizza_Name"), "Total_Sales")
            .orderBy("Pizza_Name"))


def q7_monthly_revenue_per_pizza(df):
    return (df.groupBy("name", "Order_Month")
            .agg(F.round(F.sum("line_amount"), 2).alias("Total_Sales"))
            .select(F.col("name").alias("Pizza_Name"), "Order_Month", "Total_Sales")
            .orderBy("Order_Month", F.desc("Total_Sales"), "Pizza_Name"))


QUESTIONS = {
    "q1_chicken_best_size_by_month": q1_chicken_best_size_by_month,
    "q2_top_pizza_per_category": q2_top_pizza_per_category,
    "q3_top_pizza_5pm_to_10pm": q3_top_pizza_5pm_to_10pm,
    "q4_top_large_pizza_by_month": q4_top_large_pizza_by_month,
    "q5_small_pizza_revenue_by_category": q5_small_pizza_revenue_by_category,
    "q6_chicken_revenue_in_may": q6_chicken_revenue_in_may,
    "q7_monthly_revenue_per_pizza": q7_monthly_revenue_per_pizza,
}
