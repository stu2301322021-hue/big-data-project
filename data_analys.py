from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import functions as pyspark_funcs

INPUT_PATH = Path(__file__).resolve().parent / "data" / "prepared_data"

def print_revenue_by_city(sales):
  revenue_by_city = (
    sales.groupBy("city")
    .agg(pyspark_funcs.sum("total_price_in_cents").alias("revenue_cents"))
    .orderBy(pyspark_funcs.desc("revenue_cents"))
  )

  print("Revenue by city:")
  revenue_by_city.show()

def print_revenue_by_category(sales):
  revenue_by_category = (
    sales.groupBy("category")
    .agg(pyspark_funcs.sum("total_price_in_cents").alias("revenue_cents"))
    .orderBy(pyspark_funcs.desc("revenue_cents"))
  )

  print("Revenue by category:")
  revenue_by_category.show()

def print_revenue_by_month(sales):
  revenue_by_month = (
    # temp col to group all sales from the same month
    sales.withColumn("month", pyspark_funcs.date_format("date", "yyyy-MM"))
    .groupBy("month")
    .agg(pyspark_funcs.sum("total_price_in_cents").alias("revenue_cents"))
    .orderBy("month")
  )

  print("Revenue by month:")
  revenue_by_month.show()

def print_units_by_category(sales):
  units_by_category = (
    sales.groupBy("category")
    .agg(pyspark_funcs.sum("quantity").alias("units_sold"))
    .orderBy(pyspark_funcs.desc("units_sold"))
  )

  print("Units sold by category:")
  units_by_category.show()

def main():
  spark = (
    SparkSession.builder
    .appName("UniSalesAnalysis")
    .master("local[2]") # two worker threads
    .getOrCreate()
  )

  try:
    # spark.sparkContext.setLogLevel("WARN")
    # Stackoverflow solution to loading parquet files on windows
    parquet_files = [
      path.as_uri()
      for path in sorted(INPUT_PATH.glob("*.parquet"))
    ]

    if not parquet_files:
      raise FileNotFoundError(f"No Parquet files found in {INPUT_PATH}")

    sales = spark.read.parquet(*parquet_files)
    # sales.printSchema()
    print_revenue_by_city(sales)
    print_revenue_by_category(sales)
    print_revenue_by_month(sales)
    print_units_by_category(sales)
    
  finally:
    spark.stop()

if __name__ == "__main__":
  main()