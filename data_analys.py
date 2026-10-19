from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import functions as pyspark_funcs

from time import perf_counter

from data_visualization import save_charts

INPUT_PATH = Path(__file__).resolve().parent / "data" / "prepared_data"

def get_revenue_by_city(sales):
  revenue_by_city = (
    sales.groupBy("city")
    .agg(pyspark_funcs.sum("total_price_in_cents").alias("revenue_cents"))
    .orderBy(pyspark_funcs.desc("revenue_cents"))
  )

  return revenue_by_city

def get_revenue_by_category(sales):
  revenue_by_category = (
    sales.groupBy("category")
    .agg(pyspark_funcs.sum("total_price_in_cents").alias("revenue_cents"))
    .orderBy(pyspark_funcs.desc("revenue_cents"))
  )

  return revenue_by_category

def get_revenue_by_month(sales):
  revenue_by_month = (
    # temp col to group all sales from the same month
    sales.withColumn("month", pyspark_funcs.date_format("date", "yyyy-MM"))
    .groupBy("month")
    .agg(pyspark_funcs.sum("total_price_in_cents").alias("revenue_cents"))
    .orderBy("month")
  )

  return revenue_by_month

def get_units_by_category(sales):
  units_by_category = (
    sales.groupBy("category")
    .agg(pyspark_funcs.sum("quantity").alias("units_sold"))
    .orderBy(pyspark_funcs.desc("units_sold"))
  )

  return units_by_category

ANALYSES_DICT = {
  "revenue_by_city": get_revenue_by_city,
  "revenue_by_category": get_revenue_by_category,
  "revenue_by_month": get_revenue_by_month,
  "units_by_category": get_units_by_category,
}

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

    results = {}
    timings = {}

    for name, analysis_func in ANALYSES_DICT.items():
      start = perf_counter()

      summary = analysis_func(sales)

      results[name] = summary.collect()
      timings[name] = perf_counter() - start

      print(f"{name}: {len(results[name])} summary rows, ")
      print(f"{timings[name]:.2f} seconds")

    charts_dir = Path(__file__).resolve().parent / "output" / "charts"

    chart_paths = save_charts(results, charts_dir)

    print(f"Saved {len(chart_paths)} charts to {charts_dir}")

  finally:
    spark.stop()

if __name__ == "__main__":
  main()