from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import ray

@ray.remote
def calculate_city_stats(city, values):
  return {
    "city": city,
    "sales_count": len(values),
    "median_eur": float(np.median(values)) / 100,
    "p90_eur": float(np.percentile(values, 90)) / 100,
  }

def run_city_stats(input_path):
  start = perf_counter()

  # Read only the two columns needed for these calcs
  sales = pd.read_parquet(input_path, columns=["city", "total_price_in_cents"])

  ray.init(num_cpus=2, include_dashboard=False)

  try:
    tasks = [
      calculate_city_stats.remote(city, group["total_price_in_cents"].to_numpy())
      for city, group in sales.groupby("city")
    ]

    results = ray.get(tasks)
  finally:
    ray.shutdown()

  elapsed = perf_counter() - start
  return results, elapsed

def main():
  input_path = Path(__file__).resolve().parent / "data" / "prepared_data"

  results, elapsed = run_city_stats(input_path)

  for result in results:
    print(
      f"{result['city']}: "
      f"{result['sales_count']:,} sales, "
      f"median €{result['median_eur']:,.2f}, "
      f"P90 €{result['p90_eur']:,.2f}"
    )

  print(f"Ray stage including loading and startup: {elapsed:.2f} seconds")

if __name__ == "__main__":
  main()