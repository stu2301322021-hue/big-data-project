from pathlib import Path
from time import perf_counter

from data_gen import generate_sales
from data_prep import prepare_sales
from data_analys import analyse_sales
from data_stats import run_city_stats
from data_visualization import save_charts

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
OUTPUT_DIR = PROJECT_DIR / "output"

NR_OF_RECORDS = 500_000

def main():
  pipeline_start = perf_counter()
  timings = {}

  raw_path = DATA_DIR / "sales.csv"
  prepared_path = DATA_DIR / "prepared_data"

  start = perf_counter()
  generate_sales(NR_OF_RECORDS, raw_path)
  timings["generation"] = perf_counter() - start

  start = perf_counter()
  record_count = prepare_sales(raw_path, prepared_path)
  timings["preparation"] = perf_counter() - start

  start = perf_counter()
  results, analysis_timings = analyse_sales(prepared_path)
  timings["spark_stage"] = perf_counter() - start
  timings.update(analysis_timings)

  city_stats, ray_elapsed = run_city_stats(prepared_path)
  timings["ray_city_statistics"] = ray_elapsed

  # Concistency check
  stats_count = sum(row["sales_count"] for row in city_stats)
  if stats_count != record_count:
    raise ValueError(f"Record count mismatch: Dask={record_count}, Ray={stats_count}")

  start = perf_counter()
  chart_paths = save_charts(results, OUTPUT_DIR / "charts")
  timings["visualization"] = perf_counter() - start

  timings["pipeline_total"] = perf_counter() - pipeline_start

  print(f"\nRecords processed: {record_count:,}")
  print(f"Charts saved: {len(chart_paths)}")

  for name, elapsed in timings.items():
    print(f"{name}: {elapsed:.2f} seconds")

if __name__ == "__main__":
  main()