from pathlib import Path
from time import perf_counter

import dask.dataframe as dd

DATA_TYPES = {
  "sale_id": "int64",
  "date": "string",
  "city": "string",
  "category": "string",
  "quantity": "int64",
  "unit_price_cents": "int64",
}

BLOCK_SIZE_STRING = "4MB"

INPUT_PATH = Path(__file__).resolve().parent / "data" / "sales.csv"

def main():
  start = perf_counter()

  sales = dd.read_csv(
    str(INPUT_PATH),
    blocksize=BLOCK_SIZE_STRING,
    dtype=DATA_TYPES,
  )

  print(f"Partitions: {sales.npartitions}")

  record_count = sales.shape[0].compute()
  elapsed = perf_counter() - start

  print(f"Records: {record_count:,}")
  print(f"Read and count: {elapsed:.2f} seconds")

  # Convert the date string to datetime so invalid ones can become NaT
  sales["date"] = dd.to_datetime(
    sales["date"],
    format="%Y-%m-%d",
    errors="coerce",
  )

  # Dask overloads bitwise AND (&) to perform element-wise AND on boolean columns
  valid_rows = (
    sales["date"].notnull() &
    sales["city"].isin(["Sofia", "Plovdiv", "Varna", "Burgas", "Pernik"]) &
    sales["category"].isin(["Electronics", "Clothing", "Home", "Sports", "Books"]) & 
    (sales["sale_id"] > 0) &
    (sales["quantity"] > 0) &
    (sales["unit_price_cents"] > 0)
  )

  # create a separate DF object so we can filter without modifying the original sales object
  clean_sales = sales[valid_rows].copy()

  # make int types explicit before saving to parquet format
  # Dask(int64) == (Spark)long == 64-bit signed int
  clean_sales = clean_sales.astype({
    "sale_id": "int64",
    "quantity": "int64",
    "unit_price_cents": "int64",
  })

  clean_sales["total_price_in_cents"] = clean_sales["quantity"] * clean_sales["unit_price_cents"]

  valid_count = clean_sales.shape[0].compute()

  print(f"Valid records: {valid_count:,}")
  print(f"Rejected records: {record_count - valid_count:,}")
  # print(clean_sales.head())

  output_path = INPUT_PATH.parent / "prepared_data"
  save_start = perf_counter()

  clean_sales.to_parquet(
    str(output_path),
    engine="pyarrow",
    write_index=False, # don’t save the dataframe's row index as an extra column
    overwrite=True,    # replace the previous output in data/prepared_data when rerunning
  )

  save_elapsed = perf_counter() - save_start

  print(f"Prepared data saved to: {output_path}")
  print(f"Compute and save: {save_elapsed:.2f} seconds")

if __name__ == "__main__":
  main()