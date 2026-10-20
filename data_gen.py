import csv
from datetime import date
from pathlib import Path
from time import perf_counter

from faker import Faker

CITIES = ["Sofia", "Plovdiv", "Varna", "Burgas", "Pernik"]

PRICE_RANGES = {
  "Electronics": (2000, 150000),
  "Clothing": (1000, 20000),
  "Home": (500, 50000),
  "Sports": (1000, 40000),
  "Books": (500, 6000),
}

FIELDNAMES = [
  "sale_id",
  "date",
  "city",
  "category",
  "quantity",
  "unit_price_cents",
]

def generate_sales(record_count, output_path):
  fake = Faker("bg_BG")
  fake.seed_instance(420)

  output_path.parent.mkdir(parents=True, exist_ok=True)

  with output_path.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
    writer.writeheader()

    for sale_id in range(1, record_count + 1):
      category = fake.random_element(
        elements=list(PRICE_RANGES)
      )

      min_price, max_price = PRICE_RANGES[category]

      sale = {
        "sale_id": sale_id,
        "date": fake.date_between(
          start_date=date(2025, 1, 1),
          end_date=date(2025, 12, 31),
        ).isoformat(),
        "city": fake.random_element(elements=CITIES),
        "category": category,
        "quantity": fake.random_int(min=1, max=5),
        "unit_price_cents": fake.random_int(
          min=min_price,
          max=max_price,
        ),
      }

      writer.writerow(sale)

if __name__ == "__main__":
  # Make self sustaining if called as a standalone
  data_dir = Path(__file__).resolve().parent / "data"
  output_path = data_dir / "sales.csv"
  record_count = 500_000

  start = perf_counter()
  generate_sales(record_count, output_path)
  elapsed = perf_counter() - start

  print(f"Generated {record_count:,} records in {elapsed:.2f} seconds")
  print(f"Saved to: {output_path.resolve()}")