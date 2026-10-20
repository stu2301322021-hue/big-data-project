import base64
from datetime import datetime
from html import escape


def save_report(results, city_stats, timings, chart_paths, record_count, output_path):
  total_cents = sum(row["revenue_cents"] for row in results["revenue_by_city"])
  top_city = max(results["revenue_by_city"], key=lambda row: row["revenue_cents"])
  top_category = max(results["revenue_by_category"], key=lambda row: row["revenue_cents"])
  top_month = max(results["revenue_by_month"], key=lambda row: row["revenue_cents"])

  category_share = top_category["revenue_cents"] / total_cents * 100 if total_cents else 0

  conclusions = [
    (
      f"{top_city['city']} has the highest revenue: "
      f"€{top_city['revenue_cents'] / 100:,.2f}."
    ),
    (
      f"{top_category['category']} contributes "
      f"{category_share:.2f}% of total revenue."
    ),
    (
      f"The highest monthly revenue is in {top_month['month']}: "
      f"€{top_month['revenue_cents'] / 100:,.2f}."
    ),
  ]

  conclusions_html = "".join(
    f"<li>{escape(text)}</li>" for text in conclusions
  )

  stats_html = "".join(
    f"""<tr>
      <td>{escape(row["city"])}</td>
      <td>{row["sales_count"]:,}</td>
      <td>€{row["median_eur"]:,.2f}</td>
      <td>€{row["p90_eur"]:,.2f}</td>
    </tr>"""
    for row in city_stats
  )

  timing_labels = {
    "generation": "Data generation",
    "preparation": "Dask preparation",
    "spark_stage": "Spark stage — including startup and shutdown",
    "revenue_by_city": "Spark query: revenue by city",
    "revenue_by_category": "Spark query: revenue by category",
    "revenue_by_month": "Spark query: revenue by month",
    "units_by_category": "Spark query: units by category",
    "ray_city_statistics": "Ray stage — including loading and startup",
    "visualization": "Chart generation",
    "pipeline_total": "Pipeline total — before report generation",
  }

  timings_html = "".join(
    f"<tr><td>{escape(timing_labels.get(name, name))}</td>"
    f"<td>{seconds:.2f}</td></tr>"
    for name, seconds in timings.items()
  )

  charts_html = ""

  for name, path in chart_paths.items():
    encoded_image = base64.b64encode(path.read_bytes()).decode("ascii")

    charts_html += (
      f'<img src="data:image/png;base64,{encoded_image}" '
      f'alt="{escape(name.replace("_", " "))}">'
    )

  html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sales Analysis Report</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      max-width: 1100px;
      margin: 40px auto;
      padding: 0 24px;
      color: #243247;
      line-height: 1.6;
    }}
    h1, h2 {{ color: #16365d; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 24px;
    }}
    th, td {{
      padding: 10px;
      border-bottom: 1px solid #ddd;
      text-align: left;
    }}
    th {{ background: #eef3f8; }}
    img {{ width: 100%; height: auto; margin: 16px 0; }}
    .note {{ background: #f3f5f7; padding: 16px; }}
  </style>
</head>
<body>
  <h1>Sales Analysis Report</h1>
  <p>Generated: {datetime.now():%Y-%m-%d %H:%M:%S}</p>

  <h2>Overview</h2>
  <p>Analysis type: descriptive analysis.</p>
  <p>Valid records processed: <strong>{record_count:,}</strong></p>
  <p>Total revenue: <strong>€{total_cents / 100:,.2f}</strong></p>

  <h2>Conclusions</h2>
  <ul>{conclusions_html}</ul>

  <p class="note">
    These results describe synthetic data, not actual business activity.
    Similar city totals and category unit counts reflect uniform random
    selections in the generator. Revenue differences between categories
    reflect their configured price ranges. Monthly totals are also
    affected by the number of days in each month.
  </p>

  <h2>Visualizations</h2>
  {charts_html}

  <h2>Sale values by city</h2>
  <p>
    Median is the middle sale value. P90 is the estimated threshold
    at or below which approximately 90% of sale values fall.
  </p>
  <table>
    <tr>
      <th>City</th><th>Sales</th><th>Median</th><th>P90</th>
    </tr>
    {stats_html}
  </table>

  <h2>Execution times</h2>
  <table>
    <tr><th>Operation</th><th>Seconds</th></tr>
    {timings_html}
  </table>
  <p class="note">
    Individual Spark query timings are included within the Spark stage.
    Stage timings are included within the pipeline total; these rows
    must not all be added together.
    Ray timing includes data loading, startup, task execution and shutdown.
    The pipeline total excludes report generation.
    These measurements do not establish a speed comparison between
    frameworks because they perform different work.
  </p>

  <h2>Method</h2>
  <ol>
    <li>Faker generates synthetic sales in CSV format.</li>
    <li>Dask validates records, calculates sale totals and saves Parquet.</li>
    <li>PySpark groups and aggregates the prepared sales.</li>
    <li>Ray runs independent per-city statistical tasks.</li>
    <li>Matplotlib generates four visualizations.</li>
  </ol>
  <p>
    Computation runs locally. Ray's statistical stage loads the two
    required columns into memory; it is suitable for this dataset,
    but is not an out-of-core implementation.
  </p>
</body>
</html>
"""

  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_path.write_text(html, encoding="utf-8")

  return output_path