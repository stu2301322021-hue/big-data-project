import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
matplotlib.use("Agg")

def save_charts(results, output_dir):
  output_dir.mkdir(parents=True, exist_ok=True)

  chart_specs = [
    ("revenue_by_city", "city", "revenue_cents", "Revenue by city"),
    ("revenue_by_category", "category", "revenue_cents", "Revenue by category"),
    ("revenue_by_month", "month", "revenue_cents", "Monthly revenue"),
    ("units_by_category", "category", "units_sold", "Units sold by category"),
  ]

  chart_paths = {}

  for name, label_column, value_column, title in chart_specs:
    rows = results[name]

    labels = [row[label_column] for row in rows]
    values = [row[value_column] for row in rows]

    is_revenue = value_column == "revenue_cents"
    is_monthly = name == "revenue_by_month"

    if is_revenue:
      values = [value / 100 for value in values]

    fig, ax = plt.subplots(figsize=(12, 6))

    if is_monthly:
      ax.plot(labels, values, marker="o")

      ax.set_ylabel("Revenue (EUR)")
      ax.tick_params(axis="x", labelrotation=45)
      ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
      ax.grid(axis="y", alpha=0.25)

      # Automatic limits make month-to-month changes visible.
      ax.set_title(f"{title} — vertical axis does not start at zero")
    else:
      bars = ax.barh(labels, values)
      ax.invert_yaxis()

      value_labels = [
        f"€{value:,.2f}" if is_revenue else f"{value:,}"
        for value in values
      ]

      ax.bar_label(bars, labels=value_labels, padding=5)

      # Start at zero and leave room for the value labels.
      ax.set_xlim(0, max(values) * 1.35)
      ax.set_xlabel("Revenue (EUR)" if is_revenue else "Units sold")
      ax.xaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
      ax.grid(axis="x", alpha=0.25)
      ax.set_title(title)

    ax.set_axisbelow(True)
    fig.tight_layout()

    path = output_dir / f"{name}.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)

    chart_paths[name] = path

  return chart_paths