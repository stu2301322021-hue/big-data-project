# big-data-project

# Big Data Sales Analysis

Python project for the Big Data course. Implements descriptive analysis of 500,000 synthetic sales records.

The project generates data, validates it, calculates sales statistics and produces four charts and an HTML report.

## Tools used

- Faker - generates sales records.
- Dask - validates the data, calculates sale totals and saves Parquet files.
- PySpark - calculates revenue by city, category and month, and units sold by category.
- Ray - calculates median and 90th-percentile sale values for each city using parallel tasks.
- Matplotlib - generates the charts.

All processing runs locally. The frameworks are used to meet the course requirements and demonstrate their different approaches to data processing.

## Setup

Developed and run on Windows with Git Bash, Python 3.11 and Java 17.

Java must be available on PATH. Check the installed versions:

```bash
python --version
java -version
```

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

Install the dependencies:

```bash
python -m pip install -r requirements.txt
```

When opening a new terminal, activate the existing environment again:

```bash
source .venv/Scripts/activate
```

## Run

From the project directory:

```bash
python main.py
```

This runs the complete pipeline:

1. Generate the sales CSV.
2. Validate and prepare the data with Dask.
3. Run the Spark analysis.
4. Calculate per-city statistics with Ray.
5. Generate the charts.
6. Generate the HTML report.

The number of generated records is configured through `NR_OF_RECORDS` in `main.py`. The default is 500,000.

Running the pipeline again overwrites the generated datasets, charts and report.

## Output

- `data/sales.csv` — generated sales.
- `data/prepared_data/` — prepared Parquet files.
- `output/charts/` — four PNG charts.
- `output/report.html` — report with conclusions, charts, city statistics, record count and execution times.

Open `output/report.html` in a browser. The chart images are embedded, so the report can be opened independently.

Generated datasets are excluded from Git and are recreated by running the pipeline.

## Source files

| File | Purpose |
|---|---|
| `main.py` | Runs the pipeline and collects timings |
| `data_gen.py` | Generates synthetic sales |
| `data_prep.py` | Validates and prepares the data |
| `data_analys.py` | Performs the Spark analysis |
| `data_stats.py` | Runs the Ray statistical tasks |
| `data_visualization.py` | Generates the charts |
| `data_report.py` | Generates the HTML report |

## Notes

Prices and sale totals are stored as integer cents. They are converted to euros for display.

The data is synthetic. Similar city totals and category quantities reflect the generator's uniform selections. Differences in category revenue also reflect the configured price ranges, rather than real customer behaviour.

Individual Spark query timings are included in the Spark stage timing. Stage timings are included in the pipeline total, which excludes report generation. These values should not all be added together.

The Ray stage loads the two required columns into memory before submitting the city tasks. This is suitable for the current dataset, but is not an out-of-core implementation.

On Windows, Spark may print warnings about missing native Hadoop libraries. The tested pipeline completes despite these warnings. Parquet file paths are listed through Python before being passed to Spark to avoid the directory-listing issue encountered on Windows.