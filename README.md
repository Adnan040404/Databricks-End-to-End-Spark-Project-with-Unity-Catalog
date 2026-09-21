# Pizza Sales Analytics with PySpark (Databricks and local)

A PySpark project that answers seven business questions about a pizza restaurant's
orders (48,620 order lines across 21,350 orders). It exists in two forms:

- **`local/`** runs on your own machine with PySpark. No Databricks account needed.
- **The notebooks** are the original Databricks version, using Unity Catalog and Delta
  tables. They need a Databricks workspace.

The data is a public pizza-sales dataset from Kaggle (four CSV files in `Datasets/`).
This is a learning project, not client work.

## Run it locally

You need Python 3.10+ and **Java 17** (PySpark 4 requires it; Java 11 works with PySpark 3.5).

```bash
pip install -r local/requirements.txt
python local/run_local.py
python -m pytest local/tests -q
```

`run_local.py` reads the four CSVs, checks the data, answers the seven questions and writes
one CSV per question to `local/output/`. On a laptop it takes under a minute.

```
[1/4] EXTRACT   orders=21,350, order_details=48,620, pizzas=96, pizza_types=32
[2/4] QUALITY   11 checks, all ok
[3/4] TRANSFORM 48,620 order lines joined
[4/4] LOAD      seven CSV files written
```

### What the local version adds over the notebooks

- **Data-quality checks before any analysis.** Keys must be unique, every foreign key must
  resolve, quantities and prices must be positive, and text must not be encoding-damaged.
  A failure lists exactly what is wrong. All 11 checks run as a single Spark job.
- **A real data problem it found.** `pizza_types.csv` is saved as Windows-1252, not UTF-8:
  the ingredient "Nduja Salami" contains a curly quote (byte 0x91). Read as UTF-8, Spark
  quietly replaces it with a garbage character. Spark 4 only accepts a few CSV charsets, so
  the file is read as ISO-8859-1 and the few characters where it differs from Windows-1252
  are mapped, and a check would catch the damage if it ever came back. (The seven questions don't
  use ingredients, so no answer was ever wrong.)
- **Declared schemas, not inferred ones.** A column that changes type fails at read time.
- **Exact money.** Price is a decimal, so revenue sums are exact.
- **Tests that check the answers.** Each of the seven Spark results is compared with an
  independent pandas calculation, with money compared in whole cents. There are also tests
  that show each quality rule catching a deliberately broken input.

### How the seven questions are defined

Carried over from the notebooks so the results are comparable:

- "Sales" in questions 1 to 4 is a **count of order lines**, not units. Questions 5 to 7 are
  money: price x quantity.
- `rank()` keeps ties, so a tie for first place returns every tied row.
- Question 1 ranks sizes within each chicken pizza and month (which size of each pizza sold
  most that month), not pizzas against each other.

| File in `local/output/` | Question | Rows |
|---|---|---|
| `q1_chicken_best_size_by_month` | Best-selling size of each chicken pizza, by month | 72 |
| `q2_top_pizza_per_category` | Most sold pizza in each category overall | 4 |
| `q3_top_pizza_5pm_to_10pm` | Most sold pizza in each hour from 5 pm to 10 pm | 6 |
| `q4_top_large_pizza_by_month` | Most sold large pizza, by month | 12 |
| `q5_small_pizza_revenue_by_category` | Revenue from small pizzas, by category | 30 |
| `q6_chicken_revenue_in_may` | Chicken pizza revenue in May | 6 |
| `q7_monthly_revenue_per_pizza` | Monthly revenue for every pizza | 384 |

### Layout

```
local/
  run_local.py            entry point
  pizza_local/
    session.py            local Spark session
    extract.py            typed CSV reads (declares the Windows-1252 file)
    quality.py            the 11 data-quality checks, run as one job
    transform.py          the seven questions
    load.py               writes the results to CSV
  tests/test_local.py     16 tests
  output/                 the seven result files
```

Results are written with pandas rather than Spark's own file writer, which avoids the
extra native Hadoop files Spark needs on Windows. The results are small, so this costs nothing.

## The original Databricks notebooks

### Read this before running the notebooks

The notebooks run only in a Databricks workspace with Unity Catalog enabled, and the notebooks
are not portable as they stand:

- They read from an Azure Data Lake path (`abfss://datalake@enterprisedatalake12...`)
  and `%run` other notebooks from a workspace folder under a different user's login.
  Neither belongs to you. Upload the CSVs from `Datasets/` to your own storage and
  change those paths, in the extractor, loader and table-creation notebooks, before
  running anything.
- I haven't re-run the notebooks in a fresh workspace for this README, so treat the
  saved outputs as the last run, not a fresh result.

## What the pipeline does

1. **Table creation.** Reads `orders.csv` with an explicit schema, derives an order hour
   and month, writes month-partitioned Parquet, and creates the catalog
   `pizza_data_analysis`.
2. **Extract.** An extractor class picks a reader (CSV or Delta) through a factory
   function and returns the four source DataFrames.
3. **Transform.** One method per business question, using joins, a broadcast join for
   the small lookup tables, and window functions to rank pizzas within a month or
   category.
4. **Load.** Loader classes write each result to a Delta table with `saveAsTable`,
   partitioned where it makes sense (for example by month).
5. **Verify.** A notebook of SQL queries re-derives several of the answers from the
   saved tables to check the pipeline output.
6. **Run.** The workflow notebook is the entry point that ties the steps together.

## The seven questions it answers

1. Most sold chicken pizza, month by month
2. Most sold pizza in every category overall
3. Most sold pizza between 5 pm and 10 pm
4. Most sold large pizza, month by month
5. Total sales of small pizzas in each category
6. Total chicken pizza sales in May
7. Month-by-month sales for every pizza, most sold first

## Why classes and factories

Each ETL step is a class with one job (`DataSourceExtractor`, `Transform`, `Loader`
subclasses). The factory functions choose the right reader or writer by type, so
adding a new source format or a new output table means adding a class instead of
editing the existing ones.

## Layout

```
Datasets/                     orders, order_details, pizzas, pizza_types (CSV)
Table_Creation_Notebooks/     partitioned Parquet and catalog creation
Factory_Pattern_Notebooks/    extractor, transform and loader factories
ETL_Notebooks/                extract, transform and load classes
Unit_Testing_Notebook/        SQL queries that verify the results
WorkFlow_Notebooks/           entry point notebook
```

## What's missing

- In the notebooks: data-quality steps and automated tests. The verification notebook is a
  set of queries to inspect, not assertions that fail. The `local/` version has both.

Muhammad Adnan, [LinkedIn](https://linkedin.com/in/muhammad-adnan-740336293),
adnandanish0404@gmail.com
