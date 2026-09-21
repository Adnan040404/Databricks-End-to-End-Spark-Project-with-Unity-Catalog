# Databricks Spark Pipeline with Unity Catalog

A PySpark project on Databricks that answers seven business questions about a
pizza restaurant's orders. The ETL is written as small classes with factory
functions, and the results are saved as Delta tables in a Unity Catalog catalog.

The data is a public pizza-sales dataset from Kaggle (four CSV files in `Datasets/`).
This is a learning project, not client work.

## Read this before trying to run it

This runs only in a Databricks workspace with Unity Catalog enabled, and the notebooks
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

- A version that runs without Databricks. A local PySpark version would make this
  easy to run and test, and is the natural next step.
- Data-quality steps such as null and duplicate handling, and automated tests. The
  verification notebook is a set of queries to inspect, not assertions that fail.

Muhammad Adnan, [LinkedIn](https://linkedin.com/in/muhammad-adnan-740336293),
adnandanish0404@gmail.com
