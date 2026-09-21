"""Data-quality checks that run before any analysis.

The original notebooks went straight from reading files to answering questions. Here the
inputs are checked first: keys must be unique, every foreign key must resolve, values must
be sensible, and text must not be encoding-damaged. A failure lists exactly what is wrong
instead of producing quietly wrong answers.

All checks are combined into one Spark job. Running each as its own job costs a job launch
per check, which dominates the time on small data.
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REPLACEMENT_CHAR = "�"   # what a decoder writes when it meets bytes it can't read


class DataQualityError(RuntimeError):
    pass


def _dupes(df, col):
    return df.groupBy(col).count().filter(F.col("count") > 1)


def _orphans(child, child_col, parent, parent_col):
    keys = parent.select(F.col(parent_col).alias("_k")).distinct()
    return child.join(keys, child[child_col] == F.col("_k"), "left_anti")


def _problems(frames):
    """Ordered list of (check name, DataFrame holding the offending rows)."""
    o, d, p, t = frames["orders"], frames["order_details"], frames["pizzas"], frames["pizza_types"]
    hour = F.substring("time", 1, 2).cast("int")
    damaged = (F.col("name").contains(REPLACEMENT_CHAR)
               | F.coalesce(F.col("ingredients"), F.lit("")).contains(REPLACEMENT_CHAR))
    return [
        ("duplicate order_id in orders", _dupes(o, "order_id")),
        ("duplicate order_details_id", _dupes(d, "order_details_id")),
        ("duplicate pizza_id in pizzas", _dupes(p, "pizza_id")),
        ("duplicate pizza_type_id in pizza_types", _dupes(t, "pizza_type_id")),
        ("order lines whose order does not exist", _orphans(d, "order_id", o, "order_id")),
        ("order lines whose pizza does not exist", _orphans(d, "pizza_id", p, "pizza_id")),
        ("pizzas whose type does not exist", _orphans(p, "pizza_type_id", t, "pizza_type_id")),
        ("order lines with quantity <= 0", d.filter(F.col("quantity") <= 0)),
        ("pizzas with price <= 0", p.filter(F.col("price") <= 0)),
        ("orders with an unreadable hour", o.filter(hour.isNull() | (hour < 0) | (hour > 23))),
        ("pizza types with encoding-damaged text", t.filter(damaged)),
    ]


def check(frames):
    """Return a list of (check name, problem count). A count of 0 means the check passed."""
    problems = _problems(frames)
    counted = [df.agg(F.count(F.lit(1)).alias("n")).select(F.lit(name).alias("check"), "n")
               for name, df in problems]
    rows = {r["check"]: r["n"] for r in reduce(DataFrame.unionByName, counted).collect()}
    return [(name, int(rows[name])) for name, _ in problems]


def assert_ok(results):
    failed = [(name, n) for name, n in results if n]
    if failed:
        raise DataQualityError("Data quality checks failed: "
                               + "; ".join(f"{name}: {n}" for name, n in failed))
