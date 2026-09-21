"""Every Spark answer is compared with an independent pandas calculation (money in exact
cents), and the data-quality rules are shown to catch deliberately broken inputs."""

import shutil
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

LOCAL = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LOCAL))

from pizza_local.extract import DATA_DIR, SCHEMAS, read_all  # noqa: E402
from pizza_local.load import write_csv  # noqa: E402
from pizza_local.quality import DataQualityError, assert_ok, check  # noqa: E402
from pizza_local.session import get_spark  # noqa: E402
from pizza_local.transform import QUESTIONS, enrich  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("pizza-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def frames(spark):
    return read_all(spark)


@pytest.fixture(scope="session")
def enriched(frames):
    return enrich(frames).cache()


# --------------------------------------------------------------- independent pandas reference
@pytest.fixture(scope="session")
def ref():
    o = pd.read_csv(DATA_DIR / "orders.csv")
    d = pd.read_csv(DATA_DIR / "order_details.csv")
    p = pd.read_csv(DATA_DIR / "pizzas.csv")
    t = pd.read_csv(DATA_DIR / "pizza_types.csv", encoding="cp1252")
    df = d.merge(p, on="pizza_id").merge(t, on="pizza_type_id").merge(o, on="order_id")
    df["Order_Month"] = pd.to_datetime(df["date"]).dt.month
    df["Order_Hour"] = df["time"].str[:2].astype(int)
    df["cents"] = (df["price"] * 100).round().astype(int) * df["quantity"]
    return df


def cents(series):
    return (series.astype(float) * 100).round().astype(int)


def rows(df, cols):
    return sorted(tuple(r) for r in df[cols].itertuples(index=False, name=None))


def top_ties(g, group_cols, value_col):
    """rank() with ties kept, as Spark's rank() does."""
    g = g.copy()
    g["r"] = g.groupby(group_cols)[value_col].rank(method="min", ascending=False)
    return g[g["r"] == 1]


def test_q1(enriched, ref):
    got = QUESTIONS["q1_chicken_best_size_by_month"](enriched).toPandas()
    g = (ref[ref["category"] == "Chicken"].groupby(["name", "size", "Order_Month"]).size()
         .reset_index(name="total_sales")
         .sort_values(["Order_Month", "name", "total_sales", "size"], ascending=[True, True, False, True])
         .groupby(["Order_Month", "name"]).head(1))
    assert rows(got, ["Pizza_Name", "Pizza_Size", "total_sales", "Order_Month"]) == \
        rows(g, ["name", "size", "total_sales", "Order_Month"])
    assert len(got) == 12 * ref[ref["category"] == "Chicken"]["name"].nunique()


def test_q2(enriched, ref):
    got = QUESTIONS["q2_top_pizza_per_category"](enriched).toPandas()
    g = top_ties(ref.groupby(["category", "size", "name"]).size().reset_index(name="n"), ["category"], "n")
    assert rows(got, ["Pizza_Name", "Pizza_Size", "Total_Sales", "Category"]) == \
        rows(g, ["name", "size", "n", "category"])


def test_q3(enriched, ref):
    got = QUESTIONS["q3_top_pizza_5pm_to_10pm"](enriched).toPandas()
    g = top_ties(ref.groupby(["Order_Hour", "name", "size"]).size().reset_index(name="n"), ["Order_Hour"], "n")
    g = g[(g["Order_Hour"] >= 17) & (g["Order_Hour"] <= 22)]
    assert rows(got, ["Pizza_Name", "Size", "Total_Sales", "Order_Hour"]) == \
        rows(g, ["name", "size", "n", "Order_Hour"])
    assert set(got["Order_Hour"]) <= set(range(17, 23))


def test_q4(enriched, ref):
    got = QUESTIONS["q4_top_large_pizza_by_month"](enriched).toPandas()
    g = top_ties(ref.groupby(["Order_Month", "name", "size"]).size().reset_index(name="n"),
                 ["Order_Month", "size"], "n")
    g = g[g["size"] == "L"]
    assert rows(got, ["Pizza_Name", "Order_Month", "Total_Sales"]) == rows(g, ["name", "Order_Month", "n"])


def test_q5_money_is_exact(enriched, ref):
    got = QUESTIONS["q5_small_pizza_revenue_by_category"](enriched).toPandas()
    got["c"] = cents(got["Total_Amount"])
    g = ref[ref["size"] == "S"].groupby(["category", "pizza_id"])["cents"].sum().reset_index()
    assert rows(got, ["Pizza_Code", "category", "c"]) == rows(g, ["pizza_id", "category", "cents"])


def test_q6_money_is_exact(enriched, ref):
    got = QUESTIONS["q6_chicken_revenue_in_may"](enriched).toPandas()
    got["c"] = cents(got["Total_Sales"])
    g = ref[(ref["Order_Month"] == 5) & (ref["category"] == "Chicken")].groupby("name")["cents"].sum().reset_index()
    assert rows(got, ["Pizza_Name", "c"]) == rows(g, ["name", "cents"])


def test_q7_money_is_exact_and_totals_match(enriched, ref):
    got = QUESTIONS["q7_monthly_revenue_per_pizza"](enriched).toPandas()
    got["c"] = cents(got["Total_Sales"])
    g = ref.groupby(["name", "Order_Month"])["cents"].sum().reset_index()
    assert rows(got, ["Pizza_Name", "Order_Month", "c"]) == rows(g, ["name", "Order_Month", "cents"])
    assert got["c"].sum() == ref["cents"].sum()             # nothing lost or double counted by the joins


def test_join_keeps_every_order_line(frames, enriched):
    assert enriched.count() == frames["order_details"].count()


# --------------------------------------------------------------- data quality
def test_real_data_passes_every_check(frames):
    results = check(frames)
    assert all(n == 0 for _, n in results), results
    assert_ok(results)


def make(spark, name, rows_):
    return spark.createDataFrame(rows_, SCHEMAS[name])


def good_frames(spark):
    return {
        "orders": make(spark, "orders", [(1, date(2015, 5, 1), "17:30:00")]),
        "order_details": make(spark, "order_details", [(1, 1, "a_s", 2)]),
        "pizzas": make(spark, "pizzas", [("a_s", "a", "S", Decimal("10.00"))]),
        "pizza_types": make(spark, "pizza_types", [("a", "The A", "Chicken", "x")]),
    }


def failing(results):
    return {name for name, n in results if n}


def test_a_clean_tiny_set_passes(spark):
    assert failing(check(good_frames(spark))) == set()


def test_every_quality_rule_catches_its_defect(spark):
    """One input with every defect injected at once. Each rule must flag its own problem.
    (A single combined check keeps the suite fast: each Spark job has a fixed startup cost.)"""
    frames = {
        "orders": make(spark, "orders", [
            (1, date(2015, 5, 1), "17:30:00"),
            (1, date(2015, 5, 2), "18:00:00"),           # duplicate order_id
            (2, date(2015, 5, 3), "99:30:00")]),         # unreadable hour
        "order_details": make(spark, "order_details", [
            (1, 1, "a_s", 2),
            (1, 1, "zzz", 0),                            # duplicate id, unknown pizza, quantity 0
            (3, 99, "a_s", 2)]),                         # unknown order
        "pizzas": make(spark, "pizzas", [
            ("a_s", "a", "S", Decimal("0.00")),          # price 0
            ("a_s", "missing", "S", Decimal("9.00"))]),  # duplicate id, unknown type
        "pizza_types": make(spark, "pizza_types", [
            ("a", "The A", "Chicken", "caf\ufffd"),      # encoding damage
            ("a", "The B", "Chicken", "x")]),            # duplicate type id
    }
    results = check(frames)
    assert all(n > 0 for _, n in results), [name for name, n in results if n == 0]
    assert len(results) == 11


def test_assert_ok_lists_what_failed(spark):
    f = good_frames(spark)
    f["order_details"] = make(spark, "order_details", [(1, 1, "a_s", 0)])
    with pytest.raises(DataQualityError, match="quantity <= 0: 1"):
        assert_ok(check(f))


def test_wrong_column_type_fails_at_read_time_not_later(spark, tmp_path):
    for name in SCHEMAS:
        shutil.copyfile(DATA_DIR / f"{name}.csv", tmp_path / f"{name}.csv")
    (tmp_path / "order_details.csv").write_text(
        "order_details_id,order_id,pizza_id,quantity\n1,1,a_s,many\n", encoding="utf-8")
    with pytest.raises(Exception):
        read_all(spark, tmp_path)["order_details"].collect()


# --------------------------------------------------------------- load
def test_write_csv_writes_all_rows(enriched, tmp_path):
    df = QUESTIONS["q7_monthly_revenue_per_pizza"](enriched)
    path, n = write_csv(df, "q7", tmp_path)
    assert path.exists() and n == df.count() == len(pd.read_csv(path))


# --------------------------------------------------------------- encoding
def test_pizza_types_is_windows_1252_and_is_read_correctly(frames):
    """The 0x91 byte in the source is a curly quote. Reading it as UTF-8 would corrupt it."""
    names = {r["pizza_type_id"]: r["ingredients"] for r in frames["pizza_types"].collect()}
    assert names["calabrese"].startswith("\u2018Nduja Salami")
    assert not any("\ufffd" in (v or "") for v in names.values())


def test_reading_it_as_utf_8_would_have_been_caught(spark):
    """Shows why the encoding is declared: the wrong one damages the text, and the check sees it."""
    wrong = (spark.read.option("header", True).option("encoding", "UTF-8")
             .schema(SCHEMAS["pizza_types"]).csv(str(DATA_DIR / "pizza_types.csv")))
    frames = good_frames(spark)
    frames["pizza_types"] = wrong
    assert "pizza types with encoding-damaged text" in failing(check(frames))
