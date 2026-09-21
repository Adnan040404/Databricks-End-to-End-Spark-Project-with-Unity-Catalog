"""EXTRACT: read the four CSV files with explicit schemas.

Schemas are declared, not inferred, so a column that changes type fails at read time
instead of silently turning a number into text. Price is a decimal, not a double, so
money sums are exact.
"""

from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

DATA_DIR = Path(__file__).resolve().parents[2] / "Datasets"

SCHEMAS = {
    "orders": T.StructType([
        T.StructField("order_id", T.IntegerType(), False),
        T.StructField("date", T.DateType(), False),
        T.StructField("time", T.StringType(), False),
    ]),
    "order_details": T.StructType([
        T.StructField("order_details_id", T.IntegerType(), False),
        T.StructField("order_id", T.IntegerType(), False),
        T.StructField("pizza_id", T.StringType(), False),
        T.StructField("quantity", T.IntegerType(), False),
    ]),
    "pizzas": T.StructType([
        T.StructField("pizza_id", T.StringType(), False),
        T.StructField("pizza_type_id", T.StringType(), False),
        T.StructField("size", T.StringType(), False),
        T.StructField("price", T.DecimalType(10, 2), False),
    ]),
    "pizza_types": T.StructType([
        T.StructField("pizza_type_id", T.StringType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("category", T.StringType(), False),
        T.StructField("ingredients", T.StringType(), True),
    ]),
}


# pizza_types.csv is saved as Windows-1252, not UTF-8: the ingredient "Nduja Salami" starts with
# a curly quote (byte 0x91). Read as UTF-8, Spark quietly replaces that byte with a garbage
# character, so the encoding has to be declared, and quality.py checks for the damage.
#
# Spark 4's CSV reader only accepts a few charsets (windows-1252 is not one of them). ISO-8859-1
# is accepted, and it differs from Windows-1252 only in the 0x80-0x9F range, so the file is read
# as ISO-8859-1 and those characters are then mapped to their Windows-1252 meaning.
ENCODINGS = {"pizza_types": "ISO-8859-1"}
CP1252_TEXT_COLUMNS = {"pizza_types": ["name", "category", "ingredients"]}


def _cp1252_fix(column):
    src, dst = [], []
    for byte in range(0x80, 0xA0):
        try:
            char = bytes([byte]).decode("cp1252")
        except UnicodeDecodeError:
            continue                     # five byte values are undefined in Windows-1252
        src.append(chr(byte))
        dst.append(char)
    return F.translate(column, "".join(src), "".join(dst))


class ExtractError(RuntimeError):
    pass


def read_all(spark, data_dir=None):
    """Return {"orders": df, "order_details": df, "pizzas": df, "pizza_types": df}."""
    data_dir = Path(data_dir or DATA_DIR)
    frames = {}
    for name, schema in SCHEMAS.items():
        path = data_dir / f"{name}.csv"
        if not path.exists():
            raise ExtractError(f"Missing input file: {path}")
        df = (spark.read.option("header", True).option("mode", "FAILFAST")
              .option("encoding", ENCODINGS.get(name, "UTF-8"))
              .schema(schema).csv(str(path)))
        for col in CP1252_TEXT_COLUMNS.get(name, []):
            df = df.withColumn(col, _cp1252_fix(F.col(col)))
        frames[name] = df
    return frames
