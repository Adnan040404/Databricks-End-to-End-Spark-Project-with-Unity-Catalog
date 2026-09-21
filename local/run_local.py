"""
Run the pizza analytics pipeline on your own machine with PySpark. No Databricks needed.

    pip install -r local/requirements.txt      (needs Java 17 or 11)
    python local/run_local.py

Steps: extract (typed CSV reads) -> data-quality checks -> transform (seven questions) -> load (CSV).
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pizza_local.extract import ExtractError, read_all  # noqa: E402
from pizza_local.load import write_csv  # noqa: E402
from pizza_local.quality import DataQualityError, assert_ok, check  # noqa: E402
from pizza_local.session import get_spark  # noqa: E402
from pizza_local.transform import QUESTIONS, enrich  # noqa: E402


def main():
    t0 = time.perf_counter()
    spark = get_spark()
    try:
        frames = read_all(spark)
        print("[1/4] EXTRACT   " + ", ".join(f"{k}={v.count():,}" for k, v in frames.items()))

        results = check(frames)
        print("[2/4] QUALITY")
        for name, n in results:
            print(f"        {'ok  ' if n == 0 else 'FAIL'} {name}" + (f": {n}" if n else ""))
        assert_ok(results)

        df = enrich(frames).cache()
        print(f"[3/4] TRANSFORM {df.count():,} order lines joined")
        print("[4/4] LOAD")
        for name, fn in QUESTIONS.items():
            path, rows = write_csv(fn(df), name)
            print(f"        {name}: {rows} rows -> output/{path.name}")
        print(f"Done in {time.perf_counter() - t0:.1f}s")
        return 0
    except (ExtractError, DataQualityError) as exc:
        print(f"Cannot continue: {exc}", file=sys.stderr)
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
