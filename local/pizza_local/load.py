"""LOAD: write each result to a CSV file.

Results are small, so they are brought to the driver and written with pandas. That also
avoids Spark's Hadoop file-writing layer, which needs extra native files on Windows.
"""

from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"


def write_csv(df, name, out_dir=None):
    out = Path(out_dir or OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    path = out / f"{name}.csv"
    pdf.to_csv(path, index=False)
    return path, len(pdf)
