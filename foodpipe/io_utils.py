import csv
import shutil
from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame


def write_single_file(
    df: DataFrame, path: str | Path, fmt: Literal["json", "csv"]
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = path.parent / f".{path.name}.tmp"
    writer = df.coalesce(1).write.mode("overwrite").format(fmt)
    if fmt == "csv":
        writer = writer.option("header", "true")
    writer.save(str(temp_dir))
    part_files = list(temp_dir.glob("part-*"))
    if len(part_files) > 1:
        raise ValueError(
            f"Expected at most one part file, found {len(part_files)} in {temp_dir}"
        )
    if part_files:
        part_files[0].replace(path)
    elif fmt == "json":
        path.write_text("", encoding="utf-8")
    else:
        with path.open("w", encoding="utf-8", newline="") as output:
            csv.writer(output).writerow(df.columns)
    shutil.rmtree(temp_dir)
