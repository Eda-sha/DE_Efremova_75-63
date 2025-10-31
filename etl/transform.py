"""Data transformation helpers for the ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from . import extract

_NUMERIC_THRESHOLD = 0.9
_DATE_THRESHOLD = 0.9
DEFAULT_PARQUET_PATH = Path("data/processed/pharmacy_data.parquet")


def _convert_object_series(series: pd.Series, *, numeric_threshold: float, date_threshold: float) -> pd.Series:
    """Attempt to convert a heterogeneous text series to numeric or datetime."""

    string_values = series.astype("string").str.strip()

    numeric_values = pd.to_numeric(string_values.str.replace(",", ".", regex=False), errors="coerce")
    if numeric_values.notna().mean() >= numeric_threshold:
        return numeric_values

    datetime_values = pd.to_datetime(string_values, errors="coerce", dayfirst=True)
    if datetime_values.notna().mean() >= date_threshold:
        return datetime_values

    return string_values


def auto_convert_dataframe(
    df: pd.DataFrame,
    *,
    numeric_threshold: float = _NUMERIC_THRESHOLD,
    date_threshold: float = _DATE_THRESHOLD,
    inplace: bool = False,
) -> pd.DataFrame:
    """Convert object columns to richer dtypes when enough values can be inferred."""

    target = df if inplace else df.copy()
    object_columns = target.select_dtypes(include=["object", "string"]).columns

    for column in object_columns:
        target[column] = _convert_object_series(
            target[column],
            numeric_threshold=numeric_threshold,
            date_threshold=date_threshold,
        )

    return target


def save_dataframe_to_parquet(
    df: pd.DataFrame,
    path: str | Path,
    *,
    engine: str = "fastparquet",
    index: bool = False,
    **kwargs,
) -> Path:
    """Persist a DataFrame to Parquet with the configured engine and return the path."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine=engine, index=index, **kwargs)
    return output_path


def auto_convert_source_dataset(
    *,
    file_id: str = extract.DEFAULT_FILE_ID,
    parquet_path: str | Path = DEFAULT_PARQUET_PATH,
    raw_csv_path: str | Path | None = None,
    numeric_threshold: float = _NUMERIC_THRESHOLD,
    date_threshold: float = _DATE_THRESHOLD,
    preview_columns: Iterable[str] | None = None,
    head: int = 5,
) -> pd.DataFrame:
    """Load the source CSV, convert column types, and persist it to Parquet."""

    raw_path = Path(raw_csv_path) if raw_csv_path is not None else extract.DEFAULT_RAW_PATH
    raw_df = extract.extract_dataset(file_id=file_id, raw_csv_path=raw_path)
    converted_df = auto_convert_dataframe(
        raw_df,
        numeric_threshold=numeric_threshold,
        date_threshold=date_threshold,
        inplace=False,
    )
    save_dataframe_to_parquet(converted_df, parquet_path)

    if preview_columns:
        print("Пример преобразованных столбцов:")
        print(converted_df.loc[:, list(preview_columns)].head(head))

    return converted_df


