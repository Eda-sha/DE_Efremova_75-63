"""Data extraction helpers for the ETL pipeline."""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

GOOGLE_DRIVE_URL_TEMPLATE = "https://drive.google.com/uc?export=download&id={file_id}"
DEFAULT_FILE_ID = "1Svje8GeeWe-hp_F-FNtnYZEGHWo1Lp-Y"
DEFAULT_RAW_PATH = Path("data/raw/pharmacy_data.csv")


def build_download_url(file_id: str) -> str:
    """Return a direct download URL for a Google Drive file."""

    return GOOGLE_DRIVE_URL_TEMPLATE.format(file_id=file_id)


def download_csv_text(
    file_id: str,
    *,
    timeout: int = 30,
    session: Optional[requests.Session] = None,
) -> str:
    """Fetch the raw CSV payload from Google Drive."""

    url = build_download_url(file_id)
    http = session or requests
    response = http.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def load_remote_csv(
    file_id: str,
    *,
    sep: str = ";",
    encoding: str = "utf-8",
    timeout: int = 30,
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """Download a CSV file from Google Drive and return it as a DataFrame."""

    csv_text = download_csv_text(file_id, timeout=timeout, session=session)
    return pd.read_csv(StringIO(csv_text), sep=sep, encoding=encoding)


def preview_dataframe(df: pd.DataFrame, *, head: int = 10) -> None:
    """Print a compact summary of the dataset."""

    print("Форма данных:", df.shape)
    print(f"\nПервые {head} строк:")
    print(df.head(head))


def ensure_raw_directory(path: Path = DEFAULT_RAW_PATH) -> Path:
    """Ensure that the raw data directory exists and return the target path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def validate_raw_dataframe(df: pd.DataFrame) -> None:
    """Validate raw dataset sanity before further processing."""

    if df.empty:
        raise ValueError("Сырые данные пустые — дальнейшая обработка невозможна")


def save_raw_csv(
    df: pd.DataFrame,
    path: str | Path = DEFAULT_RAW_PATH,
    *,
    sep: str = ";",
    encoding: str = "utf-8",
) -> Path:
    """Persist raw dataset to CSV under data/raw/ directory."""

    target = ensure_raw_directory(Path(path))
    df.to_csv(target, sep=sep, encoding=encoding, index=False)
    return target


def extract_dataset(
    *,
    file_id: str = DEFAULT_FILE_ID,
    raw_csv_path: str | Path = DEFAULT_RAW_PATH,
    sep: str = ";",
    encoding: str = "utf-8",
    timeout: int = 30,
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """Full extract stage: download, basic validation, save raw CSV."""

    dataframe = load_remote_csv(
        file_id,
        sep=sep,
        encoding=encoding,
        timeout=timeout,
        session=session,
    )
    validate_raw_dataframe(dataframe)
    save_raw_csv(dataframe, path=raw_csv_path, sep=sep, encoding=encoding)
    return dataframe


