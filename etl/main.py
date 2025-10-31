"""Command-line entry point for the ETL package."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from . import extract, load, transform


def run_pipeline(
    *,
    file_id: str = extract.DEFAULT_FILE_ID,
    raw_csv_path: str | Path = extract.DEFAULT_RAW_PATH,
    parquet_path: str | Path = transform.DEFAULT_PARQUET_PATH,
    creds_db_path: str | Path = load.DEFAULT_CREDS_DB,
    table_name: str = load.DEFAULT_TARGET_TABLE,
    schema: str = load.DEFAULT_SCHEMA,
    head_rows: int = 100,
    load_enabled: bool = True,
) -> pd.DataFrame:
    """Run extraction, transformation, validation, and loading stages sequentially."""

    print("=== ЭТАП: Extract ===")
    raw_df = extract.extract_dataset(
        file_id=file_id,
        raw_csv_path=raw_csv_path,
    )
    extract.preview_dataframe(raw_df)

    print("\n=== ЭТАП: Transform ===")
    transformed_df = transform.auto_convert_dataframe(raw_df)
    transform.save_dataframe_to_parquet(transformed_df, parquet_path)

    if load_enabled:
        if not Path(creds_db_path).exists():
            raise FileNotFoundError(
                "Файл с учетными данными не найден. Добавьте creds.db или запустите с флагом --skip-load."
            )

        print("\n=== ЭТАП: Load ===")
        load.run_upload_workflow(
            creds_db_path=creds_db_path,
            parquet_path=parquet_path,
            table_name=table_name,
            schema=schema,
            head_rows=head_rows,
            dataframe=transformed_df,
        )
    else:
        print("\n=== ЭТАП: Load (пропущен) ===")
        print("Флаг пропуска загрузки активирован, данные в БД не записываются.")

    return transformed_df


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ETL package CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute the full ETL pipeline")
    run_parser.add_argument("--file-id", default=extract.DEFAULT_FILE_ID, help="Google Drive file identifier")
    run_parser.add_argument(
        "--raw-csv-path",
        default=str(extract.DEFAULT_RAW_PATH),
        help="Path to store raw CSV (inside data/raw)",
    )
    run_parser.add_argument(
        "--parquet-path",
        default=str(transform.DEFAULT_PARQUET_PATH),
        help="Path to store processed Parquet (inside data/processed)",
    )
    run_parser.add_argument(
        "--creds-db-path",
        default=str(load.DEFAULT_CREDS_DB),
        help="Path to SQLite storage with database credentials",
    )
    run_parser.add_argument("--table-name", default=load.DEFAULT_TARGET_TABLE, help="Target table name in the database")
    run_parser.add_argument("--schema", default=load.DEFAULT_SCHEMA, help="Target schema in the database")
    run_parser.add_argument(
        "--head-rows",
        type=int,
        default=100,
        help="Maximum number of rows to upload to the database (<=100)",
    )
    run_parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip database load stage (useful when creds.db is unavailable)",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command != "run":
        parser.error("Неизвестная команда")

    head_rows = max(0, args.head_rows)

    run_pipeline(
        file_id=args.file_id,
        raw_csv_path=Path(args.raw_csv_path),
        parquet_path=Path(args.parquet_path),
        creds_db_path=Path(args.creds_db_path),
        table_name=args.table_name,
        schema=args.schema,
        head_rows=head_rows,
        load_enabled=not args.skip_load,
    )


if __name__ == "__main__":
    main()
