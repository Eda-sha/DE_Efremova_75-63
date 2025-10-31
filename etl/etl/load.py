"""Data loading (to database) helpers for the ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DEFAULT_CREDS_DB = Path("creds.db")
DEFAULT_CREDS_TABLE = "access"
DEFAULT_DATABASE = "homeworks"
DEFAULT_PARQUET_PATH = Path("data/processed/pharmacy_data.parquet")
DEFAULT_TARGET_TABLE = "efremova"
DEFAULT_SCHEMA = "public"


@dataclass
class DbCredentials:
    """Connection parameters for the target PostgreSQL database."""

    host: str
    port: int
    user: str
    password: str
    database: str = DEFAULT_DATABASE


def _sqlite_connection(path: str | Path) -> Iterator[sqlite3.Connection]:
    """Yield a SQLite connection that is closed automatically."""

    connection = sqlite3.connect(str(path))
    try:
        yield connection
    finally:
        connection.close()


def check_sqlite_integrity(path: str | Path) -> list[tuple[str]]:
    """Return the result of the SQLite PRAGMA integrity_check."""

    with _sqlite_connection(path) as connection:
        cursor = connection.execute("PRAGMA integrity_check")
        return cursor.fetchall()


def describe_sqlite_table(path: str | Path, table: str) -> list[tuple]:
    """Return column metadata for the credential table."""

    with _sqlite_connection(path) as connection:
        cursor = connection.execute(f"PRAGMA table_info({table});")
        return cursor.fetchall()


def load_credentials_from_sqlite(
    path: str | Path,
    table: str = DEFAULT_CREDS_TABLE,
) -> DbCredentials:
    """Fetch connection credentials from the SQLite helper database."""

    with _sqlite_connection(path) as connection:
        cursor = connection.execute(f"SELECT url, port, user, pass FROM {table} LIMIT 1;")
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError("Не удалось получить учетные данные из таблицы access")

    host, port, user, password = row
    return DbCredentials(host=host, port=int(port), user=user, password=password)


def create_postgres_engine(
    credentials: DbCredentials,
    *,
    pool_recycle: int = 3600,
    echo: bool = False,
) -> Engine:
    """Build a SQLAlchemy engine for the remote PostgreSQL database."""

    connection_url = (
        f"postgresql+psycopg2://{credentials.user}:{credentials.password}"
        f"@{credentials.host}:{credentials.port}/{credentials.database}"
    )
    return create_engine(connection_url, pool_recycle=pool_recycle, echo=echo)


def load_parquet_dataset(path: str | Path) -> pd.DataFrame:
    """Load the prepared Parquet dataset."""

    return pd.read_parquet(path)


def write_dataframe_to_postgres(
    df: pd.DataFrame,
    engine: Engine,
    *,
    table_name: str,
    schema: str = DEFAULT_SCHEMA,
    if_exists: str = "replace",
    index: bool = False,
) -> None:
    """Persist the transformed dataset into PostgreSQL."""

    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=index)


def preview_inserted_rows(engine: Engine, table_name: str, *, schema: str = DEFAULT_SCHEMA, limit: int = 3) -> Sequence[tuple]:
    """Read back a few rows from the freshly populated table."""

    statement = text(f'SELECT * FROM "{schema}"."{table_name}" LIMIT :limit')
    with engine.connect() as connection:
        result = connection.execute(statement, {"limit": limit})
        return result.fetchall()


def run_upload_workflow(
    *,
    creds_db_path: str | Path = DEFAULT_CREDS_DB,
    parquet_path: str | Path = DEFAULT_PARQUET_PATH,
    table_name: str = DEFAULT_TARGET_TABLE,
    schema: str = DEFAULT_SCHEMA,
    head_rows: int = 100,
    dataframe: pd.DataFrame | None = None,
) -> None:
    """Mirror the behaviour of the original script with reusable utilities."""

    print("Проверка целостности базы с учетными данными...")
    integrity_result = check_sqlite_integrity(creds_db_path)
    print("Результат проверки целостности:", integrity_result)

    print("\nОписание таблицы access:")
    for column in describe_sqlite_table(creds_db_path, DEFAULT_CREDS_TABLE):
        print(column)

    credentials = load_credentials_from_sqlite(creds_db_path)
    print("\nУчетные данные успешно считаны")

    engine = create_postgres_engine(credentials)

    if dataframe is None:
        print("\nЗагрузка датасета")
        dataframe = load_parquet_dataset(parquet_path)
    else:
        print("\nИспользование предоставленного датафрейма для загрузки")
    print(
        f"Датасет успешно загружен. Всего строк: {len(dataframe)}, "
        f"столбцов: {len(dataframe.columns)}"
    )

    if head_rows:
        limited = min(head_rows, 100)
        dataframe = dataframe.head(limited)
        print(f"\nПервые {limited} строк для загрузки")
        print(f"Подготовлено {len(dataframe)} строк для записи в БД")

    if dataframe.empty:
        raise ValueError("После отсечения строк для загрузки не осталось данных")

    if len(dataframe) > 100:
        raise ValueError("Этап загрузки допускает максимум 100 строк")

    print(f"\nЗапись данных в таблицу \"{table_name}\" (схема {schema})")
    write_dataframe_to_postgres(dataframe, engine, table_name=table_name, schema=schema)
    print(f'Таблица "{table_name}" успешно записана ({len(dataframe)} строк)')

    print("\nПроверка записи данных в таблицу")
    rows = preview_inserted_rows(engine, table_name, schema=schema)

    if rows:
        print(f'Таблица "{table_name}" существует. Пример строк:')
        for row in rows:
            print(row)
    else:
        print("Таблица создана, но не удалось прочитать данные")

    print("\nСкрипт завершен без ошибок")
