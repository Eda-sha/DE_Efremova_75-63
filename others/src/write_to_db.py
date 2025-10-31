from sqlalchemy import create_engine, text, inspect, Integer, String, Float, Boolean, BigInteger, select, func, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session, sessionmaker, declarative_base
import pandas as pd
import os
from dotenv import load_dotenv
import sqlite3

try:
    conn = sqlite3.connect('/content/creds.db')
    curs = conn.cursor()

    # Проверка целостности
    curs.execute("PRAGMA integrity_check")
    result = curs.fetchall()
    print("Результат проверки целостности:",result )

except Exception as e:
    print(f"Ошибка при проверке: {e}")

# структура таблицы access
print('\nОписание таблицы access:')
curs.execute('PRAGMA table_info(access);')
for col in curs.fetchall():
    print(col)

# учетка в access
curs.execute('SELECT url, port, user, pass FROM access LIMIT 1;')
row = curs.fetchone()
conn.close()

url, port, user, password = row
dbname = 'homeworks'

print('\nУчетные данные успешно считаны:')

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{url}:{port}/{dbname}",  # noqa
    pool_recycle=3600,
    # echo=True
)

print('\nЗагрузка датасета')
df = pd.read_parquet('/content/pharmacy_data.parquet')
print(f'Датасет успешно загружен. Всего строк: {len(df)}, столбцов: {len(df.columns)}')

print('\nПервые 50 строк для загрузки')
df = df.head(50)
print(f'Подготовлено {len(df)} строк для записи в БД')

table_name = 'efremova'
print(f'\nЗапись данных в таблицу "{table_name}" (схема public)')

df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False)

print(f'Таблица "{table_name}" успешно записана ({len(df)} строк)')

print('\nПроверка записи данных в таблицу')
with engine.connect() as conn:
    result = conn.execute(text(f'SELECT * FROM {table_name} LIMIT 3;'))
    rows = result.fetchall()

if rows:
    print(f'Таблица "{table_name}" существует. Пример строк:')
    for r in rows:
        print(r)
else:
    print('Таблица создана, но не удалось прочитать данные')

print('\nСкрипт завершен без ошибок')
