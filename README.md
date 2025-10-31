# DE_Efremova_75-63

Репозиторий курса «Инжиниринг управления данными». Содержит законченный пример ETL-конвейера по подготовке и частичной загрузке набора о продажах фармпрепаратов, вспомогательные скрипты и материалы исследования данных.

## Источники данных
- Google Drive (рабочая копия датасета): https://drive.google.com/drive/folders/1CSZcGCgXEy_ZbNvHZgqwDmDw4ahwy8xe?usp=drive_link
- Исходный набор: https://www.kaggle.com/datasets/emrahaydemr/drug-sales-data

## Структура репозитория
- `etl/` — Python-пакет с реализацией стадий extract, transform и load. CLI-доступен через `python -m etl` (запускать из каталога `etl/`).
- `others/` — примеры, ноутбуки и вспомогательные скрипты.
- `pharmacy_data_convert.csv`, `pharmacy_data.parquet` — готовые артефакты после локального запуска.
- `requirements.txt` — перечень зависимостей проекта.

```text
DE_Efremova_75-63/
|-- etl/
|   |-- __init__.py
|   |-- __main__.py
|   |-- extract.py
|   |-- load.py
|   |-- main.py
|   `-- transform.py
|-- others/
|   |-- api_example/
|   |   |-- .gitignore
|   |   |-- api_reader.py
|   |   |-- jokes.csv
|   |   |-- pharmacy_data.parquet
|   |   |-- README.md
|   |   `-- requirements.txt
|   |-- notebooks/
|   |   `-- EDA_visual.ipynb
|   `-- src/
|       `-- write_to_db.py
|-- README.md
`-- requirements.txt
```

## Быстрый старт
```powershell
python -m venv .venv
.\.venv\Scripts\activate.ps1
pip install -r requirements.txt
```

### Самый быстрый запуск (без загрузки в БД)
```powershell
cd etl
python -m etl run --skip-load
```
Что произойдет:
- стадия Extract скачает CSV с Google Drive, провалидирует и сохранит его в `data/raw/pharmacy_data.csv`;
- стадия Transform автоматически приведет типы и сохранит Parquet в `data/processed/pharmacy_data.parquet`;
- стадия Load будет пропущена, но вы увидите консольный отчет по подготовленным данным.

### Полный запуск с загрузкой в PostgreSQL
1. Создайте файл `creds.db` (SQLite) рядом с репозиторием. Таблица `access` должна содержать поля `url TEXT`, `port INTEGER`, `user TEXT`, `pass TEXT`. Значения описывают подключение к вашей БД; имя базы по умолчанию `homeworks`.
2. Запустите конвейер:
```powershell
cd etl
python -m etl run --creds-db-path ..\creds.db --table-name efremova --schema public --head-rows 100
```
Стадия Load проверит целостность `creds.db`, создаст соединение через SQLAlchemy и перезапишет таблицу `public.efremova` максимум 100 строками.

### Доступные аргументы CLI
- `--file-id` — идентификатор Google Drive; по умолчанию берется рабочее зеркало датасета.
- `--raw-csv-path`, `--parquet-path` — пути для сохранения промежуточных файлов (каталоги `data/raw` и `data/processed` создаются автоматически).
- `--creds-db-path`, `--schema`, `--table-name` — параметры подключения к PostgreSQL.
- `--head-rows` — ограничение на число строк, выгружаемых в БД (жесткий лимит 100).
- `--skip-load` — пропустить стадию загрузки (полезно, если нет доступа к БД).

## Дополнительные компоненты
- **API-демо**. В каталоге `others/api_example` находится скрипт `api_reader.py`, который выгружает 10 шуток через https://official-joke-api.appspot.com/random_ten. Для запуска:
```powershell
cd others\api_example
python -m venv .venv
.\.venv\Scripts\activate.ps1
pip install -r requirements.txt
python api_reader.py
```
- **EDA**. Ноутбук `others/notebooks/EDA_visual.ipynb` демонстрирует первичный анализ данных; читайте локально или через nbviewer по ссылке выше.

## Примечания
- Каталоги `data/raw` и `data/processed` входят в `.gitignore` и создаются автоматически при первом запуске.
- Все команды в примерах даны для Windows PowerShell 5.1+. В других окружениях синтаксис активации виртуального окружения будет отличаться.
