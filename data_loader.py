import pandas as pd
import numpy as np
import requests
import re
from io import StringIO

FILE_ID = "1Svje8GeeWe-hp_F-FNtnYZEGHWo1Lp-Y"
file_url = f"https://drive.google.com/uc?export=download&id={FILE_ID}"

response = requests.get(file_url)

raw_data = pd.read_csv(StringIO(response.text), sep=';', encoding='utf-8')

print("Форма данных:", raw_data.shape)
print("\nПервые 10 строк:")
print(raw_data.head(10))