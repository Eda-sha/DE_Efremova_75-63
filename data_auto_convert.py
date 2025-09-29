import pandas as pd
import numpy as np
import fastparquet
import requests
from io import StringIO

FILE_ID = "1Svje8GeeWe-hp_F-FNtnYZEGHWo1Lp-Y"
file_url = f"https://drive.google.com/uc?export=download&id={FILE_ID}"

response = requests.get(file_url)

df = pd.read_csv(StringIO(response.text), sep=';', encoding='utf-8')
 

def auto_convert_data():
    
    for col in df.columns:
        if  df[col].dtype == object:
            s = df[col].astype('string').str.strip() 
            
            s_num = pd.to_numeric(s.str.replace(',', '.', regex=False), errors='coerce') 
            if s_num.notna().sum() / len(s) >= 0.9:
                df[col] = s_num  
                continue

        
            
            s_dt = pd.to_datetime(s, errors='coerce', dayfirst=True) 
            if s_dt.notna().sum() / len(s) >= 0.9: 
                df[col] = s_dt
                continue

            df[col] = s

    #df.to_csv('pharmacy_data_convert.csv', index=False, encoding='utf-8-sig', sep=';')
    df.to_parquet('pharmacy_data.parquet', index=False, engine='fastparquet')

if __name__ == "__main__": 
    auto_convert_data()

