import requests
import pandas as pd
import json


url_api = "https://official-joke-api.appspot.com/random_ten"

response = requests.get(url_api)
    
if response.status_code == 200:
    answer = response.json()
else:
    print('Error:', response.status_code) 

if answer:
    data = pd.DataFrame(answer)
    data.to_csv("jokes.csv", index = False, sep='\t', header=True)

    print(data.info())
    print(data.head(10))