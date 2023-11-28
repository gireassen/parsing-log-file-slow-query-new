import requests
import json
from database import (
    insert_data,
    insert_data_merge
)

f = open('slow_query/files/data_source.json', encoding='utf-8')
data_load = json.load(f)

url = data_load['urls']['url_1']

def get_data_and_save(url):
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Ошибка при выполнении запроса")

if __name__ == "__main__":

    data = get_data_and_save(url)

    for q in data.get('data', {}).get('result', []):
        metric = q.get('metric', {})
        value = q.get('value', ["NULL", "NULL"])

        insert_data_merge(
            client_addr=metric.get('client_addr', "NULL"), 
            pid=metric.get('pid', None),
            query=metric.get('query', "NULL"), 
            state=metric.get('state', "NULL"), 
            duration=value[0],
            quantity=value[1]
        )
