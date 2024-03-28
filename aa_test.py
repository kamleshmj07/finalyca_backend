import requests

BASE_URL = 'http://localhost:5100'

obj = dict()
obj["mobile_number"] = 7900062448
obj["start_date"] = '2024-01-01'
obj["end_date"] = '2024-02-01'
resp = requests.post(F'{BASE_URL}/account_aggregator/perfios/consent_initiate', data=obj)
print(resp.status_code)
ret = resp.json()
print(ret)