import time
import json
import sys
import requests

url = "https://0zd2do1tq9.execute-api.us-west-1.amazonaws.com/default/hoddb_test"
d = json.load(open(sys.argv[1]))
start_time = time.time() * 1e3
resp = requests.post(url, json=d)
end_time = time.time() * 1e3
print(resp.json())
print(end_time - start_time)
