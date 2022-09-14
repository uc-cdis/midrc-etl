import os
import http.client
import json
import sys
from urllib.parse import urlparse

print("Inside python program")

url = os.environ.get("HOOKURL")
endpoint = urlparse(url).path

conn = http.client.HTTPSConnection("hooks.slack.com")

payload = {"text": "Your data ingestion workflow has completed."}
payload = json.dumps(payload)

headers = {"Content-Type": "application/json"}

conn.request("POST", endpoint, payload, headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))
