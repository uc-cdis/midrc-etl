import http.client
import json

import pandas as pd
from pqdm.threads import pqdm


def get_access_token():
    conn = http.client.HTTPSConnection("staging.midrc.org")

    # payload = ""
    payload = json.dumps({"api_key": "", "key_id": ""})

    headers = {
        "Content-Type": "application/json",
    }

    conn.request("POST", "/user/credentials/api/access_token", payload, headers)

    res = conn.getresponse()
    data = res.read()

    return json.loads(data.decode("utf-8"))["access_token"]


def delete_dicom_patient_ids(patient_ids):
    conn = http.client.HTTPSConnection("staging.midrc.org")

    payload = {"Resources": patient_ids}
    payload = json.dumps(payload)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_access_token()}",
    }

    conn.request("POST", "/dicom-server/tools/bulk-delete", payload, headers)

    res = conn.getresponse()
    data = res.read()

    # print(data.decode("utf-8"))
    return data.decode("utf-8")


with open("./to_remove_dicom.lst") as f:
    list_to_remove = []
    for l in f.readlines():
        l = l.strip()
        list_to_remove.append(l)

    # print(get_access_token())

    pqdm(list_to_remove, lambda v: delete_dicom_patient_ids([v]), n_jobs=6)
