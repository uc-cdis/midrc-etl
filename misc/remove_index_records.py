import json
import http.client
from os import remove

INPUT_FILE = "./indexd_guids.txt"

payload = ""
headers = {
    "cookie": "",
    "Authorization": "",
}


def get_indexd_record(guid):
    conn = http.client.HTTPSConnection("staging.midrc.org")
    conn.request("GET", f"/index/index/{guid}", payload)
    res = conn.getresponse()
    data = res.read()

    data = json.loads(data.decode("utf-8"))
    rev = data["rev"]
    return rev


def remove_indexd_record(guid, rev):
    conn = http.client.HTTPSConnection("staging.midrc.org")
    conn.request("DELETE", f"/index/index/{guid}?rev={rev}", payload, headers)

    res = conn.getresponse()
    data = res.read()

    print(data.decode("utf-8"))


def indexd_record_exist(guid):
    conn = http.client.HTTPSConnection("staging.midrc.org")
    conn.request("GET", f"/index/index/{guid}", payload)
    res = conn.getresponse()
    return True if res.status != 404 else False


def remove_file_fence(guid):
    conn = http.client.HTTPSConnection("staging.midrc.org")
    conn.request("DELETE", f"/user/data/{guid}", payload, headers)
    res = conn.getresponse()
    # data = res.read()
    print(res.status, res.reason)
    # print(data.decode("utf-8"))


if __name__ == "__main__":
    print("Hello!")

    with open(INPUT_FILE) as f:
        guids_to_remove = f.read().splitlines()

    for guid in guids_to_remove:
        # rev = get_indexd_record(guid)
        # remove_indexd_record(guid, rev)

        if indexd_record_exist(guid):
            remove_file_fence(guid)
