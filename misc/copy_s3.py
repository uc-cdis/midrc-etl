import os

import boto3


def copy_file(s3, src_bucket, key):
    s3.download_file(src_bucket, key, "./" + key)


#INPUT_FILE = "./tsv_files_acr.txt"
INPUT_FILE = "./download.txt"

s3 = boto3.client("s3")
SRC_BUCKET = "external-data-midrc-replication"

with open(INPUT_FILE) as f:
    files_to_download = f.read().splitlines()

for file in files_to_download:
    print(file)
    subfolders = "/".join(file.split("/")[:-1])
    os.makedirs(subfolders, exist_ok=True)
    copy_file(s3, SRC_BUCKET, file)
