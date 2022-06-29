#!/usr/bin/env python3

import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

import boto3
import tqdm


def copy_file(
    s3: boto3.session.Session.resource, src_bucket: str, dst_bucket: str, key: str
):
    # key = key.replace("s3://open-data-midrc/", "")
    copy_source = {"Bucket": src_bucket, "Key": key}
    bucket.copy(copy_source, key)
    # move_source = {"Bucket": src_bucket, "Key": key}
    # bucket.move(move_source, key)


INPUT_FILE = "./files.tsv"
# INPUT_FILE = "./raw_seq_imaging_data_manifest_2021_10_08.tsv"

s3 = boto3.resource("s3")
SRC_BUCKET = "external-data-midrc-replication"
# SRC_BUCKET = "internal-data-midrc-replication"
DST_BUCKET = "open-data-midrc"
# DST_BUCKET = "sequestered-data-midrc"
bucket = s3.Bucket(DST_BUCKET)

func = partial(copy_file, s3, SRC_BUCKET, DST_BUCKET)

with open(INPUT_FILE) as f:
    files_to_download = f.read().splitlines()

# List for storing possible failed downloads to retry later
failed_downloads = []

with tqdm.tqdm(desc="Copying...", total=len(files_to_download)) as pbar:
    with ThreadPoolExecutor(max_workers=16) as executor:
        # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
        futures = {
            executor.submit(func, file_to_download): file_to_download
            for file_to_download in files_to_download
        }
        for future in as_completed(futures):
            if future.exception():
                failed_downloads.append([futures[future]])
            pbar.update(1)

if len(failed_downloads) > 0:
    print("Some copies have failed. Saving paths to csv...")
    with open("./failed_downloads.csv", "w", newline="\n") as csvfile:
        wr = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        wr.writerows(failed_downloads)
