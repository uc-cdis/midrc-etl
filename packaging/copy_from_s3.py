#!/usr/bin/env python3

import csv, argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path

import boto3, tqdm

parser = argparse.ArgumentParser(
    description="Move package files to their respective buckets"
)
parser.add_argument(
    "--batch_dir",
    action="store",
    type=str,
    required=True,
    help="the batch directory containing indexd manifests like 'indexed/indexed_packages_open_${batch}.tsv'",
)
parser.add_argument(
    "--destination",
    action="store",
    type=str,
    required=True,
    help="Either 'open' or 'seq'.",
)
args = parser.parse_args()

"""
Trouble-shooting:

class Args:
    batch_dir="/home/ubuntu/wd/output/ACR_20220606"
    destination="open"
args=Args()

"""


def copy_file(
    s3: boto3.session.Session.resource, src_bucket: str, dst_bucket: str, key: str
):
    if args.destination == "open":
        key = key.replace("s3://open-data-midrc/", "")
    elif args.destination == "seq":
        key = key.replace("s3://sequestered-data-midrc/", "")
    copy_source = {"Bucket": src_bucket, "Key": key}
    bucket.copy(copy_source, key)


s3 = boto3.resource("s3")
# SRC_BUCKET = "external-data-midrc-replication"
SRC_BUCKET = "internal-data-midrc-replication"

## create input file
batch = args.batch_dir.split("/")[-1]
print("Processing batch '{}' in directory '{}'.".format(batch, args.batch_dir))

index_manifest = Path(
    "{}/indexed/indexed_packages_{}_{}.tsv".format(
        args.destination, args.batch_dir, batch
    )
)
assert (
    index_manifest.is_file()
), "Couldn't find the input index manifest file: {}".format(index_manifest)

df = pd.read_csv(index_manifest, sep="\t", header=0)
files_to_download = list(set(df.urls))
print(
    "Total of {} package files found in index manifest file:\n\t{}".format(
        len(df), index_manifest
    )
)

if args.destination == "open":
    DST_BUCKET = "open-data-midrc"
elif args.destination == "seq":
    DST_BUCKET = "sequestered-data-midrc"

bucket = s3.Bucket(DST_BUCKET)
func = partial(copy_file, s3, SRC_BUCKET, DST_BUCKET)

failed_downloads = []  # possible failed downloads to retry later
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
else:
    print("All {} copies were successful".format(len(files_to_download)))
