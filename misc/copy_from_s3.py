#!/usr/bin/env python3

import argparse
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

import boto3.session
import tqdm


def download_file(s3: boto3.session.Session.resource, src_bucket: str, src_key: str):
    subfolders = "/".join(src_key.split("/")[:-1])
    os.makedirs(subfolders, exist_ok=True)
    s3.download_file(src_bucket, src_key, f"./{src_key}")


def copy_file(
    s3: boto3.session.Session.resource,
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
):
    copy_source = {"Bucket": src_bucket, "Key": src_key}
    dst_bucket_obj = s3.Bucket(dst_bucket)
    dst_bucket_obj.copy(copy_source, dst_key)


def move_file(
    s3: boto3.session.Session.resource,
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
):
    copy_file(
        s3,
        src_bucket=src_bucket,
        src_key=src_key,
        dst_bucket=dst_bucket,
        dst_key=dst_key,
    )

    obj = s3.Object(src_bucket, src_key)
    obj.delete()


def read_manifest(filename):
    with open(filename) as f:
        manifest = f.read().splitlines()
    return manifest


def run_command(func, manifest, num_workers):
    # List for storing possible failed downloads to retry later
    failed_downloads = []

    with tqdm.tqdm(desc="Processing...", total=len(manifest)) as pbar:
        with ThreadPoolExecutor(max_workers=1) as executor:
            # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
            futures = {
                executor.submit(
                    func, src_key=file_to_download, dst_key=file_to_download
                ): file_to_download
                for file_to_download in manifest
            }
            for future in as_completed(futures):
                if future.exception():
                    failed_downloads.append([futures[future]])
                pbar.update(1)

    if len(failed_downloads) > 0:
        print("Some processing have failed. Saving paths to csv...")
        with open("./failed_downloads.csv", "w", newline="\n") as csvfile:
            wr = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            wr.writerows(failed_downloads)


num_workers_parser = argparse.ArgumentParser(add_help=False)
num_workers_parser.add_argument(
    "--num-workers",
    action="store",
    default="4",
    type=str,
    required=False,
    help="",
)
s3_src_op_parser = argparse.ArgumentParser(add_help=False)
s3_src_op_parser.add_argument(
    "-s",
    "--src",
    action="store",
    type=str,
    required=True,
    help="",
)
s3_src_op_parser.add_argument(
    "-m",
    "--manifest",
    action="store",
    type=str,
    required=True,
    help="",
)
s3_dst_op_parser = argparse.ArgumentParser(add_help=False)
s3_dst_op_parser.add_argument(
    "-d",
    "--dest",
    action="store",
    type=str,
    required=True,
    help="",
)
parser = argparse.ArgumentParser(description="Process files between S3 buckets")
subparsers = parser.add_subparsers(help="", dest="command")

copy_parser = subparsers.add_parser(
    "copy",
    help="Copy files between S3 buckets",
    parents=[s3_src_op_parser, s3_dst_op_parser, num_workers_parser],
)
move_parser = subparsers.add_parser(
    "move",
    help="Move files between S3 buckets",
    parents=[s3_src_op_parser, s3_dst_op_parser, num_workers_parser],
)
download_parser = subparsers.add_parser(
    "download",
    help="Download files from S3 buckets to local machine",
    parents=[s3_src_op_parser],
)
args = parser.parse_args()


def main(args):
    s3 = boto3.resource("s3")

    if args.command == "copy":
        command = copy_file
    elif args.command == "move":
        command = move_file

    manifest = read_manifest(args.manifest)
    func = partial(command, s3=s3, src_bucket=args.src, dst_bucket=args.dest)

    run_command(func, manifest, args.num_workers)


if __name__ == "__main__":
    main(args)
