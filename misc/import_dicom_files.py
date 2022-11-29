#!/usr/bin/env python3

import argparse
import json
import logging
import os
import os.path
import sys
import time
from datetime import datetime
from functools import partial
from urllib.parse import urlparse, urlunparse
from datetime import datetime
from botocore.exceptions import ClientError

import boto3
import jwt
import requests
from pqdm.threads import pqdm

POOL_SIZE = 1
TOKEN = None
TOKEN_EXP = None

parser = argparse.ArgumentParser(description="Import S3 data to DICOM Server")
parser.add_argument(
    "--dicom-server-endpoint",
    action="store",
    type=str,
    required=True,
    help="DICOM Server endpoint",
)
parser.add_argument(
    "--credentials",
    action="store",
    type=str,
    required=True,
    help="Path to file with credentials",
)
parser.add_argument(
    "--filelist",
    action="store",
    type=str,
    required=True,
    help="Path to file with list of files to submit",
)
args = parser.parse_args()

logging.basicConfig(
    level=logging.INFO,
    filename="app_{:%Y-%m-%dT%H-%M-%S}.log".format(datetime.now()),
    filemode="w",
    format="%(name)s - %(levelname)s - %(message)s",
)


def read_token(credentials_file):
    with open(credentials_file, "r") as f:
        creds = json.load(f)
    return creds


def get_token(access_token_url, credentials):
    """
    Get the access token.
    """
    global TOKEN
    global TOKEN_EXP
    if not TOKEN or not TOKEN_EXP or TOKEN_EXP < time.time():
        resp = requests.post(access_token_url, json=credentials)
        if resp.status_code != 200:
            logging.error("Unable to get access token: %s", resp.text)
            raise Exception(resp.reason)
        TOKEN = resp.json()["access_token"]
        TOKEN_EXP = jwt.decode(TOKEN, options={"verify_signature": False})["exp"]
    return TOKEN


def upload_file(
    dicom_server_endpoint, access_token_endpoint, credentials, bucket, filekey
):
    try:
        filebody = bucket.Object(key=filekey).get()["Body"].read()

        if not filekey.lower().endswith(".dcm"):
            logging.error("non-DICOM file: %s", filekey)
            return

        headers = {"Content-Type": "Application/DICOM"}
        headers[
            "Authorization"
        ] = f"Bearer {get_token(access_token_endpoint, credentials)}"

        try:
            resp = requests.post(dicom_server_endpoint, data=filebody, headers=headers)
            if resp.status_code == 200:
                logging.info("success %s", filekey)
            else:
                logging.error("%s: %s %s", filekey, resp.status_code, resp.reason)

        except Exception as e:
            type, value, traceback = sys.exc_info()
            sys.stderr.write(str(e))
            sys.stderr.write(str(value))
            sys.stdout.write(
                f" => unable to connect (Is Orthanc running? Is there a password?) Details: {e}\n"
            )
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchKey":
            logging.info(f"No object found: {filekey}")
            return
        else:
            raise


def send_webhook(message):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        _ = requests.post(
            webhook_url,
            json={
                "text": message,
            },
        )


def main(args):
    """
    Entrypoint for the script.
    """
    parsed_url = urlparse(args.dicom_server_endpoint)

    access_token_endpoint = urlunparse(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            "/user/credentials/api/access_token",
            "",
            "",
            "",
        )
    )

    credentials = read_token(args.credentials)

    start = datetime.now()
    message = f"started submission of {args.filelist} at {start.isoformat()}"
    send_webhook(message=message)

    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket("external-data-midrc-replication")

    files = []

    with open(args.filelist, "r") as list_of_files:
        for line in list_of_files.readlines():
            line = line.strip()
            files.append(line)

    upload = partial(
        upload_file,
        args.dicom_server_endpoint,
        access_token_endpoint,
        credentials,
        bucket,
    )

    _ = pqdm(files, upload, n_jobs=POOL_SIZE)

    end = datetime.now()
    duration = end - start
    duration_in_s = duration.total_seconds()
    days = duration.days
    days = divmod(duration_in_s, 86400)[0]
    hours = divmod(duration_in_s, 3600)[0]
    minutes = divmod(duration_in_s, 60)[0]

    message = f"""ended submission of {args.filelist} at {end.isoformat()}
runtime:
    days: {days}
    hours: {hours}
    minutes: {minutes}
    seconds: {duration_in_s}
"""
    send_webhook(message=message)


if __name__ == "__main__":
    main(args)
