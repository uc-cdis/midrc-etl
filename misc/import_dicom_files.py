#!/usr/bin/env python3

import atexit
import base64
import json
import os
import os.path
import sys
import time
from datetime import datetime
from multiprocessing.pool import ThreadPool

import boto3
import httplib2
import jwt
import requests
from botocore.exceptions import ClientError


msg = ""


def exit_handler():
    with open("last.txt", "w") as f:
        if msg:
            f.writelines([msg, "\n"])


atexit.register(exit_handler)

print(sys.argv)

if len(sys.argv) != 7 and len(sys.argv) != 9:
    print(
        """
Sample script to recursively import in Orthanc all the DICOM files
that are stored in some path. Please make sure that Orthanc is running
before starting this script. The files are uploaded through the REST
API.

Use "--s3" if the data is in S3, otherwise the script defaults to local data.

Usage: %s [hostname] [DICOM server endpoint] [path to data] [path to API key] <--s3> <files.lst>
Usage: %s [hostname] [DICOM server endpoint] [path to data] [path to API key] [username] [password] <--s3> <files.lst>
For instance: %s qa-midrc.planx-pla.net dicom-server ./my-files/ ./credentials.json
"""
        % (sys.argv[0], sys.argv[0], sys.argv[0])
    )
    exit(-1)

POOL_SIZE = 6
URL = f"https://{sys.argv[1]}/{sys.argv[2]}/instances/"

dicom_count = 0
non_dicom_count = 0
total_file_count = 0
token = None
token_exp = None
basic_auth = None


class Timer:
    def __init__(self, name=None, message="Elapsed time: {:0.2f} seconds ({})") -> None:
        self.name = name
        self._start_time = None
        self.message = message

    def start(self) -> None:
        if self._start_time is None:
            self._start_time = time.perf_counter()
        else:
            raise Exception("Timer already started. Stop it first using .stop()")

    def stop(self) -> None:
        if self._start_time is None:
            raise Exception("Timer is not running. Start it first using .start()")
        else:
            elapsed_time = time.perf_counter() - self._start_time
            self._start_time = None
            print(self.message.format(elapsed_time, self.name))


def get_token(h):
    global token
    global token_exp
    if not token or not token_exp or token_exp < time.time():
        api_key_path = sys.argv[4]
        print(f"Attempting to get a new token with API key at {api_key_path}")
        with open(api_key_path, "r") as f:
            creds = json.load(f)
        token_url = f"https://{sys.argv[1]}/user/credentials/api/access_token"
        resp = requests.post(token_url, json=creds)
        if resp.status_code != 200:
            print(f"Unable to get access token: {resp.text}")
            raise Exception(resp.reason)
        token = resp.json()["access_token"]
        token_exp = jwt.decode(token, verify=False)["exp"]
    return token


def get_basic_auth():
    global basic_auth
    if not basic_auth:
        username = sys.argv[5]
        password = sys.argv[6]
        creds_str = username + ":" + password

        print(creds_str)

        creds_str_bytes = creds_str.encode("ascii")
        creds_str_bytes_b64 = b"Basic " + base64.b64encode(creds_str_bytes)
        basic_auth = creds_str_bytes_b64.decode("ascii")
    return basic_auth


# This function will upload a single file to Orthanc through the REST API
def upload_file(file_name, content):
    global dicom_count
    global non_dicom_count
    global total_file_count
    global msg

    total_file_count += 1

    timer = Timer(file_name)
    timer.start()

    if not file_name.lower().endswith(".dcm"):
        sys.stdout.write(f" => ignored non-DICOM file {file_name}\n")
        non_dicom_count += 1
        return

    h = httplib2.Http()
    headers = {"content-type": "application/dicom"}

    if len(sys.argv) == 9:  # use basic auth instead of token
        headers["authorization"] = get_basic_auth()
    else:
        headers["authorization"] = f"bearer {get_token(h)}"

    try:
        msg = f"{total_file_count}\t{file_name}"
        resp, content = h.request(URL, "POST", body=content, headers=headers)

        if resp.status == 200:
            sys.stdout.write(f" => success ({file_name})\n")
            dicom_count += 1
        else:
            sys.stdout.write(
                f" => failure ({file_name}) (Is it a DICOM file? Is there a password?) Details: {resp}\n"
            )

    except Exception as e:
        type, value, traceback = sys.exc_info()
        sys.stderr.write(str(e))
        sys.stderr.write(str(value))
        sys.stdout.write(
            f" => unable to connect (Is Orthanc running? Is there a password?) Details: {e}\n"
        )

    timer.stop()


def upload_from_s3(filekey, filebody):
    upload_file(filekey, filebody)


def upload_from_local(path):
    ROOT_DIR = os.path.abspath(sys.argv[3])

    file_path = os.path.join(ROOT_DIR, path)
    with open(file_path, "rb") as f:
        content = f.read()
        upload_file(file_path, content)


def send_webhook(message):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        response = requests.post(
            webhook_url,
            json={
                "text": message,
            },
        )


if __name__ == "__main__":
    place = None
    if os.path.exists("last.txt"):
        with open("last.txt") as f:
            place = f.readlines()[0].strip().split("\t")[1]
            last_filename = place.split("\t")[-1]
            print(f"skipping until {last_filename}")

    start = datetime.now()
    message = f"started submission of {sys.argv[-1]} at {start.isoformat()}"
    send_webhook(message=message)

    p = ThreadPool(POOL_SIZE)
    timer_all = Timer("Uploading all files")
    timer_all.start()

    if "--s3" in sys.argv:
        # with open("config.yml", "rt", encoding="utf8") as ymlfile:
        #     cfg = yaml.safe_load(ymlfile)

        # AWS_ACCESS_KEY_ID = cfg["aws"]["access_key"]
        # AWS_ACCESS_SECRET_ACCESS_KEY = cfg["aws"]["secret_key"]
        # AWS_STORAGE_BUCKET_NAME = cfg["aws"]["bucket"]

        s3_resource = boto3.resource(
            "s3",
            # aws_access_key_id=AWS_ACCESS_KEY_ID,
            # aws_secret_access_key=AWS_ACCESS_SECRET_ACCESS_KEY,
        )
        # bucket = s3_resource.Bucket(AWS_STORAGE_BUCKET_NAME)
        bucket = s3_resource.Bucket("external-data-midrc-replication")

        skip = False
        if place:
            skip = True

        with open(sys.argv[-1]) as list_of_files:
            for line in list_of_files.readlines():
                if place and line.strip() == place.strip():
                    skip = False

                if skip:
                    continue

                file = bucket.Object(key=line.strip())
                try:
                    p.apply(
                        upload_from_s3,
                        (
                            line.strip(),
                            file.get()["Body"].read(),
                        ),
                    )
                except ClientError as ex:
                    if ex.response["Error"]["Code"] == "NoSuchKey":
                        print(f"object not found: line.strip()")
                    else:
                        pass

        # files_collection = bucket.objects.filter(Prefix=sys.argv[3]).all()
        # for file in files_collection:
        #     p.map(upload_from_s3, file)
    else:
        for root, _, files in os.walk(sys.argv[3]):
            p.map(upload_from_local, files)

    timer_all.stop()

    if dicom_count + non_dicom_count == total_file_count:
        print(f"\nSUCCESS: {dicom_count} DICOM file(s) have been successfully imported")
    else:
        print(
            f"\nWARNING: Only {dicom_count} out of {total_file_count - non_dicom_count} file(s) have been successfully imported as DICOM instance(s)"
        )

    if non_dicom_count != 0:
        print(f"NB: {non_dicom_count} non-DICOM file(s) have been ignored")

    print("")

    end = datetime.now()
    duration = end - start
    duration_in_s = duration.total_seconds()
    days = duration.days
    days = divmod(duration_in_s, 86400)[0]
    hours = divmod(duration_in_s, 3600)[0]
    minutes = divmod(duration_in_s, 60)[0]

    message = f"ended submission of {sys.argv[-1]} at {end.isoformat()}\nruntime:\n\tdays: {days}\n\thours: {hours}\n\tminutes: {minutes}\n\tseconds: {duration_in_s}\n\t"
    send_webhook(message=message)
