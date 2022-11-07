#!/usr/bin/env python3

"""
Packaging script
"""
import argparse
import re

import csv
import json
import zipfile
import os
from collections import defaultdict
from hashlib import md5
from io import BytesIO

import boto3
from pqdm.threads import pqdm

parser = argparse.ArgumentParser(description="Package a MIDRC batch submission")
parser.add_argument(
    "--batch_dir",
    action="store",
    type=str,
    required=True,
    help="the batch directory containing packages.txt",
)
args = parser.parse_args()

batch = args.batch_dir.split('/')[-1]
print("Processing batch '{}' in directory '{}'.".format(batch,args.batch_dir))

org = batch.split("_",1)[0] #org = batch.split("_",1)[0]
if org in ['ACR','RSNA']:
    SRC_BUCKET = "external-data-midrc-replication"
else:
    SRC_BUCKET = "midrcprod-default-813684607867-upload" # for non-RSNA/ACR uploads like TCIA
DST_BUCKET = "internal-data-midrc-replication"
s3 = boto3.resource("s3")
src_bucket = s3.Bucket(SRC_BUCKET)
dst_bucket = s3.Bucket(DST_BUCKET)

def read_packages_list():
    #series = defaultdict(list)
    os.makedirs("{}/packages".format(args.batch_dir), exist_ok=True)
    package_manifests = []
    with open("{}/packages.txt".format(args.batch_dir), encoding="utf8") as packages_list:
        for line in packages_list.readlines():
            line = line.strip()
            package_manifests.append(line)
    return package_manifests

def create_archive(files, series_uid):
    """
    Creates archive for package

    For trouble-shooting:
        filename = files[0][0]
        file_url = files[0][1]

    """
    archive = BytesIO()
    count,total = 0,len(files)
    with zipfile.ZipFile(archive, "w") as zip_archive:
        for filename, file_url in files:
            count+=1
            #print("Creating archive: {}/{}".format(count,total),end='\r')
            file_obj = BytesIO()
            obj_path = file_url
            obj = src_bucket.Object(obj_path)
            try:
                obj.download_fileobj(file_obj)
            except:
                print("Object failed to download: {}\n\t".format(filename,file_url))
            in_zip_path = "{}/{}".format(series_uid, filename)
            last_modified = obj.last_modified
            last_modified = tuple(last_modified.timetuple()[0:6])
            zinfo = zipfile.ZipInfo(filename=in_zip_path, date_time=last_modified)
            file_obj.seek(0)
            zip_archive.writestr(zinfo, file_obj.getvalue())
    return archive


def process_packages(package_manifest):
    """
    Process package file
    For trouble-shooting:
        package_manifest = package_manifests[0]
    """
    package_regex = re.compile(r'^.*cases\/(.*)\/(.*)\/(.*).tsv$')
    case_id, study_uid, series_uid = package_regex.match(package_manifest).groups()

    files_metadata = []
    files = []

    with open(package_manifest, encoding="utf8") as series_file:
        reader = csv.DictReader(series_file, delimiter="\t")
        for row in reader:
            file_name = row["file_name"]
            if org == "RSNA":
                url = row["storage_urls"].replace("s3://storage.ir.rsna.ai/", "")
            elif org == "ACR":
                url = row["storage_urls"].replace("//", "replicated-data-acr/")
                ## for ACR batch6
                # url = url.replace("/0914/", "/10/batch6/")
            else:
                url = row["storage_urls"].replace("[", "").replace("]", "").replace("'", "")
                url = url.replace("s3://midrcprod-default-813684607867-upload/", "")

            files.append((file_name, url))
            files_metadata.append(
                {
                    "hashes": {"md5sum": row["md5sum"]},
                    "file_name": "{}/{}".format(series_uid, file_name),
                    "size": row["file_size"],
                }
            )

    try:
        zip_obj = create_archive(files, series_uid)
    except:
        print("create_archive failed for {}".format(package_manifest))

    size = zip_obj.getbuffer().nbytes

    zip_obj.seek(0)
    md5sum = md5(zip_obj.getbuffer())
    md5sum = md5sum.hexdigest()

    zip_file_name = "{}/{}/{}.zip".format(case_id, study_uid, series_uid)
    zip_url = "zip/{}".format(zip_file_name)

    zip_obj.seek(0)
    dst_bucket.Object(zip_url).upload_fileobj(zip_obj)

    with open(
        "{}/packages/{}.txt".format(args.batch_dir,series_uid), "w", encoding="utf8"
    ) as tsv_result_file:
        fieldnames = [
            "record_type",
            "guid",
            "md5",
            "size",
            "authz",
            "url",
            "file_name",
            "package_contents",
        ]
        tsv_writer = csv.DictWriter(
            tsv_result_file, delimiter="\t", fieldnames=fieldnames
        )
        tsv_writer.writeheader()
        tsv_writer.writerow(
            {
                "record_type": "package",
                "guid": "",
                "md5": md5sum,
                "size": size,
                "authz": "",
                "url": zip_url,
                "file_name": zip_file_name,
                "package_contents": json.dumps(files_metadata),
            }
        )
    return {series_uid:len(files)}

def main():
    """
        Boilerplate for entrypoint for packaging script.
    """
    package_manifests = read_packages_list()
    result = pqdm(package_manifests, process_packages, n_jobs=6)
    results = {k: v for d in result for k, v in d.items()}
    total_packaged = sum(results.values())
    print("Total files packaged: {}".format(total_packaged))

if __name__ == "__main__":
    main()
