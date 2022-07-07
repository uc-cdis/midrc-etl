"""
Packaging script
"""

import csv
import json
import zipfile
import os
from collections import defaultdict
from hashlib import md5
from io import BytesIO

import boto3
from pqdm.threads import pqdm

s3 = boto3.resource("s3")

SRC_BUCKET = "external-data-midrc-replication"
DST_BUCKET = "internal-data-midrc-replication"

src_bucket = s3.Bucket(SRC_BUCKET)
dst_bucket = s3.Bucket(DST_BUCKET)

series = defaultdict(list)

FOLDER = "/home/ubuntu/wd/output/RSNA_20220308"
# FOLDER = "./packages_acrimage/2021/09"
# FOLDER = "./packages_acrimage/2021/0827"


def create_archive(files, series_id):
    """
    Creates archive for package
    """
    archive = BytesIO()

    with zipfile.ZipFile(archive, "w") as zip_archive:
        for filename, file_url in files:
            file_obj = BytesIO()
            # obj_path = "/".join(file_url.split("/")[3:])
            obj_path = file_url

            obj = src_bucket.Object(obj_path)
            obj.download_fileobj(file_obj)

            # filename = file_url.split("/")[-1]
            in_zip_path = "{}/{}".format(series_id, filename)

            last_modified = obj.last_modified
            last_modified = tuple(last_modified.timetuple()[0:6])

            zinfo = zipfile.ZipInfo(filename=in_zip_path, date_time=last_modified)

            file_obj.seek(0)

            zip_archive.writestr(zinfo, file_obj.getvalue())

    return archive


def process_package_file(package_file):
    """
    Process package file
    """
    line_split = package_file.split("/")
    case_id = line_split[2]
    study_id = line_split[3]
    series_id = line_split[4]
    series_id = series_id.rsplit(".", 1)[0]

    files_metadata = []
    files = []
    with open(FOLDER + "/" + package_file, encoding="utf8") as series_file:
        reader = csv.DictReader(series_file, delimiter="\t")

        # file_name	file_size	md5sum	case_id	study_uid	series_id	storage_urls
        for row in reader:
            file_name = row["file_name"]
            url = row["storage_urls"].replace("s3://storage.ir.rsna.ai/", "")
            # for ACR
            # url = row["storage_urls"].replace("//", "replicated-data-acr/")
            # for ACR batch6
            # url = row["storage_urls"]
            # url = url.replace("//", "replicated-data-acr/")
            # url = url.replace("/0914/", "/10/batch6/")
            # print(url)

            files.append((file_name, url))

            files_metadata.append(
                {
                    "hashes": {"md5sum": row["md5sum"]},
                    "file_name": "{}/{}".format(series_id, file_name),
                    "size": row["file_size"],
                }
            )

    zip_obj = create_archive(files, series_id)

    size = zip_obj.getbuffer().nbytes

    zip_obj.seek(0)
    md5sum = md5(zip_obj.getbuffer())
    md5sum = md5sum.hexdigest()

    zip_file_name = "{}/{}/{}.zip".format(case_id, study_id, series_id)
    zip_url = "zip/{}".format(zip_file_name)

    zip_obj.seek(0)
    dst_bucket.Object(zip_url).upload_fileobj(zip_obj)

    with open(
        FOLDER + "/packages/{}.txt".format(series_id), "w", encoding="utf8"
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


def main():
    """Boilerplate for entrypoint for packaging script."""


if __name__ == "__main__":
    package_files = []
    with open(FOLDER + "/packages.txt", encoding="utf8") as cases_file:
        for line in cases_file.readlines():
            line = line.strip()
            package_files.append(line)
            # break

    os.makedirs(FOLDER + "/packages", exist_ok=True)

    result = pqdm(package_files, process_package_file, n_jobs=6)
