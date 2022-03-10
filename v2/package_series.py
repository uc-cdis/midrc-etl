import csv
import zipfile
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


def create_archive(file_urls, series_id):
    """
    Creates archive for package
    """
    archive = BytesIO()

    with zipfile.ZipFile(archive, "w") as zip_archive:
        for file_url in file_urls:
            file_obj = BytesIO()
            obj_path = "/".join(file_url.split("/")[3:])

            obj = src_bucket.Object(obj_path)
            obj.download_fileobj(file_obj)

            filename = file_url.split("/")[-1]
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
    study_id = line_split[1]
    series_id = line_split[2]
    series_id = series_id.rsplit(".", 1)[0]

    urls = []
    with open(package_file) as series_file:
        for line in series_file.readlines():
            urls.append(line.strip())

    zip_obj = create_archive(urls, series_id)

    size = zip_obj.getbuffer().nbytes

    zip_obj.seek(0)
    md5sum = md5(zip_obj.getbuffer())
    md5sum = md5sum.hexdigest()

    zip_file_name = "{}/{}.zip".format(study_id, series_id)
    zip_url = "zip/{}".format(zip_file_name)

    zip_obj.seek(0)
    dst_bucket.Object(zip_url).upload_fileobj(zip_obj)

    with open("packages/{}.txt".format(series_id), "w") as tsv_result_file:
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
                "package_contents": [
                    {
                        "hashes": {"md5sum": ""},
                        "file_name": "{}/{}".format(series_id, f.split("/")[-1]),
                        "size": -1,
                    }
                    for f in urls
                ],
            }
        )


def main():
    print("Hello World!")


if __name__ == "__main__":
    package_files = []
    with open("cases.txt") as cases_file:
        for line in cases_file.readlines():
            line = line.strip()
            package_files.append(line)
            # break

    result = pqdm(package_files, process_package_file, n_jobs=6)
