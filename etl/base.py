import pandas as pd
import csv
import yaml
import createZipFiles as packaging
from urllib.parse import urlparse
import os
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.StreamHandler()
logger = logging.getLogger(__name__)
with open("config.yml", "rt", encoding="utf8") as ymlfile:
    cfg = yaml.safe_load(ymlfile)


def main():

    dataframe = pd.read_csv(
        cfg["manifest"]["file_name"],
        sep="\t",
        header=0,
        low_memory=False,
        usecols=[
            "file_name",
            "file_size",
            "md5sum",
            "acl",
            "storage_urls",
            "series_uid",
            "study_uid",
            "case_ids",
        ],
    )
    # output file to store package output
    output_file = "output/output.tsv"
    # create output file to store package contents
    with open(
        output_file, "w"
    ) as tsvfile:  # "w" open file for writing and reading plain text, create a new file if not exists or truncate the file if exists.
        writer = csv.writer(tsvfile, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "record_type",
                "md5",
                "size",
                "authz",
                "url",
                "file_name",
                "package_contents",
            ]
        )
    create_outputtsv(dataframe, writer)


# parse s3 url to get bucket name and file path
def split_s3_path(s3_path):
    path_parts = urlparse(s3_path)
    folder_path = path_parts.path  # contans file path
    return os.path.dirname(folder_path.lstrip("/"))  # return directory name


# to populate package_contents section in the output. package_contents contains details about individual files( like, size, md5sum and file_name)
def package_contents(group_dataframe):
    package_contents = []
    for row_index, row in group_dataframe.iterrows():
        size = row["file_size"]
        md5 = row["md5sum"]
        file_name = row["file_name"]
        row_entry = {"hashes": {"md5sum": md5}, "file_name": file_name, "size": size}
        package_contents.append(row_entry)
    return package_contents


# to populate output tsv files
def create_outputtsv(df, writer):
    # package_attribute determines which attribute is used for packaging
    grouped = df.groupby(cfg["manifest"]["package_attribute"])
    for group_key, group_value in grouped:
        storage_url = df.loc[df["series_uid"] == group_key, "storage_urls"].values[0]
        project_id = df.loc[df["series_uid"] == group_key, "acl"].values[0]
        authz = "/programs/Open/projects/R1"
        if "A1" in project_id:
            authz = "/programs/Open/projects/A1"
        try:
            directory_name = split_s3_path(storage_url)
            package_name, package_md5 = packaging.createZipFileStream(
                cfg["aws"]["bucket"], directory_name, group_key, "dcm"
            )
            package_size = packaging.getPackageSize(package_name)
            package_url = packaging.getPackageUrl(package_name)
            writer.writerow(
                [
                    "package",
                    package_md5,
                    package_size,
                    authz,
                    package_url,
                    package_name,
                    str(package_contents(group_value)),
                ]
            )
        except Exception as err:
            logger.error(err)
            raise


if __name__ == "__main__":
    main()
