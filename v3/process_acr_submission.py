import argparse
import csv
import locale
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd
import os
import boto3

# download file
# s3://external-data-midrc-replication/replicated-data-acr/ACR_20220415/image_file_object_manifest_ACR_20220415.tsv
#      ^ bucket                        ^ actual path we care (key)
# into specific folder
# let's say
# /midrc-data/ACR_20220415/image_file_object_manifest_ACR_20220415.tsv
#
# python3 process_acr_submission.py --submission ACR_20220415 --input_path /midrc-data --output_path /midrc-data/ACR_20220415/output
# python3 process_acr_submission.py --submission $SUBMISSION --input_path $INPUT_PATH --output_path $OUTPUT_PATH

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

parser = argparse.ArgumentParser(description="Process ACR submission")
parser.add_argument(
    "--submission",
    action="store",
    type=str,
    required=True,
    help="ACR submission name",
)
parser.add_argument(
    "--input_path",
    action="store",
    type=str,
    required=True,
    help="input path with ACR clinical data",
)
parser.add_argument(
    "--output_path",
    action="store",
    type=str,
    required=True,
    help="output path for packages information",
)
parser.add_argument(
    "--new",
    default=False,
    action="store_true",
    help='for "new"-style submissions',
)
parser.add_argument(
    "--s3key",
    action="store",
    type=str,
    help="s3key for manifest",
)

args = parser.parse_args()


def download_manifest(s3key, submission, input_path):
    fname = os.path.basename(s3key)
    s3.meta.client.download_file(
        "external-data-midrc-replication",
        s3key,
        Path(Path(input_path) / submission / fname).as_posix(),
    )


# for everything after and including ACR_20220314
def process_submission_new(submission, input_path, output_path):
    print(submission)
    packages_path = Path(output_path) / submission
    # packages_path.mkdir(parents=True, exist_ok=True)

    submission_path = Path(input_path) / submission

    image_manifest_file = list(submission_path.glob("*object_manifest*.tsv"))
    print(image_manifest_file)

    assert (
        len(image_manifest_file) == 1
    ), "only one manifest should exist in the submission"

    image_manifest_file = image_manifest_file[0]

    rename_columns = {
        "case_ids": "case_id",
        "study_uid": "study_id",
        "series_uid": "series_id",
        "instance_uid": "instance_id",
    }

    instances = pd.read_csv(image_manifest_file, sep="\t").rename(
        columns=rename_columns
    )
    instances = instances.drop(columns=["modality"])
    instances["storage_urls"] = instances["storage_urls"].apply(
        lambda v: v.replace("//", "replicated-data-acr/")
    )

    if submission in ("ACR_20220314", "ACR_20220415", "ACR_20220715"):
        instances["storage_urls"] = instances["storage_urls"].apply(
            lambda v: "/".join(v.split("/")[i] for i in [0, 1, 3, 5])
        )

    instances["file_name"] = instances["file_name"].apply(lambda v: v.split("/")[-1])

    instances.to_csv(f"{output_path}/instance_{submission}.csv", index=False)

    # list_of_packages = []

    # for _, row in instances.iterrows():
    #     case_id = row["case_id"]
    #     study_id = row["study_id"]
    #     series_id = row["series_id"]

    #     series_path = f"./cases/{case_id}/{study_id}/{series_id}.tsv\n"
    #     if series_path not in list_of_packages:
    #         list_of_packages.append(series_path)

    #     folder = packages_path / "cases" / case_id / study_id

    #     # folder.mkdir(parents=True, exist_ok=True)

    #     series_file = folder / f"{series_id}.tsv"
    #     series_file_exist = series_file.exists()

    #     url = row["storage_urls"]
    #     url_parts = url.split("/")
    #     # len_url_parts = len(url_parts)
    #     # if len_url_parts != 6:
    #     #     print(len(url_parts))
    #     #     print(url_parts)

    #     row["storage_urls"] = "/".join(list(map(url_parts.__getitem__, [0, 1, 3, 5])))

    #     with open(series_file, mode="a") as f:
    #         fieldnames = [
    #             "file_name",
    #             "file_size",
    #             "md5sum",
    #             "case_id",
    #             "study_id",
    #             "series_id",
    #             "instance_id",
    #             "storage_urls",
    #         ]
    #         writer = csv.DictWriter(f, delimiter="\t", fieldnames=fieldnames)

    #         if not series_file_exist:
    #             writer.writeheader()
    #         writer.writerow(row.to_dict())

    # with open(packages_path / "packages.txt", "w") as f:
    #     f.writelines(list_of_packages)


# for everything before (not including) ACR_20220314
def process_submission_old(submission, input_path, output_path):
    # useful paths for data manipulation
    print(submission)
    # packages_path = Path(output_path) / submission
    # packages_path.mkdir(parents=True, exist_ok=True)

    submission_path = Path(input_path) / submission

    image_manifest_file = list(
        chain(
            submission_path.glob("**/CIRR*.txt"),
            submission_path.glob("**/*image_manifest*.txt"),
            submission_path.glob("**/image_*.txt"),
            submission_path.glob("image_*.tsv"),
            submission_path.glob("Image_*.tsv"),
            submission_path.glob("*_instance_*.tsv"),
        )
    )
    print(image_manifest_file)
    studies_file = list(submission_path.glob("**/*imaging_study_*"))
    if len(studies_file) > 0:
        studies_file = studies_file[0]
    series_files = list(
        chain(
            submission_path.glob("**/*_series_*.txt"),
            submission_path.glob("*_series_*.tsv"),
        )
    )
    # instance_files = list(SUBMISSION_PATH.glob("*_instance_*.tsv"))

    rename_columns = {
        "case_ids": "case_id",
        "Subject_ID": "case_id",
        "series_uid": "series_id",
        "dr_exams.submitter_id": "study_id",
        "ct_scan.submitter_id": "study_id",
        "ct_scans.submitter_id": "study_id",
        "mr_exams.submitter_id": "study_id",
        "nm_exams.submitter_id": "study_id",
        "pt_scans.submitter_id": "study_id",
        "pr_exams.submitter_id": "study_id",
        "rf_exams.submitter_id": "study_id",
        "series-submitter": "submitter_id",
        "imaging_studies.submitter_id": "study_id",
    }

    if submission != "0827":
        rename_columns.update(
            {
                "radiography_exam.submitter_id": "study_id",
                "radiography_exams.submitter_id": "study_id",
            }
        )

    series = map(
        lambda v: pd.read_csv(v, sep="\t").rename(columns=rename_columns), series_files
    )

    rename_columns = {
        "case_ids": "case_id",
        "study_uid": "study_id",
        "ct_scans.submitter_id": "study_id",
        "radiography_exam.submitter_id": "study_id",
        "series_uid": "series_id",
        "series.submitter_id": "series_id",
        "cr_series.submitter_id": "series_id",
        "ct_series.submitter_id": "series_id",
        "dx_series.submitter_id": "series_id",
        "mr_series.submitter_id": "series_id",
        "us_series.submitter_id": "series_id",
        "*md5sum": "md5sum",
        "mdsum": "md5sum",
        "*file_name": "file_name",
        "*file_size": "file_size",
        "submitter_id": "instance_id",
        "storage_url": "storage_urls",
    }

    if submission != "0827":
        rename_columns.update(
            {
                "object_id": "instance_id",
                "instance_uid": "instance_id",
            }
        )

        if studies_file:
            studies = pd.read_csv(studies_file, sep="\t")

    instances = map(
        lambda v: pd.read_csv(v, sep="\t").rename(columns=rename_columns),
        image_manifest_file,
    )
    if submission == "08":
        instances = [list(instances)[0]]

    instances = pd.concat(instances, ignore_index=True).reset_index(drop=True)

    # instances["series_id"] = instances["series_id"].apply(lambda v: v.split("_")[1])
    # instances["study_id"] = instances["study_id"].apply(lambda v: v.split("_")[1])

    series = list(series)

    if series:
        series = pd.concat(series, ignore_index=True).reset_index(drop=True)

        # series["study_id"] = series["study_id"].apply(lambda v: v.split("_")[1])

        if submission == "0827":
            # print(series.columns)
            series["study_id"] = (
                series["radiography_exams.submitter_id"]
                .str.split("_")
                .apply(lambda v: v[1])
            )
            series = series[["series_id", "study_id", "case_id"]].drop_duplicates()
            # print(instances.columns)
            instances["study_id"] = (
                instances["storage_urls"].str.split("/").apply(lambda v: v[5])
            )
            instances = instances.merge(series, on=["case_id", "series_id"])

        elif submission == "08":
            series = series[["series_id", "case_id"]].drop_duplicates()
            instances["study_id"] = (
                instances["storage_urls"].str.split("/").apply(lambda v: v[5])
            )
            instances = instances.merge(series, on=["case_id", "series_id"])

        elif submission == "09":
            series = series[["series_id", "case_id"]].drop_duplicates()
            instances["series_id"] = (
                instances["series_id"].str.split("_").apply(lambda v: v[1])
            )
            instances["study_id"] = (
                instances["storage_urls"].str.split("/").apply(lambda v: v[5])
            )
            instances = instances.merge(series, how="left", on=["case_id", "series_id"])

        elif submission == "10":
            series = series[["series_id", "case_id"]].drop_duplicates()
            instances["storage_urls"] = instances["storage_urls"].str.replace(
                "/0914/", "/10/batch6/", regex=False
            )

            instances["study_id"] = (
                instances["storage_urls"].str.split("/").apply(lambda v: v[6])
            )
            instances["instance_id"] = (
                instances["instance_id"].str.split("_").apply(lambda v: v[1])
            )
            instances["series_id"] = (
                instances["series_id"].str.split("_").apply(lambda v: v[1])
            )

        else:
            series = series[["series_id", "study_id", "case_id"]].drop_duplicates()
            instances = instances.merge(series, on=["case_id", "series_id"])

    instances["storage_urls"] = instances["storage_urls"].str.replace(
        "//", "replicated-data-acr/"
    )

    if submission == "ACRAgeResubmission_20220606":
        instances["storage_urls"] = instances["storage_urls"].str.replace(
            "ACRAgeResubmission_20220606",
            "replicated-data-acr/ACRAgeResubmission_20220606",
        )

        instances["storage_urls"] = instances["storage_urls"].apply(
            lambda v: "/".join(v.split("/")[i] for i in [0, 1, 3, 5]) + ".dcm"
        )

    if instances["file_size"].dtype == np.dtype("O"):
        instances["file_size"] = instances["file_size"].apply(lambda v: locale.atoi(v))
    instances = instances[
        [
            "storage_urls",
            "file_name",
            "file_size",
            "md5sum",
            "case_id",
            "study_id",
            "series_id",
            "instance_id",
        ]
    ].drop_duplicates()

    # instances

    instances.to_csv(f"{output_path}/instance_{submission}.csv", index=False)

    # list_of_packages = []

    # for _, row in instances.iterrows():
    #     case_id = row["case_id"]
    #     study_id = row["study_id"]
    #     series_id = row["series_id"]

    #     series_path = f"./cases/{case_id}/{study_id}/{series_id}.tsv\n"
    #     if series_path not in list_of_packages:
    #         list_of_packages.append(series_path)

    #     folder = packages_path / "cases" / case_id / study_id

    #     folder.mkdir(parents=True, exist_ok=True)

    #     series_file = folder / f"{series_id}.tsv"
    #     series_file_exist = series_file.exists()

    #     with open(series_file, mode="a") as f:
    #         fieldnames = [
    #             "file_name",
    #             "file_size",
    #             "md5sum",
    #             "case_id",
    #             "study_id",
    #             "series_id",
    #             "instance_id",
    #             "storage_urls",
    #         ]
    #         writer = csv.DictWriter(f, delimiter="\t", fieldnames=fieldnames)

    #         if not series_file_exist:
    #             writer.writeheader()
    #         writer.writerow(row.to_dict())

    # with open(packages_path / "packages.txt", "w") as f:
    #     f.writelines(list_of_packages)


if __name__ == "__main__":
    if args.new:
        if args.s3key is not None:
            download_manifest(args.s3key, args.submission, args.input_path)
        process_submission_new(args.submission, args.input_path, args.output_path)
    else:
        process_submission_old(args.submission, args.input_path, args.output_path)
