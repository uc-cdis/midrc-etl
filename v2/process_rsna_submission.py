import argparse
import csv
import locale
import re
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")


def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix) :]


parser = argparse.ArgumentParser(description="Process RSNA submission")
parser.add_argument(
    "--submission",
    action="store",
    type=str,
    required=True,
    help="RSNA submission name",
)
parser.add_argument(
    "--input_path",
    action="store",
    type=str,
    required=True,
    help="input path with RSNA clinical data",
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
    default=True,
    action="store_true",
    help='for "new"-style submissions',
)
args = parser.parse_args()


def process_submission_new(submission, input_path, output_path):
    print(submission)
    packages_path = Path(output_path) / submission
    packages_path.mkdir(parents=True, exist_ok=True)

    submission_path = Path(input_path) / submission

    image_manifest_file = list(submission_path.glob("imaging_data_manifest_*.tsv"))
    assert (
        len(image_manifest_file) == 1
    ), "only one manifest should exist in the submission"
    image_manifest_file = image_manifest_file[0]

    rename_columns = {
        "case_ids": "case_id",
        "study_uid": "study_id",
        "series_uid": "series_id",
    }

    instances = pd.read_csv(image_manifest_file, sep="\t").rename(
        columns=rename_columns
    )
    instances = instances.drop(columns=["acl", "modality"])
    instances["storage_urls"] = instances["storage_urls"].apply(
        lambda v: v.replace("s3://storage.ir.rsna.ai/", "")
    )
    instances["file_name"] = instances["file_name"].apply(lambda v: v.split("/")[-1])
    instances["instance_id"] = instances["file_name"].apply(lambda v: v.split("/")[-1])

    list_of_packages = []

    for _, row in instances.iterrows():
        case_id = row["case_id"]
        study_id = row["study_id"]
        series_id = row["series_id"]

        series_path = f"./cases/{case_id}/{study_id}/{series_id}.tsv\n"
        if series_path not in list_of_packages:
            list_of_packages.append(series_path)

        folder = packages_path / "cases" / case_id / study_id

        folder.mkdir(parents=True, exist_ok=True)

        series_file = folder / f"{series_id}.tsv"
        series_file_exist = series_file.exists()

        with open(series_file, mode="a") as f:
            fieldnames = [
                "file_name",
                "file_size",
                "md5sum",
                "case_id",
                "study_id",
                "series_id",
                "instance_id",
                "storage_urls",
            ]
            writer = csv.DictWriter(f, delimiter="\t", fieldnames=fieldnames)

            if not series_file_exist:
                writer.writeheader()
            writer.writerow(row.to_dict())

    with open(packages_path / "packages.txt", "w") as f:
        f.writelines(list_of_packages)


def process_submission_old(submission, input_path, output_path):
    # useful paths for data manipulation
    print(submission)
    packages_path = Path(output_path) / submission
    packages_path.mkdir(parents=True, exist_ok=True)

    submission_path = Path(input_path) / submission

    # get all the necessary files

    image_manifest_file = list(submission_path.glob("imaging_data_manifest_*.tsv"))[0]
    studies_file = list(submission_path.glob("*imaging_study_*.tsv"))[0]
    series_files = list(submission_path.glob("*_series_*.tsv"))
    instance_files = list(submission_path.glob("*_instance_*.tsv"))

    # instance_files = list(SUBMISSION_PATH.glob("midrc_*_instance_*.tsv"))
    # instance_files = list(SUBMISSION_PATH.glob("midrc_*_image_*.tsv"))
    # studies_file = list(SUBMISSION_PATH.glob("midrc_imaging_study_*.tsv"))[0]

    id_pattern = r"([\d\.]+)$"
    id_regex = re.compile(id_pattern)

    # load all files into pandas DF's

    image_manifest = pd.read_csv(image_manifest_file, sep="\t")

    studies = pd.read_csv(studies_file, sep="\t")

    rename_columns_studies = {
        "cases.submitter_id": "case_id",
        "cases": "case_id",  # for "midrc-ricord-2021-08-20"
        "study_uid": "study_id",
    }

    studies = studies.rename(columns=rename_columns_studies)

    studies["case_id"] = studies["case_id"].apply(lambda v: remove_prefix(v, "Case_"))

    studies = studies[["study_id", "case_id"]]

    series = list(map(lambda v: pd.read_csv(v, sep="\t"), series_files))

    rename_columns_series = {
        "imaging_studies.submitter_id": "study_id",
        "mr_exams.submitter_id": "study_id",
        "ct_scans.submitter_id": "study_id",
        "radiography_exams.submitter_id": "study_id",
        "case_ids": "case_id",
        "series_uid": "series_id",
    }

    series = list(
        map(
            lambda v: v.rename(columns=rename_columns_series)[
                ["series_id", "study_id", "case_id"]
            ],
            series,
        )
    )

    all_series = pd.concat(series)
    all_series["case_id"] = all_series["case_id"].apply(
        lambda v: remove_prefix(v, "Case_")
    )
    all_series["study_id"] = all_series["study_id"].apply(
        lambda v: id_regex.search(v).group(0)
    )

    instances = list(map(lambda v: pd.read_csv(v, sep="\t"), instance_files))

    rename_columns_instances = {
        "cr_series.submitter_id": "series_id",
        "dx_series.submitter_id": "series_id",
        "ct_series.submitter_id": "series_id",
        "mr_series.submitter_id": "series_id",
        "submitter_id": "instance_id",
        "case_ids": "case_id",
    }

    instances = list(
        map(lambda v: v.rename(columns=rename_columns_instances), instances)
    )

    all_instances = pd.concat(instances)
    all_instances["case_id"] = all_instances["case_id"].apply(
        lambda v: remove_prefix(v, "Case_")
    )
    all_instances["instance_id"] = all_instances["instance_id"].apply(
        lambda v: id_regex.search(v).group(0)
    )
    all_instances["series_id"] = all_instances["series_id"].apply(
        lambda v: id_regex.search(v).group(0)
    )

    all_instances = all_instances[
        [
            "instance_id",
            "series_id",
            "case_id",
            "file_name",
            "file_size",
            "md5sum",
            "storage_urls",
        ]
    ]

    # fix for RSNA_20220314
    if submission == "RSNA_20220314":
        all_instances["storage_urls"] = all_instances["storage_urls"].apply(
            lambda v: v.replace("RSNA_20220307", "RSNA_20220314")
        )

    merged = image_manifest.merge(all_instances).merge(all_series).merge(studies)
    merged["file_name"] = merged["instance_id"].apply(lambda v: f"{v}.dcm")
    merged = merged[
        [
            "file_name",
            "file_size",
            "md5sum",
            "storage_urls",
            "case_id",
            "study_id",
            "series_id",
        ]
    ]

    print(
        f"image manifest: {image_manifest.shape}\nmerged manifest: {merged.shape}\nall instances: {all_instances.shape}"
    )
    # print(f"image manifest: {image_manifest.columns}\nmerged manifest: {merged.columns}\nall instances: {all_instances.columns}")
    # print(all_instances.head())
    # print(image_manifest.head())

    list_of_packages = []

    for _, row in merged.iterrows():
        case_id = row["case_id"]
        study_id = row["study_id"]
        series_id = row["series_id"]

        series_path = f"./cases/{case_id}/{study_id}/{series_id}.tsv\n"
        if series_path not in list_of_packages:
            list_of_packages.append(series_path)

        folder = packages_path / "cases" / case_id / study_id

        folder.mkdir(parents=True, exist_ok=True)

        series_file = folder / f"{series_id}.tsv"
        series_file_exist = series_file.exists()

        with open(series_file, mode="a") as f:
            fieldnames = [
                "file_name",
                "file_size",
                "md5sum",
                "case_id",
                "study_id",
                "series_id",
                "instance_id",
                "storage_urls",
            ]
            writer = csv.DictWriter(f, delimiter="\t", fieldnames=fieldnames)

            if not series_file_exist:
                writer.writeheader()
            writer.writerow(row.to_dict())

    with open(packages_path / "packages.txt", "w") as f:
        f.writelines(list_of_packages)


if __name__ == "__main__":
    if args.new:
        process_submission_new(args.submission, args.input_path, args.output_path)
    else:
        process_submission_old(args.submission, args.input_path, args.output_path)
