import argparse
import csv
import locale
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd

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
args = parser.parse_args()


def process_submission(submission, input_path, output_path):
    # useful paths for data manipulation
    print(submission)
    packages_path = Path(output_path) / submission
    packages_path.mkdir(parents=True, exist_ok=True)

    submission_path = Path(input_path) / submission

    image_manifest_file = list(
        chain(
            submission_path.glob("**/CIRR*.txt"),
            submission_path.glob("**/*image_manifest*.txt"),
            submission_path.glob("**/image_*.txt"),
            submission_path.glob("image_*.tsv"),
            submission_path.glob("*_instance_*.tsv"),
        )
    )
    # studies_file = list(SUBMISSION_PATH.glob("*imaging_study_*.tsv"))[0]
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
        "radiography_exam.submitter_id": "study_id",
        "radiography_exams.submitter_id": "study_id",
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

    series = map(
        lambda v: pd.read_csv(v, sep="\t").rename(columns=rename_columns), series_files
    )
    series = pd.concat(series, ignore_index=True).reset_index(drop=True)

    # series["study_id"] = series["study_id"].apply(lambda v: v.split("_")[1])

    series = series[["series_id", "study_id", "case_id"]].drop_duplicates()

    # series

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
        "*md5sum": "md5sum",
        "mdsum": "md5sum",
        "*file_name": "file_name",
        "*file_size": "file_size",
        "submitter_id": "instance_id",
        "object_id": "instance_id",
    }

    instances = map(
        lambda v: pd.read_csv(v, sep="\t").rename(columns=rename_columns),
        image_manifest_file,
    )
    instances = pd.concat(instances, ignore_index=True).reset_index(drop=True)

    # instances["series_id"] = instances["series_id"].apply(lambda v: v.split("_")[1])
    # instances["study_id"] = instances["study_id"].apply(lambda v: v.split("_")[1])

    instances = instances.merge(series, on=["case_id", "series_id"])

    if instances["file_size"].dtype == np.dtype("O"):
        instances["file_size"] = instances["file_size"].apply(lambda v: locale.atoi(v))
    instances = instances[
        [
            "file_name",
            "file_size",
            "md5sum",
            "case_id",
            "study_id",
            "series_id",
            "instance_id",
            "storage_urls",
        ]
    ].drop_duplicates()

    # instances

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


if __name__ == "__main__":
    process_submission(args.submission, args.input_path, args.output_path)
