import argparse
import csv
import locale
import re
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

parser = argparse.ArgumentParser(description="Process a MIDRC batch submission")
# batch = "RSNA_20220427"
parser.add_argument(
    "--batch",
    action="store",
    type=str,
    required=True,
    help="the batch name",
)
# input_path = "/home/ubuntu/download/replicated-data-rsna"
parser.add_argument(
    "--input_path",
    action="store",
    type=str,
    required=True,
    help="input path with structured data TSVs",
)
# output_path = "/home/ubuntu/wd/output"
parser.add_argument(
    "--output_path",
    action="store",
    type=str,
    required=True,
    help="output path for packages information",
)
args = parser.parse_args()


def process_batch(batch, input_path, output_path):
    print("Processing batch '{}'".format(batch))

    """
        Read manifest and create output directory
    """
    batch_path = Path(input_path) / batch
    assert (batch_path.is_dir()), "The batch input directory is not found! {}".format(batch_path)

    mani_name = "image_manifest_{}.tsv".format(batch)
    mani_path = Path("{}/{}".format(input_path,mani_name))
    if not mani_path.is_file():
        mani = list(batch_path.glob("imag*manifest*.tsv"))
        assert (len(mani) == 1), "only one manifest should exist in the TSVs directory"
        mani_path = Path(mani[0])
        assert (mani_path.is_file()), "Couldn't find the manifest file! {}".format(mani_path)

    instances = pd.read_csv(mani_path, sep="\t")
    instances.drop(columns={'acl','modality'},inplace=True, errors='ignore')

    org = batch.split("_",1)[0]
    if org == "RSNA":
        instances["storage_urls"] = instances["storage_urls"].apply(
            lambda v: v.replace("s3://storage.ir.rsna.ai/", "")
        )
    elif org == "ACR":
        instances["storage_urls"] = instances["storage_urls"].apply(
            lambda v: v.replace("//", "replicated-data-acr/")
        )

    instances["file_name"] = instances["file_name"].apply(lambda v: v.split("/")[-1])
    instances["instance_uid"] = instances["file_name"].apply(lambda v: v.replace(".dcm",""))

    """
        Write packages to output directory
    """

    packages_path = Path(output_path) / batch
    packages_path.mkdir(parents=True, exist_ok=True)

    packages = []
    series_uids = list(set(instances.series_uid))
    count,total = 0,len(series_uids)
    print("Creating package manifests for {} instances in {} series.".format(len(instances),total))
    for series_uid in series_uids:
        count+=1
        print("Series {}/{}".format(count,total), end='\r')
        sdf = instances.loc[instances['series_uid']==series_uid]

        case_ids = list(set(sdf.case_ids))
        assert(len(case_ids)==1), "Got multiple case_ids '{}' for series '{}'".format(case_ids,series_uid)
        case_id = case_ids[0]

        study_uid = list(set(sdf.study_uid))
        assert(len(study_uid)==1), "Got multiple study_uids for series '{}'".format(series_uid)
        study_uid = study_uid[0]

#        series_path = f"./cases/{case_id}/{study_uid}/{series_uid}.tsv\n"
        series_path = "{}/{}/cases/{}/{}/{}.tsv\n".format(output_path,batch,case_id,study_uid,series_uid)
        if series_path not in packages:
            packages.append(series_path)

        folder = packages_path / "cases" / case_id / study_uid
        folder.mkdir(parents=True, exist_ok=True)

        series_file = folder / f"{series_uid}.tsv"
        sdf.to_csv(series_file,sep='\t',index=False)

    with open(packages_path / "packages.txt", "w") as f:
        f.writelines(packages)

if __name__ == "__main__":
    process_batch(args.batch, args.input_path, args.output_path)
