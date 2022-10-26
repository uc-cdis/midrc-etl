#!/usr/bin/env python3

import argparse
import csv, json
import os, sys
import random, uuid, hashlib
from pathlib import PosixPath
import pandas as pd

csv.field_size_limit(sys.maxsize)

parser = argparse.ArgumentParser(description="Split MIDRC packages into open and sequestered")
parser.add_argument(
    "--batch_dir",
    action="store",
    type=str,
    required=True,
    help="the batch directory containing packages.txt",
)
parser.add_argument(
    "--master_seq_file",
    action="store",
    type=str,
    required=True,
    help="master TSV file containing sequestration locations for cases",
)
parser.add_argument(
    "--exclude_cases",
    action="store",
    type=str,
    required=False,
    help="master TSV file containing case_ids column to exclude",
)
parser.add_argument(
    "--exclude_studies",
    action="store",
    type=str,
    required=False,
    help="master TSV file containing study_uid column to exclude",
)

args = parser.parse_args()
batch = args.batch_dir.split('/')[-1]
batch_dir = PosixPath(args.batch_dir)
org = batch.split("_",1)[0] #org = batch.split("_",1)[0]

# read in master sequestration list
seq_master = {}
with open(args.master_seq_file) as sequestration_master_file:
    reader = csv.DictReader(sequestration_master_file, delimiter="\t")
    for row in reader:
        seq_master[row["case_ids"]] = row["dataset"]

# get list of study_uids to exclude:
if args.exclude_studies is not None:
    df = pd.read_csv(args.exclude_studies,sep='\t',dtype=str)
    exclude_studies = list(set(df.study_uid))
else:
    exclude_studies = []

# get list of case_ids to exclude:
if args.exclude_cases is not None:
    df = pd.read_csv(args.exclude_cases,sep='\t',dtype=str)
    exclude_cases = list(set(df.case_ids))
else:
    exclude_cases = []

package_dir = batch_dir / "packages"
assert(package_dir.is_dir()), "Packages dir does not exist: {}".format(package_dir)

open_packages,seq_packages,remove_packages,missing_packages = [],[],[],[]

packages = list(package_dir.glob('*.txt'))
count,total = 0,len(packages)

for package_filepath in package_dir.iterdir():
    count+=1
    with open(package_filepath) as package_file:
        reader = csv.DictReader(package_file, delimiter="\t")
        for row in reader:
            item = row
            file_name = item["file_name"]
            case_id, study_id, _ = file_name.split("/")
            study_id = study_id.split("_")[-1] # remove any study_id prefix
            package_contents = json.loads(item["package_contents"].replace("'", "\""))
            for p in package_contents:
                p["size"] = int(p["size"])
            item["package_contents"] = json.dumps(package_contents)
            dataset = seq_master.get(case_id, None)
            if dataset == "Open":
                bucket = "s3://open-data-midrc/"
                if org == "ACR":
                    authz = json.dumps(["/programs/Open/projects/A1"])
                if org == "RSNA":
                    authz = json.dumps(["/programs/Open/projects/R1"])
            elif dataset == "Seq":
                bucket = "s3://sequestered-data-midrc/"
                if org == "ACR":
                    authz = json.dumps(["/programs/SEQ_Open/projects/A3"])
                if org == "RSNA":
                    authz = json.dumps(["/programs/SEQ_Open/projects/R3"])
            elif dataset == "Ignore":
                bucket = "s3://open-data-midrc/"
                authz = json.dumps(["/programs/TCIA"])
            else:
                authz = ""
                bucket = ""
            item["authz"] = authz
            item["url"] = f"{bucket}{item['url']}"
            if study_id in exclude_studies:
                remove_packages.append(item)
                continue
            if case_id in exclude_cases:
                remove_packages.append(item)
                continue
            m = hashlib.md5()
            m.update(f"{item['md5']}{item['size']}".encode('utf-8'))
            item["guid"] = f"dg.MD1R/{uuid.UUID(m.hexdigest(), version=4)}"
            if (dataset == "Open") | (dataset == "Ignore"):
                open_packages.append(item)
            elif dataset == "Seq":
                seq_packages.append(item)
            else:
                print('\n')
                missing_packages.append(item)

    print("{}/{}; Open: {}, Seq: {}, Remove: {}, Missing: {}; Processing case_id '{}': {}           ".format(count, total, len(open_packages), len(seq_packages), len(remove_packages), len(missing_packages), case_id, dataset), end='\r')

datasets = []
if len(open_packages) > 0:
    datasets.append(("packages_open_{}.tsv".format(batch), open_packages))
if len(seq_packages) > 0:
    datasets.append(("packages_seq_{}.tsv".format(batch), seq_packages))
if len(remove_packages) > 0:
    datasets.append(("packages_remove_{}.tsv".format(batch), remove_packages))
if len(missing_packages) > 0:
    datasets.append(("packages_missing_{}.tsv".format(batch), missing_packages))

fieldnames = [
    "record_type",
    "guid",
    "md5",
    "size",
    "authz",
    "url",
    "file_name",
    "package_contents"]

to_index_path = batch_dir / "to_index"
to_index_path.mkdir(parents=True, exist_ok=True)

for filename, dataset in datasets:
    if not dataset:
        continue
    with open(to_index_path / filename, "w") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()
        for item in dataset:
            writer.writerow(item)

print("\nOutput written to: {}".format(to_index_path))
