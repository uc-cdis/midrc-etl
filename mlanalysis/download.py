import os
import gen3
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
import pandas as pd
from os import path
import boto3
from pathlib import Path
import argparse

parser = argparse.ArgumentParser(description="Download Metadata Files")
parser.add_argument(
    "--name",
    action="store",
    type=str,
    required=False,
    help="Workflow Name",
)
args = parser.parse_args()

api = "https://data.midrc.org"
auth = Gen3Auth()
sub = Gen3Submission(api, auth)


def get_project_ids_with_access(sub: Gen3Submission):
    """What project_ids do I have access to?"""
    query = "{project (first:0){project_id}}"
    res = sub.query(query)
    return [i.get("project_id") for i in res.get("data", {}).get("project", [])]


def get_project_ids_with_access_for_node(sub: Gen3Submission, node: str):
    pids = get_project_ids_with_access(sub)
    has_access = []
    for pid in pids:
        query = f'{{node (first:1,of_type:"{node}",project_id:"{pid}"){{project_id}}}}'
        res = sub.query(query).get("data", {}).get("node", [])
        if len(res) > 0:
            has_access.append(pid)
    return has_access


def export_node_tsv_with_access(sub: Gen3Submission, node: str, filename: str):
    pids = get_project_ids_with_access_for_node(sub, node)
    is_first = True
    with open(filename, "wt") as o:
        for pid in pids:
            program, project = pid.split("-", 1)
            result = sub.export_node(program, project, node, "tsv")
            if is_first:
                o.write(result)
                is_first = False
            else:
                for line in result.split("\n")[1:]:
                    curr = line.rstrip("\r\n")
                    if curr:
                        o.write(curr + "\n")


nodes = ["ct_series_file", "cr_series_file", "mr_series_file", "dx_series_file"]
for node in nodes:
    export_node_tsv_with_access(sub, node, node + ".tsv")

# read CSV files
cr_file = Path("cr_series_file.tsv").as_posix()
cr = pd.read_csv(cr_file, sep="\t", header=0, dtype=str)

ct_file = Path("ct_series_file.tsv").as_posix()
ct = pd.read_csv(ct_file, sep="\t", header=0, dtype=str)

dx_file = Path("dx_series_file.tsv").as_posix()
dx = pd.read_csv(dx_file, sep="\t", header=0, dtype=str)

mr_file = Path("mr_series_file.tsv").as_posix()
mr = pd.read_csv(mr_file, sep="\t", header=0, dtype=str)

# concatting data into one data frame
base_mdata = pd.concat([cr, ct, dx, mr])

# Uploading file for next step
base_mdata.to_csv("baseMLData.csv")

s3 = boto3.resource("s3")

SRC_BUCKET = "processing-data-midrc-replication"

src_bucket = s3.Bucket(SRC_BUCKET)

s3.client.upload_file(
    "baseMLData.csv", "external-data-midrc-replication", (args.name / "baseMLData.csv")
)
