import json
import pandas as pd
import csv
import yaml
import os
import logging
from gen3.auth import Gen3Auth
from gen3.query import Gen3Query
from jsonobject import *

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.StreamHandler()
logger = logging.getLogger(__name__)
with open("config.yml", "rt", encoding="utf8") as ymlfile:
    cfg = yaml.safe_load(ymlfile)
COMMONS = "https://{{cfg[n3c][commons]}}/"


def main():

    dataframe = pd.read_csv(
        cfg["n3c"]["file_name"],
        sep=",",
        header=0,
        low_memory=False,
        usecols=[
            cfg["n3c"]["query_attribute"],
        ],
    )
    get_guids(dataframe)


def get_guids(df):
    auth = Gen3Auth(COMMONS, refresh_file="credentials.json")
    query = Gen3Query(auth)
    grouped = df.groupby(cfg["n3c"]["query_attribute"])
    query_string = '{\n  datanode(\n    case_ids: ""\n    type: "imaging_data_file"\n    first: 0\n  ) {\n    id\n    case_ids\n    object_id\n  }\n}'
    for group_key in grouped:
        query_string["case_ids"] = group_key
        list = Gen3Query.query(query_string)
        create_output_file(list)


def create_output_file(list):
    output_file = "output/output.tsv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(
        output_file, "w"
    ) as tsvfile:  # "w" open file for writing and reading plain text, create a new file if not exists or truncate the file if exists.
        writer = csv.writer(tsvfile, delimiter="\t", lineterminator="\n", quotechar="&")
        writer.writerow(
            [
                "id",
                "midrc_image_guid",
                "midrc_case_submitter_id",
            ]
        )
    for entry in list["data"]["datanode"]:
        writer.writerow(
            [
                entry["case_ids"],
                entry["object_id"],
                entry["id"],
            ]
        )
