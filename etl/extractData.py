import argparse
import csv
import os

import pandas as pd
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

parser = argparse.ArgumentParser(description="N3C Crosswalk Packaging")
parser.add_argument(
    "-cf",
    "--crosswalk-file",
    action="store",
    type=str,
    required=True,
    help="N3C crosswalk file with Datavant tokens",
)
parser.add_argument(
    "-qa",
    "--query-attribute",
    action="store",
    type=str,
    required=True,
    help="Query attribute for N3C crosswalk file",
)
parser.add_argument(
    "--endpoint",
    action="store",
    type=str,
    required=True,
    help="Data Commons endpoint URL",
)
parser.add_argument(
    "--creds",
    action="store",
    type=str,
    required=True,
    help="Path to JSON file with credentials",
)

args = parser.parse_args()


def main():
    crosswalk_file = args.crosswalk_file
    query_attribute = args.query_attribute
    endpoint = args.endpoint
    creds = args.creds

    dataframe = pd.read_csv(
        crosswalk_file,
        sep=",",
        header=0,
        low_memory=False,
        usecols=[query_attribute],
    )
    get_guids(dataframe, endpoint, creds, query_attribute)


def get_guids(df, endpoint, creds, query_attribute):
    auth = Gen3Auth(endpoint, refresh_file=creds)
    sub = Gen3Submission(auth)
    query_string = """query($case_ids: [String]) {
      datanode(
        case_ids: $case_ids
        type: "imaging_data_file"
        first: 0
      ) {
        id
        case_ids
        object_id
      }
    }"""

    cases_query = """query($submitter_id: [String]) {
      case(
        submitter_id: $submitter_id
        first: 0
    ) {
        id
        submitter_id
      }
    }"""

    crosswalk = []

    cases = df[query_attribute].tolist()
    case_data = sub.query(cases_query, {"submitter_id": cases})
    case_ids = {v["submitter_id"]: v["id"] for v in case_data["data"]["case"]}

    for case, case_uuid in case_ids.items():
        r = sub.query(query_string, {"case_ids": case})
        for i in r["data"]["datanode"]:
            crosswalk.append(
                {
                    "record_id": i["case_ids"][0],
                    "midrc_image_guid": i["object_id"],
                    "midrc_case_id": case_uuid,
                }
            )

    create_output_file(crosswalk)


def create_output_file(crosswalk):
    output_file = "output/METADATA.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        fieldnames = ["record_id", "midrc_image_guid", "midrc_case_id"]
        writer = csv.DictWriter(
            f, delimiter="|", lineterminator="\n", quotechar="&", fieldnames=fieldnames
        )
        writer.writeheader()
        for e in crosswalk:
            writer.writerow(e)


if __name__ == "__main__":
    main()
