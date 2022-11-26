import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)

SUBMISSION = "ACR_20220107"
INPUT_PATH = (
    "/Users/andrewprokhorenkov/CTDS/projects/midrc/data/ssot-s3/replicated-data-acr"
)
OUTPUT_PATH = "/Users/andrewprokhorenkov/Downloads"


def list_files(submission, input_path):
    submission_path = Path(input_path) / submission
    return submission_path.glob("*.tsv")


if __name__ == "__main__":
    # print("Hello World!")
    submission_files = list_files(SUBMISSION, INPUT_PATH)
    logging.info(list(submission_files))
