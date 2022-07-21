import pandas as pd
import csv
import yaml
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
        cfg["n3c"]["file_name"],
        sep=",",
        header=0,
        low_memory=False,
        usecols=[
            cfg["n3c"]["query_attribute"],
        ],
    )
