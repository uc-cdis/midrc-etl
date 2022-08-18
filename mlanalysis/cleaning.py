import pandas as pd
import boto3
from pathlib import Path
import argparse

### STEP 2 (Dependent on Step 1) ###
### data cleaning (Have to save to csv file) ###

parser = argparse.ArgumentParser(description="Download Data File")
parser.add_argument(
    "--name",
    action="store",
    type=str,
    required=False,
    help="Workflow Name",
)
args = parser.parse_args()

# Downloading Files
s3 = boto3.resource("s3")

SRC_BUCKET = "processing-data-midrc-replication"

src_bucket = s3.Bucket(SRC_BUCKET)

s3.client.download_file(
    "processing-data-midrc-replication",
    args.name / "baseMLData.csv",
    Path("/midrc-etl/mlAnalysis/baseMLData.csv").as_posix(),
)

# read CSV files
data_file = Path("/midrc-etl/mlAnalysis/baseMLData.csv").as_posix()
base_mdata = pd.read_csv(data_file, sep="\t", header=0, dtype=str)

# All data analysis will be done in relativity to the manufacturer, thus we must drop rows with that column empty
mdata_m = base_mdata.dropna(
    subset=base_mdata.columns[base_mdata.columns.get_loc("manufacturer")], how="any"
)

# Creating data set with colums with BOTH manufacturer and pixel_spacing
mdata_mp = mdata_m.dropna(
    subset=mdata_m.columns[mdata_m.columns.get_loc("pixel_spacing")], how="any"
)

# Dropping unused columns
dataset = mdata_mp[["pixel_spacing", "manufacturer"]]

# Writing new CSV file for next step
dataset.to_csv("cleanedData.csv")

s3.client.upload_file(
    "cleanedData.csv",
    "external-data-midrc-replication",
    (args.name / "cleanedData.csv"),
)
