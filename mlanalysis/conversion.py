import pandas as pd
import re
import boto3
from pathlib import Path
import argparse

# Step 3 (Dependent on Step 2)
# Converting pixel spacing column to integer

parser = argparse.ArgumentParser(description="Download Cleaned Metadata")
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
    args.name / "cleanedData.csv",
    Path("/midrc-etl/mlAnalysis/cleanedData.csv").as_posix(),
)

# Reading cleaned data
data_file = "/midrc-etl/mlAnalysis/cleanedData.csv"
data = pd.read_csv(data_file)

# pixel spacing is a string object as a default so to convert the data into a graphable form we must convert the string
# object of the square pixel spacing into an integer.
def pixel_conv(df):
    pixel_spacing = []
    counter = 0
    for i in df.pixel_spacing:
        pixel_spacing.append(i)

    counter = 0
    for i in pixel_spacing:
        result = re.search("(.*),", i)
        result = result.group(1)
        result = float(result)
        pixel_spacing[counter] = result
        counter += 1
    return pixel_spacing


# Assigning converted pixel spacing
dataset = data.assign(pixel_spacing=pixel_conv(data))
dataset = dataset[["pixel_spacing", "manufacturer"]]

# Writing new CSV file for next step
dataset.to_csv("convertedData.csv")

s3.client.upload_file(
    "convertedData.csv" "external-data-midrc-replication",
    (args.name / "convertedData.csv"),
)
