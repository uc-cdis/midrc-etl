import csv
import locale
from pathlib import Path

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

DATA_DIR = Path("/Users/andrew/CTDS/misc-projects/midrc/data/")
S3_RSNA_DATA_DIR = DATA_DIR.joinpath("s3-data/rsna/")
IMAGING_DATA_MANIFESTS = [
    # "midrc-ricord-2021-08-20/imaging_data_manifest_419639-2021-08-19.tsv",
    # "midrc-ricord-2021-09-02/imaging_data_manifest_2021_09_09.tsv",
    # "midrc-ricord-2021-09-22/imaging_data_manifest_2021_09_23.tsv",
    "midrc-ricord-2021-10-06/imaging_data_manifest_2021_10_08.tsv",
    "midrc-ricord-2021-10-26/imaging_data_manifest_2021_10_28.tsv",
]
OUTPUT_DIR = "to_index_manifests/rsna/"


def get_storage_url(s3bucket: str, storage_url: str) -> str:
    storage_url = storage_url.removeprefix("s3://storage.ir.rsna.ai/")
    return f"s3://{s3bucket}/{storage_url}"


def get_filename(url):
    url = "/".join(url.split("/")[-4:])
    return url


def read_manifest(filename):
    indexing_data = []
    with open(filename) as manifest_file:
        reader = csv.DictReader(manifest_file, delimiter="\t")
        for row in reader:
            urls = get_storage_url("open-data-midrc", row["storage_urls"])

            filesize = row["file_size"]
            filesize = locale.atoi(filesize)
            data = {
                "guid": "",
                "md5": row["md5sum"],
                "file_name": get_filename(urls),
                "size": filesize,
                "authz": "/programs/Open/projects/R1",
                "acl": "['R1','Open']",
                "urls": urls,
            }
            indexing_data.append(data)
    return indexing_data


# output file
# guid	md5	file_name	size	authz	acl	urls
def write_manifest_tsv(indexing_data, filename):
    with open(filename, "w") as csvfile:
        fieldnames = ["guid", "md5", "file_name", "size", "authz", "acl", "urls"]
        writer = csv.DictWriter(csvfile, delimiter="\t", fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(indexing_data)


if __name__ == "__main__":
    convert_manifests = [
        (
            S3_RSNA_DATA_DIR.joinpath(manifest),
            DATA_DIR.joinpath(OUTPUT_DIR, Path(manifest).name),
        )
        for manifest in IMAGING_DATA_MANIFESTS
    ]

    for from_manifest, to_manifest in convert_manifests:
        indexing_data = read_manifest(from_manifest)
        write_manifest_tsv(indexing_data, to_manifest)
