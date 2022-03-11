import csv
import locale
from os import read, write
from pathlib import Path

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

S3_ACR_DATA_DIR = Path("/Users/andrew/CTDS/projects/midrc/s3-data/raw/acrimage/")
IMAGING_DATA_MANIFESTS = [
    # "2021/06/manifest/CIRR_202106_Submission_Manifest.txt",
    # "2021/07/manifest/CIRR_202107_Submission_Manifest.txt",
    # "2021/08/manifest/20210820image_manifest.txt",
    # "2021/0827/updated_manifests/image_20210827.txt",
    # "2021/09/manifest/image_20210903.txt",
    # "2021/10/batch6/manifest/image_202110_batch6b.txt",
    # "2021/10/batch6/manifest/image_20210914a.txt",
    # "2021/10/batch6/manifest/image_20210914b.txt",
    # "2021/10/batch7/manifest/image_202110_batch7a.txt",
    # "2021/10/batch7/manifest/image_202110_batch7b.txt",
    # "2021/ACRPETAL_20211220/image_ACRPETAL_20211220.tsv",
    "2021/11/image_ACR_20211115.tsv",
    # "2022/ACR_20220107/image_ACR_20220107.tsv",
]

# indexing manifest
# guid	md5	size	authz	acl	url

sequestered_files = "./midrc_indexing_scripts/updated_10_29_sequestration_ACR_RSNA_complete_29-Oct-2021.tsv"


def read_sequestered_file(filename):
    open_files = []
    seq_files = []
    with open(filename) as manifest_file:
        reader = csv.DictReader(manifest_file, delimiter="\t")
        for row in reader:
            project_id = row["project_id"]
            case_id = row["submitter_id"]

            if project_id == "Open-A1":
                open_files.append(case_id)
            if project_id == "SEQ_Open-A3":
                seq_files.append(case_id)

    return open_files, seq_files


def get_storage_url(s3bucket, storage_url):
    storage_url = storage_url.removeprefix("//").replace("/0914/", "/10/batch6/")
    return f"s3://{s3bucket}/replicated-data-acr/{storage_url}"


def get_filename(url, case_id, study_id, series_id):
    # url = "/".join(url.split("/")[-1])
    url = url.split("/")[-1]
    if case_id:
        return f"{case_id}/{study_id}/{series_id}/{url}"
    else:
        return f"{study_id}/{series_id}/{url}"


def read_manifest_txt_v2(filename, open_files, seq_files):
    open_indexing_data = []
    open_authz = "/programs/Open/projects/A1"
    open_acl = "['A1','Open']"

    seq_indexing_data = []
    seq_authz = "/programs/SEQ_Open/projects/A3"
    seq_acl = "['A3','SEQ_Open']"

    with open(filename) as manifest_file:
        reader = csv.DictReader(manifest_file, delimiter="\t")
        for row in reader:
            case_id = row["submitter_id"].split("_")[0]
            if case_id in open_files:
                s3_bucket = "open-data-midrc"
            elif case_id in seq_files:
                s3_bucket = "sequestered-data-midrc"
            else:
                print("!!!Unsequestered!!!")

            urls = get_storage_url(s3_bucket, row["storage_urls"])
            if "*md5sum" in row:
                md5sum = row["*md5sum"]
            if "md5sum" in row:
                md5sum = row["md5sum"]

            filesize = row["file_size"] if "file_size" in row else row["*file_size"]
            filesize = locale.atoi(filesize)

            case_id = row["submitter_id"].split("_")[0]
            # study_id = row["storage_urls"].split("/")[6]
            study_id = row["storage_urls"].split("/")[5]
            series_id = row["series.submitter_id"].split("_")[1]
            file_name = get_filename(urls, case_id, study_id, series_id)

            data = {
                "guid": "",
                "md5": md5sum,
                "file_name": file_name,
                "size": filesize,
                "urls": urls,
            }

            if case_id in open_files:
                data.update({"authz": open_authz, "acl": open_acl})
                open_indexing_data.append(data)
            elif case_id in seq_files:
                data.update({"authz": seq_authz, "acl": seq_acl})
                seq_indexing_data.append(data)
            else:
                print("!!!Unsequestered!!!")
            # break
    return open_indexing_data, seq_indexing_data


def read_manifest_txt(filename):
    indexing_data = []
    with open(filename) as manifest_file:
        reader = csv.DictReader(manifest_file, delimiter="\t")
        for row in reader:
            urls = get_storage_url("open-data-midrc", row["storage_urls"])
            if "*md5sum" in row:
                md5sum = row["*md5sum"]
            if "md5sum" in row:
                md5sum = row["md5sum"]
            if "mdsum" in row:
                md5sum = row["mdsum"]

            # print(md5sum)

            filesize = row["file_size"] if "file_size" in row else row["*file_size"]
            filesize = locale.atoi(filesize)

            study_id = row["storage_urls"].split("/")[5]
            series_id = row["series.submitter_id"]
            file_name = get_filename(row["file_name"], None, study_id, series_id)

            # case_id = row["submitter_id"].split("_")[0]
            # study_id = row["storage_urls"].split("/")[6]
            # series_id = row["series.submitter_id"]
            # # series_id = row["series.submitter_id"].split("_")[1]
            # file_name = get_filename(urls, case_id, study_id, series_id)

            data = {
                "guid": "",
                "md5": md5sum,
                "file_name": file_name,
                "size": filesize,
                # "authz": "/programs/Open/projects/A1",
                "authz": "/programs/Open/projects/A1_PETAL_REDCORAL",
                # "acl": "['A1','Open']",
                "acl": "['A1_PETAL_REDCORAL','Open']",
                "urls": urls,
            }
            indexing_data.append(data)
            # break
    return indexing_data


# output file
# guid	md5	file_name	size	authz	acl	urls
def write_manifest_tsv(indexing_data, filename):
    with open(filename, "w") as csvfile:
        fieldnames = ["guid", "md5", "file_name", "size", "authz", "acl", "urls"]
        writer = csv.DictWriter(csvfile, delimiter="\t", fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(indexing_data)


def main():
    open_files, seq_files = read_sequestered_file(sequestered_files)

    convert_manifests = [
        (
            S3_ACR_DATA_DIR.joinpath(manifest),
            Path("./to_index/acr/").joinpath(Path(manifest).name),
        )
        for manifest in IMAGING_DATA_MANIFESTS
    ]

    for from_manifest, to_manifest in convert_manifests:
        indexing_data = read_manifest_txt(from_manifest)
        write_manifest_tsv(
            indexing_data, to_manifest.with_stem("open_" + to_manifest.stem)
        )

    # for from_manifest, to_manifest in convert_manifests:
    #     open_indexing_data, seq_indexing_data = read_manifest_txt_v2(from_manifest, open_files, seq_files)
    #     write_manifest_tsv(open_indexing_data, to_manifest.with_stem("open_" + to_manifest.stem))
    #     write_manifest_tsv(seq_indexing_data, to_manifest.with_stem("seq_" + to_manifest.stem))


if __name__ == "__main__":
    main()
