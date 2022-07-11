import csv
import logging
import sys

from gen3.auth import Gen3Auth
from gen3.index import Gen3Index

# from gen3.tools.indexing import index_object_manifest

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

MANIFEST = "./to_remove/TODELETE_data.midrc.org_indexd_records_2021-8-31.tsv"


def main():
    """
    Remove indexd.
    """
    auth = Gen3Auth(refresh_file="data.midrc.org.json")

    print(auth.endpoint)
    index = Gen3Index(auth_provider=auth)

    with open(MANIFEST) as to_remove_file:
        to_remove_reader = csv.DictReader(to_remove_file, delimiter='\t', quotechar='"')
        for row in to_remove_reader:
            did = row["did"]
            r = index.delete_record(guid=did)
            print(r)


if __name__ == "__main__":
    main()
