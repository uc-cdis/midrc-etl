import sys
import logging

from gen3.auth import Gen3Auth
from gen3.tools.indexing import index_object_manifest

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

MANIFEST = "./submit.tsv"


def main():
    auth = Gen3Auth(refresh_file="staging.midrc.org.json")

    print(auth.endpoint)

    # use basic auth for admin privileges in indexd
    # auth = ("basic_auth_username", "basic_auth_password")

    index_object_manifest(
        commons_url=auth.endpoint,
        manifest_file=MANIFEST,
        thread_num=6,
        auth=auth,
        replace_urls=False,
        manifest_file_delimiter="\t"  # put "," if the manifest is csv file
    )


if __name__ == "__main__":
    main()
