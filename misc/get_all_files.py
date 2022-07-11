import json
import logging
import sys

from gen3.auth import Gen3Auth
from gen3.index import Gen3Index

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def main():
    auth = Gen3Auth(refresh_file="data.midrc.org.json")

    print(auth.endpoint)

    index = Gen3Index(auth_provider=auth)
    all_files = index.get_all_records(limit=1000, paginate=True)
    with open("all_files_prod.json", "w") as f:
        f.write(json.dumps(all_files))


if __name__ == "__main__":
    main()
