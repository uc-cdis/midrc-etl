import asyncio
import csv
import time

import aiohttp
from tqdm import tqdm

start_time = time.time()

headers = {
    "Authorization": ""
}


async def main():
    """drop it"""
    async with aiohttp.ClientSession(headers=headers) as session:
        with open("total_seq.csv") as to_remove_files:
            with open("result.csv", "w") as result_file:
                csv_reader = csv.DictReader(to_remove_files)
                for row in tqdm(csv_reader, total=1234812):
                    guid = row["guid"]
                    url = "https://validate.midrc.org/index/index/{}".format(guid)

                    async with session.get(url) as r_rev:
                        if r_rev.status == 200:
                            json_body = await r_rev.json()
                            rev = json_body["rev"]
                            del_url = "https://validate.midrc.org/index/index/{}?rev={}".format(guid, rev)

                            async with session.delete(del_url) as r:
                                result_file.write("{} {}\n".format(guid, r.status))

                    # async with session.delete(url) as resp:
                    #     result_file.write("{} {}\n".format(guid, resp.status))

loop = asyncio.get_event_loop()
result = loop.run_until_complete(main())
print("--- {} seconds ---".format(time.time() - start_time))
