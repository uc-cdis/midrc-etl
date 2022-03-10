import os
import csv
from collections import defaultdict

filename = "narrow_open_image_ACR_20211115.tsv"
# filename = "narrow_open_image_ACR_20220107.tsv"

series = defaultdict(list)

with open(filename) as manifest_file:
    reader = csv.DictReader(manifest_file, delimiter="\t")
    for row in reader:
        url = row["urls"]
        instance_path = row["file_name"]
        instance_path_parts = instance_path.split("/")
        study_id = instance_path_parts[-3]
        series_id = instance_path_parts[-2]

        # print(instance_path)
        # print(instance_path_parts)
        # print(series_id)

        series[(study_id, series_id)].append(url)

        # break

# print(series.keys())

with open("cases.txt", "w") as cases_file:
    for (study_id, series_id), urls in series.items():
        os.makedirs(os.path.dirname("series/{}/".format(study_id)), exist_ok=True)

        fname = "series/{}/{}.txt".format(study_id, series_id)
        cases_file.write("{}\n".format(fname))

        with open(fname, "w") as series_file_list:
            series_file_list.writelines("\n".join(sorted(urls)))
