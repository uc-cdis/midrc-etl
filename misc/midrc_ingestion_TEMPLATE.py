"""
we have added new properties in the 'case' and 'imaging_study' nodes, 'age_at_index_gt89' and 'age_at_imaging_gt89', respectively. All ages "greater than 89"  must be replaced with NaN
"""
########################
import pandas as pd
import numpy as np
import sys
import gen3
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.query import Gen3Query

git_dir = "/Users/ericgiger/Documents/GitHub"
sdk_dir = "/cgmeyer/gen3sdk-python"
sys.path.insert(1, "{}{}".format(git_dir, sdk_dir))
from expansion.expansion import Gen3Expansion

#%run /Users/ericgiger/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py
########################
vapi = "https://validate.midrc.org"
vcred = "/Users/ericgiger/Downloads/validate-credentials.json"
vauth = Gen3Auth(vapi, refresh_file=vcred)
vsub = Gen3Submission(vapi, vauth)
vquery = Gen3Query(vauth)
vexp = Gen3Expansion(vapi, vauth, vsub)
vexp.get_project_ids()
########################
sapi = "https://staging.midrc.org"
scred = "/Users/ericgiger/Downloads/midrc-staging-credentials.json"
sauth = Gen3Auth(sapi, refresh_file=scred)
ssub = Gen3Submission(sapi, sauth)
squery = Gen3Query(sauth)
sexp = Gen3Expansion(sapi, sauth, ssub)
sexp.get_project_ids()
########################################################################
########################################################################
sub_dir = "/Users/ericgiger/Documents/Notes/MIDRC/submission_tsvs"
os.chdir(sub_dir)
org = "RSNA"
date = "20220228"
batch_name = "{}_{}".format(org, date)
batch_dir = "{}/{}_{}".format(sub_dir, org, date)
os.chdir(batch_dir)
tsv_dir = "{}/{}_{}/originals".format(sub_dir, org, date)

################################################################################
################################################################################
""" Create dataset and core_metadata_collection
"""
################################################################################
### Staging:
pid = "Open-R1"
prog, proj = pid.split("-", 1)

batch_name = "{}_{}".format(org, date)
dataset_txt = """{
  "type": "dataset",
  "data_contributor": "%s",
  "data_description": "%s",
  "projects": [
    {
      "code": "R1"
    }
  ],
  "submitter_id": "%s"
}""" % (
    org,
    batch_name,
    batch_name,
)

dataset_json = json.loads(dataset_txt)
data = ssub.submit_record(program=prog, project=proj, json=dataset_json)
print(data)

# create a generic CMC
cmc_txt = """{
        "description": "Data from the %s study %s.",
        "submitter_id": "%s",
        "title": "%s",
        "project_id": "%s",
        "type": "core_metadata_collection",
        "projects": [
            {
                "code": "%s"
            }
        ]
    }""" % (
    pid,
    batch_name,
    batch_name,
    batch_name,
    pid,
    proj,
)

cmc_json = json.loads(cmc_txt)
data = ssub.submit_record(program=prog, project=proj, json=cmc_json)
print(data)

################################################################################
### Validate
pid = "SEQ_Open-R3"
prog, proj = pid.split("-", 1)

batch_name = "{}_{}".format(org, date)
dataset_txt = """{
  "type": "dataset",
  "data_contributor": "%s",
  "data_description": "%s",
  "projects": [
    {
      "code": "%s"
    }
  ],
  "submitter_id": "%s"
}""" % (
    org,
    batch_name,
    proj,
    batch_name,
)

dataset_json = json.loads(dataset_txt)
data = vsub.submit_record(program=prog, project=proj, json=dataset_json)
print(data)

# create a generic CMC
cmc_txt = """{
        "description": "Data from the %s study %s.",
        "submitter_id": "%s",
        "title": "%s",
        "project_id": "%s-%s",
        "type": "core_metadata_collection",
        "projects": [
            {
                "code": "%s"
            }
        ]
    }""" % (
    pid,
    batch_name,
    batch_name,
    batch_name,
    prog,
    proj,
    proj,
)

cmc_json = json.loads(cmc_txt)
data = vsub.submit_record(program=prog, project=proj, json=cmc_json)
print(data)

################################################################################
################################################################################
""" Create list of case_ids for grand challenge to filter from
"""
################################################################################
remove_dir = "/Users/ericgiger/Documents/Notes/MIDRC/submission_tsvs/{}/originals/unaltered/remove_case_ids".format(
    batch_name
)
os.chdir(remove_dir)

# let's grab the case_ids to delete
filename = "/Users/ericgiger/Documents/Notes/MIDRC/admin/grand challenge 2022/grand_challenge_cases_2022.tsv"
cf = pd.read_csv(filename, sep="\t", header=0, dtype=str)
cdel = list(set(cf.submitter_id))


########################################################################
""" Fix case, measurement and imaging_study TSVs
"""
########################################################################
os.chdir(tsv_dir)
tfiles = glob.glob("*_{}.tsv".format(date))
tsv_regex = re.compile(r"(.*)_{}_{}\.tsv$".format(org, date))
tnames = [tsv_regex.match(i).groups()[0] for i in tfiles if tsv_regex.match(i)]
tsvs = dict(zip(tnames, tfiles))

remove_tsvs = [
    "ct_scan",
    "radiography_exam",
    "mr_exam",
    "ct_series",
    "cr_series",
    "dx_series",
    "mr_series",
    "us_series",
    "manifestfile",
]
for tname in list(tsvs):
    if tname.startswith("MISSING") or tname in remove_tsvs:
        del tsvs[tname]
display(tsvs)
############################################
############################################
node = "case"
filename = tsvs[node]
df = pd.read_csv(filename, sep="\t", header=0, dtype=str)
df.type = "case"
df["datasets.submitter_id"] = batch_name
# remove PHI
df["age_at_index"] = pd.to_numeric(df["age_at_index"])
df["age_at_index_gt89"] = ""
for i in df.index:
    temp = df["age_at_index"][i]
    int(temp)
    if temp > 89:
        df["age_at_index_gt89"][i] = "Yes"
    else:
        df["age_at_index_gt89"][i] = "No"
# df.loc[df['age_at_index']>89]=np.nan
df["age_at_index"].loc[df["age_at_index"] > 89] = np.nan  # (this worked?)
display(df.submitter_id[0])
df["case_ids"] = df["submitter_id"]
display(df.case_ids[0])
df.loc[df["sex"] == "Unknown", "sex"] = "Not Reported"
display(list(set(df.sex)))
df.loc[df["race"] == "Unknown/Declined", "race"] = "Not Reported"
display(list(set(df.race)))
hdf = df.loc[~df["case_ids"].isin(cdel)]
hdf.to_csv(filename, sep="\t", index=False)
############################################
############################################
node = "measurement"
filename = tsvs[node]
df = pd.read_csv(filename, sep="\t", header=0, dtype=str)
display(df.submitter_id[0])
df.type = "measurement"
df["cases.submitter_id"] = df["cases.submitter_id"].str.replace("Case_", "")
display(df["cases.submitter_id"][0])
df["case_ids"] = df["cases.submitter_id"]
display(df.case_ids[0])
hdf = df.loc[~df["case_ids"].isin(cdel)]
hdf.to_csv(filename, sep="\t", index=False)
############################################
############################################
node = "imaging_study"
filename = tsvs[node]
df = pd.read_csv(filename, sep="\t", header=0, dtype=str)
df.type = "imaging_study"
df["age_at_imaging"] = pd.to_numeric(df["age_at_imaging"])
df["age_at_imaging_gt89"] = ""
for i in df.index:
    temp = df["age_at_imaging"][i]
    int(temp)
    if temp > 89:
        df["age_at_imaging_gt89"][i] = "Yes"
    else:
        df["age_at_imaging_gt89"][i] = "No"
df["age_at_imaging"].loc[df["age_at_imaging"] > 89] = np.nan  # (this worked?)
#############
df["cases.submitter_id"] = df["cases.submitter_id"].str.replace("Case_", "")
display(df["cases.submitter_id"][0])
df["case_ids"] = df["case_ids"].str.replace("Case_", "")
assert len(df.loc[df["case_ids"].isna()]) == 0
display(df["case_ids"][0])
df.loc[df["study_year_shifted"] == "Yes", "study_year_shifted"] = True
df.loc[df["study_year_shifted"] == "No", "study_year_shifted"] = False
display(list(set(df["study_year_shifted"])))
if "study_covid_status" in df:
    df.drop(columns="study_covid_status", inplace=True)
if "series_count" in df:
    df.drop(columns="series_count", inplace=True)
# we don't need 'study_covid_status' and 'series_count'
hdf = df.loc[~df["case_ids"].isin(cdel)]
hdf.to_csv(filename, sep="\t", index=False)

display(list(set(df.study_modality)))
# ['MR', 'DX', 'RF', 'CT', 'CR']
########################################################################
########################################################################
""" Prep the imaging_series_file TSVs using the indexed zip packages
"""
########################################################################

index_dir = "{}/indexed".format(batch_dir)
# packages_missing_RSNA_20220105.tsv
# indexed_packages_open_RSNA_20220105.tsv
# indexed_packages_seq_RSNA_20220105.tsv

open_file = "{}/indexed_packages_open_{}.tsv".format(index_dir, batch_name)
seq_file = "{}/indexed_packages_seq_{}.tsv".format(index_dir, batch_name)
oi = pd.read_csv(open_file, sep="\t", header=0, dtype=str)
si = pd.read_csv(seq_file, sep="\t", header=0, dtype=str)

i = pd.concat([oi, si], ignore_index=True)
assert len(i) == (len(oi) + len(si))

display(i["file_name"][0])
i["case_ids"] = i["file_name"].str.extract("(.*)\/.*\/.*\.zip")
i["study_uid"] = i["file_name"].str.extract(".*\/(.*)\/.*\.zip")
i["series_uid"] = i["file_name"].str.extract(".*\/(.*)\.zip")
# checks
display(i.case_ids[0])
display(i.study_uid[0])
display(i.series_uid[0])
# '419639-000363'
# '1.2.826.0.1.3680043.10.474.419639.151143839557569966163444578318'
# '1.2.826.0.1.3680043.10.474.419639.271935778840325009817577328566'
i.rename(
    columns={"guid": "object_id", "md5": "md5sum", "size": "file_size"}, inplace=True
)


os.chdir(tsv_dir)
series_tsvs = glob.glob("*_series_{}_{}*".format(org, date))
series_regex = re.compile(r"(.*_series)_{}_{}.*".format(org, date))
series_nodes = [
    series_regex.match(i).groups()[0] for i in series_tsvs if series_regex.match(i)
]
series_tsvs = dict(zip(series_nodes, series_tsvs))

missing_series_uids = []
for node in series_nodes:  # node = series_nodes[0]
    print("\n\tProcessing {}: {}".format(node, series_tsvs[node]))
    df = pd.read_csv(series_tsvs[node], sep="\t", header=0, dtype=str)

    df["case_ids"] = df.case_ids.str.replace("Case_", "")
    display(df.case_ids[0])
    if "pixel_spacing" in df:
        df["pixel_spacing"] = df.pixel_spacing.str.replace("\\", ",")
        display(df.pixel_spacing[0])
    if "imager_pixel_spacing" in df:
        df["imager_pixel_spacing"] = df.pixel_spacing.str.replace("\\", ",")
        display(df.imager_pixel_spacing[0])
    if "scanning_sequence" in df:
        df["scanning_sequence"] = df.scanning_sequence.str.replace("\\", ",")
        display(df.scanning_sequence[0])
    df.loc[df["lossy_image_compression"] == "0.0", "lossy_image_compression"] = "00"
    df.loc[df["lossy_image_compression"] == "0", "lossy_image_compression"] = "00"
    df.loc[df["lossy_image_compression"] == "1.0", "lossy_image_compression"] = "01"
    df.loc[df["lossy_image_compression"] == "1", "lossy_image_compression"] = "01"
    display(list(set(df.lossy_image_compression)))
    df.type = "{}_file".format(node)
    display(list(set(df.type)))
    df["data_format"] = "DCM"
    df["data_type"] = "DICOM"
    df["data_category"] = df["modality"]
    display(list(set(df.data_category)))

    df.drop(columns={"series_type", "series_name"}, inplace=True)
    # df['contrast_bolus_agent'].astype(str)
    # df['contrast_bolus_agent'] = df['contrast_bolus_agent'].str.replace("nan","")
    if node == "mr_series":
        df.drop(columns={"detector_type"}, inplace=True)
    if node == "ct_series":
        df.drop(columns={"magnetic_field_strength", "scanning_sequence"}, inplace=True)
        df.loc[df["contrast_bolus_agent"] == "120.0", "contrast_bolus_agent"] = "120.00"

    df["series_description"] = df["series_description"].str.replace("²", "^2")

    if (
        node in ["dx_series", "cr_series", "ct_series"]
        and "contrast_bolus_agent_number" in df
    ):
        df.drop(columns="contrast_bolus_agent_number", inplace=True)

    if "radiography_exams.submitter_id" in df:
        df.rename(
            columns={"radiography_exams.submitter_id": "imaging_studies.submitter_id"},
            inplace=True,
        )
    elif "ct_scans.submitter_id" in df:
        df.rename(
            columns={"ct_scans.submitter_id": "imaging_studies.submitter_id"},
            inplace=True,
        )
    elif "mr_exams.submitter_id" in df:
        df.rename(
            columns={"mr_exams.submitter_id": "imaging_studies.submitter_id"},
            inplace=True,
        )
    display(df["imaging_studies.submitter_id"][0])  # check

    df["core_metadata_collections.submitter_id"] = batch_name

    series_uids = list(set(df.series_uid))
    print(
        "\t\tThe {} TSV contains {} records and {} unique series UIDs.".format(
            node, len(df), len(series_uids)
        )
    )
    d = pd.merge(
        df,
        i[["object_id", "md5sum", "file_size", "file_name", "series_uid"]],
        on="series_uid",
        how="left",
    )
    missing = d.loc[d["object_id"].isna()]
    ready = d.loc[~d["object_id"].isna()]
    ready_name = "{}_file_{}_{}.tsv".format(node, org, date)
    ready.to_csv(ready_name, sep="\t", index=False)
    if len(missing) > 0:
        missing_name = "MISSING_GUID_{}_file_{}_{}.tsv".format(node, org, date)
        missing.to_csv(missing_name, sep="\t", index=False)
    print(
        "\t\tThere are {} records with matching series file GUID in indexd.".format(
            len(ready)
        )
    )
    print(
        "\t\tThere are {} records MISSING a series file GUID in indexd.".format(
            len(missing)
        )
    )
    # save the series records with no index metadata in "i"
    missing_series_uids = missing_series_uids + list(set(missing["series_uid"]))  #

# CT has [nan]

################################################################
################################################################
""" Split tsvs
"""
################################################################


def split_tsvs(
    tsv_dir=tsv_dir,
    org=org,
    date=date,
    sfile="/Users/ericgiger/Documents/Notes/MIDRC/sequestration/MIDRC-Restricted/Open & SEQ Assignment Master List/master_sequestration_locations_16188_2022-04-13.tsv",
    remove_tsvs=[
        "ct_scan",
        "radiography_exam",
        "mr_exam",
        "ct_series",
        "cr_series",
        "dx_series",
        "mr_series",
        "manifestfile",
    ],
):

    os.chdir(tsv_dir)
    from pathlib import Path

    split_dir = "{}/split".format(tsv_dir)
    Path(split_dir).mkdir(parents=True, exist_ok=True)

    s = pd.read_csv(sfile, sep="\t", header=0, dtype=str)
    cids = s.set_index("case_ids").to_dict()["dataset"]  # case_ids
    open_ids = {i for i in cids if cids[i] == "Open"}
    seq_ids = {i for i in cids if cids[i] == "Seq"}
    ignore_ids = {i for i in cids if cids[i] == "Ignore"}
    assert len(seq_ids) + len(open_ids) + len(ignore_ids) == len(cids)
    initial = org[0]
    seq_pid = "SEQ_Open-{}3".format(initial)
    open_pid = "Open-{}1".format(initial)

    tfiles = glob.glob("*_{}.tsv".format(date))
    tsv_regex = re.compile(r"(.*)_{}_{}\.tsv$".format(org, date))
    tnames = [tsv_regex.match(i).groups()[0] for i in tfiles if tsv_regex.match(i)]
    tsvs = dict(zip(tnames, tfiles))
    for tname in list(tsvs):
        if tname.startswith("MISSING") or tname in remove_tsvs:
            del tsvs[tname]
    display(tsvs)

    for tname in tsvs:
        print("\n\tProcessing {}: {}".format(tname, tsvs[tname]))
        df = pd.read_csv(tsvs[tname], sep="\t", header=0, dtype=str)
        if "case_ids" in list(df) or tname == "case":
            if tname == "case":
                sdf = copy.deepcopy(df[df["submitter_id"].isin(seq_ids)])  # 188
                odf = copy.deepcopy(df[df["submitter_id"].isin(open_ids)])  # 1384
                # assert(len(odf)+len(sdf)==len(df))
                sdf["datasets.submitter_id"] = "{}_{}".format(org, date)
                odf["datasets.submitter_id"] = "{}_{}".format(org, date)
            else:
                sdf = copy.deepcopy(df[df["case_ids"].isin(seq_ids)])  #
                odf = copy.deepcopy(df[df["case_ids"].isin(open_ids)])  #
            sdf["project_id"] = seq_pid
            odf["project_id"] = open_pid
            seq_name = "split/SEQ_{}".format(tsvs[tname])
            open_name = "split/OPEN_{}".format(tsvs[tname])
            if len(sdf) > 0:
                sdf.to_csv(seq_name, sep="\t", index=False)
            else:
                print("\n\t\tNo sequestered cases in '{}'".format(tname))
            if len(odf) > 0:
                odf.to_csv(open_name, sep="\t", index=False)
            else:
                print("\n\t\tNo open cases in '{}'".format(tname))
            if (len(odf) + len(sdf)) != len(df):
                d = pd.merge(s, df, on="case_ids", how="right")
                # d.to_csv('split/{}_sequestration_locations.tsv'.format(tname),sep='\t',index=False)
                missing = d.loc[d["dataset"].isna()]
                print(
                    "\n\t\t{} records for {} cases missing from sequestration list in '{}'".format(
                        len(missing), len(list(set(missing.case_ids))), tname
                    )
                )
                missing.to_csv(
                    "split/MISSING_{}.tsv".format(tname), sep="\t", index=False
                )
        else:
            print("\n\t\tFound no case_ids in {}!!".format(tsvs[tname]))


os.chdir(tsv_dir)
split_tsvs(
    org=org,
    date=date,
    sfile="/Users/ericgiger/Documents/Notes/MIDRC/sequestration/MIDRC-Restricted/Open & SEQ Assignment Master List/master_sequestration_locations_23315_2022-06-01.tsv",
)


#################################################################
#################################################################
""" Submit Files
"""
#################################################################

sub_order = [
    "case",
    "measurement",
    "imaging_study",
    "dx_series_file",
    "ct_series_file",
    "cr_series_file",
    "mr_series_file" "us_series_file",
]

# sub_order = ['dx_series_file',
#  'ct_series_file',
#  'cr_series_file',
#  'mr_series_file']

split_dir = "{}/split".format(tsv_dir)
os.chdir(split_dir)

open_files = glob.glob("OPEN_*_{}.tsv".format(date))
open_regex = re.compile(r"OPEN_(.*)_{}_{}\.tsv$".format(org, date))
open_names = [
    open_regex.match(i).groups()[0] for i in open_files if open_regex.match(i)
]
open_tsvs = dict(zip(open_names, open_files))
display(open_tsvs)

seq_files = glob.glob("SEQ_*_{}.tsv".format(date))
seq_regex = re.compile(r"SEQ_(.*)_{}_{}\.tsv$".format(org, date))
seq_names = [seq_regex.match(i).groups()[0] for i in seq_files if seq_regex.match(i)]
seq_tsvs = dict(zip(seq_names, seq_files))
display(seq_tsvs)

for node in sub_order:
    if node in open_tsvs:
        sd = sexp.submit_file(
            project_id="Open-R1", filename=open_tsvs[node], chunk_size=250
        )
    if node in seq_tsvs:
        vd = vexp.submit_file(
            project_id="SEQ_Open-R3", filename=seq_tsvs[node], chunk_size=250
        )
##
