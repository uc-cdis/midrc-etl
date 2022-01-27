
import pandas as pd
import csv
import yaml
import createZipFiles as packaging

with open("config.yml", "rt", encoding='utf8') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

df = pd.read_csv(cfg["manifest"]["file_name"], sep='\t', 
                      
                      header = 0,
                      index_col=1,
                      
                      usecols = ["file_name", "file_size", "md5sum", "acl", "storage_urls", "series_uid", "study_uid", "case_ids"]
                      #dtype = {"file_name":str,"file_size":str,"md5sum":str,"acl":str,"storage_urls":object,"series_uid":str,"study_uid":str,"case_ids":str}
                      )
# determines which attribute is used for packaging 
grouped = df.groupby(cfg["manifest"]["package_attribute"])
output_file = "output/output.tsv"

# create output file to store package contents 
with open(output_file, 'w') as tsvfile:
    writer = csv.writer(tsvfile, delimiter="\t", lineterminator="\n")
    writer.writerow(["record_type"+"    "+"md5"+"   "+"size"+"  "+"authz"+" "+"url"+"   "+"file_name"+" "+"package_contents"])

# parse s3 url to get bucket name and file path
def split_s3_path(s3_path):
    path_parts = s3_path.replace("s3://","").split("/")
    bucket = path_parts.pop(0)
    file_name = path_parts.pop(len(path_parts) - 1) # remove file_name from url so the whole series_uid folder can be packaged 
    key = "/".join(path_parts)
    return bucket, key

# to populate package_contents section in the output. package_contents contains details about individual files( like, size, md5sum and file_name)
def package_contents(group_dataframe):
    package_contents = []
    for row_index, row in group_dataframe.iterrows():        
        size = row.get("file_size")
        md5 = row["md5sum"]
        file_name = row["file_name"]
        row_entry = {"hashes":{"md5sum": md5}, "file_name": file_name, "size": size}
        package_contents.append(row_entry)
    return package_contents

# to populate output tsv files 
def create_outputtsv():
    for group_key,group_value in grouped:
        study_id = df.loc[df["series_uid"] == group_key,"study_uid"].values[0]
        case_id = df.loc[df["series_uid"] == group_key,"case_ids"].values[0]
        storage_url = df.loc[df["series_uid"] == group_key, "storage_urls"].values[0]
        project_id = df.loc[df["series_uid"] == group_key, "acl"].values[0]
        authz = "/programs/Open/projects/R1"
        if("A1" in project_id):
            authz = "/programs/Open/projects/A1"
        s3_bucket, folder_path = split_s3_path(storage_url)
        package_name = packaging.createZipFileStream(s3_bucket, folder_path, group_key, "dcm")
        package_size = packaging.getPackageSize(package_name)
        package_md5 = packaging.getPackagemd5(package_name)
        package_url = packaging.getPackageUrl(package_name)
        writer.writerow(["package"+"    "+(package_md5)+"   "+(package_size)+"  "+(authz)+" "+(package_url)+"   "+case_id+"/"+study_id+"/"+group_key+".zip"
                            +"  "+str(package_contents(group_value))])
 
create_outputtsv()
 
   
      
    

    
   


