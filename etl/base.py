
import pandas as pd


df = pd.read_csv('./data/imaging_data_manifest_2021_09_23.tsv', sep='\t', 
                      
                      header = 0,
                      index_col=1,
                      
                      usecols = ["file_name", "file_size", "md5sum", "acl", "storage_urls", "series_uid", "study_uid", "case_ids"],
                      dtype = {"file_name":str,"file_size":str,"md5sum":str,"acl":str,"storage_urls":object,"series_uid":str,"study_uid":str,"case_ids":str}
                      )
grouped = df.groupby("series_uid")


# for name in grouped:
#     print(grouped.get_group(name).get("storage_urls"))

for group_key,group_value in grouped:
    #print(grouped.get_group(name).get("storage_urls"))
    print(group_key)
    for row_index, row in group_value.iterrows():
        url = row["storage_urls"]
        print(url)
        size = row.get("file_size")
        print((size))
        md5 = row["md5sum"]
        print(md5)
        study_uid = row["study_uid"]
        print(study_uid)
        case_id = row["case_ids"]
        print(case_id)
        print(group_key)
    #urls = grouped.get_group(name).get("storage_urls")
    #file_size = grouped["file_size"].tolist()
    #md5sum = grouped.get_group(name).get("md5sum")
    #study_uid = grouped.get_group(name).get("study_uid")
    #case_ids = grouped.get_group(name).get("case_ids")
    #series_uid = name
    #index = 0
    #print(file_size)
    #for  storage_url in urls:
     #   print(storage_url)
      
    

    
   


