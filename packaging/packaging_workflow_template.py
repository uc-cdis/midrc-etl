"""
General workflow for packaging MIDRC image files given an image_manifest_<batch>.tsv
"""


"""
0) QC on image_manifest_<batch>.tsv
"""
## Fix the headers?
sed -i 's/storage_url/storage_urls/g' image_manifest_${batch}.tsv
sed -i 's/case_id/case_ids/g' image_manifest_${batch}.tsv

"""
1) Process the submission to prepare for packaging
- This creates a folder in the output dir that should contain a "cases" directory containing one directory per case ID each of which contains one package manifest TSV per imaging series for that case. All the package manifests are listed in packages.txt.

"""
# upload the script
scp /Users/christopher/Documents/Notes/MIDRC/packaging/scripts/process_midrc_submission.py utilityvm.midrc.csoc:/home/ubuntu/wd/scripts/


# input shell variables
batch="ACR_20220606"
input_path="/home/ubuntu/download/replicated-data-acr/"
output_path="/home/ubuntu/wd/output"
script="/home/ubuntu/wd/scripts/process_midrc_submission.py"

# command to run in terminal
python3 ${script} --batch ${batch} --input_path ${input_path} --output_path ${output_path}

# checks
find ${output_path}/${batch}/cases -name "*.tsv" | wc -l
head ${output_path}/${batch}/packages.txt

"""
2) Run packaging script
- This creates the packages in s3 and also creates the "packages" dir in the batch's output directory, which contains series package manifest files named "<series_uid>.txt"
- Probably want to use tmux to keep packaging running in case you're disconnected.
"""
# upload the script
scp /Users/christopher/Documents/Notes/MIDRC/packaging/scripts/package_midrc_series.py utilityvm.midrc.csoc:/home/ubuntu/wd/scripts/

tmux
batch="ACR_20220606"
script="/home/ubuntu/wd/scripts/package_midrc_series.py"
batch_dir="/home/ubuntu/wd/output/${batch}"
python3 ${script} --batch_dir ${batch_dir}


# tmux commands:
tmux list-sessions # this lists all running tmux sessions along w ID number
tmux attach-session -t N # this attaches to the session number "N" specified
ctrl+b then d # detaches from a tmux session, leaving it running
ctrl+d # deletes a tmux session


"""
3) Split indexing manifests based on sequestration data

python3 sequestration_split.py --batch_dir --master_seq_file --exclude_cases --exclude_studies

- This creates the packages indexing manifests in directory "to_index" in the batch directory

"""
# upload the script
scp /Users/christopher/Documents/Notes/MIDRC/packaging/scripts/sequestration_split.py utilityvm.midrc.csoc:/home/ubuntu/wd/scripts/

# run the script
script="/home/ubuntu/wd/scripts/sequestration_split.py"
batch_dir="/home/ubuntu/wd/output/${batch}"
master_seq_file="/home/ubuntu/wd/master_sequestration_locations_54536_2022-10-24.tsv"
python3 ${script} --batch_dir ${batch_dir} --master_seq_file ${master_seq_file}

# checks
ll ${batch_dir}/to_index
head ${batch_dir}/to_index/packages_open_${batch}.tsv


"""
4) Run indexing script using the SEQ/OPEN indexing manifests
- This will create indexd records for the packages in either staging.midrc.org (open) or validate.midrc.org (seq)
- It also creates MDS "package" records, which are zip file manifests
"""
## upload credentials to VM from local:
rsync -rP /Users/christopher/Downloads/midrc-staging-credentials.json utilityvm.midrc.csoc:/home/ubunutu/wd/creds/midrc-staging-credentials.json

rsync -rP /Users/christopher/Downloads/midrc-validate-credentials.json utilityvm.midrc.csoc:/home/ubunutu/wd/creds/midrc-validate-credentials.json

# input shell variables
batch="ACR_20220606"
open_input="/home/ubuntu/wd/output/${batch}/to_index/packages_open_${batch}.tsv"
open_output="/home/ubuntu/wd/output/${batch}/to_index/packages_open_${batch}.tsv"
seq_input="/home/ubuntu/wd/output/${batch}/to_index/packages_seq_${batch}.tsv"
seq_output="/home/ubuntu/wd/output/${batch}/to_index/packages_seq_${batch}.tsv"
ocred = "/home/ubunutu/wd/creds/midrc-staging-credentials.json"
vcred = "/home/ubunutu/wd/creds/midrc-validate-credentials.json"

mkdir -p /home/ubuntu/wd/output/${batch}/indexed

gen3 --auth ${ocred} objects manifest publish ${open_input} --out-manifest-file ${open_output}
gen3 --auth ${vcred} objects manifest publish ${seq_input} --out-manifest-file ${seq_output}

# Copy the result to local
rsync -rP utilityvm.midrc.csoc:/home/ubuntu/wd/output/${batch}/indexed/ /Users/christopher/Documents/Notes/MIDRC/packaging/indexed/${batch}/


"""
5) move the files to open/sequestered bucket
run copy_from_s3_OPEN/SEQ.py on open/seq_files.tsv
check the failed_downloads.tsv for failures

script="/home/ubuntu/wd/scripts/copy_from_s3.py"
batch="ACR_20220606"
batch_dir="/home/ubuntu/wd/output/${batch}"

python3 ${script} --batch_dir ${batch_dir} --destination open
python3 ${script} --batch_dir ${batch_dir} --destination seq

"""
# copy the script
scp /Users/christopher/Documents/Notes/MIDRC/packaging/scripts/copy_from_s3.py utilityvm.midrc.csoc:/home/ubuntu/wd/scripts/

script="/home/ubuntu/wd/scripts/copy_from_s3.py"
batch="ACR_20220606"
batch_dir="/home/ubuntu/wd/output/${batch}"

python3 ${script} --batch_dir ${batch_dir} --destination open
python3 ${script} --batch_dir ${batch_dir} --destination seq


"""
6) Spot check some downloads using newly indexed packages

"""
gen3-client configure --profile=midrc-staging --apiendpoint=https://staging.midrc.org --cred=~/Downloads/midrc-staging-credentials.json

gen3-client download-single --profile=midrc-staging --guid=dg.MD1R/28dc4502-999b-4725-a277-6f391c1aa44d
