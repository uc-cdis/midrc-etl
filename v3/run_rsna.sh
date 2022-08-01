#!/usr/bin/env bash

python3 process_rsna_submission.py \
    --submission RSNA_20220314 \
    --input_path /Users/andrewprokhorenkov/CTDS/projects/midrc/ssot-s3/replicated-data-rsna \
    --output_path /Users/andrewprokhorenkov/CTDS/projects/midrc/indexing-data/packages_rsna
