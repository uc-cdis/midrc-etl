#!/usr/bin/env bash

python3 process_acr_submission.py \
    --submission ACR_20220314 \
    --input_path /Users/andrewprokhorenkov/CTDS/projects/midrc/ssot-s3/replicated-data-acr \
    --output_path /Users/andrewprokhorenkov/CTDS/projects/midrc/indexing-data/packages_acr
