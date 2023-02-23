#!/usr/bin/env bash

$v=ACR_20220715
python3 process_acr_submission.py \
    --new \
    --submission $v \
    --input_path /Users/andrewprokhorenkov/CTDS/proj/midrc/data/replicated-data-acr/acrimage/2021 \
    --output_path /Users/andrewprokhorenkov/CTDS/proj/midrc/processed
