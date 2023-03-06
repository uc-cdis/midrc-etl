#!/usr/bin/env bash

for v in midrc-ricord-2021-08-20; do
    python3 process_rsna_submission.py \
        --submission $v \
        --input_path /Users/andrewprokhorenkov/CTDS/proj/midrc/data/replicated-data-rsna \
        --output_path /Users/andrewprokhorenkov/CTDS/proj/midrc/processed
done
