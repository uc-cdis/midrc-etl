#!/usr/bin/env bash

for d in */; do
  echo "$d"
  (cd "$d" && python3 package_series.py);
done
