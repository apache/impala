#!/usr/bin/env bash

cd $IMPALA_BE_DIR

stmt="$@"
tmp_file="profile.tmp"

cd $IMPALA_BE_DIR/build/release/service
./runquery -query="$stmt" -profile_output_file="$tmp_file" -iterations=3

google-pprof --text ./runquery "$tmp_file" | head -n 30
