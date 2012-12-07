#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -e
set -u

echo "Copying data files from the share.  If the file already exists locally, the files"\
     "will not be copied.  It's not check summing the files or anything like that, if"\
     "you need to force a copy, delete the local directory:"\
     "IMPALA_HOME/testdata/impala-data"

# TODO: Waiting on helpdesk ticket (HD-2861) to move this to a better location
DATASRC="a2226.halxg.cloudera.com:/data/1/workspace/impala-data"
DATADST=$IMPALA_HOME/testdata/impala-data
mkdir -p $DATADST

scp -i $IMPALA_HOME/ssh_keys/id_rsa_impala -o "StrictHostKeyChecking=no" -r $DATASRC/* $DATADST
