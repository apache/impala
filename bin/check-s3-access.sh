#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# This file is invoked if TARGET_FILESYSTEM is set to "s3" to check
# preconditions for accessing the specified S3 bucket. It inherits the
# environment variables from its caller and uses them as
# implicit parameters.
#
# The following environment variables are used:
#   TARGET_FILESYSTEM
#   S3_BUCKET
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#
# Returns:
#   0 (success): if preconditions for S3 access are satisfied.
#   1 (failure): if S3 access is unsuccessful.
#   2 (error): if the 'aws' executable is not on the path, or other
#                environmental problems cause the script to fail.
#
# If tests are to be run against S3 as the backing file system, verify that
# the assigned S3 bucket can be accessed.
# Access can be authorized by AWS_ credentials passed in environment variables
# or an EC2 IAM role assigned to the VM running the tests.
#
# If S3 access is granted via an IAM role assigned to the VM instance,
# then the credentials bound to the IAM role are retrieved automatically
# both by the Hadoop s3a: provider and by AWSCLI.
# In this case AWS keys must not be present in environment variables or in
# core-site.xml because their presence would preempt the IAM-based
# credentials.
#
# For further details on IAM roles refer to the Amazon docs at
# http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#instance-metadata-security-credentials
#
# The assigned IAM role and the security credentials provided
# by the role can be queried through the AWS instance metadata mechanism.
# Instance metadata is served through an HTTP connection to the special
# address 169.254.169.254. Details are described at:
# http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html#instancedata-data-retrieval
AWS_METADATA_IP_ADDRESS="169.254.169.254"

# safeguard against bad calls: if no S3 access is requested, just succeed
# silently.
if [ "${TARGET_FILESYSTEM-}" != "s3" ]; then
  exit 0
fi

echo "Checking S3 access"

# Check if the S3 bucket name is NULL.
if [[ -z ${S3_BUCKET-} ]]; then
  echo "Error: S3_BUCKET cannot be an empty string"
  exit 1
fi
# S3 access can be granted using access keys passed in via the environment
# or specifying an IAM role that has S3 access privileges.
# First check the environment variables, they have precedence over the IAM
# role: invalid credentials in the environment variables will prevent S3 access
# even if a valid IAM role is present.
# Use a subshell to prevent leaking AWS secrets.
if (set +x; [[ -z ${AWS_ACCESS_KEY_ID-} && -z ${AWS_SECRET_ACCESS_KEY-} ]]); then
  # If the environment variables are missing check the assumed IAM role.
  # The IAM role can be queried via http://169.254.169.254/ using instance
  # properties.
  # Wget will fail if the address is not present (i.e. the script is not running on
  # an EC2 VM) or the IAM role cannot be retrieved.
  # Set short timeouts so the script is not blocked if run outside of EC2.

  WGET_ARGS=(-T 1)
  WGET_ARGS+=(-t 1)
  WGET_ARGS+=(-q)
  WGET_ARGS+=(-o /dev/null)
  WGET_ARGS+=(http://${AWS_METADATA_IP_ADDRESS}/latest/meta-data/iam/security-credentials/)

  if ! wget "${WGET_ARGS[@]}" ; then
    echo \
"Error: missing valid S3 credentials.
You wanted to access an S3 bucket but you did not supply valid credentials.
The AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
have to be assigned valid credentials that belong to the owner of the
specified S3 bucket, or an IAM role authorized to access the S3 bucket
has to be assigned to the VM instance if this is run inside an EC2 VM."
    exit 1
  fi
fi

if [ ! -x "$(command -v aws)" ] ; then
  echo "Error: AWS CLI not found, unable to check S3 access."
  exit 2
fi

aws s3 ls "s3://${S3_BUCKET}/" 1>/dev/null
if [ $? != 0 ]; then
  echo "Error: accessing S3_BUCKET '${S3_BUCKET}' failed."
  exit 1
else
  exit 0
fi

