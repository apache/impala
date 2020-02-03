#!/bin/bash
#
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
# The script automates some of the steps required to set up Kerberos servers and
# keytabs for an Impala minicluster running on Ubuntu.
#
# It installs the kerberos clients and servers, creates principals for the user and
# services and generates a keytab with all of the required users.
#
# This script is tested only on Ubuntu 18.04.
#
# References:
# * https://linuxconfig.org/how-to-install-kerberos-kdc-server-and-client-on-ubuntu-18-04
set -euo pipefail

# Source impala-config.sh to get config variables, including those set by
# testdata/bin/minikdc_env.sh.
DIR=$(dirname "$0")
. "${DIR}/../impala-config.sh"

if [[ "$IMPALA_KERBERIZE" != "true" ]]; then
  echo "IMPALA_KERBERIZE must be true, but was: $IMPALA_KERBERIZE"
  exit 1
fi

echo "Installing required packages. Sudo password may be required."
sudo apt install -y krb5-kdc krb5-admin-server krb5-config krb5-user

export KRB5CCNAME=/tmp/krb5cc_${USER}_dev


# IN /etc/krb5.conf
# default_realm = EXAMPLE.COM
echo "Please modify $KRB5_CONFIG to set default_realm to $MINIKDC_REALM."
echo "Also add $MINIKDC_REALM to the [realms] section with kdc and admin_server set, e.g."
echo "
[realms]
  EXAMPLE.COM = {
    kdc = $HOSTNAME
    admin_server = $HOSTNAME
  }
"

read -p "Press enter to continue"

# Create kerberos database for realm if not present.
if sudo kadmin.local -q "list_principals"; then
  echo "Using existing realm"
else
  echo "Creating new Kerberos database for realm"
  sudo krb5_newrealm
fi

echo "Please add or uncomment this line in /etc/krb5kdc/kadm5.acl:"
echo "  */admin *"
read -p "Press enter to continue"

sudo service krb5-admin-server restart
sudo service krb5-kdc restart

# Adds a principal if not present, and add its key to the keytab.
# This will prompt for a password.
add_principal() {
  local princ=$1
  if ! sudo kadmin.local -q "get_principal $princ" | grep -F "Principal: $princ"
  then
    echo "Principal $princ does not exist, creating"
    sudo kadmin.local -q "add_principal $princ"
  fi
  sudo kadmin.local -q "ktadd -k "$KRB5_KTNAME" $princ"
}

# Adds a service principal if not present, and add its key to the keytab.
# This will generate a random key and not prompt for a password.
add_service_principal() {
  local princ=$1
  if ! sudo kadmin.local -q "get_principal $princ" | grep -F "Principal: $princ"
  then
    echo "Principal $princ does not exist, creating"
    sudo kadmin.local -q "add_principal -randkey $princ"
  fi
  sudo kadmin.local -q "ktadd -k "$KRB5_KTNAME" $princ"
}

# Create an admin user.
add_principal $USER/admin@$MINIKDC_REALM

# Add service principals.
for svc in $USER hdfs mapred yarn HTTP hive hbase zookeeper impala impala-be
do
  add_service_principal $svc/localhost@$MINIKDC_REALM
done

# Kinit as the regular users.
sudo chown $USER $KRB5_KTNAME
kinit -kt $KRB5_KTNAME $USER/localhost@$MINIKDC_REALM

echo "Keytab contents:"
klist -kt $KRB5_KTNAME
