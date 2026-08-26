#!/usr/bin/env bash
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

set -euo pipefail

echo "Starting container initialization..."

if [[ $(grep -cE "127.0.0.1\s+$(hostname)" /etc/hosts || true) == "0" ]]; then
  echo "Adding container hostname '$(hostname)' to /etc/hosts..."
  echo "127.0.0.1 $(hostname) $(hostname -s)" | sudo tee -a /etc/hosts
  echo "DNS setup complete."
fi

echo "Setting up SSH..."
/bin/bash -c 'sudo ssh-keygen -A; sudo service ssh start'
echo "SSH setup complete."

echo "Starting PostgreSQL..."
sudo service postgresql start
echo "PostgreSQL started."

echo "Finished container initialization."
