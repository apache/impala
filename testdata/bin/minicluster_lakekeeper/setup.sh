#!/bin/sh
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

# Give services time to fully initialize
echo "Waiting for services..."
sleep 5

# Get Token from Keycloak
echo "Getting admin token..."
TOKEN=$(curl -s -X POST \
  "http://localhost:7070/realms/lakekeeper-realm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=lakekeeper-admin" \
  -d "password=password" \
  -d "grant_type=password" \
  -d "client_id=lakekeeper-client" | jq -r '.access_token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Failed to get token from Keycloak"
  exit 1
fi

echo "Token acquired successfully."

# Bootstrap Lakekeeper
echo "Bootstrapping Lakekeeper..."
curl -f -s -X POST "http://localhost:8181/management/v1/bootstrap" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"accept-terms-of-use": true}' \
  -o "/dev/null"

if [ $? -ne 0 ]; then
  echo "Bootstrap failed!"
  exit 1
fi

echo "Bootstrap successful."

# Create warehouse
echo "Creating warehouse..."
curl -f -s -X POST "http://localhost:8181/management/v1/warehouse" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "@/create-default-warehouse.json" \
  -o "/dev/null"

if [ $? -ne 0 ]; then
  echo "Warehouse creation failed!"
  exit 1
fi

echo "Warehouse created."
echo "Setup complete!"
