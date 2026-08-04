#!/usr/bin/env bash
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

# Jenkins usage:
#   ./bin/jenkins/run-k8s-e2e-tests.sh
#
# This script provisions an ephemeral kind/k3d cluster, deploys the Impala Helm chart,
# runs remote-cluster E2E smoke tests, and tears down the cluster.

set -euo pipefail
: "${IMPALA_HOME:=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

cd "${IMPALA_HOME}"

: "${K8S_E2E_KIND_CLUSTER_NAME:=impala-e2e}"
: "${K8S_E2E_K3D_CLUSTER_NAME:=impala-e2e}"
: "${K8S_E2E_RUNTIME:=kind}"
: "${K8S_E2E_NAMESPACE:=impala}"
: "${K8S_E2E_RELEASE_NAME:=impala-e2e}"
: "${K8S_E2E_HELM_VALUES_FILE:=helm/impala/values-example.yaml}"
: "${K8S_E2E_TEST_TARGET:=infra/test_k8s_external_cluster.py}"
: "${K8S_E2E_WAIT_TIMEOUT:=600s}"
: "${K8S_E2E_KEEP_CLUSTER:=false}"
: "${K8S_E2E_KIND_IMAGE:=}"
: "${K8S_E2E_K3D_IMAGE:=}"
: "${K8S_E2E_PORT_FORWARD_MODE:=auto}"
: "${K8S_E2E_DRY_RUN:=false}"

if [[ "${1:-}" == "--dry-run" ]]; then
  K8S_E2E_DRY_RUN=true
fi

require_cmd() {
  local tool="$1"
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "Missing required tool: ${tool}"
    return 1
  fi
}

resolve_impalad_service() {
  local expected_service="${K8S_E2E_RELEASE_NAME}-impala-impalad"
  if kubectl -n "${K8S_E2E_NAMESPACE}" get svc \
      "${expected_service}" >/dev/null 2>&1; then
    echo "${expected_service}"
    return 0
  fi

  kubectl -n "${K8S_E2E_NAMESPACE}" get svc -o json | python3 - \
    "${K8S_E2E_RELEASE_NAME}" <<'PY'
import json
import sys

release_name = sys.argv[1]
services = json.load(sys.stdin).get("items", [])
for service in services:
    selector = ((service.get("spec") or {}).get("selector") or {})
    if (
        selector.get("app.kubernetes.io/instance") == release_name
        and selector.get("app.kubernetes.io/name") == "impala"
        and selector.get("app.kubernetes.io/component") == "impalad"
    ):
        print(service.get("metadata", {}).get("name", ""))
        break
PY
}

if [[ "${K8S_E2E_DRY_RUN}" == "true" ]]; then
  echo "DRY RUN: runtime='${K8S_E2E_RUNTIME}'"
  if [[ "${K8S_E2E_RUNTIME}" == "kind" ]]; then
    echo "DRY RUN: would create kind cluster '${K8S_E2E_KIND_CLUSTER_NAME}'"
  elif [[ "${K8S_E2E_RUNTIME}" == "k3d" ]]; then
    echo "DRY RUN: would create k3d cluster '${K8S_E2E_K3D_CLUSTER_NAME}'"
  fi
  echo "DRY RUN: would deploy release '${K8S_E2E_RELEASE_NAME}' to namespace"
  echo "         '${K8S_E2E_NAMESPACE}' with values file"
  echo "         '${K8S_E2E_HELM_VALUES_FILE}'."
  echo "DRY RUN: would run test target '${K8S_E2E_TEST_TARGET}'."
  exit 0
fi

require_cmd docker
require_cmd kubectl
require_cmd helm
if [[ "${K8S_E2E_RUNTIME}" == "kind" ]]; then
  require_cmd kind
elif [[ "${K8S_E2E_RUNTIME}" == "k3d" ]]; then
  require_cmd k3d
else
  echo "Unsupported K8S_E2E_RUNTIME='${K8S_E2E_RUNTIME}'. Use kind or k3d."
  exit 1
fi

TMP_KUBECONFIG="$(mktemp)"
export KUBECONFIG="${TMP_KUBECONFIG}"
RELEASE_SELECTOR="app.kubernetes.io/instance=${K8S_E2E_RELEASE_NAME},"
RELEASE_SELECTOR+="app.kubernetes.io/name=impala"

on_exit() {
  local exit_code=$?
  if [[ ${exit_code} -ne 0 ]]; then
    kubectl -n "${K8S_E2E_NAMESPACE}" get pods -o wide || true
  fi
  if [[ "${K8S_E2E_KEEP_CLUSTER}" != "true" ]]; then
    if [[ "${K8S_E2E_RUNTIME}" == "kind" ]]; then
      kind delete cluster --name "${K8S_E2E_KIND_CLUSTER_NAME}" >/dev/null 2>&1 || true
    else
      k3d cluster delete "${K8S_E2E_K3D_CLUSTER_NAME}" >/dev/null 2>&1 || true
    fi
  fi
  rm -f "${TMP_KUBECONFIG}" || true
  return ${exit_code}
}
trap on_exit EXIT

if [[ "${K8S_E2E_RUNTIME}" == "kind" ]]; then
  kind delete cluster --name "${K8S_E2E_KIND_CLUSTER_NAME}" >/dev/null 2>&1 || true
  CREATE_CLUSTER_CMD=(kind create cluster --name "${K8S_E2E_KIND_CLUSTER_NAME}")
  if [[ -n "${K8S_E2E_KIND_IMAGE}" ]]; then
    CREATE_CLUSTER_CMD+=(--image "${K8S_E2E_KIND_IMAGE}")
  fi
  "${CREATE_CLUSTER_CMD[@]}"
  kind get kubeconfig --name "${K8S_E2E_KIND_CLUSTER_NAME}" > "${TMP_KUBECONFIG}"
else
  k3d cluster delete "${K8S_E2E_K3D_CLUSTER_NAME}" >/dev/null 2>&1 || true
  CREATE_CLUSTER_CMD=(
    k3d cluster create "${K8S_E2E_K3D_CLUSTER_NAME}" --servers 1 --agents 0 --wait
  )
  if [[ -n "${K8S_E2E_K3D_IMAGE}" ]]; then
    CREATE_CLUSTER_CMD+=(--image "${K8S_E2E_K3D_IMAGE}")
  fi
  "${CREATE_CLUSTER_CMD[@]}"
  k3d kubeconfig write "${K8S_E2E_K3D_CLUSTER_NAME}" \
    --output "${TMP_KUBECONFIG}" \
    --overwrite >/dev/null
fi

kubectl create namespace "${K8S_E2E_NAMESPACE}" >/dev/null 2>&1 || true

helm upgrade --install "${K8S_E2E_RELEASE_NAME}" ./helm/impala \
  -n "${K8S_E2E_NAMESPACE}" \
  -f "${K8S_E2E_HELM_VALUES_FILE}" \
  --set persistence.enabled=false

DEPLOYMENTS="$(
  kubectl -n "${K8S_E2E_NAMESPACE}" get deployments \
    -l "${RELEASE_SELECTOR}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
)"
if [[ -z "${DEPLOYMENTS}" ]]; then
  echo "No Impala deployments found for release '${K8S_E2E_RELEASE_NAME}'."
  exit 1
fi
while IFS= read -r deployment; do
  if [[ -z "${deployment}" ]]; then
    continue
  fi
  kubectl -n "${K8S_E2E_NAMESPACE}" rollout status \
    "deployment/${deployment}" \
    --timeout="${K8S_E2E_WAIT_TIMEOUT}"
done <<< "${DEPLOYMENTS}"

K8S_IMPALAD_SERVICE="$(resolve_impalad_service)"
if [[ -z "${K8S_IMPALAD_SERVICE}" ]]; then
  echo "Unable to resolve impalad service for release '${K8S_E2E_RELEASE_NAME}'."
  exit 1
fi

K8S_NAMESPACE="${K8S_E2E_NAMESPACE}" \
K8S_IMPALAD_SERVICE="${K8S_IMPALAD_SERVICE}" \
K8S_PORT_FORWARD_MODE="${K8S_E2E_PORT_FORWARD_MODE}" \
K8S_TEST_TARGET="${K8S_E2E_TEST_TARGET}" \
./bin/run-k8s-e2e-tests.sh
