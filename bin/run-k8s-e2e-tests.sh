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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${IMPALA_HOME:=$(cd "${SCRIPT_DIR}/.." && pwd)}"
export IMPALA_HOME
cd "${IMPALA_HOME}"

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
  shift
fi

if [[ "${DRY_RUN}" != "true" ]]; then
  # Populate the standard Impala test environment (python wrapper, toolchain vars, etc).
  set +u
  source "${IMPALA_HOME}/bin/impala-config.sh" >/dev/null 2>&1
  set -u
  export PATH="${IMPALA_HOME}/bin:${PATH}"
fi

K8S_NAMESPACE="${K8S_NAMESPACE:-impala}"
K8S_IMPALAD_SERVICE="${K8S_IMPALAD_SERVICE:-}"
K8S_IMPALAD_POD="${K8S_IMPALAD_POD:-}"
K8S_ENABLE_PORT_FORWARD="${K8S_ENABLE_PORT_FORWARD:-true}"
K8S_PORT_FORWARD_MODE="${K8S_PORT_FORWARD_MODE:-auto}"
K8S_PORT_FORWARD_LOG="${K8S_PORT_FORWARD_LOG:-/tmp/impala-k8s-port-forward.log}"

K8S_TEST_HOST="${K8S_TEST_HOST:-127.0.0.1}"
K8S_BEESWAX_PORT="${K8S_BEESWAX_PORT:-21000}"
K8S_HS2_PORT="${K8S_HS2_PORT:-21050}"
K8S_HS2_HTTP_PORT="${K8S_HS2_HTTP_PORT:-28000}"
K8S_WEB_PORT="${K8S_WEB_PORT:-25000}"

K8S_TEST_TARGET="${K8S_TEST_TARGET:-infra/test_k8s_external_cluster.py}"
K8S_KUDU_MASTER_HOSTS="${K8S_KUDU_MASTER_HOSTS:-}"
export ENABLE_BEESWAX=false

REMOTE_URL="${IMPALA_REMOTE_URL:-http://${K8S_TEST_HOST}:${K8S_WEB_PORT}}"

PORT_FORWARD_PID=""
cleanup() {
  if [[ -n "${PORT_FORWARD_PID}" ]] && kill -0 "${PORT_FORWARD_PID}" 2>/dev/null; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_hs2_socket() {
  "${IMPALA_HOME}/bin/impala-python3" - "${K8S_TEST_HOST}" "${K8S_HS2_PORT}" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(1.0)
try:
  s.connect((host, port))
except Exception:
  sys.exit(1)
finally:
  s.close()
PY
}

resolve_impalad_pod() {
  if [[ -n "${K8S_IMPALAD_POD}" ]]; then
    echo "${K8S_IMPALAD_POD}"
    return 0
  fi

  local selector=""
  if [[ -n "${K8S_IMPALAD_SERVICE}" ]]; then
    local selector_template='{{range $k, $v := .spec.selector}}'
    selector_template+='{{printf "%s=%s," $k $v}}{{end}}'
    selector="$(kubectl -n "${K8S_NAMESPACE}" get svc "${K8S_IMPALAD_SERVICE}" \
      -o "go-template=${selector_template}" \
      2>/dev/null || true)"
    selector="${selector%,}"
  fi

  if [[ -z "${selector}" ]]; then
    selector="app.kubernetes.io/component=impalad"
  fi

  kubectl -n "${K8S_NAMESPACE}" get pod -l "${selector}" \
    -o jsonpath='{.items[0].metadata.name}'
}

start_port_forward() {
  local forward_target="$1"
  : >"${K8S_PORT_FORWARD_LOG}"
  kubectl -n "${K8S_NAMESPACE}" port-forward "${forward_target}" \
    "${K8S_HS2_PORT}:21050" "${K8S_HS2_HTTP_PORT}:28000" "${K8S_WEB_PORT}:25000" \
    >"${K8S_PORT_FORWARD_LOG}" 2>&1 &
  PORT_FORWARD_PID="$!"

  local ready=false
  for _ in $(seq 1 30); do
    if ! kill -0 "${PORT_FORWARD_PID}" 2>/dev/null; then
      break
    fi
    if wait_for_hs2_socket; then
      ready=true
      break
    fi
    sleep 1
  done

  if [[ "${ready}" == "true" ]]; then
    return 0
  fi

  cleanup
  PORT_FORWARD_PID=""
  return 1
}

if [[ "${DRY_RUN}" != "true" ]] && [[ "${K8S_ENABLE_PORT_FORWARD}" == "true" ]]; then
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "kubectl must be installed when K8S_ENABLE_PORT_FORWARD=true"
    exit 1
  fi
  if [[ "${K8S_PORT_FORWARD_MODE}" != "auto" ]] && \
      [[ "${K8S_PORT_FORWARD_MODE}" != "service" ]] && \
      [[ "${K8S_PORT_FORWARD_MODE}" != "pod" ]]; then
    echo "K8S_PORT_FORWARD_MODE must be one of: auto, service, pod"
    exit 1
  fi

  PORT_FORWARD_TARGET=""
  if [[ "${K8S_PORT_FORWARD_MODE}" == "service" ]] || \
      [[ "${K8S_PORT_FORWARD_MODE}" == "auto" && -n "${K8S_IMPALAD_SERVICE}" ]]; then
    if [[ -z "${K8S_IMPALAD_SERVICE}" ]]; then
      echo "Set K8S_IMPALAD_SERVICE when K8S_PORT_FORWARD_MODE=service"
      exit 1
    fi
    PORT_FORWARD_TARGET="svc/${K8S_IMPALAD_SERVICE}"
    echo "Starting service port-forward via ${PORT_FORWARD_TARGET}"
    if ! start_port_forward "${PORT_FORWARD_TARGET}"; then
      if [[ "${K8S_PORT_FORWARD_MODE}" == "service" ]]; then
        echo "Timed out waiting for HS2 service port-forward readiness."
        echo "See ${K8S_PORT_FORWARD_LOG} for details."
        exit 1
      fi
      echo "Service port-forward failed; attempting pod port-forward fallback."
    fi
  fi

  if [[ -z "${PORT_FORWARD_PID}" ]] || ! kill -0 "${PORT_FORWARD_PID}" 2>/dev/null; then
    local_impalad_pod="$(resolve_impalad_pod)"
    if [[ -z "${local_impalad_pod}" ]]; then
      echo "Unable to resolve an impalad pod for port-forward fallback."
      exit 1
    fi
    PORT_FORWARD_TARGET="pod/${local_impalad_pod}"
    echo "Starting pod port-forward via ${PORT_FORWARD_TARGET}"
    if ! start_port_forward "${PORT_FORWARD_TARGET}"; then
      echo "Timed out waiting for HS2 pod port-forward readiness."
      echo "See ${K8S_PORT_FORWARD_LOG} for details."
      exit 1
    fi
  fi
fi

TEST_CMD=(
  ./run-tests.py
  "${K8S_TEST_TARGET}"
  --testing_remote_cluster
  --default_test_protocol=hs2
  --impalad="${K8S_TEST_HOST}:${K8S_BEESWAX_PORT}"
  --impalad_hs2_port="${K8S_HS2_PORT}"
  --impalad_hs2_http_port="${K8S_HS2_HTTP_PORT}"
)

if [[ -n "${K8S_KUDU_MASTER_HOSTS}" ]]; then
  TEST_CMD+=("--kudu_master_hosts=${K8S_KUDU_MASTER_HOSTS}")
fi

if [[ "$#" -gt 0 ]]; then
  TEST_CMD+=("$@")
fi

echo "Using IMPALA_REMOTE_URL=${REMOTE_URL}"
echo "Running: ${TEST_CMD[*]}"

if [[ "${DRY_RUN}" == "true" ]]; then
  exit 0
fi

pushd "${IMPALA_HOME}/tests" >/dev/null
IMPALA_REMOTE_URL="${REMOTE_URL}" "${TEST_CMD[@]}"
popd >/dev/null
