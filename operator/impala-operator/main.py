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

import datetime
import os
import subprocess
from typing import Dict, List

import kopf
from kubernetes import config


GROUP = "impala.apache.org"
VERSION = "v1alpha1"
PLURAL = "impalaclusters"

DEFAULT_IMPALA_RELEASE = "impala"
DEFAULT_LDAP_RELEASE = "impala-ldap"
DEFAULT_IMPALA_CHART_PATH = os.getenv("DEFAULT_IMPALA_CHART_PATH", "/charts/impala")
DEFAULT_IMPALA_VALUES_FILE = os.getenv(
    "DEFAULT_IMPALA_VALUES_FILE", f"{DEFAULT_IMPALA_CHART_PATH}/values-example.yaml"
)
DEFAULT_LDAP_VALUES_FILE = os.getenv(
    "DEFAULT_LDAP_VALUES_FILE", f"{DEFAULT_IMPALA_CHART_PATH}/values-ldap-example.yaml"
)
DEFAULT_HELM_TIMEOUT_SECONDS = int(os.getenv("DEFAULT_HELM_TIMEOUT_SECONDS", "900"))
_SENSITIVE_SET_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "privatekey",
    "keytab",
    "credential",
)


def _load_kube() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def _is_sensitive_set_key(key: str) -> bool:
    lowered = key.lower().replace("-", "").replace("_", "")
    return any(
        part.replace("-", "").replace("_", "") in lowered
        for part in _SENSITIVE_SET_KEY_PARTS
    )


def _sanitize_helm_set_arg(arg: str) -> str:
    if "=" not in arg:
        return arg
    key, value = arg.split("=", 1)
    if _is_sensitive_set_key(key):
        return f"{key}=<redacted>"
    return f"{key}={value}"


def _sanitize_cmd(cmd: List[str]) -> str:
    safe: List[str] = []
    idx = 0
    while idx < len(cmd):
        token = cmd[idx]
        if token in ("--set", "--set-string") and idx + 1 < len(cmd):
            safe.append(token)
            safe.append(_sanitize_helm_set_arg(cmd[idx + 1]))
            idx += 2
        else:
            safe.append(token)
            idx += 1
    return " ".join(safe)


def _run(cmd: List[str], logger, ignore_error: bool = False) -> str:
    safe_cmd = _sanitize_cmd(cmd)
    logger.info("Running: %s", safe_cmd)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0 and not ignore_error:
        logger.error("Command failed: %s", safe_cmd)
        if stdout:
            logger.error("stdout: %s", stdout)
        if stderr:
            logger.error("stderr: %s", stderr)
        raise RuntimeError(f"command failed ({proc.returncode}): {safe_cmd}")
    if stdout:
        logger.info("stdout: %s", stdout)
    if stderr:
        logger.info("stderr: %s", stderr)
    return stdout


def _bool_string(value: bool) -> str:
    return "true" if value else "false"


def _to_string_dict(values: Dict) -> Dict[str, str]:
    return {str(key): str(val) for key, val in (values or {}).items()}


def _flag_args(flags: Dict[str, str]) -> List[str]:
    args: List[str] = []
    for key in sorted(flags.keys()):
        normalized = key.lstrip("-")
        args.append(f"-{normalized}={flags[key]}")
    return args


def _query_defaults(defaults: Dict[str, str]) -> str:
    pairs = []
    for key in sorted(defaults.keys()):
        pairs.append(f"{key}={defaults[key]}")
    return ",".join(pairs)


def _helm_set_value(value: str) -> str:
    # Helm parses --set values as CSV-like entries; escape commas to keep a single value.
    return value.replace("\\", "\\\\").replace(",", "\\,")


def _set_args(spec: Dict) -> List[str]:
    ldap_enabled = spec.get("ldapEnabled", False)
    ldap_uri = spec.get("ldapUri", "ldaps://impala-ldap-openldap:636")
    ldap_bind_pattern = spec.get("ldapBindPattern", "cn=#UID,dc=example,dc=org")
    config_spec = spec.get("config") or {}

    values = {
        "kudu.enabled": _bool_string(spec.get("kuduEnabled", False)),
        "ranger.enabled": _bool_string(spec.get("rangerEnabled", False)),
        "auth.ranger.enabled": _bool_string(spec.get("rangerAuthEnabled", False)),
        "auth.ldap.enabled": _bool_string(ldap_enabled),
    }
    storage_class_name = spec.get("storageClassName")
    if storage_class_name:
        values["persistence.storageClassName"] = storage_class_name
        values["kudu.master.persistence.storageClassName"] = storage_class_name
        values["kudu.tserver.persistence.storageClassName"] = storage_class_name

    impalad_config = config_spec.get("impalad") or {}
    impalad_flags = _to_string_dict(impalad_config.get("flags") or {})
    impalad_query_defaults = _to_string_dict(impalad_config.get("queryDefaults") or {})

    for idx, arg in enumerate(_flag_args(impalad_flags)):
        values[f"impalad.extraArgs[{idx}]"] = arg
    if impalad_query_defaults:
        values["impalad.defaultQueryOptions"] = _query_defaults(impalad_query_defaults)

    catalogd_flags = _to_string_dict(
        (config_spec.get("catalogd") or {}).get("flags") or {}
    )
    for idx, arg in enumerate(_flag_args(catalogd_flags)):
        values[f"catalogd.extraArgs[{idx}]"] = arg

    statestored_flags = _to_string_dict(
        (config_spec.get("statestored") or {}).get("flags") or {}
    )
    for idx, arg in enumerate(_flag_args(statestored_flags)):
        values[f"statestored.extraArgs[{idx}]"] = arg

    hms_flags = _to_string_dict((config_spec.get("hms") or {}).get("flags") or {})
    for idx, arg in enumerate(_flag_args(hms_flags)):
        values[f"hms.extraArgs[{idx}]"] = arg

    # Keep user-provided --set list indices contiguous (e.g. extraArgs[0..N]).
    # Sparse indices can cause Helm to render empty list items.
    for key, val in (spec.get("set") or {}).items():
        values[key] = str(val)

    args = []
    for key, val in values.items():
        args.extend(["--set", f"{key}={_helm_set_value(val)}"])
    if ldap_enabled:
        args.extend(["--set-string", f"auth.ldap.uri={_helm_set_value(ldap_uri)}"])
        bind_pattern_value = _helm_set_value(ldap_bind_pattern)
        args.extend(
            ["--set-string", f"auth.ldap.bindPattern={bind_pattern_value}"]
        )
    return args


def _helm_timeout(spec: Dict) -> str:
    timeout = int(spec.get("helmTimeoutSeconds", DEFAULT_HELM_TIMEOUT_SECONDS))
    return f"{timeout}s"


def _release_exists(release: str, namespace: str) -> bool:
    proc = subprocess.run(
        ["helm", "status", release, "-n", namespace],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0


def _ensure_ldap(spec: Dict, target_ns: str, logger) -> None:
    ldap_release = spec.get("ldapReleaseName", DEFAULT_LDAP_RELEASE)
    ldap_enabled = spec.get("ldapEnabled", False)
    if not ldap_enabled:
        if _release_exists(ldap_release, target_ns):
            logger.info(
                "ldapEnabled=false; uninstalling existing LDAP release %s "
                "in namespace %s",
                ldap_release,
                target_ns,
            )
            _delete_release(ldap_release, target_ns, logger)
        return

    ldap_values = spec.get("ldapValuesFile", DEFAULT_LDAP_VALUES_FILE)
    timeout = _helm_timeout(spec)

    _run(
        ["helm", "repo", "add", "openldap", "https://jp-gouin.github.io/helm-openldap/"],
        logger,
        ignore_error=True,
    )
    _run(["helm", "repo", "update"], logger)

    cmd = [
        "helm",
        "upgrade" if _release_exists(ldap_release, target_ns) else "install",
        ldap_release,
        "openldap/openldap",
        "-n",
        target_ns,
        "-f",
        ldap_values,
        "--wait",
        "--timeout",
        timeout,
    ]
    _run(cmd, logger)


def _ensure_impala(spec: Dict, target_ns: str, logger) -> None:
    release = spec.get("impalaReleaseName", DEFAULT_IMPALA_RELEASE)
    chart_path = spec.get("impalaChartPath", DEFAULT_IMPALA_CHART_PATH)
    values_file = spec.get("impalaValuesFile", DEFAULT_IMPALA_VALUES_FILE)
    timeout = _helm_timeout(spec)

    cmd = [
        "helm",
        "upgrade" if _release_exists(release, target_ns) else "install",
        release,
        chart_path,
        "-n",
        target_ns,
        "-f",
        values_file,
        "--wait",
        "--timeout",
        timeout,
    ]
    cmd.extend(_set_args(spec))
    _run(cmd, logger)


def _delete_release(release: str, namespace: str, logger) -> None:
    cmd = ["helm", "uninstall", release, "-n", namespace]
    logger.info("Running: %s", _sanitize_cmd(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode == 0:
        if stdout:
            logger.info("stdout: %s", stdout)
        if stderr:
            logger.info("stderr: %s", stderr)
        return
    # Treat "release not found" as idempotent success.
    not_found_error = "release: not found"
    if not_found_error in stdout.lower() or not_found_error in stderr.lower():
        logger.info("Release %s already absent.", release)
        return
    if stdout:
        logger.error("stdout: %s", stdout)
    if stderr:
        logger.error("stderr: %s", stderr)
    raise RuntimeError(
        f"helm uninstall failed ({proc.returncode}) for release {release} in {namespace}"
    )


@kopf.on.startup()
def on_startup(settings: kopf.OperatorSettings, **_):
    _load_kube()
    settings.persistence.finalizer = "impala.apache.org/finalizer"


@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
def reconcile(spec, name, namespace, patch, logger, meta, **_):
    if spec.get("namespace") and spec.get("namespace") != namespace:
        logger.warning(
            "Ignoring spec.namespace=%s and reconciling in metadata.namespace=%s",
            spec.get("namespace"),
            namespace,
        )
    target_ns = namespace

    try:
        _ensure_ldap(spec, target_ns, logger)
        _ensure_impala(spec, target_ns, logger)
    except Exception as exc:
        patch.status["phase"] = "Failed"
        patch.status["message"] = str(exc)
        patch.status["lastReconcileTime"] = datetime.datetime.utcnow().isoformat() + "Z"
        raise

    patch.status["phase"] = "Ready"
    patch.status["message"] = "Impala releases reconciled successfully."
    patch.status["targetNamespace"] = target_ns
    patch.status["observedGeneration"] = meta.get("generation")
    patch.status["lastReconcileTime"] = datetime.datetime.utcnow().isoformat() + "Z"
    logger.info("Reconciled ImpalaCluster %s in namespace %s", name, target_ns)


@kopf.on.delete(GROUP, VERSION, PLURAL)
def delete(spec, namespace, logger, **_):
    if spec.get("namespace") and spec.get("namespace") != namespace:
        logger.warning(
            "Ignoring spec.namespace=%s and deleting releases in metadata.namespace=%s",
            spec.get("namespace"),
            namespace,
        )
    target_ns = namespace
    impala_release = spec.get("impalaReleaseName", DEFAULT_IMPALA_RELEASE)
    ldap_release = spec.get("ldapReleaseName", DEFAULT_LDAP_RELEASE)

    _delete_release(impala_release, target_ns, logger)
    _delete_release(ldap_release, target_ns, logger)
