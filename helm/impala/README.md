# Impala on Kubernetes (Helm and Operator)

## Scope

- Fast deployment with Helm
- Optional Kudu, Ranger, and LDAP components
- Equivalent operator workflow through `ImpalaCluster`

## Prerequisites

- Kubernetes cluster access (`kubectl` configured)
- Helm 3.x
- A default StorageClass (or set one explicitly)

From repository root:

```bash
cd /path/to/impala
```

## Helm chart structure

Chart root:

- `helm/impala/Chart.yaml`
- `helm/impala/values.yaml` (base defaults)
- `helm/impala/values-example.yaml` (generic runnable example)
- `helm/impala/templates/` (core and optional resources)

Core templates:

- `statestored-*`, `catalogd-*`, `impalad-*`, `hms-*`
- `configmap.yaml`, `pvc.yaml`

Optional templates:

- Kudu: `kudu-master-*`, `kudu-tserver-*`, `kudu-pvc.yaml`
- Ranger: `ranger-deployment.yaml`, `ranger-service.yaml`

## Deploy with Helm

Create namespace:

```bash
kubectl create namespace impala --dry-run=client -o yaml | kubectl apply -f -
```

Minimal Impala (first install):

```bash
helm install impala ./helm/impala \
  -n impala \
  -f ./helm/impala/values-example.yaml
```

For subsequent updates, use:

```bash
helm upgrade impala ./helm/impala \
  -n impala \
  -f ./helm/impala/values-example.yaml
```

Verify:

```bash
kubectl -n impala get pods
kubectl -n impala get svc
kubectl -n impala get pvc
```

Storage note for shared warehouse PVC:

- The chart defaults `persistence.accessModes` to `ReadWriteMany` because
  HMS, catalogd, and impalad share the same warehouse volume and may run on
  different nodes.
- If your cluster does not provide RWX-capable storage classes, override
  `persistence.accessModes` to `["ReadWriteOnce"]` and ensure these pods are
  co-located on the same node.

## Configure Impala (Helm)

Update any setting by editing values and running `helm upgrade`, or by `--set`.

Examples:

```bash
# Update memory/JVM
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set impalad.memLimit=12gb \
  --set impalad.javaToolOptions="-Xms2g -Xmx4g"
```

```bash
# Expose Impala externally
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set service.impalad.type=LoadBalancer
```

## Optional services (Helm)

### Enable Kudu

```bash
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set kudu.enabled=true
```

### Enable Ranger service and Ranger auth flags

```bash
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set ranger.enabled=true \
  --set auth.ranger.enabled=true
```

### Enable LDAP

`values-ldap-example.yaml` is for the OpenLDAP chart (`openldap/openldap`).
`values-impala-ldap-example.yaml` is for the Impala chart LDAP auth settings.

Deploy OpenLDAP first install:

```bash
helm repo add openldap https://jp-gouin.github.io/helm-openldap/
helm repo update
helm install impala-ldap openldap/openldap \
  -n impala \
  -f ./helm/impala/values-ldap-example.yaml
```

For subsequent LDAP updates, use:

```bash
helm upgrade impala-ldap openldap/openldap \
  -n impala \
  -f ./helm/impala/values-ldap-example.yaml
```

Enable LDAP auth on Impala:

```bash
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-impala-ldap-example.yaml
```

For custom bind patterns, use `--set-string auth.ldap.bindPattern='cn=#UID\,dc=example\,dc=org'`.

When using `--set` for LDAP bind patterns, escape commas (`\,`) so Helm does
not split the value into multiple assignments.

### Enable OAuth token authentication

```bash
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set auth.oauth.enabled=true \
  --set auth.oauth.jwksUrl="https://idp.example.org/.well-known/jwks.json" \
  --set auth.oauth.jwtCustomClaimUsername="sub"
```

For non-TLS development environments only:

```bash
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set auth.oauth.enabled=true \
  --set auth.oauth.jwtValidateSignature=false \
  --set auth.oauth.allowWithoutTls=true
```

## Run Impala shell from laptop (tunnel)

Port-forward HS2:

```bash
kubectl -n impala port-forward \
  pod/$(kubectl -n impala get pod -o name | awk -F/ '/impala-impalad/{print $2; exit}') \
  21050:21050
```

In another terminal:

```bash
impala-shell --protocol=hs2 -i 127.0.0.1:21050 -q "select version();"
```

If LDAP is enabled:

```bash
impala-shell --protocol=hs2 \
  --ldap --auth_creds_ok_in_clear \
  --user impalauser --ldap_password_cmd="echo -n impala123" \
  -i 127.0.0.1:21050 \
  -q "select version();"
```

## Operator structure

Operator root:

- `operator/impala-operator/main.py` (reconciler)
- `operator/impala-operator/manifests/crd-impalacluster.yaml`
- `operator/impala-operator/manifests/rbac.yaml`
- `operator/impala-operator/manifests/deployment.yaml`
- `operator/impala-operator/manifests/sample-impalacluster.yaml`

Current model: the operator reconciles an `ImpalaCluster` CR and performs Helm installs/upgrades.

## Deploy with Operator

Build and push operator image:

```bash
docker build -f operator/impala-operator/Dockerfile -t <registry>/impala-operator:latest .
docker push <registry>/impala-operator:latest
```

Install CRD/RBAC/deployment:

```bash
kubectl apply -k operator/impala-operator/manifests
kubectl -n impala-operator-system set image deploy/impala-operator \
  operator=<registry>/impala-operator:latest
kubectl -n impala-operator-system rollout status deploy/impala-operator
```

Create the target namespace used by the sample CR:

```bash
kubectl create namespace impala --dry-run=client -o yaml | kubectl apply -f -
```

Create cluster CR:

```bash
kubectl apply -f operator/impala-operator/manifests/sample-impalacluster.yaml
kubectl get impalacluster -n impala
```

Update daemon and query configs with typed CR keys:

```bash
kubectl patch impalacluster impala-demo -n impala --type merge -p '{
  "spec": {
    "config": {
      "impalad": {
        "flags": {
          "num_reactor_threads": "0"
        },
        "queryDefaults": {
          "mt_dop": "4",
          "default_file_format": "parquet"
        }
      }
    }
  }
}'
```

For advanced overrides that are not modeled by typed keys, continue to use `spec.set`.

Enable optional services via CR patch:

```bash
kubectl patch impalacluster impala-demo -n impala --type merge -p '{
  "spec": {
    "kuduEnabled": true,
    "rangerEnabled": true,
    "rangerAuthEnabled": true,
    "ldapValuesFile": "/charts/impala/values-ldap-example.yaml",
    "ldapEnabled": true,
    "ldapUri": "ldaps://impala-ldap-openldap:636",
    "ldapBindPattern": "cn=#UID,dc=example,dc=org"
  }
}'
```

For `ImpalaCluster.spec.ldapBindPattern`, provide raw DN syntax (no comma escaping).
The operator handles Helm `--set-string` escaping internally.

Enable OAuth via advanced `spec.set` overrides:

```bash
kubectl patch impalacluster impala-demo -n impala --type merge -p '{
  "spec": {
    "set": {
      "auth.oauth.enabled": "true",
      "auth.oauth.jwksUrl": "https://idp.example.org/.well-known/jwks.json",
      "auth.oauth.jwtCustomClaimUsername": "sub"
    }
  }
}'
```

## Troubleshooting

If HMS fails creating directories on the storage volume, set HMS pod user/group:

```bash
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set hms.securityContext.runAsUser=0 \
  --set hms.securityContext.runAsGroup=0
```

If catalog startup fails due to HMS notification API compatibility, disable HMS
event polling:

```bash
helm upgrade impala ./helm/impala -n impala \
  -f ./helm/impala/values-example.yaml \
  --set catalogd.hmsEventPollingIntervalS=0
```

## Control-plane guidance

Use one control path per release/namespace:

- Helm-managed release: update with Helm commands
- Operator-managed release: update via `ImpalaCluster` spec

Do not manage the same release with both at the same time.
