<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Impala Security Threat Model

## §1 Header

- **Project**: Apache Impala — distributed massively-parallel C++ SQL query engine for
  data in HDFS, Apache Iceberg, Apache Kudu, Apache HBase, Amazon S3, Azure Data Lake
  Storage, Apache Ozone, and other Hadoop-compatible storage *(documented: `README.md`)*.
- **Version / commit**: this model is drafted against the default branch (`master`),
  most recently HEAD `b8be513` ("IMPALA-13033: Parse WebUI thrift profile downloads").
  A report against project release *N* should be triaged against the model as it stood
  at *N*, not at HEAD.
- **Date**: 2026-06-02.
- **Authors**: ASF Security team draft, Impala PMC.
- **Status**: v1.
- **Reporting**: vulnerabilities that fall under §8 (claimed properties) should be
  reported per the Apache Security Team disclosure channel
  (<security@impala.apache.org>); reports that fall under §3 (out of scope), §9
  (properties not provided), or §11a (known non-findings) will be closed by Impala
  triagers citing this document.
- **Provenance legend** —
  *(documented)* = drawn from in-repo docs or website docs, with citation;
  *(maintainer)* = stated by an Impala maintainer in response to this draft.

Impala is a MPP SQL engine: clients submit SQL over the HiveServer2 (HS2) Thrift
protocol or HS2-over-HTTP; the coordinator `impalad` parses, plans, and distributes
query fragments to worker `impalad` instances; metadata is served by a central
`catalogd` and propagated to workers via `statestored`; data is read and written
directly from/to the underlying storage (HDFS, S3, ADLS, Ozone, Kudu, HBase) using
the impersonated impala-process credentials. Authentication is via Kerberos, LDAP,
SAML, JWT, or OAuth bearer token; authorization is delegated to Apache Ranger.

## §2 Scope and intended use

### Intended use

- Production analytic SQL queries against tabular data residing in distributed
  storage, served to authenticated end users via JDBC/ODBC clients,
  `impala-shell`, BI tools, or Apache Hue *(documented: `README.md`,
  `docs/topics/impala_security.xml`)*.
- Multi-tenant analytic clusters where authorization is enforced by Apache Ranger
  and authentication by Kerberos and/or LDAP *(documented:
  `docs/topics/impala_security.xml` lines 38–62)*.

### Deployment shape

Impala is **not** an in-process library and is **not** a single-binary daemon. It
is a cluster of cooperating processes, deployed by an operator inside a network
perimeter the operator controls. The threat model is therefore that of a
distributed service, not a library *(maintainer)*.

### Caller roles

Following §2 of the output-structure rubric (network service split):

| Role | Trust level | Notes |
| --- | --- | --- |
| **End-user client** | untrusted but authenticated | Connects via HS2 / HS2-HTTP / Beeswax; identity verified by Kerberos, LDAP, SAML, JWT, or OAuth *(documented: `docs/topics/impala_security.xml`, `docs/topics/impala_ldap.xml`, `be/src/rpc/authentication.cc`)*. |
| **Operator / cluster admin** | trusted | Sets startup flags, manages keytabs, configures Ranger, owns the Web UI `.htpasswd` *(documented: `docs/topics/impala_security_guidelines.xml`)*. |
| **Internal Impala peer** | trusted (mutually-authenticated) | `impalad`↔`statestored`↔`catalogd` RPC; KRPC + Thrift RPC; auth is Kerberos-only between internal components *(documented: `docs/topics/impala_ldap.xml`, "Consideration for Connections Between Impala Components")*. |
| **Hive Metastore / Ranger** | trusted control plane | Source of metadata + policy decisions; assumed honest *(maintainer)*. |
| **Underlying storage** | trusted by virtue of operator-granted credentials | HDFS / S3 / ADLS / Ozone / Kudu / HBase; Impala holds delegation tokens or static credentials and reads/writes as the `impala` Unix user (or impersonated user when Ranger is enabled) *(documented: `docs/topics/impala_security_files.xml`)*. |
| **Delegated proxy user** | conditionally trusted | When `--authorized_proxy_user_config` is set, an authenticated front-end (Hue, BI tool) may forward queries as a different end user *(documented: `docs/topics/impala_delegation.xml`)*. |

### Component-family table

| Family | Representative entry point | Touches outside the process? | In-model? |
| --- | --- | --- | --- |
| HS2 / Beeswax / HS2-HTTP server | `:21000` (Beeswax), `:21050` (HS2 binary), `:28000` (HS2-HTTP) *(documented: `docs/topics/impala_ports.xml`)* | network (TCP, optionally TLS) | **yes** |
| Internal KRPC + Thrift RPC | `:27000` (KRPC), `:23000`/`:24000`/`:26000` (statestore/catalog) | network within the cluster | **yes** (peer trust depends on Kerberos / mTLS) |
| Web UI / metrics / `/admin/` endpoints | `:25000`/`:25010`/`:25020` | network (TCP, optionally TLS + SPNEGO + `.htpasswd`) | **yes** |
| Query frontend (Java, `fe/`) — parser, analyzer, planner, Ranger checker | invoked from coordinator `impalad` via JNI | none directly | **yes** |
| Query backend (C++, `be/`) — exec engine, codegen (LLVM), expression eval, scanners | invoked from coordinator `impalad` and worker `impalad` | reads/writes storage with process credentials | **yes** |
| Catalog server (`catalogd`) | invoked by coordinators; reads HMS + storage | reads HMS, lists HDFS, reads object stores | **yes** |
| Storage scanners (Parquet, ORC, Avro, text, Iceberg, Kudu, HBase, JDBC external table) | reads operator-configured locations | reads object stores / HDFS / external JDBC | **yes** (data trust = §6) |
| User-defined functions (UDFs) | `CREATE FUNCTION … LOCATION …` (C++ native or Java) | runs operator-/user-permitted binaries in-process | **out of model** for UDF code itself *(§3)*; in-model for the privilege check that admits the UDF |
| External Data Sources / JDBC external tables | `CREATE DATA SOURCE` / Iceberg REST catalog | outbound JDBC / HTTPS *(documented: `docs/topics/impala_jdbc_external_table.xml`, `docs/topics/impala_iceberg_rest_catalog.xml`)* | in-model for credential handling and privilege checks; **out of model** for the remote endpoint and data source JAR code |
| `ai_generate_text` LLM connector | SQL function calling external LLM endpoint *(documented: `docs/topics/impala_ai_functions.xml`)* | outbound HTTPS | in-model for credential / prompt handling; out-of-model for the LLM provider |
| `shell/` Python impala-shell | client-side, not server | n/a | **out of model** for server claims *(§3)*; in-model for credential handling of the shell binary itself |
| `docker/`, `testdata/`, `infra/`, `tests/` | tooling | n/a | **out of model** *(§3)* |
| Vendored Kudu security code under `be/src/kudu/security/` | TLS/SASL primitives shared with the Apache Kudu codebase | n/a | in-model only insofar as Impala calls into it *(maintainer)* |

## §3 Out of scope (explicit non-goals)

Impala is not, and does not aim to be, the following — reports requiring any of
these will be closed with the cited disposition:

1. **The root authority for storage-level authorization.** HDFS POSIX
   permissions, S3 IAM, ADLS RBAC, Ozone ACLs, etc. are enforced by the storage
   provider and the credentials the operator hands to Impala. Reports that depend
   primarily on over-broad bucket / IAM permissions are deployment-sensitive, not
   Impala-side *(documented: `docs/topics/impala_security_files.xml`)*. →
   `OUT-OF-MODEL: adversary-not-in-scope`.
2. **A defender against a malicious Hive Metastore, Ranger Admin, or other
   trusted control-plane component.** If the report requires the HMS or Ranger
   to be hostile to Impala, it is out of model *(maintainer)*. →
   `OUT-OF-MODEL: trusted-input`.
3. **A defender against the operator.** Anyone with `root`, `sudo`, the
   `impala` Unix account, the keytab file, the cookie-secret file, or the Web
   UI `.htpasswd` already has unbounded power; "the operator misconfigured X" is
   not a vulnerability *(documented:
   `docs/topics/impala_security_guidelines.xml`)*. → `OUT-OF-MODEL:
   adversary-not-in-scope`.
4. **An isolation boundary between an authorized user's SQL and the
   `impalad` process.** SQL is interpreted by a trusted engine running as the
   `impala` Unix user; an authenticated user with appropriate SQL privileges
   can already cause arbitrary reads, writes, and resource consumption within
   the scope Ranger grants. A new way for an authorized user to do something
   they are already authorized to do is not a vulnerability *(maintainer)*. →
   `OUT-OF-MODEL: equivalent-harm`.
5. **A sandbox for user-defined functions.** Native (C++) UDFs, Hive Java
   UDFs, and user-defined DATA SOURCES run in-process with the privileges of
   the `impalad` daemon. UDF sandboxing is not provided; defining binaries or
   JARs to be loaded by Impala are gated by Ranger requiring `ALL` privileges on
   the location; admission of a `CREATE FUNCTION` is also gated by Ranger.
   *(maintainer)*. → `BY-DESIGN: property-disclaimed` (§9).
6. **A defender against malformed-but-parseable user data in scanned files.**
   Decoders (Parquet, ORC, Avro, text, Iceberg manifests) must not corrupt
   process memory, but raw runtime exceptions, slow paths on adversarial
   inputs, and OOM on pathological files are robustness work, not security
   issues, unless they cross a trust boundary *(maintainer)*. →
   `OUT-OF-MODEL: equivalent-harm` for writer-controlled files,
   `VALID-HARDENING` for reader-controlled files.
7. **Code that ships but is not part of the supported product:**
   `tests/`, `testdata/`, `infra/`, `docker/`, `package/`, `ssh_keys/`,
   `cmake_modules/`, `experiments/`, `udf_samples/`. State the policy
   explicitly so integrators do not extend core guarantees to them
   *(maintainer)*. → `OUT-OF-MODEL: unsupported-component`.
8. **Apache Kudu, Apache Iceberg, Apache Ranger, Apache Hive client libraries,
   Hadoop libraries, OpenSSL, Apache Thrift, and other upstream dependencies.**
   Where Impala vendors source (e.g. `be/src/kudu/`), the vendored code is
   modeled at the wrapper boundary; vulnerabilities intrinsic to the upstream
   project should be reported upstream *(maintainer)*. →
   `OUT-OF-MODEL: unsupported-component` (with an upstream pointer).
9. **The Impala documentation site, asf-site branch, downloads page, gem/npm
   packages with similar names, and other non-product surfaces.** Out of scope.

## §4 Trust boundaries and data flow

Impala has at least eight distinct trust transitions; a finding is in-model
only when it cleanly maps to one of them.

| # | Transition | Authentication | Authorization |
| --- | --- | --- | --- |
| B1 | End-user client → HS2 / Beeswax / HS2-HTTP | Kerberos / LDAP / SAML / JWT / OAuth / trusted-domain header *(documented: `be/src/rpc/authentication.cc`)* | Ranger on submitted SQL *(documented: `docs/topics/impala_authorization.xml`)* |
| B2 | End-user client → Web UI (`:25000` and siblings) | `.htpasswd` + SPNEGO, optional TLS *(documented: `docs/topics/impala_security_webui.xml`)* | none beyond authentication; the Web UI exposes operator-grade endpoints *(maintainer)* |
| B3 | `impalad` ↔ `statestored` ↔ `catalogd` internal RPC (KRPC + Thrift) | Kerberos (mandatory for prod) + optional TLS *(documented: `docs/topics/impala_ssl.xml`, `docs/topics/impala_ldap.xml`)* | "internal_principals_whitelist" of allowed principals *(documented: `be/src/rpc/authentication.cc` line 121)* |
| B4 | Coordinator `impalad` → worker `impalad` (query fragments over KRPC) | same as B3 | same as B3 |
| B5 | Coordinator / catalogd → Hive Metastore | Kerberos / delegation token | HMS-side; Impala assumes truthful responses |
| B6 | Coordinator → Ranger Admin (policy fetch) | service principal | Ranger-side |
| B7 | Worker `impalad` → underlying storage (HDFS, S3, ADLS, Ozone, Kudu, HBase) | Kerberos / IAM / service-account keys / delegation tokens | storage-side ACLs |
| B8 | Operator → impalad startup flags + configuration files | filesystem permissions on the host | OS-level |

### Reachability preconditions per family

For each family in §2, a finding is in-model only if it is reachable as
follows:

- **HS2 / Beeswax / HS2-HTTP server**: reachable from an *unauthenticated* network
  peer who can reach the listening port. Findings that require an already-
  authenticated peer collapse to "authenticated user with SQL privileges", and
  must additionally clear B7 (storage ACL) or B6 (Ranger policy) to be
  security-relevant.
- **Internal KRPC**: reachable from a network peer who has compromised the
  Kerberos trust (B3) — i.e., has stolen a service keytab or impersonated a
  principal on `internal_principals_whitelist`. A flat "internal RPC has no
  auth" finding is `OUT-OF-MODEL: adversary-not-in-scope` because the model
  *requires* Kerberos between components in production *(documented: `docs/topics/impala_ldap.xml`)*.
- **Web UI**: reachable from a network peer with an `.htpasswd` credential
  (per §10) or who can reach an unprotected port. A finding that
  needs `.htpasswd` to be absent is `OUT-OF-MODEL: trusted-input` against a
  guideline-violating operator (§3 item 3) *(maintainer)*.
- **Query frontend / backend**: reachable from SQL submitted by an authenticated
  user with sufficient Ranger privileges. Findings here matter only if they
  break out of the user's Ranger-granted privilege set.
- **Scanners**: reachable from bytes in operator-configured storage locations.
  Bytes are *partially* attacker-controlled when an authorized writer has
  `INSERT` privilege on a table that other users read (B7). Compromise of the
  storage layer itself is out of model (§3 item 1).
- **UDFs**: reachable only via `CREATE FUNCTION`, which is Ranger-gated.
  Anything past the privilege check is out of model (§3 item 5).

## §5 Assumptions about the environment

- **Operating system**: Linux (Ubuntu 20.04/22.04/24.04, Rocky/RHEL 8/9 are the
  declared supported set; others "may also be supported but are not tested by
  the community") *(documented: `README.md` — Supported Platforms)*. x86_64 and
  arm64 supported.
- **Process model**: at least three long-lived daemons (`impalad`,
  `statestored`, `catalogd`); operator runs them as the `impala` Unix user
  *(documented: `docs/topics/impala_security_files.xml`)*.
- **Network**: operator-controlled L2/L3; no NAT or middlebox assumed to
  inspect KRPC payloads; ports per `docs/topics/impala_ports.xml`. Mutually-
  reachable cluster members assumed.
- **Time**: Kerberos requires loosely-synchronized clocks across the realm
  (KDC tolerance, default 5 min) — operator's responsibility, not Impala's
  *(maintainer)*.
- **Filesystem**: keytab and `.htpasswd` files have OS-level permissions
  restricted to the `impala` user and admins *(documented:
  `docs/topics/impala_security_webui.xml`)*.
- **Cryptography**: the OpenSSL library shipped with the OS provides TLS,
  symmetric/asymmetric primitives, and RNG *(documented: `EXPORT_CONTROL.md`)*.
- **Kerberos**: assumes a working MIT KDC with renewable-ticket support
  configured per `docs/topics/impala_kerberos.xml`.
- **What Impala does to its host**:
  - **does** open listening sockets on the documented ports;
  - **does** spawn child processes for `--ldap_bind_password_cmd`,
    `--s3a_access_key_cmd`, `--s3a_secret_key_cmd`,
    `--saml2_keystore_password_cmd`, `--saml2_private_key_password_cmd`,
    `--ssl_private_key_password_cmd`, `--webserver_private_key_password_cmd`;
    and `java -version` *(maintainer)*;
  - **does** install signal handlers for crash reporting (SIGUSR1 → breakpad)
    and generating stack traces (SIGRTMIN+10), ignores SIGPIPE, and handles
    graceful shutdown with SIGTERM *(maintainer)*;
  - **does** read a documented set of environment variables (e.g.
    `IMPALA_HOME`, `JAVA_HOME`, `JAVA_TOOL_OPTIONS`) but does not consume
    arbitrary `LD_*` for security-sensitive behavior *(maintainer)*;
  - **does** write logs to operator-configured locations; redacted query text
    if log redaction is enabled *(documented:
    `docs/topics/impala_logging.xml`)*.

## §5a Build-time and configuration variants

Impala ships as a single product but a sizable number of runtime flags
materially change the security envelope. The maintainer-confirmed list is at
`be/src/rpc/authentication.cc` and equivalent files; the security-relevant
subset:

| Flag | Default | Maintainer stance | Effect |
| --- | --- | --- | --- |
| `--enable_ldap_auth` | `false` *(documented)* | dev/test or operating with Kerberos, operator must enable per §10 *(maintainer)* | enables LDAP auth on HS2 client port |
| `--ssl_server_certificate` / `--ssl_private_key` | unset *(documented)* | dev/test, operator must enable per §10 *(maintainer)* | enables TLS on all listening sockets |
| `--ssl_minimum_version` | `tlsv1.2` *(documented: `docs/topics/impala_ssl.xml`)* | hardened in Impala 4.0 from `tlsv1` *(documented)* | rejects pre-1.2 handshakes |
| `--webserver_password_file` | unset *(documented: `docs/topics/impala_security_webui.xml`)* | an unprotected Web UI is `OUT-OF-MODEL: non-default-build` *(maintainer)* | Web UI authenticates against this `.htpasswd` |
| `--webserver_certificate_file` | unset *(documented)* | dev/test, operator must enable per §10 *(maintainer)* | enables HTTPS on Web UI |
| `--principal`, `--keytab-file` | unset | dev/test or operating with LDAP, operator must enable per §10 *(maintainer)* | enables Kerberos auth |
| `--authorization_provider=ranger` | unset *(documented: `docs/topics/impala_authorization.xml`)* | dev/test, operator must enable per §10 *(maintainer)* | enables Ranger authz; absent → all queries run as `impala` user (no enforcement) |
| `--jwt_token_auth` / `--oauth_token_auth` | `false` | optional alternative auth *(documented: `be/src/rpc/authentication.cc`)* | enables bearer-token auth |
| `--jwt_validate_signature`, `--oauth_jwt_validate_signature` | `true` | hardened default; flipping to `false` voids §8 P3 *(maintainer)* | turns off JWT/OAuth signature check |
| `--jwt_allow_without_tls`, `--oauth_allow_without_tls`, `--saml2_allow_without_tls_debug_only` | `false`, marked `_hidden` | "debug only" per name *(→)* | permits bearer / SAML auth over unencrypted transport |
| `--trusted_domain`, `--trusted_auth_header` | unset *(documented)* | when set, Impala accepts identity assertions from named peer without re-auth | reachability for `OUT-OF-MODEL: trusted-input` reports |
| `--trusted_domain_use_xff_header` | `false` | when `true`, parses `X-Forwarded-For` to identify the originating client *(documented: `be/src/rpc/authentication.cc` line 132)* | exposes a path where a misconfigured proxy can let a client claim any source address *(maintainer)* |
| `--internal_principals_whitelist` | `hdfs` *(documented: `be/src/rpc/authentication.cc` line 121)* | governs which Kerberos principals are accepted on internal RPC ports | misconfiguration permits external service to speak as a peer |
| `--authorized_proxy_user_config` / `--authorized_proxy_group_config` | unset *(documented: `docs/topics/impala_delegation.xml`)* | required for Hue-style impersonation; whitelists which authenticated principals may `doas` to which users | breaks B1 if mis-scoped |
| `--cookie_secret_file` | empty *(documented: `be/src/rpc/authentication.cc` line 98)* | when unset, HS2-HTTP cookies fall back to per-process random — sessions do not survive cluster restarts but are not forgeable *(maintainer)* | shared cluster-wide secret for cookie HMAC |
| `--abort_on_config_error` | `true` *(maintainer)* | when off, security misconfigurations may not prevent startup | |
| `impala-shell --ssl` | `false` | when true, must also configure `ca_cert` or `verify_cert` to validate server certificate | required for TLS configured endpoints |

**The insecure-default case.** A number of these flags ship in the "off, must
be turned on for production" posture. The maintainer ruling on whether the
*default* is a supported production posture is captured in "Maintainer stance".

## §6 Assumptions about inputs

### Per-endpoint trust table (network surfaces)

| Surface / route | Parameter | Attacker-controllable? | Caller must enforce |
| --- | --- | --- | --- |
| HS2 binary `:21050`, HS2-HTTP `:28000`, Beeswax `:21000` | SQL text | **yes** | nothing — Impala parses, plans, and applies Ranger |
| HS2-HTTP `:28000` | `X-Forwarded-For` header | **yes** if `--trusted_domain_use_xff_header` is on; **never trust** otherwise *(maintainer)* | per §10, only enable behind a load balancer that strips and resets XFF |
| HS2-HTTP `:28000` | session cookie | signed with `--cookie_secret_file` HMAC; not attacker-forgeable when secret is unguessable *(maintainer)* | per §10, rotate the cookie-secret file if compromised |
| HS2-HTTP `:28000` | JWT / OAuth bearer | **yes**; signature checked when `--jwt_validate_signature=true` (default) *(documented: `be/src/rpc/authentication.cc`)* | per §10, leave signature checking on, set `--jwt_allow_without_tls=false` |
| HS2-HTTP `:28000` | `--trusted_auth_header` value | **yes**; treated as the authenticated identity | **never** expose the port directly to untrusted peers when this flag is set *(maintainer)* |
| Web UI `:25000`/`:25010`/`:25020` | `.htpasswd` credential | **yes** if `--webserver_password_file` is set | per §10, set the flag; per §10, set `--webserver_certificate_file` for HTTPS |
| Web UI `:25000`/`:25010`/`:25020` | session cookie | signed with `--cookie_secret_file` | per §10, rotate the cookie-secret file if compromised |
| Web UI `:25000` — query-profile and admin endpoints | profile ID / GET parameters | **yes** | Web UI auth is the only gate; sensitive query bytes appear unless log redaction is enabled |
| KRPC `:27000` and statestore/catalog ports `:23000`/`:24000`/`:26000` | Thrift / KRPC payload | **only by a peer that has cleared B3** | Kerberos + `internal_principals_whitelist` are the gate |
| Scanned table files (Parquet, ORC, Avro, text, Iceberg manifests) | file bytes | **yes** if an authorized writer can land bytes the reader will scan | Ranger separates writers from readers; B7 enforces who can land bytes |
| `ai_generate_text` LLM endpoint | LLM response | trusted only as far as the LLM is trusted | per §10, treat LLM output as untrusted text (do not pipe to executable contexts) *(maintainer)* |
| JDBC external table endpoint | rows returned by remote JDBC | trusted only as far as the remote endpoint is trusted | per §10, model JDBC external tables as data crossing a trust boundary |

### Size / shape / rate

- Impala accepts arbitrary-length SQL but the analyzer rejects queries above
  implementation limits (controlled by query option `max_statement_length_bytes`
  with max value of 2^31 - 1) *(maintainer)*.
- Scanned files may be terabytes; row groups are streamed. Pathological
  encodings (e.g. enormous string lengths in Parquet headers) are robustness
  concerns *(maintainer)*.
- The HS2 / Beeswax surfaces have **limited built-in rate limiting**; admission
  control via the `--default_pool_max_requests` family of flags bounds
  in-flight queries; `--fe_service_threads` provides a limited bound on
  connection and auth-attempt rate *(maintainer)*.

## §7 Adversary model

### Actors

| Actor | In scope? | Capabilities granted |
| --- | --- | --- |
| Unauthenticated network peer reaching HS2 / Beeswax / HS2-HTTP | **yes** | TCP to the listening ports; may attempt authentication; may attempt to violate the protocol pre-auth |
| Unauthenticated peer reaching Web UI | **yes**, *if* the deployment exposes the Web UI publicly | as above for Web UI |
| Authenticated end user with limited Ranger privileges | **yes** | execute SQL, read tables the user has `SELECT` on, write tables the user has `INSERT` on |
| Authenticated end user with broad Ranger privileges | partial | only escapes from their Ranger envelope are in scope |
| Co-tenant on the same cluster | **yes** | same as authenticated end user; cross-tenant leakage is in scope |
| Authorized table writer producing data read by another user | **yes** for scanner robustness across the B7 boundary, but bounded — `VALID-HARDENING`, not `VALID`, unless memory corruption is reachable *(maintainer)* |
| Authenticated proxy front-end (Hue) using `doas` | **yes** only when `--authorized_proxy_user_config` is mis-scoped |
| Hostile peer impalad / statestored / catalogd | **out of scope** — see §3 item 2 |
| Hostile HMS / Ranger | **out of scope** — see §3 item 2 |
| Operator | **out of scope** — see §3 item 3 |
| Local process on the same host as `impalad` running as a different user | **partial** *(maintainer)*: same-host attackers with non-`impala` UID can read the Web UI / HS2 ports unless host firewalling forbids or authentication secured from non-`impala` local users; Impala does not defend against same-host UID-0 attackers |
| Side-channel observer (cache timing, network timing) | **out of scope** *(maintainer)* |
| Quantum adversary | **out of scope** |

### Authenticated-but-Byzantine peer (distributed-systems threshold)

Impala is **not** a Byzantine-fault-tolerant system. A compromised
`impalad`/`catalogd`/`statestored` peer with a valid Kerberos identity can
cause unbounded damage (read any data the cluster can read, produce wrong
results, leak intermediate state). The cluster trusts its own membership
*(maintainer)*. → reports requiring a Byzantine internal peer are
`OUT-OF-MODEL: adversary-not-in-scope`.

## §8 Security properties the project provides

For each property: condition, violation symptom, severity tier, provenance.

### P1 — Authentication of HS2 / Beeswax / HS2-HTTP clients

- **Condition**: an authentication mode is enabled
  (`--enable_ldap_auth`, `--principal`+`--keytab-file`, `--jwt_token_auth`,
  `--oauth_token_auth`, or a SAML configuration). With none of these set,
  Impala accepts unauthenticated SQL *(documented: `be/src/rpc/authentication.cc`)*.
- **Violation symptom**: a network peer holding no valid credential successfully
  executes SQL.
- **Severity**: **security-critical**, `VALID` per §13.
- *(documented)*

### P2 — Authorization of SQL operations via Apache Ranger

- **Condition**: `--authorization_provider=ranger` is set and Ranger is
  reachable *(documented: `docs/topics/impala_authorization.xml`)*. With this
  flag unset, no authorization is enforced and all queries run as the
  `impala` user *(documented: `docs/topics/impala_security.xml`,
  `docs/topics/impala_authorization.xml`)*.
- **Violation symptom**: a query reads or modifies data not licensed by the
  authenticated principal's Ranger policy. Failure mode includes both the
  authorization-bypass case (Impala fails to apply a policy) and the
  authorization-confusion case (Impala applies the wrong policy).
- **Severity**: **security-critical**, `VALID` per §13.
- *(documented)*

### P3 — TLS confidentiality and integrity on the wire, when configured

- **Condition**: `--ssl_server_certificate` + `--ssl_private_key` set on the
  relevant daemon; minimum version per `--ssl_minimum_version` (default
  `tlsv1.2` since Impala 4.0) *(documented: `docs/topics/impala_ssl.xml`)*.
- **Violation symptom**: cleartext on the wire after TLS is configured, or a
  TLS handshake completing with a deprecated cipher despite
  `--ssl_minimum_version=tlsv1.2`.
- **Severity**: **security-critical**, `VALID` per §13.
- *(documented)*

### P4 — Kerberos authentication on internal RPCs in production

- **Condition**: `--principal` and `--keytab-file` are set on all three
  daemons *(documented: `docs/topics/impala_kerberos.xml`,
  `docs/topics/impala_ldap.xml`)*. Without this, internal RPCs are
  unauthenticated and a cleartext-internal-RPC report is *not* a §8 break —
  the operator violated §10.
- **Violation symptom**: internal RPC completing successfully from a principal
  not on `--internal_principals_whitelist`.
- **Severity**: **security-critical**, `VALID` per §13.
- *(documented)*

### P5 — Bounded scope of authenticated impersonation (`doas`)

- **Condition**: `--authorized_proxy_user_config` /
  `--authorized_proxy_group_config` set; the authenticated front-end principal
  appears as a key *(documented: `docs/topics/impala_delegation.xml`)*.
- **Violation symptom**: an authenticated principal successfully runs a query
  as a delegated user not in their allow-list.
- **Severity**: **security-critical**, `VALID` per §13.
- *(documented)*

### P6 — Log redaction, when configured

- **Condition**: redaction rules configured per
  `docs/topics/impala_logging.xml#redaction`.
- **Violation symptom**: literal values matching configured redaction patterns
  appearing un-redacted in logs or Web UI query profiles.
- **Severity**: **security-critical** for data-protection-regulated deployments;
  `VALID` per §13.
- *(documented)*

### P7 — Web UI authentication, when configured

- **Condition**: `--webserver_password_file` set; optionally Kerberos SPNEGO.
- **Violation symptom**: an unauthenticated peer accesses an authenticated
  Web UI endpoint, or an authenticated peer accesses an endpoint above their
  Web UI auth tier.
- **Severity**: **security-critical**, `VALID` per §13.
- *(documented: `docs/topics/impala_security_webui.xml`)*

### P8 — Memory safety on well-formed inputs across documented surfaces

- **Condition**: input matches the documented protocol (HS2 / Beeswax / Thrift
  / KRPC / Parquet / ORC / Avro / Iceberg manifest / etc.); the host
  conformant to §5; no `_hidden` debug flag is in use *(maintainer)*.
- **Violation symptom**: heap or stack corruption, out-of-bounds read/write,
  use-after-free, double-free reachable from a §6 input.
- **Severity**: **security-critical** when reachable from network input or from
  table data crossing B7; **`VALID-HARDENING`** when reachable only by a writer
  who already controls the bytes (§3 item 6).
- *(maintainer)*

### P9 — No SQL injection from end-user-supplied parameters into back-end queries against other systems

- **Condition**: applies only to flows where Impala emits SQL to a remote
  system on behalf of an Impala client (JDBC external tables; `ai_generate_text`
  with prompt-as-SQL patterns).
- **Violation symptom**: end-user SQL text appearing un-escaped in a remote-
  query string.
- **Severity**: case-dependent; `VALID-HARDENING` if the remote system is also
  Impala-trusted, `VALID` if the remote system is a tenant boundary.
- *(maintainer)*

## §9 Security properties the project does *not* provide

State each plainly so a triager can route an inbound report to the matching
disclaimer.

- **No isolation between authenticated user SQL and the `impalad` process.** A
  user with Ranger privilege to `CREATE FUNCTION` and to `SELECT` from the
  resulting function can run arbitrary native or JVM code inside `impalad`. UDFs
  are **not** sandboxed. See §3 item 5 *(maintainer)*.
- **No defense against decompression / decoding bombs in scanned files.** A
  malicious or buggy table writer can land Parquet / ORC / Avro / text files
  designed to maximize CPU and memory; the reader has no built-in cap on
  per-file resource use *(maintainer)*.
- **No quotas on per-query or per-user resource consumption beyond what
  admission control provides.** A user with `SELECT` on a large table can
  cause arbitrary wall-clock and memory burn. Operator must configure
  `--default_pool_*` admission-control flags *(maintainer)*.
- **No defense against intra-cluster Byzantine failure.** A compromised peer
  with a valid Kerberos identity can read any data the cluster can read; see
  §7 *(maintainer)*.
- **No protection against the operator.** Anyone with the keytab, the
  cookie-secret file, the `.htpasswd`, the impala Unix account, or root on
  any impala host wins. See §3 item 3.
- **No protection against a malicious HMS / Ranger.** See §3 item 2.
- **No data-at-rest encryption.** Impala writes file bytes through the storage
  layer's existing protections (HDFS Transparent Data Encryption, S3 SSE,
  etc.). Impala does not encrypt at the table format level *(maintainer)*.
- **No defense against side-channel observation** (cache, timing, branch
  prediction) of query plans or data *(maintainer)*.
- **No constant-time comparison of authentication secrets** beyond what the
  underlying SASL/Kerberos libraries provide *(maintainer)*.
- **No defender stance against an attacker on the same Linux host running as
  a non-`impala` UID** — Impala defends only across the network surface;
  same-host attackers with shell access on the impala host already have many
  paths to win *(maintainer)*.

### False-friend properties (call out separately)

- **`SHOW TABLES` / `SHOW DATABASES` filtering is an authorization view, not
  an information-flow channel.** Object names a user is not authorized to
  see are hidden, but error messages, query-profile timing, and Web UI traces
  may reveal existence indirectly *(maintainer)*.
- **Log redaction is a *display* feature, not a confidentiality boundary.** It
  obfuscates literals in *new* log entries when patterns match; it cannot
  retroactively cleanse leaked log files, and a regex miss leaks the literal.
- **Kerberos authenticates the *principal*, not the *host* the principal
  connects from.** A stolen keytab is a stolen identity.
- **TLS encrypts but does not authenticate the application-layer identity.**
  Authentication is layered on (LDAP, JWT, etc.); TLS by itself does not
  authorize.
- **`.htpasswd` Web-UI authentication does not provide per-user authorization
  on the Web UI.** Any authenticated `.htpasswd` user sees all Web UI
  contents, including query bytes and profiles *(maintainer)*.
- **`--trusted_domain` / `--trusted_auth_header` is an explicit bypass of
  client authentication.** Setting it without controlling the load balancer
  hands an attacker the keys.
- **Ranger column-masking and row-filter policies operate at the planner
  level, not the storage level.** Anyone bypassing the planner (a hostile
  peer reading the file directly via HDFS, a UDF reading raw bytes) is not
  constrained by them.

### Well-known attack classes the project does not defend against

- **SQL-engine-amplified DoS** ("malicious analytic query"): a user with
  `SELECT` privilege issuing a Cartesian product across petabyte tables. The
  fix surface is admission control, not the engine.
- **Decompression / decoding bombs** in supported file formats (see above).
- **Adversarial table-writer collusion**: a writer landing files that crash
  a downstream reader is `VALID-HARDENING` at most, because the writer could
  simply have written wrong data.
- **Confused-deputy via `doas`** when the proxy list is mis-scoped.
- **Time-of-check-to-time-of-use** between Ranger policy fetch and query
  execution: policy changes mid-query are not retroactively enforced
  *(maintainer)*.

## §10 Downstream responsibilities

The operator deploying Impala in production **must**:

1. Set `--principal` + `--keytab-file` on `impalad`, `statestored`,
   `catalogd`. Without these, internal RPC has no authentication
   *(documented: `docs/topics/impala_kerberos.xml`,
   `docs/topics/impala_ldap.xml`)*.
2. Set `--authorization_provider=ranger` and configure Ranger. Without this,
   no authorization is enforced *(documented: `docs/topics/impala_authorization.xml`)*.
3. Enable TLS — `--ssl_server_certificate` and `--ssl_private_key` on all
   daemons, and `--ssl_client_ca_certificate` to authenticate the peer of
   internal RPC *(documented: `docs/topics/impala_ssl.xml`)*.
4. Set `--webserver_password_file` and `--webserver_certificate_file` so the
   Web UI is authenticated and TLS-served *(documented:
   `docs/topics/impala_security_webui.xml`,
   `docs/topics/impala_security_guidelines.xml`)*.
5. Restrict Web UI ports (`:25000`/`:25010`/`:25020`) at the network layer
   to a trusted operator subnet *(documented:
   `docs/topics/impala_security_guidelines.xml`)*.
6. Restrict membership in `--internal_principals_whitelist` to the actual
   Kerberos principals of cluster members *(documented: `be/src/rpc/authentication.cc`)*.
7. **Never** set `--jwt_allow_without_tls=true`,
   `--oauth_allow_without_tls=true`, or `--saml2_allow_without_tls_debug_only=true`
   in production *(maintainer)*.
8. **Never** set `--trusted_domain` / `--trusted_auth_header` /
   `--trusted_domain_use_xff_header` unless the listening port is exposed
   only to a load balancer that strips and resets the relevant header
   *(maintainer)*.
9. Set `--cookie_secret_file` to a long, random, cluster-wide secret with
   filesystem permissions restricted to the `impala` user *(documented:
   `be/src/rpc/authentication.cc`)*.
10. Set `--authorized_proxy_user_config` / `--authorized_proxy_group_config`
    to the smallest set of front-end principals that need `doas`, with the
    smallest set of impersonated users *(documented:
    `docs/topics/impala_delegation.xml`)*.
11. Configure log redaction patterns for any sensitive literal that may
    appear in WHERE-clause queries *(documented:
    `docs/topics/impala_logging.xml#redaction`)*.
12. Secure the OS-level `impala` Unix user and the `root` / `sudoers` set on
    every Impala host *(documented:
    `docs/topics/impala_security_guidelines.xml`)*.
13. Configure admission control (`--default_pool_max_requests`,
    `--default_pool_max_queued`, `--default_pool_mem_limit`) to bound
    per-query and per-pool resource use; Impala does not enforce DoS
    protection by itself *(maintainer)*.
14. Treat `ai_generate_text` results and JDBC external-table rows as
    crossing a trust boundary; do not assume the remote system is honest
    *(maintainer)*.
15. Secure the underlying storage (HDFS, S3, ADLS, Ozone) with native ACLs;
    Impala enforces only what it can see *(documented:
    `docs/topics/impala_security_files.xml`)*.
16. Provide `.impalarc` for `impala-shell` users configuring `ssl` and either
    `ca_cert` or `verify_cert` *(maintainer)*.

## §11 Known misuse patterns

- **Exposing port 25000 (Web UI) directly to the public Internet without
  `--webserver_password_file`.** Anyone reaching the port reads in-flight
  query bytes, server flags, table names. → operator hardening per §10.
- **Running Impala with `--authorization_provider` unset in a multi-tenant
  cluster.** All queries succeed as the `impala` Unix user — there is no
  authorization at all *(documented:
  `docs/topics/impala_authorization.xml`)*.
- **Setting `--trusted_domain` without ensuring the listening port is only
  reachable from the trusted reverse proxy.** The flag is a deliberate
  client-auth bypass for proxy deployments; the operator owns the network
  fence.
- **Using `CREATE FUNCTION` to load a UDF binary supplied by an end user.**
  UDFs run in-process. → Ranger-gate `CREATE FUNCTION` to administrators.
- **Treating Impala's `SHOW TABLES` view as a confidentiality boundary.**
  Existence of a hidden object may leak through error messages or query
  profiles *(maintainer)*.
- **Re-using `--cookie_secret_file` across clusters of different trust
  levels.** A leak in cluster A becomes a forgery primitive in cluster B
  *(maintainer)*.
- **Disabling TLS internally between `impalad`/`statestored`/`catalogd` in
  production.** Cleartext internal RPC + Kerberos `auth-int` is the documented
  minimum; many deployments leave it at `auth` *(maintainer)*.
- **Mixing authenticated and unauthenticated coordinator daemons in the same
  cluster.** Impala 2.0+ accepts both Kerberos and LDAP on the same port; an
  operator who *also* leaves a single coordinator unauthenticated produces a
  bypass *(documented: `docs/topics/impala_mixed_security.xml`)*.
- **`impala-shell` failure to verify server certificate.** Invoking
  `impala-shell --ssl` without specifying `--ca_cert` or `--verify_cert` is a
  known insecure default that will be addressed in a future release.

## §11a Known non-findings (recurring false positives)

This section is the highest-leverage input for automated agentic security
scans. Each entry: tool symptom, why it is safe under the model, the §
that licenses the call.

- **"Internal RPC accepts plaintext / no auth" report against `:23000`,
  `:24000`, `:26000`, `:27000`.** In a model-conforming deployment the
  operator has set `--principal` + Kerberos per §10; cleartext is a §10
  violation by the operator, not an Impala bug. → `OUT-OF-MODEL:
  non-default-build` per §5a.
- **"Web UI on `:25000` reachable without authentication" against an
  un-`.htpasswd`-protected cluster.** Same shape as above; operator
  responsibility per §10. → `OUT-OF-MODEL: non-default-build`.
- **"`--jwt_allow_without_tls=true` permits credentials over plaintext" in a
  config file.** The flag is `_hidden` and named "debug only"; setting it
  voids §8 P3. → `OUT-OF-MODEL: non-default-build` *(maintainer)*.
- **"Path traversal in `gzopen`-style filename" against scanners.** All
  scanner paths are Ranger-checked URIs, not OS paths; the URI namespace is
  rooted at the operator-configured warehouse. → `OUT-OF-MODEL:
  trusted-input` *(maintainer)*.
- **"Hardcoded test password / keytab in `tests/`, `testdata/`,
  `ssh_keys/`."** `tests/`, `testdata/`, `ssh_keys/` are unsupported
  components. → `OUT-OF-MODEL: unsupported-component`.
- **"SQL injection via end-user SQL text."** End-user SQL **is the input**;
  the engine is designed to interpret it. The Ranger envelope is the
  authorization boundary, not SQL-text sanitization. → `BY-DESIGN:
  property-disclaimed`.
- **"User-defined function executes arbitrary code in the impalad process."**
  Documented and intentional; admission is Ranger-gated. → `BY-DESIGN:
  property-disclaimed` per §9.
- **"DoS via expensive analytic query on a large table."** Admission control
  is the fix surface, not the engine. → `BY-DESIGN: property-disclaimed`
  per §9.
- **"Decompression bomb in a Parquet/ORC file landed in a warehouse table."**
  Writers must have `INSERT`; the harm is reachable from an already-
  privileged actor. → `VALID-HARDENING` at most, unless it reaches §8 P8
  memory safety.
- **"Unchecked return from `malloc` in `udf_samples/`."** `udf_samples/` is
  unsupported sample code. → `OUT-OF-MODEL: unsupported-component`.
- **"Vendored Apache Kudu code under `be/src/kudu/security/` has CVE-X."**
  Report upstream to Apache Kudu; Impala will pick up the fix on the next
  vendored sync. → `OUT-OF-MODEL: unsupported-component` (upstream pointer)
  *(maintainer)*.

## §12 Conditions that would change this model

Revise this document when any of the following lands:

- A new authentication mechanism on a client-facing surface (e.g. mTLS-as-
  auth on HS2-HTTP, OIDC, U2F).
- A new authorization provider beyond Ranger (e.g. native Impala policy
  store, OPA integration).
- A new data-at-rest encryption story at the Impala layer (currently
  delegated; see §9).
- A new external-data surface (a new JDBC external-table connector, a new
  REST catalog beyond Iceberg, a new LLM connector beyond
  `ai_generate_text`).
- A UDF sandboxing story (changes §9 and §3 item 5).
- A change in the default value of any §5a flag, especially flags
  controlling auth (`--ssl_minimum_version`, `--jwt_validate_signature`).
- A vulnerability report that cannot be cleanly routed to one of the §13
  dispositions: that is evidence the model is incomplete.

## §13 Triage dispositions

A report against Impala receives exactly one of the following:

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope §7 adversary using an in-scope §6 input. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property violated, but a §11 misuse pattern can be made harder to fall into by code change. Fixed at maintainer discretion, typically no CVE. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires attacker control of a §6 parameter the model marks trusted (e.g. HMS-supplied metadata, Ranger-supplied policy, operator-supplied config flag). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires a §7 actor the model excludes (operator, malicious HMS/Ranger, Byzantine peer, side-channel observer, same-host non-`impala` UID-0). | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `tests/`, `testdata/`, `infra/`, `ssh_keys/`, `udf_samples/`, vendored upstream code under `be/src/kudu/`, etc. | §3 item 7, §3 item 8 |
| `OUT-OF-MODEL: non-default-build` | Only manifests under a §5a flag the maintainer has ruled is dev/test (e.g. `--jwt_allow_without_tls=true`, no `--principal`). | §5a |
| `OUT-OF-MODEL: equivalent-harm` | An actor already-authorized under the model can cause the same harm via a documented path (writer landing arbitrary file bytes, SQL-privileged user submitting expensive queries). | §3 item 4, §3 item 6 |
| `BY-DESIGN: property-disclaimed` | Concerns a §9 property the project explicitly does not provide (UDF sandboxing, DoS protection, side channels, etc.). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a recurring false positive. | §11a |
| `MODEL-GAP` | Cannot be cleanly routed to any of the above — triggers §12 model revision. | §12 |

## Appendix: SECURITY.md → §x back-map

Impala does not currently ship an in-repo `SECURITY.md`, and the website
does not publish a project-level security policy page
(`https://impala.apache.org/security.html` returns 404 at draft time; the
landing page only links the generic ASF security URL). The de facto
SECURITY-policy artifacts are the in-repo DITA docs under
`docs/topics/impala_security*.xml`, which are the source for the published
documentation at `https://impala.apache.org/docs/build/html/topics/impala_security.html`.
The back-map below covers the in-repo source.

| Source | Claim | Lands in |
| --- | --- | --- |
| `docs/topics/impala_security.xml` | "Impala includes a fine-grained authorization framework … based on Apache Ranger" | §8 P2 |
| `docs/topics/impala_security.xml` | "Impala relies on the Kerberos subsystem for authentication" | §8 P1, §8 P4 |
| `docs/topics/impala_security.xml` | "auditing capability … Impala generates the audit data which can be consumed … by cluster-management components focused on governance" | §10 item 11 (operator picks the audit sink) |
| `docs/topics/impala_security_guidelines.xml` | "Secure the root account", restrict `sudoers` | §3 item 3, §10 item 12 |
| `docs/topics/impala_security_guidelines.xml` | "Ensure that the Impala web UI … is password-protected" | §10 item 4, §11 first bullet |
| `docs/topics/impala_security_files.xml` | "All Impala read and write operations are performed under the filesystem privileges of the impala user" | §4 B7, §10 item 15 |
| `docs/topics/impala_authentication.xml` | "Impala supports authentication using either Kerberos or LDAP. You can also make proxy connections through Apache Knox." | §8 P1, §11 (Knox proxy is `--trusted_domain`-shaped) |
| `docs/topics/impala_authorization.xml` | "By default … Impala does all read and write operations with the privileges of the impala user" | §5a default-row "authorization_provider unset", §8 P2 violation symptom |
| `docs/topics/impala_ldap.xml` | "You must use the Kerberos authentication mechanism for connections between internal Impala components" | §4 B3, §8 P4 |
| `docs/topics/impala_ssl.xml` | "Impala supports TLS/SSL network encryption … default version was changed from 'tlsv1' to 'tlsv1.2' starting in Impala 4.0" | §8 P3, §5a row |
| `docs/topics/impala_security_webui.xml` | "This file should only be readable by the Impala process and machine administrators" | §10 item 9 |
| `docs/topics/impala_mixed_security.xml` | "Impala 2.0 and later automatically handles both Kerberos and LDAP authentication" | §11 (mixed-mode misconfig) |
| `docs/topics/impala_delegation.xml` | "Impala supports delegation where users whose names you specify can delegate the execution of a query to another user" | §8 P5, §10 item 10 |
| `docs/topics/impala_logging.xml#redaction` | "log redaction is a security feature that prevents sensitive information from being displayed in locations used by administrators for monitoring and troubleshooting" | §8 P6, §9 false-friend |
| `docs/topics/impala_ports.xml` | port inventory | §2 component table, §4 boundary table |
| `EXPORT_CONTROL.md` | "This software uses OpenSSL to enable TLS-encrypted connections, generate keys for asymmetric cryptography, and generate and verify signatures" | §5 cryptography assumption |
| `be/src/rpc/authentication.cc` lines 98–283 | flag inventory (cookie_secret_file, jwt_*, oauth_*, trusted_*, internal_principals_whitelist) | §5a, §6, §10 |
