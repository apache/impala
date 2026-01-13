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

# Apache Impala

## Agent Execution Rules

This file is optimized for AI agents. Follow these terms exactly:
- MUST: required behavior.
- SHOULD: preferred behavior unless a stronger instruction conflicts.
- MAY: optional behavior.

### Preconditions

- MUST run commands in a shell that supports `source`.
- MUST perform environment configuration before any build, test, or git command.
- MUST execute the environment configuration sequence in a single shell session.
- MUST keep the working directory at `${IMPALA_HOME}` unless a step explicitly requires
  another directory.
- SHOULD use non-interactive commands.

### Deterministic Command Policy

- MUST prefer explicit commands and explicit paths over inferred behavior.
- MUST use a complete command sequence for environment setup (see Environment
  Configuration).
- SHOULD use non-interactive git commands.
- SHOULD run the narrowest build and test scope that validates the current change.
- MAY expand scope to broader builds/tests when narrow validation fails or is insufficient.

### Failure Handling Policy

- MUST stop at the first environment setup failure and report:
  - the exact failing command
  - the key stderr/stdout message
  - the next retry command
- MUST stop and report if a required path, target, or artifact is missing.
- MUST report build/test failures with the executed command and the first actionable error.
- SHOULD retry once when failure is clearly transient (for example, network flake).
- MUST ask before destructive or irreversible operations.

### Minimal Task Playbook

1. Configure environment.
2. Identify changed scope.
3. Build only the touched component(s).
4. Run the narrowest relevant tests.
5. Expand validation only if needed.
6. Summarize exact commands run, outputs, and follow-up actions.

## Environment Configuration

Before running shell commands, configure the environment.

Required one-line sequence. If the absolute repo root path is unknown, resolve it
with `git rev-parse --show-toplevel` from any directory inside the repo:

`export IMPALA_HOME=$(git rev-parse --show-toplevel) && cd "${IMPALA_HOME}" && source "${IMPALA_HOME}/bin/impala-config.sh" && source "${IMPALA_HOME}/bin/set-classpath.sh"`

If you need stepwise execution, use this exact order:
1. `export IMPALA_HOME=$(git rev-parse --show-toplevel)`
2. `cd "${IMPALA_HOME}"`
3. `source "${IMPALA_HOME}/bin/impala-config.sh"`
4. `source "${IMPALA_HOME}/bin/set-classpath.sh"`

After sourcing, `IMPALA_BUILD_THREADS` is automatically set. Verify setup succeeded
by checking that `echo $IMPALA_HOME` prints the repo root and `echo $IMPALA_BUILD_THREADS`
prints a positive integer.

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository MUST consult `SECURITY.md` for threat model,
in-scope and out-of-scope declarations, and known non-findings before reporting
security issues.

## Project Overview

Apache Impala is the open source, native analytic database for open data and table
formats. It provides low latency and high concurrency for BI and analytic queries
on the Hadoop ecosystem, including Iceberg, open data formats, and most cloud
storage options. Impala also scales linearly in multitenant environments.

Impala is divided into several components:
- The Impala Daemon (`impalad`) serves as query coordinator and executor. In most
  deployments, each daemon instance is either a coordinator or an executor. Clients
  connect to coordinator instances. This daemon handles query planning and
  coordination and is implemented in C++ (query handling) and Java (query planning).
  It also manages metadata caching and retrieval from `catalogd`.
- The Impala Catalog Daemon (`catalogd`) is written mostly in Java with some C++
  for network communication. It manages metadata for all Impala daemons in a
  cluster and distributes table and database metadata to all `impalad` nodes. It
  acts as a caching layer over other catalogs (for example Hive Metastore and
  Iceberg REST catalogs).
- The Impala State Store (`statestored`) is written in C++ and tracks health and
  status of all Impala daemons in a cluster.
- The Impala Shell (`impala-shell`) is a Python command-line interface for
  connecting to and interacting with Impala daemons.

## Folder Structure

- `be`: Backend C++ codebase (query execution engine, storage engine, and low-level
  components). Uses CMake. Uses CTest and GoogleTest.
- `bin`: Utility scripts and executables for development, build, and test.
- `common`: Thrift, Flatbuffers, and Protobuf serialization definitions.
- `docker`: Container image build resources. Not used during normal development or
  build of Impala itself.
- `fe`: Frontend Java codebase for query planning. Maven is used for dependencies
  and build workflows. Parent POM is in `java/pom.xml`. Java 17 is required.
- `java`: Additional Java sub-projects used for development, testing, and runtime.
  Each subfolder is a Maven sub-project under parent `java/pom.xml`. Java 17 is
  required.
- `shell`: Python implementation of Impala shell.
- `testdata`: Test data resources and Python data-loading helpers.
- `tests`: Integration and end-to-end Python tests.
- `toolchain`: Compiled libraries used during builds.
- `www`: Web UI codebase for monitoring and managing Impala daemons.

## Build and Test Selection Policy

- MUST choose the smallest command set that validates the requested change.
- SHOULD avoid running full-repo builds/tests when a scoped build/test is sufficient.
- SHOULD only run Java project-local Maven commands inside the corresponding
  `java/*` project directory.

## Building

- To build C++ (`be`) and Java (`fe`/`java`) together, use `buildall.sh`.
  Run `buildall.sh --help` for flags.
- The default build type is `debug`. Pass `-release` to `buildall.sh` to build with type
  `release`.
- All `make` commands run from `${IMPALA_HOME}`.
- To build only C++ code under `be`, run:
  `make -j ${IMPALA_BUILD_THREADS} impalad`
- To build Java components (`fe` and related Java modules) through the make target,
  run:
  `make -j ${IMPALA_BUILD_THREADS} java`
- C++ unit test sources are under `be/src` and typically named `*-test.cc`.
  To build one such test target, run `make <testname>` where `<testname>` is the
  filename without `.cc`.
  Example: `make mem-pool-test`
- To build an individual project under `java`, `cd` to that project root (where
  `pom.xml` exists) and run `mvn install`. Run `cd "${IMPALA_HOME}"` afterwards to
  restore the working directory.

## Running Tests

- C++ CTest binaries are generated under `be/build/` with subdirectories depending
  on build type (`debug`, `release`, and so on). Test binary names correspond to
  source files named `*-test.cc`.
  Example (default `debug` build type):
  `be/src/runtime/mem-pool-test.cc` -> `be/build/debug/runtime/mem-pool-test`
  Run the binary directly.
- Run Java JUnit tests with Maven from the `fe` directory:
  `mvn test -Dtest="<target_test>"`
  `<target_test>` is either a class name or `ClassName#testMethod`.
  Examples:
  - `mvn test -Dtest="LocalCatalogTest"`
  - `mvn test -Dtest="LocalCatalogTest#testDbs"`
  Run `cd "${IMPALA_HOME}"` afterwards to restore the working directory.
- Debug Java JUnit tests with:
  `mvn -Dmaven.surefire.debug="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000 -Xnoagent -Djava.compiler=NONE" -fae -Dtest="<target_test>" test`
- Run Python tests under `tests` with:
  `${IMPALA_HOME}/bin/impala-py.test <path_to_test>`
  Example:
  `${IMPALA_HOME}/bin/impala-py.test tests/custom_cluster/test_otel_trace.py`
  To run one test function, add:
  `-k <test_name>`
  For tests under `tests/custom_cluster`, check whether the file contains
  `@SkipIfExploration.is_not_exhaustive()` (e.g., `grep -q SkipIfExploration <file>`).
  If it does, add `--exploration_strategy=exhaustive` to the invocation.

## Coding Standards

- Most files have a hard 90-character line limit. Exclusions are in
  `bin/jenkins/critique-gerrit-review.py` under `EXCLUDE_FILE_PATTERNS` and
  `EXCLUDE_THRIFT_FILES`.
- When breaking long lines, SHOULD break at higher-level syntax boundaries and use
  clear continuation indentation.
- Use 2 spaces, not tabs.
- Remove trailing whitespace.
- Blank lines must not contain spaces or tabs.
- For C++ in `/be` (`*.h`, `*.cc`), use Google C++ Style Guide
  (https://google.github.io/styleguide/cppguide.html) with these exceptions:
  - Self-contained headers: inline functions MAY be placed in `.inline.h` files.
  - Header guards: use `#pragma once`.
  - Constant names: use `UPPER_CASE` instead of `kConstantName`.
  - `using namespace` is allowed in `.cc` files only, never in header files.
  - Formatting is defined in `${IMPALA_HOME}/.clang-format`.
  - If-condition formatting: single-line `if` is allowed only when the full
    statement fits within 90 characters; otherwise use braces and a new line body.
- Most new files must include the Apache 2.0 license header. Exceptions apply when
  comments are not allowed (for example Markdown and JSON files).

## Git Commit Message Standards

- The first line of every commit message MUST start with `<jira_id>: `.
  `<jira_id>` must match `IMPALA-\d{4,}` and correspond to a Jira from:
  https://issues.apache.org/jira/projects/IMPALA/issues/
- Agents MUST NOT guess Jira IDs. Ask the user for the Jira ID when missing.
- Commit messages MUST include an `Assisted-by:` trailer.
  If missing, ask whether to add it.
  If approved, append:
  `Assisted-by: <model> (<agent-name>)`
  where `<model>` is the AI model name (for example `Claude Sonnet 4.5`) and
  `<agent-name>` is the agent or tool name (for example `GitHub Copilot`).
  If declined, proceed without it.
  If user input is invalid, abort.
- Gerrit requires `Change-Id:` in the final footer paragraph.
  Keep footer trailers contiguous (for example `Change-Id:` next to `Assisted-by:`)
  with no blank lines between trailer entries.

## Using Skills

Skills are defined under `.agents/skills` at repo root. To enumerate available
skills, run: `ls "${IMPALA_HOME}/.agents/skills/"`
When using scripts that have relative paths, resolve them relative to each skill
root directory.

Example:
To use `scripts/omake.sh` for skill `build-one-cc-file`, resolve from:
`.agents/skills/build-one-cc-file/`
