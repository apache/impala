---
name: build-one-cc-file
description: Build a single C++ file. Use when a single .cc file needs to be compiled to check for errors.
license: Apache-2.0
metadata:
  version: "1.0"
---

## Build One C++ File

Use this skill when needing to compile one `*.cc` C++ file to check for errors.
This provides a fast validation path without building the entire project.

## Deterministic Rules

- MUST run only from `${IMPALA_HOME}`.
- MUST configure the environment in the same shell session before building:
  `export IMPALA_HOME=<git_repo_root> && cd "${IMPALA_HOME}" && source "${IMPALA_HOME}/bin/impala-config.sh" && source "${IMPALA_HOME}/bin/set-classpath.sh"`
- MUST use `.agents/skills/build-one-cc-file/scripts/omake.sh` as the only build command
  path.
- MUST NOT use the agent's compile tool, Ninja, or direct CMake/Ninja invocation.

## Available Scripts

- `.agents/skills/build-one-cc-file/scripts/omake.sh [--dry-run] <input>` — builds the
  single object file for one C++ source.
  - Accepts a bare basename (`rpc-trace`).
  - `--dry-run`: prints the resolved `make` command on stdout and exits 0. Does not build.
  - Without `--dry-run`: runs the `make` command; exits 0 on success, non-zero on failure.
  - If no build target is found: exits non-zero (grep failure propagated via `set -e`).

## Input Contract

Pass exactly one argument: the source basename only.

- Input MUST be one token with no path separators.
- Input MUST NOT include a directory path.
- If input ends with `.cc`, strip `.cc` and continue.
- Basename MUST match this pattern: `[A-Za-z0-9_-]+`
- If validation fails, stop and request corrected input.

Examples:

- Correct: `.agents/skills/build-one-cc-file/scripts/omake.sh rpc-trace`
- Incorrect: `.agents/skills/build-one-cc-file/scripts/omake.sh be/src/rpc/rpc-trace.cc`
- Incorrect: `.agents/skills/build-one-cc-file/scripts/omake.sh rpc-trace.cc`

If starting from a path like `be/src/rpc/rpc-trace.cc`, convert it to `rpc-trace`
before invocation.

## Execution Procedure

1. Preflight and setup:
   - Ensure current directory is `${IMPALA_HOME}`.
   - Ensure environment configuration has run in the current shell session.
2. First attempt MUST use dry-run:
   - Run: `.agents/skills/build-one-cc-file/scripts/omake.sh --dry-run <basename>`
   - If dry-run fails, stop and report failure details.
3. Execute real build after successful dry-run:
   - Run: `.agents/skills/build-one-cc-file/scripts/omake.sh <basename>`

## Ambiguity Handling

If basename collision occurs (multiple candidate source files map to the same basename),
do not guess.

- MUST ask the user to clarify which source path to use.
- MUST wait for clarification before running the real build command.

## Failure and Retry Policy

On any failure:

- MUST report the exact command run.
- MUST report exit code.
- MUST report the first non-empty line from stderr; if stderr is empty, first stdout line
  containing "error:"

Retry behavior:

- MUST analyze the failure first.
- MUST attempt a concrete fix (for example corrected basename input or environment setup).
- MAY retry the real build (step 3) once after analysis and fix attempt.
- MUST NOT perform blind repeated retries.

## Success Output Contract

On success, report:

- Normalized basename used.
- Dry-run command and result.
- Real build command and result.
- Final exit code and success confirmation.
