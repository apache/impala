#!/usr/bin/env ambari-python-wrap
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
# Usage: critique-gerrit-review.py [--dryrun]
#
# This script is meant to run on an jenkins.impala.io build slave and post back comments
# to a code review. It does not need to run on all supported platforms so we use system
# python instead of the full Impala virtualenv.
#
# This script runs in the context of a source checkout. It posts comments for issues
# introduced between HEAD^ and HEAD. It picks up metadata from environment variables
# set by the jenkins gerrit trigger: GERRIT_CHANGE_NUMBER, GERRIT_PATCHSET_NUMBER, etc.
#
# It uses the gerrit ssh interface to post the review, connecting as
# impala-public-jenkins.
# Ref: https://gerrit-review.googlesource.com/Documentation/cmd-review.html
#
# Dependencies:
# ssh, pip, virtualenv
#
# TODO: generalise to other warnings
# * clang-tidy

from __future__ import absolute_import, division, print_function
from argparse import ArgumentParser
from collections import defaultdict
import json
import os
from os import environ
import os.path
import re
from subprocess import check_call, check_output, Popen, PIPE
import sys
import virtualenv

FLAKE8_VERSION = "3.9.2"
FLAKE8_DIFF_VERSION = "0.2.2"

VENV_PATH = "gerrit_critic_venv"
VENV_BIN = os.path.join(VENV_PATH, "bin")
PIP_PATH = os.path.join(VENV_BIN, "pip")
FLAKE8_DIFF_PATH = os.path.join(VENV_BIN, "flake8-diff")

# Limit on length of lines in source files.
LINE_LIMIT = 90

# Source file extensions that we should apply our line limit and whitespace rules to.
SOURCE_EXTENSIONS = set([".cc", ".h", ".java", ".py", ".sh", ".thrift"])

# Source file patterns that we exclude from our checks.
EXCLUDE_FILE_PATTERNS = [
    re.compile(r".*be/src/kudu.*"),  # Kudu source code may have different rules.
    re.compile(r".*-benchmark.cc"),  # Benchmark files tend to have long lines.
    re.compile(r".*/function-registry/impala_functions.py"),  # Many long strings.
    re.compile(r".*/catalog/BuiltinsDb.java"),  # Many long strings.
    re.compile(r".*/codegen/gen_ir_descriptions.py"),  # Many long strings.
    re.compile(r".*shell/ext-py/.*"),  # Third-party code.
    re.compile(r".*be/src/thirdparty/.*"),  # Third-party code.
    re.compile(r".*/.*\.xml\.py")  # Long lines in config template files.
]


def setup_virtualenv():
  """Set up virtualenv with flake8-diff."""
  virtualenv.cli_run([VENV_PATH])
  check_call([PIP_PATH, "install",
              "flake8=={0}".format(FLAKE8_VERSION),
              "flake8-diff=={0}".format(FLAKE8_DIFF_VERSION)])


def get_flake8_comments(revision):
  """Get flake8 warnings for code changes made in the git commit 'revision'.
  Returns a dict with file path as keys and a list of CommentInput objects. See
  https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#review-input
  for information on the format."""
  comments = defaultdict(lambda: [])
  # flake8 needs to be on the path.
  flake8_env = os.environ.copy()
  flake8_env["PATH"] = "{0}:{1}".format(VENV_BIN, flake8_env["PATH"])

  base_revision = "{0}^".format(revision)
  flake8_diff_proc = Popen(
      [FLAKE8_DIFF_PATH, "--standard-flake8-output", "--color", "off", base_revision,
       revision],
      stdin=PIPE, stdout=PIPE, stderr=PIPE, env=flake8_env)
  stdout, stderr = flake8_diff_proc.communicate()
  # Ignore the return code since it will be non-zero if any violations are found. We want
  # to continue in that case. Instead check stderr for any errors.
  if stderr:
    raise Exception("Did not expect flake8-diff to write to stderr:\n{0}".format(stderr))

  # Match output lines like:
  #   bin/jenkins/flake8-gerrit-review.py:25:1: F401 'json' imported but unused
  VIOLATION_RE = re.compile(r"^([^:]*):([0-9]*):([0-9]*): (.*)$")

  for line in stdout.splitlines():
    match = VIOLATION_RE.match(line)
    if not match:
      raise Exception("Pattern did not match line:\n{0}".format(line))
    file, line, col, details = match.groups()
    line = int(line)
    col = int(col)
    skip_file = False
    for pattern in EXCLUDE_FILE_PATTERNS:
      if pattern.match(file):
        skip_file = True
        break
    if skip_file:
      continue
    comments_for_file = comments[file]
    comment = {"message": "flake8: {0}".format(details)}
    # Heuristic: if the error is on the first column, assume it applies to the whole line.
    if col == 1:
      comment["line"] = line
    else:
      comment["range"] = {"start_line": line, "end_line": line,
                          "start_character": col - 1, "end_character": col}
    comments_for_file.append(comment)
  return comments


def get_misc_comments(revision):
  """Get miscellaneous warnings for code changes made in the git commit 'revision', e.g.
  long lines and trailing whitespace. These warnings are produced by directly parsing the
  diff output."""
  comments = defaultdict(lambda: [])
  # Matches range information like:
  #  @@ -128 +133,2 @@ if __name__ == "__main__":
  RANGE_RE = re.compile(r"^@@ -[0-9,]* \+([0-9]*).*$")

  diff = check_output(["git", "diff", "-U0", "{0}^..{0}".format(revision)],
      universal_newlines=True)
  curr_file = None
  check_source_file = False
  curr_line_num = 0
  for diff_line in diff.splitlines():
    if diff_line.startswith("+++ "):
      # Start of diff for a file. Strip off "+++ b/" to get the file path.
      curr_file = diff_line[6:]
      check_source_file = os.path.splitext(curr_file)[1] in SOURCE_EXTENSIONS
      if check_source_file:
        for pattern in EXCLUDE_FILE_PATTERNS:
          if pattern.match(curr_file):
            check_source_file = False
            break
    elif diff_line.startswith("@@ "):
      # Figure out the starting line of the hunk. Format of unified diff is:
      #  @@ -128 +133,2 @@ if __name__ == "__main__":
      # We want to extract the start line for the added lines
      match = RANGE_RE.match(diff_line)
      if not match:
        raise Exception("Pattern did not match diff line:\n{0}".format(diff_line))
      curr_line_num = int(match.group(1))
    elif diff_line.startswith("+") and check_source_file:
      # An added or modified line - check it to see if we should generate warnings.
      add_misc_comments_for_line(comments, diff_line[1:], curr_file, curr_line_num)
      curr_line_num += 1
  return comments


def add_misc_comments_for_line(comments, line, curr_file, curr_line_num):
  """Helper for get_misc_comments to generate comments for 'line' at 'curr_line_num' in
  'curr_file' and append them to 'comments'."""
  # Check for trailing whitespace.
  if line.rstrip() != line:
    comments[curr_file].append(
        {"message": "line has trailing whitespace", "line": curr_line_num})

  # Check for long lines. Skip .py files since flake8 already flags long lines.
  if len(line) > LINE_LIMIT and os.path.splitext(curr_file)[1] != ".py":
    msg = "line too long ({0} > {1})".format(len(line), LINE_LIMIT)
    comments[curr_file].append(
        {"message": msg, "line": curr_line_num})

  if '\t' in line:
    comments[curr_file].append(
        {"message": "tab used for whitespace", "line": curr_line_num})

  if 'ThriftDebugString' in line:
    comments[curr_file].append(
        {"message": ("Please make sure you don't output sensitive data with "
                     "ThriftDebugString(). If so, use impala::RedactedDebugString() "
                     "instead."),
         "line": curr_line_num })

def post_review_to_gerrit(review_input):
  """Post a review to the gerrit patchset. 'review_input' is a ReviewInput JSON object
  containing the review comments. The gerrit change and patchset are picked up from
  environment variables set by the gerrit jenkins trigger."""
  change_num = environ["GERRIT_CHANGE_NUMBER"]
  patch_num = environ["GERRIT_PATCHSET_NUMBER"]
  proc = Popen(["ssh", "-p", environ["GERRIT_PORT"],
                "impala-public-jenkins@" + environ["GERRIT_HOST"], "gerrit", "review",
                "--project", environ["GERRIT_PROJECT"], "--json",
                "{0},{1}".format(change_num, patch_num)], stdin=PIPE)
  proc.communicate(json.dumps(review_input))
  if proc.returncode != 0:
    raise Exception("Error posting review to gerrit.")


def merge_comments(a, b):
  for k, v in b.items():
    a[k].extend(v)


if __name__ == "__main__":
  parser = ArgumentParser(description="Generate and post gerrit comments")
  parser.add_argument("--dryrun", action='store_true',
                      help="Don't post comments back to gerrit")
  args = parser.parse_args()

  setup_virtualenv()
  # flake8-diff only actually works correctly on HEAD, so this is the only revision
  # we can correctly handle.
  revision = 'HEAD'
  comments = get_flake8_comments(revision)
  merge_comments(comments, get_misc_comments(revision))
  review_input = {"comments": comments}
  print(json.dumps(review_input, indent=True))
  if not args.dryrun:
    post_review_to_gerrit(review_input)
