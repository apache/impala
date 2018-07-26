#!/usr/bin/python
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
# Usage: critique-gerrit-review.py <git commit>
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
# * Lines too long and trailing whitespace
# * clang-tidy

from collections import defaultdict
import json
import os
from os import environ
import os.path
import re
from subprocess import check_call, Popen, PIPE
import sys
import virtualenv

FLAKE8_VERSION = "3.5.0"
FLAKE8_DIFF_VERSION = "0.2.2"

VENV_PATH = "gerrit_critic_venv"
VENV_BIN = os.path.join(VENV_PATH, "bin")
PIP_PATH = os.path.join(VENV_BIN, "pip")
FLAKE8_DIFF_PATH = os.path.join(VENV_BIN, "flake8-diff")


def setup_virtualenv():
  """Set up virtualenv with flake8-diff."""
  virtualenv.create_environment(VENV_PATH)
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


if __name__ == "__main__":
  setup_virtualenv()
  review_input = {"comments": get_flake8_comments(sys.argv[1])}
  print json.dumps(review_input, indent=True)
  post_review_to_gerrit(review_input)
