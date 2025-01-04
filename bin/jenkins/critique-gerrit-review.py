#!/usr/bin/python3
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
# ssh, pip, venv
#
# TODO: generalise to other warnings
# * clang-tidy

from argparse import ArgumentParser
from collections import defaultdict
import json
import os
from os import environ
import os.path
import re
from subprocess import check_call, check_output, DEVNULL, Popen, PIPE
import sys
import venv


FLAKE8_VERSION = "7.1.1"
FLAKE8_DIFF_VERSION = "0.2.2"
PYPARSING_VERSION = "3.1.4"
FLAKE8_UNUSED_ARG_VERSION = "0.0.13"

VENV_PATH = "gerrit_critic_venv"
VENV_BIN = os.path.join(VENV_PATH, "bin")
PIP_PATH = os.path.join(VENV_BIN, "pip3")
FLAKE8_DIFF_PATH = os.path.join(VENV_BIN, "flake8-diff")

# Limit on length of lines in source files.
LINE_LIMIT = 90

# Source file extensions that we should apply our line limit and whitespace rules to.
SOURCE_EXTENSIONS = {".cc", ".h", ".java", ".py", ".sh", ".thrift"}

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

# Thrift files that are not used in communication between impalad and catalogd/statestore
EXCLUDE_THRIFT_FILES = {
  "BackendGflags.thrift",    # Only used between FE and BE
  "beeswax.thrift",          # Only used between client and impalad
  "DataSinks.thrift",        # Only used in impalads
  "Descriptors.thrift",      # Only used in impalads
  "ExecStats.thrift",        # Only used in impalads
  "LineageGraph.thrift",     # Only used in impalads
  "NetworkTest.thrift",      # Unused in production
  "Planner.thrift",          # Only used in impalads
  "PlanNodes.thrift",        # Only used in impalads
  "parquet.thrift",          # Only used in impalads
  "ResourceProfile.thrift",  # Only used in impalads
  "SystemTables.thrift",     # Only used in impalads
  "Zip.thrift",              # Unused
}

THRIFT_FILE_COMMENT = (
    "This file is used in communication between impalad and catalogd/statestore. "
    "Please make sure impalads can still work with new/old versions of catalogd and "
    "statestore. Basically only new optional fields can be added.")
FBS_FILE_COMMENT = (
    "This file is used in communication between impalad and catalogd/statestore. "
    "Please make sure impalads can still work with new/old versions of catalogd and "
    "statestore. Basically only new fields can be added and should be added at the end "
    "of a table definition.\n"
    "https://flatbuffers.dev/flatbuffers_guide_writing_schema.html")
PLANNER_TEST_TABLE_STATS = (
    "Table numRows and totalSize is changed in this planner test. It is unnecessary to "
    "change them. Consider running testdata/bin/restore-stats-on-planner-tests.py "
    "script to restore them to consistent values.")
UNCOMMITTED_CHANGE = "Uncommitted change detected!"
COMMENT_REVISION_SIDE = "REVISION"
COMMENT_PARENT_SIDE = "PARENT"

# Matches range information like:
#  @@ -229,0 +230,2 @@ struct TColumnStats {
RANGE_RE = re.compile(r"^@@ -([0-9]*).* \+([0-9]*).*@@\s*(.*)$")
# Matches required/optional fields like:
#  7: required i64 num_trues
REQUIRED_FIELD_RE = re.compile(r"[0-9]*: required \w* \w*")
OPTIONAL_FIELD_RE = re.compile(r"[0-9]*: optional \w* \w*")
# Matches include statements like:
#  include "Exprs.thrift"
INCLUDE_FILE_RE = re.compile(r"include \"(\w+\.thrift)\"")
THRIFT_WARNING_SUFFIX = (" might break the compatibility between impalad and "
                         "catalogd/statestore during upgrade")


def setup_virtualenv():
  """Set up virtualenv with flake8-diff."""
  venv.create(VENV_PATH, with_pip=True, system_site_packages=True)
  check_call([PIP_PATH, "install",
              "wheel",
              f"flake8=={FLAKE8_VERSION}",
              f"flake8-diff=={FLAKE8_DIFF_VERSION}",
              f"pyparsing=={PYPARSING_VERSION}",
              f"flake8-unused-arguments=={FLAKE8_UNUSED_ARG_VERSION}"])
  # Add the libpath of the installed venv to import pyparsing
  sys.path.append(os.path.join(VENV_PATH, f"lib/python{sys.version_info.major}."
                                          f"{sys.version_info.minor}/site-packages/"))


def get_comment_input(message, line_number=0, side=COMMENT_REVISION_SIDE,
                      context_line="", dryrun=False):
  comment = {"message": message, "line": line_number, "side": side}
  if dryrun:
    comment["context_line"] = context_line
  return comment


def get_flake8_comments(base_revision, revision):
  """Get flake8 warnings for code changes made in the git commit 'revision'.
  Returns a dict with file path as keys and a list of CommentInput objects. See
  https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#review-input
  for information on the format."""
  comments = defaultdict(lambda: [])
  # flake8 needs to be on the path.
  flake8_env = os.environ.copy()
  flake8_env["PATH"] = "{0}:{1}".format(VENV_BIN, flake8_env["PATH"])

  flake8_diff_proc = Popen(
      [FLAKE8_DIFF_PATH, "--standard-flake8-output", "--color", "off", base_revision,
       revision],
      stdin=PIPE, stdout=PIPE, stderr=PIPE, env=flake8_env, universal_newlines=True)
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


def get_misc_comments(base_revision, revision, dryrun=False):
  """Get miscellaneous warnings for code changes made in the git commit 'revision', e.g.
  long lines and trailing whitespace. These warnings are produced by directly parsing the
  diff output."""
  comments = defaultdict(lambda: [])
  # Matches range information like:
  #  @@ -128 +133,2 @@ if __name__ == "__main__":
  RANGE_RE = re.compile(r"^@@ -[0-9,]* \+([0-9]*).*$")

  diff = check_output(["git", "diff", "-U0", "{0}..{1}".format(base_revision, revision)],
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
      add_misc_comments_for_line(comments, diff_line[1:], curr_file, curr_line_num,
                                 dryrun)
      curr_line_num += 1
  return comments


def add_misc_comments_for_line(comments, line, curr_file, curr_line_num, dryrun=False):
  """Helper for get_misc_comments to generate comments for 'line' at 'curr_line_num' in
  'curr_file' and append them to 'comments'."""
  # Check for trailing whitespace.
  if line.rstrip() != line:
    comments[curr_file].append(get_comment_input(
        "line has trailing whitespace", curr_line_num, context_line=line, dryrun=dryrun))

  # Check for long lines. Skip .py files since flake8 already flags long lines.
  if len(line) > LINE_LIMIT and os.path.splitext(curr_file)[1] != ".py":
    msg = "line too long ({0} > {1})".format(len(line), LINE_LIMIT)
    comments[curr_file].append(get_comment_input(
        msg, curr_line_num, context_line=line, dryrun=dryrun))

  if '\t' in line:
    comments[curr_file].append(get_comment_input(
        "tab used for whitespace", curr_line_num, context_line=line, dryrun=dryrun))

  if 'ThriftDebugString' in line and curr_file.startswith("be/src/"):
    comments[curr_file].append(get_comment_input(
        "Please make sure you don't output sensitive data with ThriftDebugString(). "
        "If so, use impala::RedactedDebugString() instead.",
        curr_line_num, context_line=line, dryrun=dryrun))


def compare_thrift_structs(curr_file, old_structs, curr_structs, comments):
  # Skip if the file is removed or new
  if old_structs is None or curr_structs is None:
    return
  # Adding new structs is OK. Compare fields of existing structs.
  for struct_name, old_fields in old_structs.items():
    if struct_name not in curr_structs:
      print(f"Removed struct {struct_name}")
      continue
    curr_fields = curr_structs[struct_name]
    for fid, old_field in old_fields.items():
      if fid not in curr_fields:
        if old_field.qualifier == 'required':
          comments[curr_file].append(get_comment_input(
            f"Deleting required field '{old_field.name}' of {struct_name}"
            + THRIFT_WARNING_SUFFIX,
            old_field.line_num, COMMENT_PARENT_SIDE))
        continue
      curr_field = curr_fields.pop(fid)
      if curr_field.name != old_field.name:
        comments[curr_file].append(get_comment_input(
          f"Renaming field '{old_field.name}' to '{curr_field.name}' in {struct_name}"
          + THRIFT_WARNING_SUFFIX,
          curr_field.line_num))
        continue
      if curr_field.qualifier != old_field.qualifier:
        comments[curr_file].append(get_comment_input(
          f"Changing field '{old_field.name}' from {old_field.qualifier} to "
          f"{curr_field.qualifier} in {struct_name}" + THRIFT_WARNING_SUFFIX,
          curr_field.line_num))
        continue
      if curr_field.type != old_field.type:
        comments[curr_file].append(get_comment_input(
          f"Changing type of field '{old_field.name}' from {old_field.type} to "
          f"{curr_field.type} in {struct_name}" + THRIFT_WARNING_SUFFIX,
          curr_field.line_num))
    if len(curr_fields) > 0:
      for new_field in curr_fields.values():
        if new_field.qualifier == 'required':
          comments[curr_file].append(get_comment_input(
            f"Adding a required field '{new_field.name}' in {struct_name}"
            + THRIFT_WARNING_SUFFIX,
            new_field.line_num))
  new_struct_names = curr_structs.keys() - old_structs.keys()
  if len(new_struct_names) > 0:
    print(f"New structs {new_struct_names}")


def compare_thrift_enums(curr_file, old_enums, curr_enums, comments):
  # Skip if the file is removed or new
  if old_enums is None or curr_enums is None:
    return
  for enum_name, old_items in old_enums.items():
    if enum_name not in curr_enums:
      print(f"Removed enum {enum_name}")
      continue
    curr_items = curr_enums[enum_name]
    for value, old_item in old_items.items():
      if value not in curr_items:
        comments[curr_file].append(get_comment_input(
          f"Removing the enum item {enum_name}.{old_item.name}={value}"
          + THRIFT_WARNING_SUFFIX,
          old_item.line_num, COMMENT_PARENT_SIDE))
        continue
      if curr_items[value].name != old_item.name:
        comments[curr_file].append(get_comment_input(
          f"Enum item {old_item.name}={value} changed to "
          f"{curr_items[value].name}={value} in {enum_name}. This"
          + THRIFT_WARNING_SUFFIX,
          curr_items[value].line_num))


def extract_thrift_defs_of_revision(revision, file_name):
  """Extract a dict of thrift structs from pyparsing.ParseResults"""
  # Importing thrift_parser depends on pyparsing being installed in setup_virtualenv().
  from thrift_parser import extract_thrift_defs
  try:
    contents = check_output(["git", "show", f"{revision}:{file_name}"],
                            universal_newlines=True)
  except Exception as e:
    # Usually it's due to file doesn't exist in that revision
    print(f"Failed to read {file_name} of revision {revision}: {e}")
    return None, None
  return extract_thrift_defs(contents)


def get_catalog_compatibility_comments(base_revision, revision):
  """Get comments on Thrift/FlatBuffers changes that might break the communication
  between impalad and catalogd/statestore"""
  comments = defaultdict(lambda: [])

  diff = check_output(
      ["git", "diff", "--name-only", "{0}..{1}".format(base_revision, revision),
       "--", "common/thrift/*.thrift"],
      universal_newlines=True)
  for curr_file in diff.splitlines():
    if os.path.basename(curr_file) in EXCLUDE_THRIFT_FILES:
      continue
    print(f"Parsing {curr_file}")
    curr_structs, curr_enums = extract_thrift_defs_of_revision(revision, curr_file)
    old_structs, old_enums = extract_thrift_defs_of_revision(base_revision, curr_file)
    compare_thrift_structs(curr_file, old_structs, curr_structs, comments)
    compare_thrift_enums(curr_file, old_enums, curr_enums, comments)

  merge_comments(
      comments, get_flatbuffers_compatibility_comments(base_revision, revision))
  return comments


def get_flatbuffers_compatibility_comments(base_revision, revision):
  comments = defaultdict(lambda: [])
  diff = check_output(
      ["git", "diff", "--numstat", "{0}..{1}".format(base_revision, revision),
       "--", "common/fbs"],
      universal_newlines=True)
  for diff_line in diff.splitlines():
    _, _, path = diff_line.split()
    if os.path.splitext(path)[1] == ".fbs":
      comments[path].append(get_comment_input(FBS_FILE_COMMENT))
  return comments


def get_planner_tests_comments():
  comments = defaultdict(lambda: [])
  # First, check that testdata/ dir is clean.
  diff = check_output(
      ["git", "diff", "--numstat", "--", "testdata"],
      universal_newlines=True)
  for diff_line in diff.splitlines():
    _, _, path = diff_line.split()
    comments[path].append(get_comment_input(UNCOMMITTED_CHANGE))
  if len(comments) > 0:
    return comments

  # All clean. Run script and comment any changes.
  check_call(['./testdata/bin/restore-stats-on-planner-tests.py'], stdout=DEVNULL)
  diff = check_output(
      ["git", "diff", "--numstat", "--", "testdata"],
      universal_newlines=True)
  for diff_line in diff.splitlines():
    _, _, path = diff_line.split()
    if os.path.splitext(path)[1] == ".test":
      comments[path].append(get_comment_input(PLANNER_TEST_TABLE_STATS))

  # Restore testdata/ dir.
  check_call(['git', 'checkout', 'testdata'])
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
                "{0},{1}".format(change_num, patch_num)], stdin=PIPE,
                universal_newlines=True)
  proc.communicate(json.dumps(review_input))
  if proc.returncode != 0:
    raise Exception("Error posting review to gerrit.")


def merge_comments(a, b):
  for k, v in b.items():
    a[k].extend(v)


if __name__ == "__main__":
  parser = ArgumentParser(description="Generate and post gerrit comments")
  parser.add_argument("--dryrun", action='store_true',
                      help="Don't post comments back to gerrit. Also shows the context "
                           "lines if possible.")
  parser.add_argument("--revision", default="HEAD",
                      help="The revision to check. Defaults to HEAD. Note that "
                           "flake8-diff only actually works correctly on HEAD. So "
                           "specifying other commits might miss the results of "
                           "flake8-diff.")
  parser.add_argument("--base-revision",
                      help="The base revision to check. Defaults to the parent commit of"
                           " revision")
  args = parser.parse_args()

  setup_virtualenv()
  revision = args.revision
  base_revision = args.base_revision if args.base_revision else "{0}^".format(revision)
  comments = get_flake8_comments(base_revision, revision)
  merge_comments(comments, get_misc_comments(base_revision, revision, args.dryrun))
  merge_comments(
    comments, get_catalog_compatibility_comments(base_revision, revision))
  merge_comments(comments, get_planner_tests_comments())
  review_input = {"comments": comments}
  if len(comments) > 0:
    review_input["message"] = (
        "gerrit-auto-critic failed. You can reproduce it locally using command:\n\n"
        "  python3 bin/jenkins/critique-gerrit-review.py --dryrun\n\n"
        "To run it, you might need a virtual env with Python3's venv installed.")
  print(json.dumps(review_input, indent=True))
  if not args.dryrun:
    post_review_to_gerrit(review_input)
