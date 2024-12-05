#!/usr/bin/env ambari-python-wrap
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Future imports must happen at the beginning of the file
from __future__ import absolute_import, division, print_function

HELP = '''
Compares two specified branches, using the Gerrit Change-Id as the
primary identifier. Ignored commits can be added via a JSON
configuration file or with a special string in the commit message.
Changes can be cherrypicked with the --cherry_pick argument.

This script can be used to keep two development branches
(by default, "master" and "2.x", in sync). It is equivalent
to cherry-picking commits one by one, but automates identifying
the commits to cherry-pick. Unlike "git cherry", it uses
the Gerrit Change-Id identifier in the commit message
as a key.

The ignored_commits.json configuration file is of the following
form. Note that commits are the full 20-byte git hashes.

[
  {
    "source": "master",
    "target": "2.x",
    "commits": [
      { "hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "comment": "..."},
      { "hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "comment": "..."}
    ]
  }
]

The --target_remote_name is optional. If not specified, the target remote is set to
the value of the --source_remote_name.  Debug logging to stderr can be enabled with
--verbose.

Example:

$bin/compare_branches.py --source_branch master --target_branch 2.x
--------------------------------------------------------------------------------
Commits in asf-gerrit/master but not in asf-gerrit/2.x:
--------------------------------------------------------------------------------
35a3e186d61b8f365b0f7d1127be311758437e16 IMPALA-5478: Run TPCDS queries with decimal_v2 enabled (Thu Jan 18 03:28:51 2018 +0000) - Taras Bobrovytsky
d9b6fd073055b436c7404d49454dc215b2c7a369 IMPALA-6386: Invalidate metadata at table level for dataload (Wed Jan 17 22:52:58 2018 +0000) - Joe McDonnell
dcc7be0ed483b332dac22d6596f56ff2a6cfdaa3 IMPALA-4315: Allow USE and SHOW TABLES if the user has only column privileges (Wed Jan 17 22:40:13 2018 +0000) - Csaba Ringhofer
b6e43133e671773d2757612f72cfcdb0ff303226 IMPALA-6399: Increase timeout in test_observability to reduce flakiness (Wed Jan 17 22:31:33 2018 +0000) - Lars Volker
--------------------------------------------------------------------------------
Jira keys referenced (Note: not all commit messages will reference a jira key):
IMPALA-5478,IMPALA-6386,IMPALA-4315,IMPALA-6399
--------------------------------------------------------------------------------
'''

import argparse
import json
import logging
import os
import re
import subprocess
import sys

from collections import defaultdict
from collections import OrderedDict
from pprint import pformat

def create_parser():
  class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
      argparse.RawDescriptionHelpFormatter):
    """
    Mix-in to leave the description alone, but show
    defaults.
    """
    pass

  parser = argparse.ArgumentParser(
      formatter_class=CustomFormatter,
      description=HELP)

  parser.add_argument('--cherry_pick', action='store_true', default=False,
      help='Cherry-pick mismatched commits to current branch. This ' +
        'must match (in the hash sense) the target branch.')
  parser.add_argument('--partial_ok', action='store_true', default=False,
      help='Exit with success if at least one cherrypick succeeded.')
  parser.add_argument('--source_branch', default='master')
  parser.add_argument('--target_branch', default='2.x')
  parser.add_argument('--source_remote_name', default='asf-gerrit',
      help='Name of the source git remote. If set to empty string, ' +
           'this remote is not fetched and branch names are used ' +
           ' as is; otherwise, the source ref is remote/branch.')
  parser.add_argument('--target_remote_name', default=None,
      help='Name of the target git remote; defaults to source remote. ' +
           'Empty strings are handled the same way as --source_remote_name.')
  default_ignored_commits_path = os.path.join(
      os.path.dirname(os.path.abspath(__file__)), 'ignored_commits.json')
  parser.add_argument('--ignored_commits_file', default=default_ignored_commits_path,
      help='JSON File that contains ignored commits as specified in the help')
  parser.add_argument('--skip_commits_matching',
      default="Cherry-pick.?:.?not (for|to) {branch}",
      help='Regex searched for in commit messages that causes the commit to be ignored.' +
           ' {branch} is replaced with target branch; the search is case-insensitive')
  parser.add_argument('--verbose', '-v', action='store_true', default=False,
      help='Turn on DEBUG and INFO logging')
  return parser

def read_ignored_commits(ignored_commits_file):
  '''Returns a dictionary containing commits that should be ignored.

  ignored_commits_file is a path to a JSON file with schema
  specified at the top of this file.

  The return structure has dictionary keys are a tuple containing
  (source_branch, target_branch) and values are a set of git hashes.
  '''
  ignored_commits = defaultdict(set)
  with open(ignored_commits_file) as f:
    json_data = json.load(f)
    for result_dict in json_data:
      logging.debug("Parsing result_dict: {0}".format(result_dict))
      ignored_commits[(result_dict['source'], result_dict['target'])] =\
          set([ commit["hash"] for commit in result_dict['commits'] ])
  return ignored_commits

def build_commit_map(branch, merge_base):
  '''Creates a map from change id to (hash, subject, author, date, body).'''
  # Disable git pager in order for the sh.git.log command to work
  os.environ['GIT_PAGER'] = ''

  fields = ['%H', '%s', '%an', '%cd', '%b']
  pretty_format = '\x1f'.join(fields) + '\x1e'
  result = OrderedDict()
  for line in subprocess.check_output(
      ["git", "log", branch, "^" + merge_base, "--pretty=" + pretty_format,
       "--color=never"], universal_newlines=True).split('\x1e'):
    if line == "":
      # if no changes are identified by the git log, we get an empty string
      continue
    if line == "\n":
      # git log adds a newline to the end; we can skip it
      continue
    commit_hash, subject, author, date, body = [t.strip() for t in line.split('\x1f')]
    change_id_matches = re.findall('Change-Id: (.*)', body)
    if change_id_matches:
      if len(change_id_matches) > 1:
        logging.warning("Commit %s contains multiple change ids; using first one.",
            commit_hash)
      change_id = change_id_matches[0]
      result[change_id] = (commit_hash, subject, author, date, body)
    else:
      logging.warning('Commit {0} ({1}...) has no Change-Id.'.format(
        commit_hash, subject[:40]))
  logging.debug("Commit map for branch %s has size %d.", branch, len(result))
  return result

def cherrypick(cherry_pick_hashes, full_target_branch_name, partial_ok):
  """Cherrypicks the given commits.

  Also, asserts that full_target_branch_name matches the current HEAD.

  cherry_pick_hashes is a list of git hashes, in the order to
  be cherry-picked.

  If partial_ok is true, return gracefully if at least one cherrypick
  has succeeded.

  Note that this function does not push to the remote.
  """
  print("Cherrypicking %d changes." % (len(cherry_pick_hashes),))

  if len(cherry_pick_hashes) == 0:
    return

  # Cherrypicking only makes sense if we're on the equivalent of the target branch.
  head_sha = subprocess.check_output(
      ['git', 'rev-parse', 'HEAD'], universal_newlines=True).strip()
  target_branch_sha = subprocess.check_output(
      ['git', 'rev-parse', full_target_branch_name], universal_newlines=True).strip()
  if head_sha != target_branch_sha:
    print("Cannot cherrypick because %s (%s) and HEAD (%s) are divergent." % (
        full_target_branch_name, target_branch_sha, head_sha))
    sys.exit(1)

  cherry_pick_hashes.reverse()
  for i, cherry_pick_hash in enumerate(cherry_pick_hashes):
    ret = subprocess.call(
        ['git', 'cherry-pick', '--keep-redundant-commits', cherry_pick_hash])
    if ret != 0:
      if partial_ok and i > 0:
        subprocess.check_call(['git', 'cherry-pick', '--abort'])
        print("Failed to cherry-pick %s; stopping picks." % (cherry_pick_hash,))
        return
      else:
        raise Exception("Failed to cherry-pick: %s" % (cherry_pick_hash,))

def main():
  parser = create_parser()
  options = parser.parse_args()

  log_level = logging.WARNING
  if options.verbose:
    log_level = logging.DEBUG
  logging.basicConfig(level=log_level,
      format='%(asctime)s %(threadName)s %(levelname)s: %(message)s')

  if options.target_remote_name is None:
    options.target_remote_name = options.source_remote_name

  # Ensure all branches are up to date, unless remotes are disabled
  # by specifying them with an empty string.
  if options.source_remote_name != "":
    subprocess.check_call(['git', 'fetch', options.source_remote_name,
        options.source_branch])
    full_source_branch_name = options.source_remote_name + '/' + options.source_branch
  else:
    full_source_branch_name = options.source_branch
  if options.target_remote_name != "":
    if options.source_remote_name != options.target_remote_name\
        or options.source_branch != options.target_branch:
      subprocess.check_call(['git', 'fetch', options.target_remote_name,
          options.target_branch])
    full_target_branch_name = options.target_remote_name + '/' + options.target_branch
  else:
    full_target_branch_name = options.target_branch

  merge_base = subprocess.check_output(["git", "merge-base",
      full_source_branch_name, full_target_branch_name], universal_newlines=True).strip()
  source_commits = build_commit_map(full_source_branch_name, merge_base)
  target_commits = build_commit_map(full_target_branch_name, merge_base)

  ignored_commits = read_ignored_commits(options.ignored_commits_file)
  logging.debug("ignored commits from {0}:\n{1}"
               .format(options.ignored_commits_file, pformat(ignored_commits)))
  commits_ignored = []  # Track commits actually ignored for debug logging

  cherry_pick_hashes = []
  print('-' * 80)
  print('Commits in {0} but not in {1}:'.format(
      full_source_branch_name, full_target_branch_name))
  print('-' * 80)
  jira_keys = []
  jira_key_pat = re.compile(r'(IMPALA-\d+)')
  skip_commits_matching = options.skip_commits_matching.format(
      branch=options.target_branch)
  for change_id, (commit_hash, msg, author, date, body) in source_commits.items():
    change_in_target = change_id in target_commits
    ignore_by_config = commit_hash in ignored_commits[
        (options.source_branch, options.target_branch)]
    ignore_by_commit_message = re.search(skip_commits_matching, "\n".join([msg, body]),
        re.IGNORECASE)
    # This conditional block just for debug logging of ignored commits
    if ignore_by_config or ignore_by_commit_message:
      if change_in_target:
        logging.debug("Not ignoring commit because change is already in target: {0}"
                     .format(commit_hash))
      else:
        if ignore_by_commit_message:
          logging.debug("Ignoring commit {0} by commit message.".format(commit_hash))
        else:
          logging.debug("Ignoring commit {0} by config file.".format(commit_hash))
        commits_ignored.append(commit_hash)
    else:
      logging.debug("NOT ignoring commit {0} since not in ignored commits ({1},{2})"
                   .format(commit_hash, options.source_branch, options.target_branch))
    if not change_in_target and not ignore_by_config and not ignore_by_commit_message:
      print('{0} {1} ({2}) - {3}'.format(commit_hash, msg, date, author))
      cherry_pick_hashes.append(commit_hash)
      jira_keys += jira_key_pat.findall(msg)

  print('-' * 80)

  print("Jira keys referenced (Note: not all commit messages will reference a jira key):")
  print(','.join(jira_keys))
  print('-' * 80)

  logging.debug("Commits actually ignored (change was not in target): {0}"
               .format(pformat(commits_ignored)))

  if options.cherry_pick:
    cherrypick(cherry_pick_hashes, full_target_branch_name, options.partial_ok)

if __name__ == '__main__':
  main()
