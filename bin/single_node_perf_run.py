#!/usr/bin/env impala-python
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

# Usage:
# single_node_perf_run.py [options] git_hash_A [git_hash_B]
#
# When one hash is given, measures the performance on the specified workloads.
# When two hashes are given, compares their performance. Output is in
# $IMPALA_HOME/perf_results/latest. In the performance_result.txt file,
# git_hash_A is referred to as the "Base" result. For example, if you run with
# git_hash_A = aBad1dea... and git_hash_B = 8675309... the
# performance_result.txt will say at the top:
#
#   Run Description: "aBad1dea... vs 8675309..."
#
# The different queries will have their run time statistics in columns
# "Avg(s)", "StdDev(%)", "BaseAvg(s)", "Base StdDev(%)". The first two refer
# to git_hash_B, the second two refer to git_hash_A. The column "Delta(Avg)"
# is negative if git_hash_B is faster and is positive if git_hash_A is faster.
#
# WARNING: This script will run git checkout. You should not touch the tree
# while the script is running. You should start the script from a clean git
# tree.
#
# WARNING: When --load is used, this script calls load_data.py which can
# overwrite your TPC-H and TPC-DS data.
#
# Options:
#   -h, --help            show this help message and exit
#   --workloads=WORKLOADS
#                         comma-separated list of workloads. Choices: tpch,
#                         targeted-perf, tpcds. Default: targeted-perf
#   --scale=SCALE         scale factor for the workloads
#   --iterations=ITERATIONS
#                         number of times to run each query
#   --table_formats=TABLE_FORMATS
#                         comma-separated list of table formats. Default:
#                         parquet/none
#   --num_impalads=NUM_IMPALADS
#                         number of impalads. Default: 1
#   --query_names=QUERY_NAMES
#                         comma-separated list of regular expressions. A query
#                         is executed if it matches any regular expression in
#                         this list
#   --load                load databases for the chosen workloads
#   --start_minicluster   start a new Hadoop minicluster
#   --ninja               use ninja, rather than Make, as the build tool

from optparse import OptionParser
from tempfile import mkdtemp

import json
import os
import pipes
import sh
import shutil
import subprocess
import sys
import textwrap

from tests.common.test_dimensions import TableFormatInfo

IMPALA_HOME = os.environ["IMPALA_HOME"]


def configured_call(cmd):
  """Call a command in a shell with config-impala.sh."""
  if type(cmd) is list:
    cmd = " ".join([pipes.quote(arg) for arg in cmd])
  cmd = "source {0}/bin/impala-config.sh && {1}".format(IMPALA_HOME, cmd)
  return subprocess.check_call(["bash", "-c", cmd])


def load_data(db_to_load, table_formats, scale):
  """Loads a database with a particular scale factor."""
  configured_call(["{0}/bin/load-data.py".format(IMPALA_HOME),
                   "--workloads", db_to_load, "--scale_factor", str(scale),
                   "--table_formats", "text/none," + table_formats])
  for table_format in table_formats.split(","):
    suffix = TableFormatInfo.create_from_string(None, table_format).db_suffix()
    db_name = db_to_load + scale + suffix
    configured_call(["{0}/tests/util/compute_table_stats.py".format(IMPALA_HOME),
                     "--stop_on_error", "--db_names", db_name])

def get_git_hash_for_name(name):
  return sh.git("rev-parse", name).strip()


def build(git_hash, options):
  """Builds Impala in release mode; doesn't build tests."""
  sh.git.checkout(git_hash)
  buildall = ["{0}/buildall.sh".format(IMPALA_HOME), "-notests", "-release", "-noclean"]
  if options.ninja:
    buildall += ["-ninja"]
  configured_call(buildall)


def start_minicluster():
  configured_call(["{0}/bin/create-test-configuration.sh".format(IMPALA_HOME)])
  configured_call(["{0}/testdata/bin/run-all.sh".format(IMPALA_HOME)])


def start_impala(num_impalads):
  configured_call(["{0}/bin/start-impala-cluster.py".format(IMPALA_HOME), "-s",
                   str(num_impalads), "-c", str(num_impalads)])


def run_workload(base_dir, workloads, options):
  """Runs workload with the given options.

  Returns the git hash of the current revision to identify the output file.
  """
  git_hash = get_git_hash_for_name("HEAD")

  run_workload = ["{0}/bin/run-workload.py".format(IMPALA_HOME)]

  impalads = ",".join(["localhost:{0}".format(21000 + i)
                       for i in range(0, int(options.num_impalads))])

  run_workload += ["--workloads={0}".format(workloads),
                   "--impalads={0}".format(impalads),
                   "--results_json_file={0}/{1}.json".format(base_dir, git_hash),
                   "--query_iterations={0}".format(options.iterations),
                   "--table_formats={0}".format(options.table_formats),
                   "--plan_first"]

  if options.query_names:
    run_workload += ["--query_names={0}".format(options.query_names)]

  configured_call(run_workload)


def report_benchmark_results(file_a, file_b, description):
  """Wrapper around report_benchmark_result.py."""
  result = "{0}/perf_results/latest/performance_result.txt".format(IMPALA_HOME)
  with open(result, "w") as f:
    subprocess.check_call(
      ["{0}/tests/benchmark/report_benchmark_results.py".format(IMPALA_HOME),
       "--reference_result_file={0}".format(file_a),
       "--input_result_file={0}".format(file_b),
       '--report_description="{0}"'.format(description)],
      stdout=f)
  sh.cat(result, _out=sys.stdout)


def compare(base_dir, hash_a, hash_b):
  """Take the results of two performance runs and compare them."""
  file_a = os.path.join(base_dir, hash_a + ".json")
  file_b = os.path.join(base_dir, hash_b + ".json")
  description = "{0} vs {1}".format(hash_a, hash_b)
  report_benchmark_results(file_a, file_b, description)

  # From the two json files extract the profiles and diff them
  generate_profile_file(file_a, hash_a, base_dir)
  generate_profile_file(file_b, hash_b, base_dir)

  sh.diff("-u",
          os.path.join(base_dir, hash_a + "_profile.txt"),
          os.path.join(base_dir, hash_b + "_profile.txt"),
          _out=os.path.join(IMPALA_HOME, "performance_result_profile_diff.txt"),
          _ok_code=[0, 1])


def generate_profile_file(name, hash, base_dir):
  """Extracts runtime profiles from the JSON file 'name'.

  Writes the runtime profiles back in a simple text file in the same directory.
  """
  with open(name) as fid:
    data = json.load(fid)
    with open(os.path.join(base_dir, hash + "_profile.txt"), "w+") as out:
      # For each query
      for key in data:
        for iteration in data[key]:
          out.write(iteration["runtime_profile"])
          out.write("\n\n")


def backup_workloads():
  """Copy the workload folder to a temporary directory and returns its name.

  Used to keep workloads from being clobbered by git checkout.
  """
  temp_dir = mkdtemp()
  sh.cp(os.path.join(IMPALA_HOME, "testdata", "workloads"),
        temp_dir, R=True, _out=sys.stdout, _err=sys.stderr)
  print "Backed up workloads to {0}".format(temp_dir)
  return temp_dir


def restore_workloads(source):
  """Restores the workload directory from source into the Impala tree."""
  sh.cp(os.path.join(source, "workloads"), os.path.join(IMPALA_HOME, "testdata"),
        R=True, _out=sys.stdout, _err=sys.stderr)


def perf_ab_test(options, args):
  """Does the main work: build, run tests, compare."""
  hash_a = get_git_hash_for_name(args[0])

  # Create the base directory to store the results in
  results_path = os.path.join(IMPALA_HOME, "perf_results")
  if not os.access(results_path, os.W_OK):
    os.makedirs(results_path)

  temp_dir = mkdtemp(dir=results_path, prefix="perf_run_")
  latest = os.path.join(results_path, "latest")
  if os.path.islink(latest):
    os.remove(latest)
  os.symlink(os.path.basename(temp_dir), latest)
  workload_dir = backup_workloads()

  build(hash_a, options)
  restore_workloads(workload_dir)

  if options.start_minicluster:
    start_minicluster()
  start_impala(options.num_impalads)

  workloads = set(options.workloads.split(","))

  if options.load:
    WORKLOAD_TO_DATASET = {"tpch": "tpch", "tpcds": "tpcds", "targeted-perf": "tpch",
                           "tpcds-unmodified": "tpcds-unmodified"}
    datasets = set([WORKLOAD_TO_DATASET[workload] for workload in workloads])
    for dataset in datasets:
      load_data(dataset, options.table_formats, options.scale)

  workloads = ",".join(["{0}:{1}".format(workload, options.scale)
                        for workload in workloads])

  run_workload(temp_dir, workloads, options)

  if len(args) > 1 and args[1]:
    hash_b = get_git_hash_for_name(args[1])
    # discard any changes created by the previous restore_workloads()
    shutil.rmtree("testdata/workloads")
    sh.git.checkout("--", "testdata/workloads")
    build(hash_b, options)
    restore_workloads(workload_dir)
    start_impala(options.num_impalads)
    run_workload(temp_dir, workloads, options)
    compare(temp_dir, hash_a, hash_b)


def parse_options():
  """Parse and return the options and positional arguments."""
  parser = OptionParser()
  parser.add_option("--workloads", default="targeted-perf",
                    help="comma-separated list of workloads. Choices: tpch, "
                    "targeted-perf, tpcds. Default: targeted-perf")
  parser.add_option("--scale", help="scale factor for the workloads")
  parser.add_option("--iterations", default=30, help="number of times to run each query")
  parser.add_option("--table_formats", default="parquet/none", help="comma-separated "
                    "list of table formats. Default: parquet/none")
  parser.add_option("--num_impalads", default=3, help="number of impalads. Default: 1")
  # Less commonly-used options:
  parser.add_option("--query_names",
                    help="comma-separated list of regular expressions. A query is "
                    "executed if it matches any regular expression in this list")
  parser.add_option("--load", action="store_true",
                    help="load databases for the chosen workloads")
  parser.add_option("--start_minicluster", action="store_true",
                    help="start a new Hadoop minicluster")
  parser.add_option("--ninja", action="store_true",
                    help = "use ninja, rather than Make, as the build tool")

  parser.set_usage(textwrap.dedent("""
    single_node_perf_run.py [options] git_hash_A [git_hash_B]

    When one hash is given, measures the performance on the specified workloads.
    When two hashes are given, compares their performance. Output is in
    $IMPALA_HOME/perf_results/latest. In the performance_result.txt file,
    git_hash_A is referred to as the "Base" result. For example, if you run with
    git_hash_A = aBad1dea... and git_hash_B = 8675309... the
    performance_result.txt will say at the top:

      Run Description: "aBad1dea... vs 8675309..."

    The different queries will have their run time statistics in columns
    "Avg(s)", "StdDev(%)", "BaseAvg(s)", "Base StdDev(%)". The first two refer
    to git_hash_B, the second two refer to git_hash_A. The column "Delta(Avg)"
    is negative if git_hash_B is faster and is positive if git_hash_A is faster.

    WARNING: This script will run git checkout. You should not touch the tree
    while the script is running. You should start the script from a clean git
    tree.

    WARNING: When --load is used, this script calls load_data.py which can
    overwrite your TPC-H and TPC-DS data."""))

  options, args = parser.parse_args()

  if not 1 <= len(args) <= 2:
    parser.print_usage(sys.stderr)
    raise Exception("Invalid arguments: either 1 or 2 Git hashes allowed")

  return options, args


def main():
  """A thin wrapper around perf_ab_test that restores git state after."""
  options, args = parse_options()

  os.chdir(IMPALA_HOME)

  if sh.git("status", "--porcelain", "--untracked-files=no", _out=None).strip():
    sh.git("status", "--porcelain", "--untracked-files=no", _out=sys.stdout)
    raise Exception("Working copy is dirty. Consider 'git stash' and try again.")

  # Save the current hash to be able to return to this place in the tree when done
  current_hash = sh.git("rev-parse", "--abbrev-ref", "HEAD").strip()
  if current_hash == "HEAD":
    current_hash = get_git_hash_for_name("HEAD")

  try:
    workloads = backup_workloads()
    perf_ab_test(options, args)
  finally:
    # discard any changes created by the previous restore_workloads()
    shutil.rmtree("testdata/workloads")
    sh.git.checkout("--", "testdata/workloads")
    sh.git.checkout(current_hash)
    restore_workloads(workloads)


if __name__ == "__main__":
  main()
