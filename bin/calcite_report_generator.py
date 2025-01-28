#!/usr/bin/env python3
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
#
# This processes the JSON files produces by the pytest
# "calcite_report_mode" option to produce a set of HTML pages
# with layers of aggregation / navigation. It produces the following
# layers:
# Level 0: Base results
#  - Individual HTML file for each test
#  - Leaf nodes in the directory structure
#  - e.g. query_test/test_foo.py::TestFoo::test_single_foo[test_dimension: x]
#  - Directory location: {out_dir}/{test_file_dir}/{test_function_dir}/{unique}
# Level 1: Aggregation of results for single test function
#  - Summary HTML file for each test function
#  - e.g. query_test/test_foo.py::TestFoo::test_single_foo
#  - Directory location: {out_dir}/{test_file_dir}/{test_function_dir}/index.html
# Level 2: Aggregation of results for single test class
#  - Summary HTML file for each test file
#  - e.g. query_test/test_foo.py
#  - Directory location: {out_dir}/{test_file_dir}/index.html
# Level 3: Top level aggregation of results
#  - Summary HTML file across all test files
#  - Directory location: {out_dir}/index.html
#
# It is designed to compare two separate runs to show the differences.

import glob
import json
import os
import sys
from argparse import ArgumentParser

HEADER_TEMPLATE = """
<!DOCTYPE html>
<html>
  <head>
    <link rel="stylesheet" href="{0}">
  </head>
<body>
"""

FOOTER = """
</body>
</html>
"""

RESULT_CATEGORIES = ["Success", "Parse Failure", "Analysis Failure",
                     "Unsupported Feature", "Result Difference",
                     "Profile Difference", "Different Error Msg", "Other"]

RESULT_CATEGORY_STYLE_MAP = {
  "Success": "success",
  "Parse Failure": "expected_fail",
  "Analysis Failure": "fail",
  "Unsupported Feature": "expected_fail",
  "Result Difference": "fail",
  "Profile Difference": "fail",
  "Different Error Msg": "expected_fail",
  "Other": "fail"
}


# This steals the logic in tests_file_parser.py to produce
# a single section string
def produce_section_string(test_case):
  SUBSECTION_DELIMITER = "----"
  s = ""
  for section_name, section_value in test_case.items():
    if section_name in ['QUERY_NAME', 'VERIFIER']:
      continue
    full_section_name = section_name
    if section_name == 'QUERY' and test_case.get('QUERY_NAME'):
      full_section_name = '%s: %s' % (section_name, test_case['QUERY_NAME'])
    if section_name == 'RESULTS' and test_case.get('VERIFIER'):
      full_section_name = '%s: %s' % (section_name, test_case['VERIFIER'])
    s += ("%s %s\n" % (SUBSECTION_DELIMITER, full_section_name))
    section_value = ''.join(test_case[section_name]).strip()
    if section_value:
      s += section_value
      s += "\n"
  return s


def categorize_error_string(error_string):
  if error_string is None:
    return "Success"
  elif "ParseException" in error_string:
    return "Parse Failure"
  elif "Unexpected exception string" in error_string and \
      "Not found in actual" in error_string:
    return "Different Error Msg"
  elif "AnalysisException" in error_string:
    return "Analysis Failure"
  elif "UnsupportedFeatureException" in error_string:
    return "Unsupported Feature"
  elif "Comparing QueryTestResults" in error_string:
    return "Result Difference"
  elif "PROFILE" in error_string:
    return "Profile Difference"
  else:
    return "Other"


def process_single_test_node(before_json_contents, after_json_contents, out_filename):
  test_node_id = before_json_contents["test_node_id"]
  test_file, test_class, _, test_function = test_node_id.split("[")[0].split("::")
  result_category_counts = {}
  for result_category in RESULT_CATEGORIES:
    # Total count, number increased, number decreased
    result_category_counts[result_category] = [0, 0, 0]
  with open(out_filename, "w") as f:
    f.write(HEADER_TEMPLATE.format("../../style.css"))
    parent_node = "{0}::{1}::{2}".format(test_file, test_class, test_function)
    f.write('<a href="index.html">Up to {0}</a>'.format(parent_node))
    f.write("<p>{0}</p>\n".format(before_json_contents["test_node_id"]))
    f.write("<p>{0}</p>\n".format(before_json_contents["test_file"]))
    f.write("<table>\n")
    # Table header
    f.write("<tr>\n")
    f.write("<th>Test Section</th>\n")
    f.write("<th>Before Result</th>\n")
    f.write("<th>After Result</th>\n")
    f.write("</tr>\n")
    # All the result rows
    for before_result, after_result in zip(before_json_contents["results"],
                                           after_json_contents["results"]):
      before_section = before_result["section"]
      after_section = after_result["section"]
      if "QUERY" in before_section:
        if before_section["QUERY"] != after_section["QUERY"]:
          raise Exception("Mismatch in test sections: BEFORE: {0} AFTER: {1}".format(
              before_section, after_section))
      f.write("<tr>\n")
      section_string = produce_section_string(before_section)
      f.write("<td><pre>{0}</pre></td>".format(section_string))
      before_error_category = categorize_error_string(before_result["error"])
      f.write('<td id="{0}"><pre>{1}</pre></td>'.format(
        RESULT_CATEGORY_STYLE_MAP[before_error_category],
        "Success" if before_error_category == "Success" else before_result["error"]))

      after_error_category = categorize_error_string(after_result["error"])
      f.write('<td id="{0}"><pre>{1}</pre></td>'.format(
        RESULT_CATEGORY_STYLE_MAP[after_error_category],
        "Success" if after_error_category == "Success" else after_result["error"]))

      after_error_counts = result_category_counts[after_error_category]
      # Always bump the first counter to count the total
      after_error_counts[0] += 1
      if after_error_category != before_error_category:
        before_error_counts = result_category_counts[before_error_category]
        # Bump before's counter for number decreased
        before_error_counts[2] += 1
        # Bump after's counter for number increased
        after_error_counts[1] += 1
      f.write("</tr>")

    f.write("</table>")
    f.write(FOOTER)

  return result_category_counts


def produce_function_index(out_filename, description, parent_description, stylesheet_link,
                           values):
  result_category_counts = {}
  for result_category in RESULT_CATEGORIES:
    # Total count, number increased, number decreased
    result_category_counts[result_category] = [0, 0, 0]
  with open(out_filename, "w") as f:
    f.write(HEADER_TEMPLATE.format(stylesheet_link))
    if parent_description is not None:
      f.write('<a href="../index.html">Up to {0}</a>'.format(parent_description))
    f.write("<p>{0}</p>".format(description))
    f.write("<table>\n")
    # Table header
    f.write("<tr>\n")
    f.write("<th>Name</th>\n")
    for result_category in RESULT_CATEGORIES:
      f.write("<th>{0}</th>".format(result_category))
    f.write("</tr>\n")
    for value in sorted(values):
      item_description, filename, stats = value
      f.write("<tr>\n")
      f.write('<td><a href="{0}">{1}</a></td>'.format(filename, item_description))
      for result_category in stats:
        result_counts = stats[result_category]
        if result_counts[1] == 0 and result_counts[2] == 0:
          f.write("<td>{0}</td>".format(result_counts[0]))
        else:
          f.write("<td>{0} (+{1}, -{2}) </td>".format(*result_counts))
        total_result_counts = result_category_counts[result_category]
        for i, val in enumerate(result_counts):
          total_result_counts[i] += val
      f.write("</tr>\n")

    # Add summary
    f.write("<tr>\n")
    f.write("<td>Total</td>")
    for result_category in stats:
      total_result_counts = result_category_counts[result_category]
      if total_result_counts[1] == 0 and total_result_counts[2] == 0:
        f.write("<td>{0}</td>".format(total_result_counts[0]))
      else:
        f.write("<td>{0} (+{1}, -{2}) </td>".format(*total_result_counts))
    f.write("</tr>\n")
    f.write("</table>\n")
    f.write(FOOTER)

  return result_category_counts


def get_output_files_set(directory):
  glob_list = glob.glob(os.path.join(directory, "output_*.json"))
  return set([os.path.basename(x) for x in glob_list])


def main():
  parser = ArgumentParser()
  parser.add_argument("--before_directory", required=True)
  parser.add_argument("--after_directory", required=True)
  parser.add_argument("--output_directory", required=True)
  parser.add_argument("--allow_file_differences", default=False, action="store_true")
  args = parser.parse_args()

  # Right now, only cover the simplest possible case: we have the same set of files in
  # the before and after directories. That lets us pair them up easily.
  # This assumption would be violated if we add/remove/change the test dimensions.
  # Hopefully, that won't be necessary for Calcite reports for a while.
  before_files = get_output_files_set(args.before_directory)
  after_files = get_output_files_set(args.after_directory)
  if before_files == after_files:
    files_intersection = before_files
  elif args.allow_file_differences:
    files_intersection = before_files.intersection(after_files)
    if len(files_intersection) == 0:
      print("ERROR: there are no files in common for the directories")
    else:
      print("There are file differences between the directories. Ignoring these files:")
      for f in before_files - after_files:
        print(os.path.join(args.before_directory, f))
      for f in after_files - before_files:
        print(os.path.join(args.after_directory, f))
  else:
    print("ERROR: the directories contain different sets of files")
    sys.exit(1)

  if not os.path.exists(args.output_directory):
    os.mkdir(args.output_directory)

  # Write out CSS to root directory.
  # Note: This needs to be in its own file separate from the HTML to avoid issues with
  # Content-Security-Policy.
  with open(os.path.join(args.output_directory, "style.css"), "w") as css:
    css.write("table, th, td { border: 1px solid black; border-collapse: collapse; }\n")
    css.write("#success { background-color: #d2ffd2; }\n")
    css.write("#fail { background-color: #ffd2d2; }\n")
    css.write("#expected_fail { background-color: #ffffa0; }\n")

  # Multiple levels of information that build up from the individual tests
  # to higher levels.
  # Level 0: Base results
  #  - Individual HTML file for each test
  #  - Leaf nodes in the directory structure
  #  - e.g. query_test/test_foo.py::TestFoo::test_single_foo[test_dimension: x]
  #  - Directory location: {out_dir}/{test_file_dir}/{test_function_dir}/{unique}
  # Level 1: Aggregation of results for single test function
  #  - Summary HTML file for each test function
  #  - e.g. query_test/test_foo.py::TestFoo::test_single_foo
  #  - Directory location: {out_dir}/{test_file_dir}/{test_function_dir}/index.html
  # Level 2: Aggregation of results for single test class
  #  - Summary HTML file for each test file
  #  - e.g. query_test/test_foo.py
  #  - Directory location: {out_dir}/{test_file_dir}/index.html
  # Level 3: Top level aggregation of results
  #  - Summary HTML file across all test files
  #  - Directory location: {out_dir}/index.html

  # Iterate over all the files and write out the level 0 individual test results.
  # While doing the iteration, also build the data structure for the level 1
  # aggregation.
  level1_index = {}
  for filename in files_intersection:
    before_filename = os.path.join(args.before_directory, filename)
    with open(before_filename) as f:
      after_filename = os.path.join(args.after_directory, filename)
      with open(after_filename) as g:
        before_json_contents = json.load(f)
        after_json_contents = json.load(g)
        test_node_id = before_json_contents["test_node_id"]
        # We are expecting the test files to match, so bail out if the files don't
        # match.
        if test_node_id != after_json_contents["test_node_id"]:
          raise Exception("File {0} does not have the same test node id as {1}".format(
              before_filename, after_filename))
        if len(before_json_contents["results"]) != len(after_json_contents["results"]):
          raise Exception("File {0} has different number of tests from file {1}".format(
              before_filename, after_filename))

        # Break apart the test node id to allow aggregating at various levels and
        # organizing the directory structure
        test_file, test_class, _, test_function = test_node_id.split("[")[0].split("::")

        # Step 1: Write out individual test html files
        # (When this becomes a diff, we'll have pairs of files to put into this)
        out_subdir = os.path.join(args.output_directory, test_file.replace("/", "_"),
                                  "{0}_{1}".format(test_class, test_function))
        if not os.path.exists(out_subdir):
          os.makedirs(out_subdir)
        output_filename = os.path.join(out_subdir,
            os.path.basename(before_filename).replace(".json", ".html"))
        out_stats = process_single_test_node(before_json_contents, after_json_contents,
            output_filename)

        # Build the data structure for the level 1 aggregation
        level1_id = (test_file, test_class, test_function)
        if level1_id not in level1_index:
          level1_index[level1_id] = []
        level1_index[level1_id].append(
              [test_node_id, os.path.basename(output_filename), out_stats])

  # Write out level 1 (aggregation per test function) while also building the data
  # structure for level 2 (aggregation per test file).
  level2_index = {}
  for key, value in level1_index.items():
    out_filename = os.path.join(args.output_directory, key[0].replace("/", "_"),
        "{0}_{1}".format(key[1], key[2]), "index.html")
    relative_filename = os.path.join("{0}_{1}".format(key[1], key[2]), "index.html")
    out_description = "{0}::{1}::{2}".format(key[0], key[1], key[2])
    parent_description = key[0]
    out_stats = produce_function_index(out_filename, out_description, parent_description,
        "../../style.css", value)
    # Grab the python file level key
    level2_key = key[0]
    if level2_key not in level2_index:
      level2_index[level2_key] = []
    level2_index[level2_key].append([out_description, relative_filename, out_stats])

  # Write out level 2 (aggregation per test file) while also building the data
  # structure for level 3 (top level aggregation)
  level3_index = {}
  level3_index["Top"] = []
  for key, value in level2_index.items():
      out_filename = os.path.join(args.output_directory, key.replace("/", "_"),
          "index.html")
      relative_filename = os.path.join(key.replace("/", "_"), "index.html")
      out_description = key
      parent_description = "Top"
      out_stats = produce_function_index(out_filename, out_description,
          parent_description, "../style.css", value)
      level3_index["Top"].append([out_description, relative_filename, out_stats])

  # Write out level 3 (top level aggregation)
  for key, value in level3_index.items():
    out_filename = os.path.join(args.output_directory, "index.html")
    out_description = "Top"
    parent_description = None
    out_stats = produce_function_index(out_filename, out_description, parent_description,
        "style.css", value)


if __name__ == "__main__":
  main()
