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

from __future__ import absolute_import, division, print_function
import json
import subprocess
import os
import shutil
from tempfile import mkdtemp

ALL_BUILD_OPTIONS_JOB = "all-build-options-ub2004"
JENKINS_IMPALA_IO = "jenkins.impala.io"
M2_ARCHIVE_NAME = "m2_archive.tar.gz"


class JenkinsBuild(object):
  """
  Basic information about a Jenkins build (number, url) to allow retrieving
  more detailed information.
  """
  def __init__(self, number, url):
    self.number = number
    self.url = url


class JenkinsBuildDetails(object):
  """
  Detailed information about the parameters and artifacts for a particular
  Jenkins build.
  """
  def __init__(self, parameter_dict, artifact_dict):
    self.parameter_dict = parameter_dict
    self.artifact_dict = artifact_dict


def get_build_list(jenkins_server, job):
  """
  Get the list of recent builds for the specified job on the jenkins server.
  This returns a list of JenkinsBuild objects containing the build numbers
  and corresponding urls.
  """
  # Make a temporary directory
  tmpdir = mkdtemp()

  json_dict = {}
  try:
    # This uses Jenkin's JSON API to get the list of build numbers for this job
    # along with the URL to each build. This downloads the JSON to a temporary file
    # and reads it back. This uses wget to avoid any python dependencies.
    json_url_tmpl = "https://{0}/job/{1}/api/json?tree=builds[number,url]&pretty=true"
    json_url = json_url_tmpl.format(jenkins_server, job)
    json_filename = os.path.join(tmpdir, "job_{0}_build_list.json".format(job))
    subprocess.check_call(["wget", "-q", json_url, "-O", json_filename])
    # Open the JSON file
    with open(json_filename) as f:
      json_dict = json.load(f)
  finally:
    # Cleanup temporary directory
    shutil.rmtree(tmpdir)

  # Convert the JSON dictionaries to JenkinsBuild objects
  builds = []
  for build_info in json_dict["builds"]:
    builds.append(JenkinsBuild(build_info["number"], build_info["url"]))

  return builds


def get_build_details(build):
  """
  Download detailed build information for the build number at the provided URL using
  the Jenkins JSON API. This returns a JenkinsBuildDetails, which includes information
  about the parameters of the Jenkins job and the artifacts produced by the Jenkins job.
  """

  tmpdir = mkdtemp()
  json_dict = {}
  try:
    # This downloads a json job to the temporary directory
    json_url = "{0}/api/json?&pretty=true".format(build.url)
    json_filename = os.path.join(tmpdir, "build_details_{0}.json".format(build.number))
    subprocess.check_call(["wget", "-q", json_url, "-O", json_filename])
    # Parse the JSON file
    json_dict = {}
    with open(json_filename) as f:
      json_dict = json.load(f)
  finally:
    shutil.rmtree(tmpdir)

  # Convert the JSON dictionaries to a JenkinsBuildDetail object
  parameter_dict = {}
  parameter_section = None
  for section in json_dict["actions"]:
    if "parameters" in section:
      parameter_section = section["parameters"]
      break
  for parameter in parameter_section:
    parameter_dict[parameter["name"]] = parameter["value"]

  artifact_dict = {}
  for artifact in json_dict["artifacts"]:
    artifact_url = "{0}/artifact/{1}".format(build.url, artifact["relativePath"])
    artifact_dict[artifact["fileName"]] = artifact_url

  return JenkinsBuildDetails(parameter_dict, artifact_dict)


def get_m2_archive_url(jenkins_server, jenkins_job):
  # Get the JSON list of builds for the all-build-options-ub1604 job. This code
  # is specific to how the Jenkins job is structured (i.e. parameters, archives),
  # so this is not generic.
  build_list = get_build_list(jenkins_server, jenkins_job)

  for build in build_list:
    # Go get more detailed information about the job
    build_details = get_build_details(build)
    # There are two criteria for a valid m2 archive:
    # 1. The build is based on the master branch
    # 2. The build has the appropriate m2 archive artifact
    is_master_build = "IMPALA_REPO_BRANCH" in build_details.parameter_dict and \
        build_details.parameter_dict["IMPALA_REPO_BRANCH"] == "master"
    has_m2_archive = M2_ARCHIVE_NAME in build_details.artifact_dict
    if is_master_build and has_m2_archive:
      return build_details.artifact_dict[M2_ARCHIVE_NAME]

  return None


def download_and_unpack_m2_archive(url, directory):
  print("Downloading m2 archive from {0} to {1}".format(url, directory))
  tarball_name = os.path.basename(url)
  tmp_tarball_location = os.path.join(directory, tarball_name)
  subprocess.check_call(["wget", "-q", url, "-O", tmp_tarball_location])
  m2_directory = os.path.expanduser("~/.m2")
  if not os.path.exists(m2_directory):
    print("{0} does not exist, creating...".format(m2_directory))
    os.makedirs(m2_directory)
  print("Unpacking {0} to {1}".format(tarball_name, m2_directory))
  tar_command = ["tar", "-zxf", tmp_tarball_location]
  # Unpack into m2 directory, but don't overwrite any files
  tar_command.extend(["-C", m2_directory, "--skip-old-files"])
  subprocess.check_call(tar_command)


def main():
  m2_archive_url = get_m2_archive_url(JENKINS_IMPALA_IO, ALL_BUILD_OPTIONS_JOB)
  if not m2_archive_url:
    print("Could not find any m2 archive for {0} {1}".format(JENKINS_IMPALA_IO,
        ALL_BUILD_OPTIONS_JOB))
  tmpdir = mkdtemp()
  try:
    download_and_unpack_m2_archive(m2_archive_url, tmpdir)
  finally:
    shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main()
