#!/usr/bin/env impala-python
# Copyright (c) 2015, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
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

# Bootstrapping the native toolchain with prebuilt binaries
#
# The purpose of this script is to download prebuilt artifacts of the native toolchain to
# satisfy the third-party dependencies for Impala. The script checks for the presence of
# IMPALA_HOME and IMPALA_TOOLCHAIN. IMPALA_HOME indicates that the environment is
# correctly setup and that we can deduce the version settings of the dependencies from the
# environment. IMPALA_TOOLCHAIN indicates the location where the prebuilt artifacts should
# be extracted to.
#
# The script is called as follows without any additional parameters:
#
#     python bootstrap_toolchain.py
import sh
import os
import sys
import re

HOST = "https://native-toolchain.s3.amazonaws.com/build"

OS_MAPPING = {
  "centos6" : "ec2-package-centos-6",
  "centos5" : "ec2-package-centos-5",
  "centos7" : "ec2-package-centos-7",
  "debian6" : "ec2-package-debian-6",
  "debian7" : "ec2-package-debian-7",
  "debian8" : "ec2-package-debian-8",
  "suselinux11": "ec2-package-sles-11",
  "suselinux12": "ec2-package-sles-12",
  "ubuntu12.04" : "ec2-package-ubuntu-12-04",
  "ubuntu14.04" : "ec2-package-ubuntu-14-04"
}

def get_release_label():
  """Gets the right package label from the OS version"""
  release = "".join(map(lambda x: x.lower(), sh.lsb_release("-irs").split()))
  for k, v in OS_MAPPING.iteritems():
    if re.search(k, release):
      return v

  print("Pre-built toolchain archives not available for your platform.")
  print("Clone and build native toolchain from source using this repository:")
  print("    https://github.com/cloudera/native-toolchain")
  raise Exception("Could not find package label for OS version: {0}.".format(release))

def download_package(destination, product, version, compiler):
  label = get_release_label()
  file_name = "{0}-{1}-{2}-{3}.tar.gz".format(product, version, compiler, label)
  url_path="/{0}/{1}-{2}/{0}-{1}-{2}-{3}.tar.gz".format(product, version, compiler, label)
  download_path = HOST + url_path

  print "URL {0}".format(download_path)
  print "Downloading {0} to {1}".format(file_name, destination)
  # --no-clobber avoids downloading the file if a file with the name already exists
  sh.wget(download_path, directory_prefix=destination, no_clobber=True)
  print "Extracting {0}".format(file_name)
  sh.tar(z=True, x=True, f=os.path.join(destination, file_name), directory=destination)
  sh.rm(os.path.join(destination, file_name))


def bootstrap(packages):
  """Validates the presence of $IMPALA_HOME and $IMPALA_TOOLCHAIN in the environment. By
  checking $IMPALA_HOME is present, we assume that IMPALA_{LIB}_VERSION will be present as
  well. Will create the directory specified by $IMPALA_TOOLCHAIN if it does not yet
  exist. Each of the packages specified in `packages` is downloaded and extracted into
  $IMPALA_TOOLCHAIN.

  """
  if not os.getenv("IMPALA_HOME"):
    print("Impala environment not set up correctly, make sure "
          "impala-config.sh is sourced.")
    sys.exit(1)

  # Create the destination directory if necessary
  destination = os.getenv("IMPALA_TOOLCHAIN")
  if not destination:
    print("Impala environment not set up correctly, make sure "
          "$IMPALA_TOOLCHAIN is present.")
    sys.exit(1)

  if not os.path.exists(destination):
    os.makedirs(destination)

  # Detect the compiler
  compiler = "gcc-{0}".format(os.environ["IMPALA_GCC_VERSION"])

  for p in packages:
    pkg_name, pkg_version = unpack_name_and_version(p)
    download_package(destination, pkg_name, pkg_version, compiler)

def unpack_name_and_version(package):
  """A package definition is either a string where the version is fetched from the
  environment or a tuple where the package name and the package version are fully
  specified.
  """
  if isinstance(package, basestring):
    env_var = "IMPALA_{0}_VERSION".format(package).replace("-", "_").upper()
    try:
      return package, os.environ[env_var]
    except KeyError:
      raise Exception("Could not find version for {0} in environment var {1}".format(
        package, env_var))
  return package[0], package[1]

if __name__ == "__main__":
  packages = ["avro", "boost", "bzip2", "gcc", "gflags", "glog",
              "gperftools", "gtest", "llvm", ("llvm", "3.7.0"), "lz4", "openldap",
              "rapidjson", "re2", "snappy", "thrift", "zlib"]
  bootstrap(packages)
