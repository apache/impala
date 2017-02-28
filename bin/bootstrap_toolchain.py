#!/usr/bin/env impala-python
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
# The purpose of this script is to download prebuilt binaries and jar files to satisfy the
# third-party dependencies for Impala. The script checks for the presence of IMPALA_HOME
# and IMPALA_TOOLCHAIN. IMPALA_HOME indicates that the environment is correctly setup and
# that we can deduce the version settings of the dependencies from the environment.
# IMPALA_TOOLCHAIN indicates the location where the prebuilt artifacts should be extracted
# to. If DOWNLOAD_CDH_COMPONENTS is set to true, this script will also download and extract
# the CDH components (i.e. Hadoop, Hive, HBase and Sentry) into
# CDH_COMPONENTS_HOME.
#
# The script is called as follows without any additional parameters:
#
#     python bootstrap_toolchain.py
import os
import re
import sh
import shutil
import subprocess
import sys
import tempfile

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
  "ubuntu14.04" : "ec2-package-ubuntu-14-04",
  "ubuntu15.04" : "ec2-package-ubuntu-14-04",
  "ubuntu15.10" : "ec2-package-ubuntu-14-04",
  "ubuntu16.04" : "ec2-package-ubuntu-16-04",
}

def try_get_platform_release_label():
  """Gets the right package label from the OS version. Return None if not found."""
  try:
    return get_platform_release_label()
  except:
    return None

def get_platform_release_label(release=None):
  """Gets the right package label from the OS version. Raise exception if not found.
     'release' can be provided to override the underlying OS version.
  """
  if not release:
    release = "".join(map(lambda x: x.lower(), sh.lsb_release("-irs").split()))
  for k, v in OS_MAPPING.iteritems():
    if re.search(k, release):
      return v

  raise Exception("Could not find package label for OS version: {0}.".format(release))


def wget_and_unpack_package(download_path, file_name, destination, wget_no_clobber):
  print "URL {0}".format(download_path)
  print "Downloading {0} to {1}".format(file_name, destination)
  # --no-clobber avoids downloading the file if a file with the name already exists
  sh.wget(download_path, directory_prefix=destination, no_clobber=wget_no_clobber)
  print "Extracting {0}".format(file_name)
  sh.tar(z=True, x=True, f=os.path.join(destination, file_name), directory=destination)
  sh.rm(os.path.join(destination, file_name))

def download_package(destination, product, version, compiler, platform_release=None):
  remove_existing_package(destination, product, version)

  toolchain_build_id = os.environ["IMPALA_TOOLCHAIN_BUILD_ID"]
  label = get_platform_release_label(release=platform_release)
  format_params = {'product': product, 'version': version, 'compiler': compiler,
      'label': label, 'toolchain_build_id': toolchain_build_id}
  file_name = "{product}-{version}-{compiler}-{label}.tar.gz".format(**format_params)
  format_params['file_name'] = file_name
  url_path = "/{toolchain_build_id}/{product}/{version}-{compiler}/{file_name}".format(
      **format_params)
  download_path = HOST + url_path

  wget_and_unpack_package(download_path, file_name, destination, True)

def bootstrap(toolchain_root, packages):
  """Downloads and unpacks each package in the list `packages` into `toolchain_root` if it
  doesn't exist already.
  """
  if not try_get_platform_release_label():
    check_custom_toolchain(toolchain_root, packages)
    return

  # Detect the compiler
  compiler = "gcc-{0}".format(os.environ["IMPALA_GCC_VERSION"])

  for p in packages:
    pkg_name, pkg_version = unpack_name_and_version(p)
    if check_for_existing_package(toolchain_root, pkg_name, pkg_version, compiler):
      continue
    if pkg_name != "kudu" or os.environ["KUDU_IS_SUPPORTED"] == "true":
      download_package(toolchain_root, pkg_name, pkg_version, compiler)
    else:
      build_kudu_stub(toolchain_root, pkg_version, compiler)
    write_version_file(toolchain_root, pkg_name, pkg_version, compiler,
        get_platform_release_label())

def check_output(cmd_args):
  """Run the command and return the output. Raise an exception if the command returns
     a non-zero return code. Similar to subprocess.check_output() which is only provided
     in python 2.7.
  """
  process = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  stdout, _ = process.communicate()
  if process.wait() != 0:
    raise Exception("Command with args '%s' failed with exit code %s:\n%s"
        % (cmd_args, process.returncode, stdout))
  return stdout

def package_directory(toolchain_root, pkg_name, pkg_version):
  dir_name = "{0}-{1}".format(pkg_name, pkg_version)
  return os.path.join(toolchain_root, dir_name)

def version_file_path(toolchain_root, pkg_name, pkg_version):
  return os.path.join(package_directory(toolchain_root, pkg_name, pkg_version),
      "toolchain_package_version.txt")

def check_custom_toolchain(toolchain_root, packages):
  missing = []
  for p in packages:
    pkg_name, pkg_version = unpack_name_and_version(p)
    pkg_dir = package_directory(toolchain_root, pkg_name, pkg_version)
    if not os.path.isdir(pkg_dir):
      missing.append((p, pkg_dir))

  if missing:
    print("The following packages are not in their expected locations.")
    for p, pkg_dir in missing:
      print("  %s (expected directory %s to exist)" % (p, pkg_dir))
    print("Pre-built toolchain archives not available for your platform.")
    print("Clone and build native toolchain from source using this repository:")
    print("    https://github.com/cloudera/native-toolchain")
    raise Exception("Toolchain bootstrap failed: required packages were missing")

def check_for_existing_package(toolchain_root, pkg_name, pkg_version, compiler):
  """Return true if toolchain_root already contains the package with the correct
  version and compiler.
  """
  version_file = version_file_path(toolchain_root, pkg_name, pkg_version)
  if not os.path.exists(version_file):
    return False

  label = get_platform_release_label()
  pkg_version_string = "{0}-{1}-{2}-{3}".format(pkg_name, pkg_version, compiler, label)
  with open(version_file) as f:
    return f.read().strip() == pkg_version_string

def write_version_file(toolchain_root, pkg_name, pkg_version, compiler, label):
  with open(version_file_path(toolchain_root, pkg_name, pkg_version), 'w') as f:
    f.write("{0}-{1}-{2}-{3}".format(pkg_name, pkg_version, compiler, label))

def remove_existing_package(toolchain_root, pkg_name, pkg_version):
  dir_path = package_directory(toolchain_root, pkg_name, pkg_version)
  if os.path.exists(dir_path):
    print "Removing existing package directory {0}".format(dir_path)
    shutil.rmtree(dir_path)

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

def build_kudu_stub(toolchain_root, kudu_version, compiler):
  # When Kudu isn't supported, the CentOS 7 package will be downloaded and the client
  # lib will be replaced with a stubbed client.
  download_package(toolchain_root, "kudu", kudu_version, compiler,
      platform_release="centos7")

  # Find the client lib files in the extracted dir. There may be several files with
  # various extensions. Also there will be a debug version.
  kudu_dir = package_directory(toolchain_root, "kudu", kudu_version)
  client_lib_paths = []
  for path, _, files in os.walk(kudu_dir):
    for file in files:
      if not file.startswith("libkudu_client.so"):
        continue
      file_path = os.path.join(path, file)
      if os.path.islink(file_path):
        continue
      client_lib_paths.append(file_path)
  if not client_lib_paths:
    raise Exception("Unable to find Kudu client lib under '%s'" % kudu_dir)

  # The client stub will be create by inspecting a real client and extracting the
  # symbols. The choice of which client file to use shouldn't matter.
  client_lib_path = client_lib_paths[0]

  # Use a newer version of binutils because on older systems the default binutils may
  # not be able to read the newer binary.
  binutils_dir = package_directory(
      toolchain_root, "binutils", os.environ["IMPALA_BINUTILS_VERSION"])
  nm_path = os.path.join(binutils_dir, "bin", "nm")
  objdump_path = os.path.join(binutils_dir, "bin", "objdump")

  # Extract the symbols and write the stubbed client source. There is a special method
  # kudu::client::GetShortVersionString() that is overridden so that the stub can be
  # identified by the caller.
  get_short_version_sig = "kudu::client::GetShortVersionString()"
  nm_out = check_output([nm_path, "--defined-only", "-D", client_lib_path])
  stub_build_dir = tempfile.mkdtemp()
  stub_client_src_file = open(os.path.join(stub_build_dir, "kudu_client.cc"), "w")
  try:
    stub_client_src_file.write("""
#include <string>

static const std::string kFakeKuduVersion = "__IMPALA_KUDU_STUB__";

static void KuduNotSupported() {
    *((char*)0) = 0;
}

namespace kudu { namespace client {
std::string GetShortVersionString() { return kFakeKuduVersion; }
}}
""")
    found_start_version_symbol = False
    cpp_filt_path = os.path.join(binutils_dir, "bin", "c++filt")
    for line in nm_out.splitlines():
      addr, sym_type, mangled_name = line.split(" ")
      # Skip special functions an anything that isn't a strong symbol. Any symbols that
      # get passed this check must be related to Kudu. If a symbol unrelated to Kudu
      # (ex: a boost symbol) gets defined in the stub, there's a chance the symbol could
      # get used and crash Impala.
      if mangled_name in ["_init", "_fini"] or sym_type not in "Tt":
        continue
      demangled_name = check_output([cpp_filt_path, mangled_name]).strip()
      assert "kudu" in demangled_name, \
          "Symbol doesn't appear to be related to Kudu: " + demangled_name
      if demangled_name == get_short_version_sig:
        found_start_version_symbol = True
        continue
      stub_client_src_file.write("""
extern "C" void %s() {
  KuduNotSupported();
}
""" % mangled_name)

    if not found_start_version_symbol:
      raise Exception("Expected to find symbol a corresponding to"
          " %s but it was not found." % get_short_version_sig)
    stub_client_src_file.flush()

    # The soname is needed to avoid problem in packaging builds. Without the soname,
    # the library dependency as listed in the impalad binary will be a full path instead
    # of a short name. Debian in particular has problems with packaging when that happens.
    objdump_out = check_output([objdump_path, "-p", client_lib_path])
    for line in objdump_out.splitlines():
      if "SONAME" not in line:
        continue
      # The line that needs to be parsed should be something like:
      # "  SONAME               libkudu_client.so.0"
      so_name = line.split()[1]
      break
    else:
      raise Exception("Unable to extract soname from %s" % client_lib_path)

    # Compile the library.
    stub_client_lib_path = os.path.join(stub_build_dir, "libkudu_client.so")
    subprocess.check_call(["g++", stub_client_src_file.name, "-shared", "-fPIC",
        "-Wl,-soname,%s" % so_name, "-o", stub_client_lib_path])

    # Replace the real libs with the stub.
    for client_lib_path in client_lib_paths:
      shutil.copyfile(stub_client_lib_path, client_lib_path)
  finally:
    shutil.rmtree(stub_build_dir)

def download_cdh_components(toolchain_root, cdh_components):
  """Downloads and unpacks the CDH components into $CDH_COMPONENTS_HOME if not found."""
  cdh_components_home = os.getenv("CDH_COMPONENTS_HOME")
  if not cdh_components_home:
    print("Impala environment not set up correctly, make sure "
          "$CDH_COMPONENTS_HOME is present.")
    return

  # Create the directory where CDH components live if necessary.
  if not os.path.exists(cdh_components_home):
    os.makedirs(cdh_components_home)

  # The URL prefix of where CDH components live in S3.
  download_path_prefix = HOST + "/cdh_components/"

  for component in cdh_components:
    pkg_name, pkg_version = unpack_name_and_version(component)
    pkg_directory = package_directory(cdh_components_home, pkg_name, pkg_version)
    if os.path.isdir(pkg_directory):
      continue

    # Download the package if it doesn't exist
    file_name = "{0}-{1}.tar.gz".format(pkg_name, pkg_version)
    download_path = download_path_prefix + file_name
    wget_and_unpack_package(download_path, file_name, cdh_components_home, False)

if __name__ == "__main__":
  """Validates the presence of $IMPALA_HOME and $IMPALA_TOOLCHAIN in the environment.-
  By checking $IMPALA_HOME is present, we assume that IMPALA_{LIB}_VERSION will be present
  as well. Will create the directory specified by $IMPALA_TOOLCHAIN if it doesn't exist
  yet. Each of the packages specified in `packages` is downloaded and extracted into
  $IMPALA_TOOLCHAIN. If $DOWNLOAD_CDH_COMPONENTS is true, this function will also download
  the CDH components (i.e. hadoop, hbase, hive, llama, llama-minikidc and sentry) into the
  directory specified by $CDH_COMPONENTS_HOME.
  """
  if not os.getenv("IMPALA_HOME"):
    print("Impala environment not set up correctly, make sure "
          "impala-config.sh is sourced.")
    sys.exit(1)

  # Create the destination directory if necessary
  toolchain_root = os.getenv("IMPALA_TOOLCHAIN")
  if not toolchain_root:
    print("Impala environment not set up correctly, make sure "
          "$IMPALA_TOOLCHAIN is present.")
    sys.exit(1)

  if not os.path.exists(toolchain_root):
    os.makedirs(toolchain_root)

  packages = ["avro", "binutils", "boost", "breakpad", "bzip2", "cmake", "crcutil",
      "flatbuffers", "gcc", "gflags", "glog", "gperftools", "gtest", "kudu", "libev",
      "llvm", ("llvm", "3.8.0-asserts-p1"), "lz4", "openldap", "protobuf", "rapidjson",
      "re2", "snappy", "thrift", "tpc-h", "tpc-ds", "zlib"]
  bootstrap(toolchain_root, packages)

  # Download the CDH components if necessary.
  if os.getenv("DOWNLOAD_CDH_COMPONENTS", "false") == "true":
    cdh_components = ["hadoop", "hbase", "hive", "llama-minikdc", "sentry"]
    download_cdh_components(toolchain_root, cdh_components)
