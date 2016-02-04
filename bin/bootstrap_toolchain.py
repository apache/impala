#!/usr/bin/env impala-python
# Copyright (c) 2015, Cloudera, inc.
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
import os
import re
import sh
import shutil
import subprocess
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

def download_package(destination, product, version, compiler, platform_release=None):
  remove_existing_package(destination, product, version)

  label = get_platform_release_label(release=platform_release)
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
  toolchain_root = os.getenv("IMPALA_TOOLCHAIN")
  if not toolchain_root:
    print("Impala environment not set up correctly, make sure "
          "$IMPALA_TOOLCHAIN is present.")
    sys.exit(1)

  if not os.path.exists(toolchain_root):
    os.makedirs(toolchain_root)

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
  get_short_version_symbol = "_ZN4kudu6client21GetShortVersionStringEv"
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
    for line in nm_out.splitlines():
      addr, sym_type, name = line.split(" ")
      if name in ["_init", "_fini"]:
        continue
      if name == get_short_version_symbol:
        found_start_version_symbol = True
        continue
      if sym_type.upper() in "TW":
        stub_client_src_file.write("""
extern "C" void %s() {
  KuduNotSupported();
}
""" % name)
    if not found_start_version_symbol:
      raise Exception("Expected to find symbol " + get_short_version_symbol +
          " corresponding to kudu::client::GetShortVersionString() but it was not found.")
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

if __name__ == "__main__":
  packages = ["avro", "binutils", "boost", "breakpad", "bzip2", "gcc", "gflags", "glog",
      "gperftools", "gtest", "kudu", "llvm", ("llvm", "3.8.0-asserts-p1"), "lz4",
      "openldap", "rapidjson", "re2", "snappy", "thrift", "zlib"]
  bootstrap(packages)
