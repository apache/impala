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
# to. If DOWNLOAD_CDH_COMPONENTS is set to true, this script will also download and
# extract the CDH components (i.e. Hadoop, Hive, HBase and Sentry) into
# CDH_COMPONENTS_HOME.
#
# Kudu can be downloaded either from the toolchain or as a CDH component, depending on the
# value of USE_CDH_KUDU. If KUDU_IS_SUPPORTED is false, we download the toolchain Kudu and
# use the symbols to compile a non-functional stub library so that Impala has something to
# link against.
#
# By default, packages are downloaded from an S3 bucket named native-toolchain.
# The exact URL is based on IMPALA_<PACKAGE>_VERSION environment variables
# (configured in impala-config.sh) as well as the OS version being built on.
# The URL can be overridden with an IMPALA_<PACKAGE>_URL environment variable
# set in impala-config-{local,branch}.sh.
#
# The script is called as follows without any additional parameters:
#
#     python bootstrap_toolchain.py
import logging
import multiprocessing.pool
import os
import random
import re
import sh
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

from collections import namedtuple

TOOLCHAIN_HOST = "https://native-toolchain.s3.amazonaws.com/build"

# Maps return values from 'lsb_release -irs' to the corresponding OS labels for both the
# toolchain and the CDH components.
OsMapping = namedtuple('OsMapping', ['lsb_release', 'toolchain', 'cdh'])
OS_MAPPING = [
  OsMapping("centos5", "ec2-package-centos-5", None),
  OsMapping("centos6", "ec2-package-centos-6", "redhat6"),
  OsMapping("centos7", "ec2-package-centos-7", "redhat7"),
  OsMapping("redhatenterpriseserver5", "ec2-package-centos-5", None),
  OsMapping("redhatenterpriseserver6", "ec2-package-centos-6", "redhat6"),
  OsMapping("redhatenterpriseserver7", "ec2-package-centos-7", "redhat7"),
  OsMapping("debian6", "ec2-package-debian-6", None),
  OsMapping("debian7", "ec2-package-debian-7", None),
  OsMapping("debian8", "ec2-package-debian-8", "debian8"),
  OsMapping("suselinux11", "ec2-package-sles-11", None),
  OsMapping("suselinux12", "ec2-package-sles-12", "sles12"),
  OsMapping("suse12.2", "ec2-package-sles-12", "sles12"),
  OsMapping("suse12.3", "ec2-package-sles-12", "sles12"),
  OsMapping("ubuntu12.04", "ec2-package-ubuntu-12-04", None),
  OsMapping("ubuntu14.04", "ec2-package-ubuntu-14-04", None),
  OsMapping("ubuntu15.04", "ec2-package-ubuntu-14-04", None),
  OsMapping("ubuntu15.10", "ec2-package-ubuntu-14-04", None),
  OsMapping('ubuntu16.04', "ec2-package-ubuntu-16-04", "ubuntu1604"),
  OsMapping('ubuntu18.04', "ec2-package-ubuntu-18-04", "ubuntu1804")
]

class Package(object):
  """
  Represents a package to be downloaded. A version, if not specified
  explicitly, is retrieved from the environment variable IMPALA_<NAME>_VERSION.
  URLs are retrieved from IMPALA_<NAME>_URL, but are optional.
  """
  def __init__(self, name, version=None, url=None):
    self.name = name
    self.version = version
    self.url = url
    package_env_name = name.replace("-", "_").upper()
    if self.version is None:
      version_env_var = "IMPALA_{0}_VERSION".format(package_env_name)

      self.version = os.environ.get(version_env_var)
      if not self.version:
        raise Exception("Could not find version for {0} in environment var {1}".format(
          name, version_env_var))
    if self.url is None:
      url_env_var = "IMPALA_{0}_URL".format(package_env_name)
      self.url = os.environ.get(url_env_var)


class CdpComponent(object):
  def __init__(self, basename, makedir=False, pkg_directory=None):
    """
    basename: the name of the file to be downloaded, without its .tar.gz suffix
    makedir: if false, it is assumed that the downloaded tarball will expand
             into a directory with the same name as 'basename'. If True, we
             assume that the tarball doesn't have any top-level directory,
             and so we need to manually create a directory within which to
             expand the tarball.
    """
    self.basename = basename
    self.makedir = makedir
    cdp_components_home = os.environ.get("CDP_COMPONENTS_HOME")
    if cdp_components_home is None:
      raise Exception("CDP_COMPONENTS_HOME is not set. Cannot determine the "
                      "component package directory")
    self.pkg_directory = "{0}/{1}".format(cdp_components_home,
                            pkg_directory if pkg_directory else basename)


def try_get_platform_release_label():
  """Gets the right package label from the OS version. Returns an OsMapping with both
     'toolchain' and 'cdh' labels. Return None if not found.
  """
  try:
    return get_platform_release_label()
  except Exception:
    return None


# Cache "lsb_release -irs" to avoid excessive logging from sh, and
# to shave a little bit of time.
lsb_release_cache = None


def get_platform_release_label(release=None):
  """Gets the right package label from the OS version. Raise exception if not found.
     'release' can be provided to override the underlying OS version.
  """
  global lsb_release_cache
  if not release:
    if lsb_release_cache:
      release = lsb_release_cache
    else:
      release = "".join(map(lambda x: x.lower(), sh.lsb_release("-irs").split()))
      # Only need to check against the major release if RHEL or CentOS
      for platform in ['centos', 'redhatenterpriseserver']:
        if platform in release:
          release = release.split('.')[0]
          break
      lsb_release_cache = release
  for mapping in OS_MAPPING:
    if re.search(mapping.lsb_release, release):
      return mapping

  raise Exception("Could not find package label for OS version: {0}.".format(release))

def wget_and_unpack_package(download_path, file_name, destination, wget_no_clobber):
  if not download_path.endswith("/" + file_name):
    raise Exception("URL {0} does not match with expected file_name {1}"
        .format(download_path, file_name))
  NUM_ATTEMPTS = 3
  for attempt in range(1, NUM_ATTEMPTS + 1):
    logging.info("Downloading {0} to {1}/{2} (attempt {3})".format(
      download_path, destination, file_name, attempt))
    # --no-clobber avoids downloading the file if a file with the name already exists
    try:
      sh.wget(download_path, directory_prefix=destination, no_clobber=wget_no_clobber)
      break
    except Exception, e:
      if attempt == NUM_ATTEMPTS:
        raise
      logging.error("Download failed; retrying after sleep: " + str(e))
      time.sleep(10 + random.random() * 5) # Sleep between 10 and 15 seconds.
  logging.info("Extracting {0}".format(file_name))
  sh.tar(z=True, x=True, f=os.path.join(destination, file_name), directory=destination)
  sh.rm(os.path.join(destination, file_name))

def download_package(destination, package, compiler, platform_release=None):
  remove_existing_package(destination, package.name, package.version)

  toolchain_build_id = os.environ["IMPALA_TOOLCHAIN_BUILD_ID"]
  label = get_platform_release_label(release=platform_release).toolchain
  format_params = {'product': package.name, 'version': package.version,
      'compiler': compiler, 'label': label, 'toolchain_build_id': toolchain_build_id}
  file_name = "{product}-{version}-{compiler}-{label}.tar.gz".format(**format_params)
  format_params['file_name'] = file_name
  if package.url is None:
    url_path = "/{toolchain_build_id}/{product}/{version}-{compiler}/{file_name}".format(
        **format_params)
    download_path = TOOLCHAIN_HOST + url_path
  else:
    download_path = package.url

  wget_and_unpack_package(download_path, file_name, destination, True)

def bootstrap(toolchain_root, packages):
  """Downloads and unpacks each package in the list `packages` into `toolchain_root` if it
  doesn't exist already.
  """
  if not try_get_platform_release_label() \
     or not try_get_platform_release_label().toolchain:
    check_custom_toolchain(toolchain_root, packages)
    return

  # Detect the compiler
  compiler = "gcc-{0}".format(os.environ["IMPALA_GCC_VERSION"])

  def handle_package(p):
    if check_for_existing_package(toolchain_root, p.name, p.version, compiler):
      return
    if p.name != "kudu" or os.environ["KUDU_IS_SUPPORTED"] == "true":
      download_package(toolchain_root, p, compiler)
    else:
      build_kudu_stub(toolchain_root, p.version, compiler)
    write_version_file(toolchain_root, p.name, p.version, compiler,
        get_platform_release_label().toolchain)
  execute_many(handle_package, packages)

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
    pkg_dir = package_directory(toolchain_root, p.name, p.version)
    if not os.path.isdir(pkg_dir):
      missing.append((p, pkg_dir))

  if missing:
    msg = "The following packages are not in their expected locations.\n"
    for p, pkg_dir in missing:
      msg += "  %s (expected directory %s to exist)\n" % (p, pkg_dir)
    msg += "Pre-built toolchain archives not available for your platform.\n"
    msg += "Clone and build native toolchain from source using this repository:\n"
    msg += "    https://github.com/cloudera/native-toolchain\n"
    logging.error(msg)
    raise Exception("Toolchain bootstrap failed: required packages were missing")

def check_for_existing_package(toolchain_root, pkg_name, pkg_version, compiler):
  """Return true if toolchain_root already contains the package with the correct
  version and compiler.
  """
  version_file = version_file_path(toolchain_root, pkg_name, pkg_version)
  if not os.path.exists(version_file):
    return False

  label = get_platform_release_label().toolchain
  pkg_version_string = "{0}-{1}-{2}-{3}".format(pkg_name, pkg_version, compiler, label)
  with open(version_file) as f:
    return f.read().strip() == pkg_version_string

def write_version_file(toolchain_root, pkg_name, pkg_version, compiler, label):
  with open(version_file_path(toolchain_root, pkg_name, pkg_version), 'w') as f:
    f.write("{0}-{1}-{2}-{3}".format(pkg_name, pkg_version, compiler, label))

def remove_existing_package(toolchain_root, pkg_name, pkg_version):
  dir_path = package_directory(toolchain_root, pkg_name, pkg_version)
  if os.path.exists(dir_path):
    logging.info("Removing existing package directory {0}".format(dir_path))
    shutil.rmtree(dir_path)

def build_kudu_stub(toolchain_root, kudu_version, compiler):
  # When Kudu isn't supported, the CentOS 7 package will be downloaded and the client
  # lib will be replaced with a stubbed client.
  download_package(toolchain_root, Package("kudu", kudu_version), compiler,
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
    gpp = os.path.join(
        toolchain_root, "gcc-%s" % os.environ.get("IMPALA_GCC_VERSION"), "bin", "g++")
    subprocess.check_call([gpp, stub_client_src_file.name, "-shared", "-fPIC",
        "-Wl,-soname,%s" % so_name, "-o", stub_client_lib_path])

    # Replace the real libs with the stub.
    for client_lib_path in client_lib_paths:
      shutil.copyfile(stub_client_lib_path, client_lib_path)
  finally:
    shutil.rmtree(stub_build_dir)

def execute_many(f, args):
  """
  Executes f(a) for a in args using a threadpool to execute in parallel.
  The pool uses the smaller of 4 and the number of CPUs in the system
  as the pool size.
  """
  pool = multiprocessing.pool.ThreadPool(processes=min(multiprocessing.cpu_count(), 4))
  return pool.map(f, args, 1)

def download_cdh_components(toolchain_root, cdh_components, url_prefix):
  """Downloads and unpacks the CDH components for a given URL prefix into
  $CDH_COMPONENTS_HOME if not found."""
  cdh_components_home = os.environ.get("CDH_COMPONENTS_HOME")
  if not cdh_components_home:
    logging.error("Impala environment not set up correctly, make sure "
          "$CDH_COMPONENTS_HOME is set.")
    sys.exit(1)

  # Create the directory where CDH components live if necessary.
  if not os.path.exists(cdh_components_home):
    os.makedirs(cdh_components_home)

  def download(component):
    try:
      pkg_directory = package_directory(cdh_components_home, component.name,
          component.version)
      if os.path.isdir(pkg_directory):
        return

      platform_label = ""
      # Kudu is the only component that's platform dependent.
      if component.name == "kudu":
        platform_label = "-%s" % get_platform_release_label().cdh
      # Download the package if it doesn't exist
      file_name = "{0}-{1}{2}.tar.gz".format(
          component.name, component.version, platform_label)

      if component.url is None:
        download_path = url_prefix + file_name
      else:
        download_path = component.url
        if "%(platform_label)" in component.url:
          download_path = \
              download_path.replace("%(platform_label)", get_platform_release_label().cdh)
      wget_and_unpack_package(download_path, file_name, cdh_components_home, False)
    except Exception:
      # IMPALA-8517: print backtrace to help debug flaky failures.
      traceback.print_exc()
      raise

  execute_many(download, cdh_components)


def download_cdp_components(cdp_components, url_prefix):
  """
  Downloads and unpacks the CDP components for a given URL prefix into
  $CDP_COMPONENTS_HOME if not found.

  cdp_components: list of CdpComponent instances
  """
  cdp_components_home = os.environ.get("CDP_COMPONENTS_HOME")
  if not cdp_components_home:
    logging.error("Impala environment not set up correctly, make sure "
                  "$CDP_COMPONENTS_HOME is set.")
    sys.exit(1)

  # Create the directory where CDP components live if necessary.
  if not os.path.exists(cdp_components_home):
    os.makedirs(cdp_components_home)

  def download(component):

    if os.path.isdir(component.pkg_directory): return
    file_name = "{0}.tar.gz".format(component.basename)
    download_path = "{0}/{1}".format(url_prefix, file_name)
    dst = cdp_components_home
    if component.makedir:
      # Download and unpack in a temp directory, which we'll later move into place
      dst = tempfile.mkdtemp(dir=cdp_components_home)
    try:
      wget_and_unpack_package(download_path, file_name, dst, False)
    except:  # noqa
      # Clean up any partially-unpacked result.
      if os.path.isdir(component.pkg_directory):
        shutil.rmtree(component.pkg_directory)
      # Clean up any temp directory if we made one
      if component.makedir:
        shutil.rmtree(dst)
      raise
    if component.makedir:
      os.rename(dst, component.pkg_directory)

  execute_many(download, cdp_components)


if __name__ == "__main__":
  """Validates the presence of $IMPALA_HOME and $IMPALA_TOOLCHAIN in the environment.-
  By checking $IMPALA_HOME is set, we assume that IMPALA_{LIB}_VERSION will be set
  as well. Will create the directory specified by $IMPALA_TOOLCHAIN if it doesn't exist
  yet. Each of the packages specified in `packages` is downloaded and extracted into
  $IMPALA_TOOLCHAIN. If $DOWNLOAD_CDH_COMPONENTS is true, the presence of
  $IMPALA_TOOLCHAIN_HOST and $CDH_BUILD_NUMBER will be checked and this function will also
  download the following CDH/CDP components into the directory specified by
  $CDH_COMPONENTS_HOME/$CDP_COMPONENTS_HOME.
  - hadoop (downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDH_BUILD_NUMBER)
  - hbase (downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDH_BUILD_NUMBER)
  - hive (downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDH_BUILD_NUMBER)
  - sentry (downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDH_BUILD_NUMBER)
  - llama-minikdc (downloaded from $TOOLCHAIN_HOST)
  - ranger (downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDP_BUILD_NUMBER)
  - hive3 (downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDP_BUILD_NUMBER)
  """
  logging.basicConfig(level=logging.INFO,
      format='%(asctime)s %(threadName)s %(levelname)s: %(message)s')
  # 'sh' module logs at every execution, which is too noisy
  logging.getLogger("sh").setLevel(logging.WARNING)

  if not os.environ.get("IMPALA_HOME"):
    logging.error("Impala environment not set up correctly, make sure "
          "impala-config.sh is sourced.")
    sys.exit(1)

  # Create the destination directory if necessary
  toolchain_root = os.environ.get("IMPALA_TOOLCHAIN")
  if not toolchain_root:
    logging.error("Impala environment not set up correctly, make sure "
          "$IMPALA_TOOLCHAIN is set.")
    sys.exit(1)

  if not os.path.exists(toolchain_root):
    os.makedirs(toolchain_root)

  use_cdh_kudu = os.getenv("USE_CDH_KUDU") == "true"
  if os.environ["KUDU_IS_SUPPORTED"] != "true":
    # We need gcc to build the Kudu stub, so download it first, and we also
    # need the toolchain Kudu.
    bootstrap(toolchain_root, [Package("gcc")])
    use_cdh_kudu = False

  # LLVM and Kudu are the largest packages. Sort them first so that
  # their download starts as soon as possible.
  packages = []
  if not use_cdh_kudu: packages += [Package("kudu")]
  packages += map(Package, ["llvm",
      "avro", "binutils", "boost", "breakpad", "bzip2", "cctz", "cmake", "crcutil",
      "flatbuffers", "gcc", "gdb", "gflags", "glog", "gperftools", "gtest", "libev",
      "libunwind", "lz4", "openldap", "openssl", "orc", "protobuf",
      "rapidjson", "re2", "snappy", "thrift", "tpc-h", "tpc-ds", "zlib"])
  packages.insert(0, Package("llvm", "5.0.1-asserts-p1"))
  packages.insert(0, Package("thrift", os.environ.get("IMPALA_THRIFT11_VERSION")))
  bootstrap(toolchain_root, packages)

  # Download the CDH components if necessary.
  if not os.getenv("DOWNLOAD_CDH_COMPONENTS", "false") == "true": sys.exit(0)

  if "IMPALA_TOOLCHAIN_HOST" not in os.environ or "CDH_BUILD_NUMBER" not in os.environ:
    logging.error("Impala environment not set up correctly, make sure "
                  "impala-config.sh is sourced.")
    sys.exit(1)

  toolchain_host = os.environ["IMPALA_TOOLCHAIN_HOST"]
  cdh_build_number = os.environ["CDH_BUILD_NUMBER"]

  cdh_components = map(Package, ["hadoop", "hbase", "sentry"])
  use_cdp_hive = os.getenv("USE_CDP_HIVE") == "true"
  if not use_cdp_hive:
    cdh_components += [Package("hive")]

  if use_cdh_kudu:
    if not try_get_platform_release_label() or not try_get_platform_release_label().cdh:
      logging.error("CDH Kudu is not supported on this platform. Set USE_CDH_KUDU=false "
                    "to use the toolchain Kudu.")
      sys.exit(1)
    cdh_components += [Package("kudu")]
  download_path_prefix = \
      "https://{0}/build/cdh_components/{1}/tarballs/".format(toolchain_host,
                                                              cdh_build_number)
  download_cdh_components(toolchain_root, cdh_components, download_path_prefix)


  # llama-minikdc is used for testing and not something that Impala needs to be built
  # against. It does not get updated very frequently unlike the other CDH components.
  cdh_components = [Package("llama-minikdc")]
  download_path_prefix = "{0}/cdh_components/".format(TOOLCHAIN_HOST)
  download_cdh_components(toolchain_root, cdh_components, download_path_prefix)

  cdp_build_number = os.environ["CDP_BUILD_NUMBER"]
  cdp_components = [
    CdpComponent("ranger-{0}-admin".format(os.environ.get("IMPALA_RANGER_VERSION"))),
  ]
  use_cdp_hive = os.getenv("USE_CDP_HIVE") == "true"
  if use_cdp_hive:
    hive_version = os.environ.get("IMPALA_HIVE_VERSION")
    cdp_components.append(CdpComponent("hive-{0}-source".format(hive_version),
                          pkg_directory="hive-{0}".format(hive_version))),
    cdp_components.append(CdpComponent("apache-hive-{0}-bin".format(hive_version))),
    cdp_components.append(CdpComponent(
        "tez-{0}-minimal".format(os.environ.get("IMPALA_TEZ_VERSION")),
        makedir=True))
  download_path_prefix = \
    "https://{0}/build/cdp_components/{1}/tarballs".format(toolchain_host,
                                                           cdp_build_number)
  download_cdp_components(cdp_components, download_path_prefix)
