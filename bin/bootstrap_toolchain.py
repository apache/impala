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
# The purpose of this script is to download prebuilt binaries and jar files to satisfy
# the third-party dependencies for Impala. The script expects bin/impala-config.sh to be
# sourced to initialize various environment variables (including environment variables
# specifying the versions for components). It verifies that bin/impala-config.sh
# has been sourced by verifying that IMPALA_HOME is set. This script will fail if an
# expected environment variable is not present.
#
# The following environment variables control the behavior of this script:
# IMPALA_TOOLCHAIN - Directory in which to place the artifacts. It can be useful to
#   override this to share a toolchain directory between multiple checkouts of Impala
#   or to use a cached copy to avoid downloading a new one.
# IMPALA_TOOLCHAIN_HOST - The host to use for downloading the artifacts
# CDH_COMPONENTS_HOME - Directory to store CDH Hadoop component artifacts
# CDP_COMPONENTS_HOME - Directory to store CDP Hadoop component artifacts
# CDH_BUILD_NUMBER - CDH Hadoop components are built with consistent versions so that
#   Hadoop, Hive, Kudu, etc are all built with versions that are compatible with each
#   other. The way to specify a single consistent set of components is via a build
#   number. This determines the location in s3 to get the artifacts.
# CDP_BUILD_NUMBER - The CDP equivalent of a CDH_BUILD_NUMBER.
# USE_CDP_HIVE - If false, this will use the CDH version of all Hadoop components
#   (except Ranger, which is CDP only). If true, this will use the CDP version of all
#   Hadoop components (except Sentry, which is CDH only).
# DOWNLOAD_CDH_COMPONENTS - When set to true, this script will also download and extract
#   the CDH/CDP Hadoop components (i.e. Hadoop, Hive, HBase, Sentry, Ranger, etc) into
#   CDH_COMPONENTS_HOME/CDP_COMPONENTS_HOME as appropriate.
# USE_CDH_KUDU - Kudu can be downloaded either from the toolchain or as a CDH component,
#   depending on the value of USE_CDH_KUDU.
# KUDU_IS_SUPPORTED - If KUDU_IS_SUPPORTED is false, Kudu is disabled and we download
#   the toolchain Kudu and use the symbols to compile a non-functional stub library so
#   that Impala has something to link against.
# IMPALA_<PACKAGE>_VERSION - The version expected for <PACKAGE>. This is typically
#   configured in bin/impala-config.sh and must exist for every package. This is used
#   to construct an appropriate URL and expected archive name.
# IMPALA_<PACKAGE>_URL - This overrides the download URL for <PACKAGE>. The URL must
#   end with the expected archive name for this package. This is usually used in
#   bin/impala-config-branch.sh or bin/impala-config-local.sh when using custom
#   versions or packages. When this is not specified, packages are downloaded from
#   an S3 bucket named native-toolchain, and the exact URL is based on
#   IMPALA_<PACKAGE>_VERSION as well as the OS version being built on.
#
# The script is directly executable, and it takes no parameters:
#     ./bootstrap_toolchain.py
# It should NOT be run via 'python bootstrap_toolchain.py', as it relies on a specific
# python environment.
import logging
import glob
import multiprocessing.pool
import os
import random
import re
# TODO: This file should be runnable without using impala-python, and system python
# does not have 'sh' available. Rework code to avoid importing sh (and anything else
# that gets in the way).
import sh
import shutil
import subprocess
import sys
import tempfile
import time

from collections import namedtuple
from string import Template

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
      time.sleep(10 + random.random() * 5)  # Sleep between 10 and 15 seconds.
  logging.info("Extracting {0}".format(file_name))
  sh.tar(z=True, x=True, f=os.path.join(destination, file_name), directory=destination)
  sh.rm(os.path.join(destination, file_name))


class DownloadUnpackTarball(object):
  """
  The basic unit of work for bootstrapping the toolchain is:
   - check if a package is already present (via the needs_download() method)
   - if it is not, download a tarball and unpack it into the appropriate directory
     (via the download() method)
  In this base case, everything is known: the url to download from, the archive to
  unpack, and the destination directory.
  """
  def __init__(self, url, archive_name, destination_basedir, directory_name, makedir):
    self.url = url
    self.archive_name = archive_name
    assert self.archive_name.endswith(".tar.gz")
    self.archive_basename = self.archive_name.replace(".tar.gz", "")
    self.destination_basedir = destination_basedir
    # destination base directory must exist
    assert os.path.isdir(self.destination_basedir)
    self.directory_name = directory_name
    self.makedir = makedir

  def pkg_directory(self):
    return os.path.join(self.destination_basedir, self.directory_name)

  def needs_download(self):
    if os.path.isdir(self.pkg_directory()): return False
    return True

  def download(self):
    unpack_dir = self.pkg_directory()
    if self.makedir:
      # Download and unpack in a temp directory, which we'll later move into place
      download_dir = tempfile.mkdtemp(dir=self.destination_basedir)
    else:
      download_dir = self.destination_basedir
    try:
      wget_and_unpack_package(self.url, self.archive_name, download_dir, False)
    except:  # noqa
      # Clean up any partially-unpacked result.
      if os.path.isdir(unpack_dir):
        shutil.rmtree(unpack_dir)
      if os.path.isdir(download_dir):
        shutil.rmtree(download_dir)
      raise
    if self.makedir:
      os.rename(download_dir, unpack_dir)


class TemplatedDownloadUnpackTarball(DownloadUnpackTarball):
  def __init__(self, url_tmpl, archive_name_tmpl, destination_basedir_tmpl,
               directory_name_tmpl, makedir, template_subs):
    url = self.__do_substitution(url_tmpl, template_subs)
    archive_name = self.__do_substitution(archive_name_tmpl, template_subs)
    destination_basedir = self.__do_substitution(destination_basedir_tmpl, template_subs)
    directory_name = self.__do_substitution(directory_name_tmpl, template_subs)
    super(TemplatedDownloadUnpackTarball, self).__init__(url, archive_name,
        destination_basedir, directory_name, makedir)

  def __do_substitution(self, template, template_subs):
    return Template(template).substitute(**template_subs)


class EnvVersionedPackage(TemplatedDownloadUnpackTarball):
  def __init__(self, name, url_prefix_tmpl, destination_basedir, explicit_version=None,
               archive_basename_tmpl=None, unpack_directory_tmpl=None, makedir=False,
               template_subs_in={}):
    template_subs = template_subs_in
    template_subs["name"] = name
    template_subs["version"] = self.__compute_version(name, explicit_version)
    # The common case is that X.tar.gz unpacks to X directory. archive_basename_tmpl
    # allows overriding the value of X (which defaults to ${name}-${version}).
    # If X.tar.gz unpacks to Y directory, then unpack_directory_tmpl allows overriding Y.
    if archive_basename_tmpl is None:
      archive_basename_tmpl = "${name}-${version}"
    archive_name_tmpl = archive_basename_tmpl + ".tar.gz"
    if unpack_directory_tmpl is None:
      unpack_directory_tmpl = archive_basename_tmpl
    url_tmpl = self.__compute_url(name, archive_name_tmpl, url_prefix_tmpl)
    super(EnvVersionedPackage, self).__init__(url_tmpl, archive_name_tmpl,
        destination_basedir, unpack_directory_tmpl, makedir, template_subs)

  def __compute_version(self, name, explicit_version):
    if explicit_version is not None:
      return explicit_version
    else:
      # When getting the version from the environment, we need to standardize the name
      # to match expected environment variables.
      std_env_name = name.replace("-", "_").upper()
      version_env_var = "IMPALA_{0}_VERSION".format(std_env_name)
      env_version = os.environ.get(version_env_var)
      if not env_version:
        raise Exception("Could not find version for {0} in environment var {1}".format(
          name, version_env_var))
      return env_version

  def __compute_url(self, name, archive_name_tmpl, url_prefix_tmpl):
    # The URL defined in the environment (IMPALA_*_URL) takes precedence. If that is
    # not defined, use the standard URL (url_prefix + archive_name)
    std_env_name = name.replace("-", "_").upper()
    url_env_var = "IMPALA_{0}_URL".format(std_env_name)
    url_tmpl = os.environ.get(url_env_var)
    if not url_tmpl:
      url_tmpl = os.path.join(url_prefix_tmpl, archive_name_tmpl)
    return url_tmpl


class ToolchainPackage(EnvVersionedPackage):
  def __init__(self, name, explicit_version=None, platform_release=None):
    toolchain_root = os.environ.get("IMPALA_TOOLCHAIN")
    if not toolchain_root:
      logging.error("Impala environment not set up correctly, make sure "
          "$IMPALA_TOOLCHAIN is set.")
      sys.exit(1)
    compiler = "gcc-{0}".format(os.environ["IMPALA_GCC_VERSION"])
    label = get_platform_release_label(release=platform_release).toolchain
    toolchain_build_id = os.environ["IMPALA_TOOLCHAIN_BUILD_ID"]
    toolchain_host = os.environ["IMPALA_TOOLCHAIN_HOST"]
    template_subs = {'compiler': compiler, 'label': label,
                     'toolchain_build_id': toolchain_build_id,
                     'toolchain_host': toolchain_host}
    archive_basename_tmpl = "${name}-${version}-${compiler}-${label}"
    url_prefix_tmpl = "https://${toolchain_host}/build/${toolchain_build_id}/" + \
        "${name}/${version}-${compiler}/"
    unpack_directory_tmpl = "${name}-${version}"
    super(ToolchainPackage, self).__init__(name, url_prefix_tmpl, toolchain_root,
                                           explicit_version=explicit_version,
                                           archive_basename_tmpl=archive_basename_tmpl,
                                           unpack_directory_tmpl=unpack_directory_tmpl,
                                           template_subs_in=template_subs)

  def needs_download(self):
    # If the directory doesn't exist, we need the download
    unpack_dir = self.pkg_directory()
    if not os.path.isdir(unpack_dir): return True
    version_file = os.path.join(unpack_dir, "toolchain_package_version.txt")
    if not os.path.exists(version_file): return True
    with open(version_file, "r") as f:
      return f.read().strip() != self.archive_basename

  def download(self):
    # Remove the existing package directory if it exists (since this has additional
    # conditions as part of needs_download())
    unpack_dir = self.pkg_directory()
    if os.path.exists(unpack_dir):
      logging.info("Removing existing package directory {0}".format(unpack_dir))
      shutil.rmtree(unpack_dir)
    super(ToolchainPackage, self).download()
    # Write the toolchain_package_version.txt file
    version_file = os.path.join(unpack_dir, "toolchain_package_version.txt")
    with open(version_file, "w") as f:
      f.write(self.archive_basename)


class CdhComponent(EnvVersionedPackage):
  def __init__(self, name, explicit_version=None, archive_basename_tmpl=None,
               unpack_directory_tmpl=None):
    # Compute the CDH base URL (based on the IMPALA_TOOLCHAIN_HOST and CDH_BUILD_NUMBER)
    if "IMPALA_TOOLCHAIN_HOST" not in os.environ or "CDH_BUILD_NUMBER" not in os.environ:
      logging.error("Impala environment not set up correctly, make sure "
                    "impala-config.sh is sourced.")
      sys.exit(1)
    template_subs = {"toolchain_host": os.environ["IMPALA_TOOLCHAIN_HOST"],
                     "cdh_build_number": os.environ["CDH_BUILD_NUMBER"]}
    url_prefix_tmpl = "https://${toolchain_host}/build/cdh_components/" + \
        "${cdh_build_number}/tarballs/"

    # Get the output base directory from CDH_COMPONENTS_HOME
    destination_basedir = os.environ["CDH_COMPONENTS_HOME"]
    super(CdhComponent, self).__init__(name, url_prefix_tmpl, destination_basedir,
                                       explicit_version=explicit_version,
                                       archive_basename_tmpl=archive_basename_tmpl,
                                       unpack_directory_tmpl=unpack_directory_tmpl,
                                       template_subs_in=template_subs)


class CdpComponent(EnvVersionedPackage):
  def __init__(self, name, explicit_version=None, archive_basename_tmpl=None,
               unpack_directory_tmpl=None, makedir=False):
    # Compute the CDH base URL (based on the IMPALA_TOOLCHAIN_HOST and CDP_BUILD_NUMBER)
    if "IMPALA_TOOLCHAIN_HOST" not in os.environ or "CDP_BUILD_NUMBER" not in os.environ:
      logging.error("Impala environment not set up correctly, make sure "
                    "impala-config.sh is sourced.")
      sys.exit(1)
    template_subs = {"toolchain_host": os.environ["IMPALA_TOOLCHAIN_HOST"],
                     "cdp_build_number": os.environ["CDP_BUILD_NUMBER"]}
    url_prefix_tmpl = "https://${toolchain_host}/build/cdp_components/" + \
        "${cdp_build_number}/tarballs/"

    # Get the output base directory from CDP_COMPONENTS_HOME
    destination_basedir = os.environ["CDP_COMPONENTS_HOME"]
    super(CdpComponent, self).__init__(name, url_prefix_tmpl, destination_basedir,
                                       explicit_version=explicit_version,
                                       archive_basename_tmpl=archive_basename_tmpl,
                                       unpack_directory_tmpl=unpack_directory_tmpl,
                                       makedir=makedir, template_subs_in=template_subs)


class ToolchainKudu(ToolchainPackage):
  def __init__(self, platform_label=None):
    super(ToolchainKudu, self).__init__('kudu', platform_release=platform_label)

  def needs_download(self):
    # This verifies that the unpack directory exists
    if super(ToolchainKudu, self).needs_download():
      return True
    # Additional check to distinguish this from the Kudu Java package
    # Regardless of the actual build type, the 'kudu' tarball will always contain a
    # 'debug' and a 'release' directory.
    if not os.path.exists(os.path.join(self.pkg_directory(), "debug")):
      return True
    # Both the pkg_directory and the debug directory exist
    return False


class CdhKudu(CdhComponent):
  def __init__(self, platform_label):
    kudu_archive_tmpl = "kudu-${version}-" + platform_label
    # IMPALA_KUDU_URL can contain '%(platform_label)', which needs to be replaced
    # with the platform. We override this in os.environ so that it is picked up
    # in EnvVersionedPackage.
    kudu_url = os.environ.get("IMPALA_KUDU_URL")
    if kudu_url:
       kudu_url = kudu_url.replace("%(platform_label)", platform_label)
       os.environ["IMPALA_KUDU_URL"] = kudu_url
    super(CdhKudu, self).__init__('kudu',
                                  archive_basename_tmpl=kudu_archive_tmpl,
                                  unpack_directory_tmpl="kudu-${version}")

  def needs_download(self):
    # This verifies that the unpack directory exists
    if super(CdhKudu, self).needs_download():
      return True
    # Additional check to distinguish this from the Kudu Java package
    # Regardless of the actual build type, the 'kudu' tarball will always contain a
    # 'debug' and a 'release' directory.
    if not os.path.exists(os.path.join(self.pkg_directory(), "debug")):
      return True
    # Both the pkg_directory and the debug directory exist
    return False


class CdhKuduJava(CdhComponent):
  def __init__(self):
    super(CdhKuduJava, self).__init__('kudu-java',
                                      archive_basename_tmpl="kudu-${version}")

  def needs_download(self):
    # This verify that the unpack directory exists
    if super(CdhKuduJava, self).needs_download():
      return True
    # Additional check to distinguish this from the Kudu package
    # There should be jars under the kudu directory.
    if len(glob.glob("{0}/*jar".format(self.pkg_directory()))) == 0:
      return True
    return False


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


def check_custom_toolchain(toolchain_root, packages):
  missing = []
  for p in packages:
    if not os.path.isdir(p.pkg_directory()):
      missing.append((p, p.pkg_directory()))

  if missing:
    msg = "The following packages are not in their expected locations.\n"
    for p, pkg_dir in missing:
      msg += "  %s (expected directory %s to exist)\n" % (p, pkg_dir)
    msg += "Pre-built toolchain archives not available for your platform.\n"
    msg += "Clone and build native toolchain from source using this repository:\n"
    msg += "    https://github.com/cloudera/native-toolchain\n"
    logging.error(msg)
    raise Exception("Toolchain bootstrap failed: required packages were missing")


def build_kudu_stub(kudu_dir, gcc_dir):
  """When Kudu isn't supported, the CentOS 7 Kudu package is downloaded from the
     toolchain. This replaces the client lib with a stubbed client. The
     'kudu_dir' specifies the location of the unpacked CentOS 7 Kudu package.
     The 'gcc_dir' specifies the location of the unpacked GCC/G++."""

  print "Building kudu stub"
  # Find the client lib files in the Kudu dir. There may be several files with
  # various extensions. Also there will be a debug version.
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
  binutils_dir = ToolchainPackage("binutils").pkg_directory()
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
    toolchain_root = os.environ.get("IMPALA_TOOLCHAIN")
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


def create_directory_from_env_var(env_var):
  dir_name = os.environ.get(env_var)
  if not dir_name:
    logging.error("Impala environment not set up correctly, make sure "
        "{0} is set.".format(env_var))
    sys.exit(1)
  if not os.path.exists(dir_name):
    os.makedirs(dir_name)


def get_toolchain_downloads():
  toolchain_packages = []
  # The LLVM and GCC packages are the largest packages in the toolchain (Kudu is handled
  # separately). Sort them first so their downloads start as soon as possible.
  llvm_package = ToolchainPackage("llvm")
  llvm_package_asserts = ToolchainPackage("llvm", explicit_version="5.0.1-asserts-p1")
  gcc_package = ToolchainPackage("gcc")
  toolchain_packages += [llvm_package, llvm_package_asserts, gcc_package]
  toolchain_packages += map(ToolchainPackage,
      ["avro", "binutils", "boost", "breakpad", "bzip2", "cctz", "cmake", "crcutil",
       "flatbuffers", "gdb", "gflags", "glog", "gperftools", "gtest", "libev",
       "libunwind", "lz4", "openldap", "openssl", "orc", "protobuf",
       "rapidjson", "re2", "snappy", "thrift", "tpc-h", "tpc-ds", "zlib", "zstd"])
  toolchain_packages += [ToolchainPackage("thrift",
      explicit_version=os.environ.get("IMPALA_THRIFT11_VERSION"))]
  # Check whether this platform is supported (or whether a valid custom toolchain
  # has been provided).
  if not try_get_platform_release_label() \
     or not try_get_platform_release_label().toolchain:
    toolchain_root = os.environ.get("IMPALA_TOOLCHAIN")
    # This would throw an exception if the custom toolchain were not valid
    check_custom_toolchain(toolchain_root, toolchain_packages)
    # Nothing to download
    return []
  return toolchain_packages


def get_hadoop_downloads():
  cluster_components = []
  use_cdp_hive = os.environ["USE_CDP_HIVE"] == "true"
  if use_cdp_hive:
    hadoop = CdpComponent("hadoop")
    hbase = CdpComponent("hbase", archive_basename_tmpl="hbase-${version}-bin",
                         unpack_directory_tmpl="hbase-${version}")
    hive = CdpComponent("hive", archive_basename_tmpl="apache-hive-${version}-bin")
    hive_src = CdpComponent("hive-source",
                            explicit_version=os.environ.get("IMPALA_HIVE_VERSION"),
                            archive_basename_tmpl="hive-${version}-source",
                            unpack_directory_tmpl="hive-${version}")
    tez = CdpComponent("tez", archive_basename_tmpl="tez-${version}-minimal",
                       makedir=True)
    cluster_components.extend([hadoop, hbase, hive, hive_src, tez])
  else:
    cluster_components.extend(map(CdhComponent, ["hadoop", "hbase", "hive"]))
  # Sentry is always CDH
  cluster_components.append(CdhComponent("sentry"))
  # Ranger is always CDP
  cluster_components.append(CdpComponent("ranger",
                                         archive_basename_tmpl="ranger-${version}-admin"))

  # llama-minikdc is used for testing and not something that Impala needs to be built
  # against. It does not get updated very frequently unlike the other CDH components.
  # It is stored in a special location compared to other components.
  toolchain_host = os.environ["IMPALA_TOOLCHAIN_HOST"]
  download_path_prefix = "https://{0}/build/cdh_components/".format(toolchain_host)
  destination_basedir = os.environ["CDH_COMPONENTS_HOME"]
  cluster_components += [EnvVersionedPackage("llama-minikdc", download_path_prefix,
      destination_basedir)]
  return cluster_components


def get_kudu_downloads(use_kudu_stub):
  # If Kudu is not supported, we download centos7 kudu to build the kudu stub.
  # TODO: Should this be from toolchain or CDH? Does it matter?
  kudu_downloads = []
  if use_kudu_stub:
    kudu_downloads += [ToolchainKudu("centos7")]
  else:
    use_cdh_kudu = os.getenv("USE_CDH_KUDU") == "true"
    if use_cdh_kudu:
      if not try_get_platform_release_label() \
         or not try_get_platform_release_label().cdh:
        logging.error("CDH Kudu is not supported on this platform. Set "
                      "USE_CDH_KUDU=false to use the toolchain Kudu.")
        sys.exit(1)
      kudu_downloads += [CdhKudu(get_platform_release_label().cdh)]
    else:
      kudu_downloads += [ToolchainKudu()]

  # Independent of the regular Kudu package, there is also a Kudu Java package. This
  # always needs to be downloaded from the CDH components, because the toolchain
  # does not produce the Java artifacts.
  # TODO: Does this make any sense with the Kudu stub?
  kudu_downloads += [CdhKuduJava()]
  return kudu_downloads


def main():
  """Validates that bin/impala-config.sh has been sourced by verifying that $IMPALA_HOME
  and $IMPALA_TOOLCHAIN are in the environment. We assume that if these are set, then
  IMPALA_<PACKAGE>_VERSION environment variables are also set. This will create the
  directory specified by $IMPALA_TOOLCHAIN if it does not already exist. Then, it will
  compute what packages need to be downloaded. Packages are only downloaded if they are
  not already present in $IMPALA_TOOLCHAIN. There are two main categories of packages.
  Toolchain packages are native packages built using the native toolchain. These are
  always downloaded. Hadoop component packages are the CDH or CDP builds of Hadoop
  components such as Hadoop, Hive, HBase, etc. Hadoop component packages are organized
  as a consistent set of compatible version via a build number (i.e. CDH_BUILD_NUMBER
  and CDP_BUILD_NUMBER). Hadoop component packages are only downloaded if
  $DOWNLOAD_CDH_COMPONENTS is true. CDH Hadoop packages are downloaded into
  $CDH_COMPONENTS_HOME. CDP Hadoop packages are downloaded into $CDP_COMPONENTS_HOME.
  The versions used for Hadoop components depend on whether USE_CDP_HIVE is true or
  false. If true, most components get the CDP versions based on the $CDP_BUILD_NUMBER.
  If false, most components get the CDH versions based on the $CDH_BUILD_NUMBER.
  The exceptions are:
  - sentry (always downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDH_BUILD_NUMBER)
  - llama-minikdc (always downloaded from $TOOLCHAIN_HOST)
  - ranger (always downloaded from $IMPALA_TOOLCHAIN_HOST for a given $CDP_BUILD_NUMBER)
  - kudu (currently always downloaded from $IMPALA_TOOLCHAIN_HOST for a given
    $CDH_BUILD_NUMBER)
  If Kudu is not supported on this platform (or KUDU_IS_SUPPORTED=false), then this
  builds a Kudu stub to allow for compilation without Kudu support.
  """
  logging.basicConfig(level=logging.INFO,
      format='%(asctime)s %(threadName)s %(levelname)s: %(message)s')
  # 'sh' module logs at every execution, which is too noisy
  logging.getLogger("sh").setLevel(logging.WARNING)

  if not os.environ.get("IMPALA_HOME"):
    logging.error("Impala environment not set up correctly, make sure "
          "impala-config.sh is sourced.")
    sys.exit(1)

  # Create the toolchain directory if necessary
  create_directory_from_env_var("IMPALA_TOOLCHAIN")

  use_kudu_stub = os.environ["KUDU_IS_SUPPORTED"] != "true"

  downloads = []
  downloads += get_toolchain_downloads()
  kudu_download = None
  if os.getenv("DOWNLOAD_CDH_COMPONENTS", "false") == "true":
    create_directory_from_env_var("CDH_COMPONENTS_HOME")
    create_directory_from_env_var("CDP_COMPONENTS_HOME")
    downloads += get_kudu_downloads(use_kudu_stub)
    downloads += get_hadoop_downloads()

  components_needing_download = [d for d in downloads if d.needs_download()]

  def download(component):
    component.download()

  execute_many(download, components_needing_download)

  if use_kudu_stub:
    # Find the kudu package directory and the gcc package directory
    kudu_download = [d for d in downloads if d.name == 'kudu'][0]
    gcc_download = [d for d in downloads if d.name == 'gcc'][0]
    build_kudu_stub(kudu_download.pkg_directory(), gcc_download.pkg_directory())


if __name__ == "__main__": main()
