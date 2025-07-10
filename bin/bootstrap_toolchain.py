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
#
# The purpose of this script is to download prebuilt binaries and jar files to satisfy
# the third-party dependencies for Impala. The script expects bin/impala-config.sh to be
# sourced to initialize various environment variables (including environment variables
# specifying the versions for components). It verifies that bin/impala-config.sh
# has been sourced by verifying that IMPALA_HOME is set. This script will fail if an
# expected environment variable is not present.
#
# To share a toolchain directory between multiple checkouts of Impala (or to use a
# cached copy to avoid downloading a new one), it is best to override IMPALA_TOOLCHAIN
# in bin/impala-config-local.sh or in the environment prior to sourcing
# bin/impala-config.sh. This sets IMPALA_TOOLCHAIN_PACKAGES_HOME as well as
# CDP_COMPONENTS_HOME.
#
# The following environment variables control the behavior of this script:
# IMPALA_TOOLCHAIN_PACKAGES_HOME - Directory in which to place the native-toolchain
#   packages.
# IMPALA_TOOLCHAIN_HOST - The host to use for downloading the artifacts
# CDP_COMPONENTS_HOME - Directory to store CDP Hadoop component artifacts
# CDP_BUILD_NUMBER - CDP Hadoop components are built with consistent versions so that
#   Hadoop, Hive, Kudu, etc are all built with versions that are compatible with each
#   other. The way to specify a single consistent set of components is via a build
#   number. This determines the location in s3 to get the artifacts.
# DOWNLOAD_CDH_COMPONENTS - When set to true, this script will also download and extract
#   the CDP Hadoop components (i.e. Hadoop, Hive, HBase, Ranger, Ozone, etc) into
#   CDP_COMPONENTS_HOME as appropriate.
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

from __future__ import absolute_import, division, print_function
import logging
import multiprocessing.pool
import os
import platform
import random
import shutil
import subprocess
import sys
import tempfile
import time

from collections import namedtuple
from string import Template

# Maps the ID + major version of the VERSION_ID from /etc/os-release to the corresponding
# OS label for the toolchain. See https://github.com/chef/os_release for a database of
# /etc/os-release files.
OsMapping = namedtuple('OsMapping', ['release', 'toolchain'])
OS_MAPPING = [
  OsMapping("rhel7", "ec2-package-centos-7"),
  OsMapping("centos7", "ec2-package-centos-7"),
  OsMapping("rhel8", "ec2-package-centos-8"),
  OsMapping("centos8", "ec2-package-centos-8"),
  OsMapping("rocky8", "ec2-package-centos-8"),
  OsMapping("almalinux8", "ec2-package-centos-8"),
  OsMapping("rhel9", "ec2-package-rocky-9"),
  OsMapping("rocky9", "ec2-package-rocky-9"),
  OsMapping("almalinux9", "ec2-package-rocky-9"),
  OsMapping("sles12", "ec2-package-sles-12"),
  OsMapping("sles15", "ec2-package-sles-15"),
  OsMapping('ubuntu16', "ec2-package-ubuntu-16-04"),
  OsMapping('ubuntu18', "ec2-package-ubuntu-18-04"),
  OsMapping('ubuntu20', "ec2-package-ubuntu-20-04"),
  OsMapping('ubuntu22', "ec2-package-ubuntu-22-04")
]


def get_toolchain_compiler():
  """Return the <name>-<version> string for the compiler package to use for the
  toolchain."""
  # Currently we always use GCC.
  return "gcc-{0}".format(os.environ["IMPALA_GCC_VERSION"])


def wget_and_unpack_package(download_path, file_name, destination, wget_no_clobber):
  if not download_path.endswith("/" + file_name):
    raise Exception("URL {0} does not match with expected file_name {1}"
        .format(download_path, file_name))
  if "closer.cgi" in download_path:
    download_path += "?action=download"
  NUM_ATTEMPTS = 3
  for attempt in range(1, NUM_ATTEMPTS + 1):
    logging.info("Downloading {0} to {1}/{2} (attempt {3})".format(
      download_path, destination, file_name, attempt))
    # --no-clobber avoids downloading the file if a file with the name already exists
    try:
      cmd = ["wget", "-q", download_path,
             "--output-document={0}/{1}".format(destination, file_name)]
      if wget_no_clobber:
        cmd.append("--no-clobber")
      subprocess.check_call(cmd)
      break
    except subprocess.CalledProcessError as e:
      if attempt == NUM_ATTEMPTS:
        raise
      logging.error("Download failed; retrying after sleep: " + str(e))
      time.sleep(10 + random.random() * 5)  # Sleep between 10 and 15 seconds.
  logging.info("Extracting {0}".format(file_name))
  subprocess.check_call(["tar", "xzf", os.path.join(destination, file_name),
                         "--directory={0}".format(destination)])
  os.unlink(os.path.join(destination, file_name))


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
      if self.url:
        wget_and_unpack_package(self.url, self.archive_name, download_dir, False)
    except:  # noqa
      # Clean up any partially-unpacked result.
      if os.path.isdir(unpack_dir):
        shutil.rmtree(unpack_dir)
      # Only delete the download directory if it is a temporary directory
      if download_dir != self.destination_basedir and os.path.isdir(download_dir):
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
               template_subs_in={}, target_comp=None):
    template_subs = template_subs_in
    template_subs["name"] = name
    template_subs["version"] = self.__compute_version(name, explicit_version,
        target_comp)
    # The common case is that X.tar.gz unpacks to X directory. archive_basename_tmpl
    # allows overriding the value of X (which defaults to ${name}-${version}).
    # If X.tar.gz unpacks to Y directory, then unpack_directory_tmpl allows overriding Y.
    if archive_basename_tmpl is None:
      archive_basename_tmpl = "${name}-${version}"
    archive_name_tmpl = archive_basename_tmpl + ".tar.gz"
    if unpack_directory_tmpl is None:
      unpack_directory_tmpl = archive_basename_tmpl
    url_tmpl = self.__compute_url(name, archive_name_tmpl, url_prefix_tmpl, target_comp)
    super(EnvVersionedPackage, self).__init__(url_tmpl, archive_name_tmpl,
        destination_basedir, unpack_directory_tmpl, makedir, template_subs)

  def __compute_version(self, name, explicit_version, target_comp=None):
    if explicit_version is not None:
      return explicit_version
    else:
      # When getting the version from the environment, we need to standardize the name
      # to match expected environment variables.
      std_env_name = name.replace("-", "_").upper()
      if target_comp:
        std_env_name += '_' + target_comp.upper()
      version_env_var = "IMPALA_{0}_VERSION".format(std_env_name)
      env_version = os.environ.get(version_env_var)
      if not env_version:
        raise Exception("Could not find version for {0} in environment var {1}".format(
          name, version_env_var))
      return env_version

  def __compute_url(self, name, archive_name_tmpl, url_prefix_tmpl, target_comp=None):
    # The URL defined in the environment (IMPALA_*_URL) takes precedence. If that is
    # not defined, use the standard URL (url_prefix + archive_name)
    std_env_name = name.replace("-", "_").upper()
    if target_comp:
      std_env_name += '_' + target_comp.upper()
    url_env_var = "IMPALA_{0}_URL".format(std_env_name)
    url_tmpl = os.environ.get(url_env_var)
    if not url_tmpl:
      url_tmpl = os.path.join(url_prefix_tmpl, archive_name_tmpl)
    return url_tmpl


class ToolchainPackage(EnvVersionedPackage):
  def __init__(self, name, explicit_version=None, platform_release=None):
    toolchain_packages_home = os.environ.get("IMPALA_TOOLCHAIN_PACKAGES_HOME")
    if not toolchain_packages_home:
      logging.error("Impala environment not set up correctly, make sure "
          "$IMPALA_TOOLCHAIN_PACKAGES_HOME is set.")
      sys.exit(1)
    target_comp = None
    if ":" in name:
      parts = name.split(':')
      name = parts[0]
      target_comp = parts[1]
    compiler = get_toolchain_compiler()
    label = get_platform_release_label(release=platform_release).toolchain
    # Most common return values for machine are x86_64 or aarch64
    arch = platform.machine()
    if arch not in ['aarch64', 'x86_64']:
      raise Exception("Unsupported architecture '{}' for pre-built native-toolchain. "
          "Fetch and build it locally by setting NATIVE_TOOLCHAIN_HOME".format(arch))
    toolchain_build_id = os.environ["IMPALA_TOOLCHAIN_BUILD_ID_{}".format(arch.upper())]
    toolchain_host = os.environ["IMPALA_TOOLCHAIN_HOST"]
    template_subs = {'compiler': compiler, 'label': label, 'arch': arch,
                     'toolchain_build_id': toolchain_build_id,
                     'toolchain_host': toolchain_host}
    archive_basename_tmpl = "${name}-${version}-${compiler}-${label}-${arch}"
    url_prefix_tmpl = "https://${toolchain_host}/build/${toolchain_build_id}/" + \
        "${name}/${version}-${compiler}/"
    unpack_directory_tmpl = "${name}-${version}"
    super(ToolchainPackage, self).__init__(name, url_prefix_tmpl,
                                           toolchain_packages_home,
                                           explicit_version=explicit_version,
                                           archive_basename_tmpl=archive_basename_tmpl,
                                           unpack_directory_tmpl=unpack_directory_tmpl,
                                           template_subs_in=template_subs,
                                           target_comp=target_comp)

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


class CdpComponent(EnvVersionedPackage):
  def __init__(self, name, explicit_version=None, archive_basename_tmpl=None,
               unpack_directory_tmpl=None, makedir=False):
    # Compute the CDP base URL (based on the IMPALA_TOOLCHAIN_HOST and CDP_BUILD_NUMBER)
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

class OdpComponent(EnvVersionedPackage):
  def __init__(self, name, explicit_version=None, archive_basename_tmpl=None,
               unpack_directory_tmpl=None, makedir=False):
    template_subs = {"toolchain_host": os.environ["IMPALA_TOOLCHAIN_HOST"],
                     "odp_build_number": os.environ["ODP_BUILD_NUMBER"]}
    url_prefix_tmpl = "https://mirror.odp.acceldata.dev/ODP/standalone/${odp_build_number}/"

    # Get the output base directory from CDP_COMPONENTS_HOME
    destination_basedir = os.environ["CDP_COMPONENTS_HOME"]
    super(OdpComponent, self).__init__(name, url_prefix_tmpl, destination_basedir,
                                       explicit_version=explicit_version,
                                       archive_basename_tmpl=archive_basename_tmpl,
                                       unpack_directory_tmpl=unpack_directory_tmpl,
                                       makedir=makedir, template_subs_in=template_subs)


class ApacheComponent(EnvVersionedPackage):
  def __init__(self, name, explicit_version=None, archive_basename_tmpl=None,
               unpack_directory_tmpl=None, makedir=False, component_path_tmpl=None):
    # Compute the apache base URL (based on the APACHE_MIRROR)
    if "APACHE_COMPONENTS_HOME" not in os.environ:
      logging.error("Impala environment not set up correctly, make sure "
                    "impala-config.sh is sourced.")
      sys.exit(1)
    template_subs = {"apache_mirror": os.environ["APACHE_MIRROR"]}
    # Different components have different sub-paths. For example, hive is hive/hive-xxx,
    # hadoop is hadoop/common/hadoop-xxx. The default is hive format.
    if component_path_tmpl is None:
      component_path_tmpl = "${name}/${name}-${version}/"
    url_prefix_tmpl = "${apache_mirror}/" + component_path_tmpl

    # Get the output base directory from APACHE_COMPONENTS_HOME
    destination_basedir = os.environ["APACHE_COMPONENTS_HOME"]
    super(ApacheComponent, self).__init__(name, url_prefix_tmpl, destination_basedir,
                                       explicit_version=explicit_version,
                                       archive_basename_tmpl=archive_basename_tmpl,
                                       unpack_directory_tmpl=unpack_directory_tmpl,
                                       makedir=makedir, template_subs_in=template_subs)


class ToolchainKudu(DownloadUnpackTarball):
  """
  Unpacks a Kudu toolchain tarball from a local path specified by ODP_KUDU_TARBALL_PATH.
  """
  def __init__(self, platform_label=None):
    toolchain_kudu_path = os.environ.get('ODP_KUDU_TARBALL_PATH')
    if not toolchain_kudu_path or not os.path.isfile(toolchain_kudu_path):
      logging.error("ODP_KUDU_TARBALL_PATH is not set or file does not exist: %s", toolchain_kudu_path)
      sys.exit(1)
    archive_name = os.path.basename(toolchain_kudu_path)
    archive_basename = archive_name.replace('.tar.gz', '')
    toolchain_packages_home = os.environ.get("IMPALA_TOOLCHAIN_PACKAGES_HOME")
    if not toolchain_packages_home:
      logging.error("Impala environment not set up correctly, make sure $IMPALA_TOOLCHAIN_PACKAGES_HOME is set.")
      sys.exit(1)
    directory_name = archive_basename
    makedir = False
    # url is None, we only unpack
    super(ToolchainKudu, self).__init__(
        url=None,
        archive_name=archive_name,
        destination_basedir=toolchain_packages_home,
        directory_name=directory_name,
        makedir=makedir
    )
    self.local_archive_path = toolchain_kudu_path

  def needs_download(self):
    return not os.path.isdir(self.pkg_directory())

  def download(self):
    # Unpack the archive from the local path to the destination directory
    unpack_dir = self.pkg_directory()
    if os.path.exists(unpack_dir):
      logging.info("Removing existing package directory %s", unpack_dir)
      shutil.rmtree(unpack_dir)
    logging.info("Unpacking Kudu toolchain from %s to %s", self.local_archive_path, self.destination_basedir)
    subprocess.check_call([
      "tar", "xzf", self.local_archive_path, "--directory={0}".format(self.destination_basedir)
    ])


class OdpKudu(EnvVersionedPackage):
  """
  Kudu component that downloads from ODP mirror instead of native toolchain S3.
  This allows using custom Kudu builds hosted on ODP mirror location.
  """
  def __init__(self):
    # Get the toolchain packages home for destination  
    toolchain_packages_home = os.environ.get("IMPALA_TOOLCHAIN_PACKAGES_HOME")
    if not toolchain_packages_home:
      logging.error("Impala environment not set up correctly, make sure "
          "$IMPALA_TOOLCHAIN_PACKAGES_HOME is set.")
      sys.exit(1)
    
    # Check for custom Kudu URL from environment variable
    custom_kudu_url = os.environ.get("ODP_KUDU_DIRECT_URL")
    
    if custom_kudu_url:
      # Use custom direct URL from environment variable
      logging.info("Using custom Kudu URL from ODP_KUDU_DIRECT_URL: %s", custom_kudu_url)
      
      # Extract the filename from the direct URL
      archive_name = custom_kudu_url.split('/')[-1]
      archive_basename = archive_name.replace('.tar.gz', '')

      # For direct file URLs, split the URL to get the base path
      url_parts = custom_kudu_url.rsplit('/', 1)
      url_prefix_tmpl = url_parts[0] + "/"
      template_subs = {}
    else:
      # Use default ODP mirror URL pattern
      logging.info("Using default ODP mirror URL pattern for Kudu")
      template_subs = {"odp_build_number": os.environ["ODP_BUILD_NUMBER"]}
      url_prefix_tmpl = "https://mirror.odp.acceldata.dev/ODP/standalone/${odp_build_number}/"
      archive_basename = "kudu-${version}" 
    
    super(OdpKudu, self).__init__("kudu", url_prefix_tmpl, toolchain_packages_home,
                                  archive_basename_tmpl=archive_basename,
                                  unpack_directory_tmpl="kudu-${version}",
                                  template_subs_in=template_subs)

  def needs_download(self):
    # This verifies that the unpack directory exists
    if super(OdpKudu, self).needs_download():
      return True
    # For source packages, we don't need the debug/release directory check
    # like ToolchainKudu since this is a source distribution
    return False


def try_get_platform_release_label():
  """Gets the right package label from the OS version. Returns an OsMapping with both
     'toolchain' and 'cdh' labels. Return None if not found.
  """
  try:
    return get_platform_release_label()
  except Exception:
    return None


# Cache the /etc/os-release calculation to shave a little bit of time.
os_release_cache = None


def get_platform_release_label(release=None):
  """Gets the right package label from the OS version. Raise exception if not found.
     'release' can be provided to override the underlying OS version. This uses
     ID and VERSION_ID from /etc/os-release to identify a distribution. Specifically,
     this returns the concatenation of the ID and the major version component
     of VERSION_ID. i.e. ID=ubuntu VERSION_ID=16.04 => ubuntu16
  """
  global os_release_cache
  if not release:
    if os_release_cache:
      release = os_release_cache
    else:
      os_id = None
      os_major_version = None
      with open("/etc/os-release") as f:
        for line in f:
          # We assume that ID and VERSION_ID are present and don't contain '=' inside
          # the actual value. This is true for all distributions we currently support.
          if line.startswith("ID="):
            os_id = line.split("=")[1].strip().strip('"')
          elif line.startswith("VERSION_ID="):
            os_version_id = line.split("=")[1].strip().strip('"')
            # Some distributions have a major version that doesn't change (e.g. 3.12.0
            # and 3.12.0). The distributions that we support don't do this. This
            # calculation would need to change for that circumstance.
            os_major_version = os_version_id.split(".")[0]

      if os_id is None or os_major_version is None:
        raise Exception("Error parsing /etc/os-release: "
            "os_id={0} os_major_version={1}".format(os_id, os_major_version))

      release = "{0}{1}".format(os_id, os_major_version)
      os_release_cache = release
  for mapping in OS_MAPPING:
    if mapping.release == release:
      return mapping
  raise Exception("Could not find package label for OS version: {0}.".format(release))


def check_custom_toolchain(toolchain_packages_home, packages):
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


def get_unique_toolchain_downloads(packages):
  toolchain_packages = [ToolchainPackage(p) for p in packages]
  unique_pkg_directories = set()
  unique_packages = []
  for p in toolchain_packages:
    if p.pkg_directory() not in unique_pkg_directories:
      unique_packages.append(p)
      unique_pkg_directories.add(p.pkg_directory())
  return unique_packages


def get_toolchain_downloads():
  toolchain_packages = []
  # The LLVM and GCC packages are the largest packages in the toolchain (Kudu is handled
  # separately). Sort them first so their downloads start as soon as possible.
  llvm_package = ToolchainPackage("llvm")
  llvm_package_asserts = ToolchainPackage(
      "llvm", explicit_version=os.environ.get("IMPALA_LLVM_DEBUG_VERSION"))
  gcc_package = ToolchainPackage("gcc")
  toolchain_packages += [llvm_package, llvm_package_asserts, gcc_package]
  toolchain_packages += [ToolchainPackage(p) for p in
      ["avro", "binutils", "boost", "breakpad", "bzip2", "calloncehack", "cctz",
       "cloudflarezlib", "cmake", "crcutil", "curl", "flatbuffers", "gdb", "gflags",
       "glog", "gperftools", "jwt-cpp", "libev", "libunwind", "lz4", "mold",
       "openldap", "orc", "protobuf", "python", "rapidjson", "re2", "snappy", "tpc-h",
       "tpc-ds", "zlib", "zstd"]]
  python3_package = ToolchainPackage(
      "python", explicit_version=os.environ.get("IMPALA_PYTHON3_VERSION"))
  gtest_package = ToolchainPackage(
      "googletest", explicit_version=os.environ.get("IMPALA_GTEST_VERSION"))
  toolchain_packages += [python3_package, gtest_package]
  toolchain_packages += get_unique_toolchain_downloads(
      ["thrift:cpp", "thrift:java", "thrift:py"])
  protobuf_package_clang = ToolchainPackage(
      "protobuf", explicit_version=os.environ.get("IMPALA_PROTOBUF_CLANG_VERSION"))
  toolchain_packages += [protobuf_package_clang]
  if platform.machine() == 'aarch64':
    toolchain_packages.append(ToolchainPackage("hadoop-client"))
  # Check whether this platform is supported (or whether a valid custom toolchain
  # has been provided).
  if not try_get_platform_release_label() \
     or not try_get_platform_release_label().toolchain:
    toolchain_packages_home = os.environ.get("IMPALA_TOOLCHAIN_PACKAGES_HOME")
    # This would throw an exception if the custom toolchain were not valid
    check_custom_toolchain(toolchain_packages_home, toolchain_packages)
    # Nothing to download
    return []
  return toolchain_packages


def get_hadoop_downloads():
  cluster_components = []
  hadoop = OdpComponent("hadoop")
  hbase = OdpComponent("hbase", archive_basename_tmpl="hbase-${version}-bin",
                       unpack_directory_tmpl="hbase-${version}")

  use_apache_ozone = os.environ["USE_APACHE_OZONE"] == "true"
  if use_apache_ozone:
    ozone = ApacheComponent("ozone", component_path_tmpl="ozone/${version}")
  else:
    ozone = OdpComponent("ozone")

  use_apache_hive = os.environ["USE_APACHE_HIVE"] == "true"
  if use_apache_hive:
    hive = ApacheComponent("hive", archive_basename_tmpl="apache-hive-${version}-bin")
    hive_src = ApacheComponent("hive", archive_basename_tmpl="apache-hive-${version}-src")
  else:
    hive = OdpComponent("hive", archive_basename_tmpl="apache-hive-${version}-bin")
    hive_src = OdpComponent("hive",
                            explicit_version=os.environ.get("IMPALA_HIVE_VERSION"),
                            archive_basename_tmpl="apache-hive-${version}-src",
                            unpack_directory_tmpl="hive-${version}")

  tez = OdpComponent("tez", archive_basename_tmpl="tez-${version}-minimal", makedir=True)
  ranger = OdpComponent("ranger", archive_basename_tmpl="ranger-${version}-admin")
  use_override_hive = \
      "HIVE_VERSION_OVERRIDE" in os.environ and os.environ["HIVE_VERSION_OVERRIDE"] != ""
  use_override_ranger = \
      "RANGER_VERSION_OVERRIDE" in os.environ and \
      os.environ["RANGER_VERSION_OVERRIDE"] != ""
  # If we are using a locally built Hive we do not have a need to pull hive as a
  # dependency. The same applies to Ranger.
  cluster_components.extend([hadoop, hbase, ozone])
  if not use_override_hive:
    cluster_components.extend([hive, hive_src])
  if not use_override_ranger:
    cluster_components.extend([ranger])
  cluster_components.extend([tez])
  return cluster_components


def get_kudu_downloads():
  # Check if we should use ODP mirror for Kudu instead of native toolchain
  use_odp_kudu = os.environ.get("USE_ODP_KUDU", "false") == "true"
  
  if use_odp_kudu:
    # Use ODP mirror for Kudu download
    logging.info("Using ODP mirror for Kudu download")
    return [OdpKudu()]
  else:
    # Use native toolchain Kudu (original behavior)
    logging.info("Using native toolchain for Kudu download")
    return [ToolchainKudu()]


def main():
  """
  Validates that bin/impala-config.sh has been sourced by verifying that $IMPALA_HOME
  and $IMPALA_TOOLCHAIN_PACKAGES_HOME are in the environment. We assume that if these
  are set, then IMPALA_<PACKAGE>_VERSION environment variables are also set. This will
  create the directory specified by $IMPALA_TOOLCHAIN_PACKAGES_HOME if it does not
  already exist. Then, it will compute what packages need to be downloaded. Packages are
  only downloaded if they are not already present. There are two main categories of
  packages. Toolchain packages are native packages built using the native toolchain.
  These are always downloaded. Hadoop component packages are the CDP builds of Hadoop
  components such as Hadoop, Hive, HBase, etc. Hadoop component packages are organized as
  a consistent set of compatible version via a build number (i.e. CDP_BUILD_NUMBER).
  Hadoop component packages are only downloaded if $DOWNLOAD_CDH_COMPONENTS is true. The
  versions used for Hadoop components come from the CDP versions based on the
  $CDP_BUILD_NUMBER. CDP Hadoop packages are downloaded into $CDP_COMPONENTS_HOME.
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
  create_directory_from_env_var("IMPALA_TOOLCHAIN_PACKAGES_HOME")

  downloads = []
  if os.getenv("SKIP_TOOLCHAIN_BOOTSTRAP", "false") != "true":
    downloads += get_toolchain_downloads()
  if os.getenv("DOWNLOAD_CDH_COMPONENTS", "false") == "true":
    create_directory_from_env_var("CDP_COMPONENTS_HOME")
    create_directory_from_env_var("APACHE_COMPONENTS_HOME")
    if os.getenv("SKIP_TOOLCHAIN_BOOTSTRAP", "false") != "true":
      # Kudu is currently sourced from native-toolchain
      downloads += get_kudu_downloads()
    downloads += get_hadoop_downloads()

  components_needing_download = [d for d in downloads if d.needs_download()]

  def download(component):
    component.download()

  execute_many(download, components_needing_download)


if __name__ == "__main__": main()
