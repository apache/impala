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

# This module will create a python virtual env and install external dependencies. If the
# virtualenv already exists and it contains all the expected packages, nothing is done.
#
# A multi-step bootstrapping process is required to build and install all of the
# dependencies:
# 1. install basic non-C/C++ packages into the virtualenv
# 1b. install packages that depend on step 1 but cannot be installed together with their
#     dependencies
# 2. use the virtualenv Python to bootstrap the toolchain
# 3. use toolchain gcc to build C/C++ packages
# 4. build the kudu-python package with toolchain gcc and Cython
#
# Every time this script is run, it completes as many of the bootstrapping steps as
# possible with the available dependencies.
#
# This module can be run with python >= 2.4 but python >= 2.6 must be installed on the
# system. If the default 'python' command refers to < 2.6, python 2.6 will be used
# instead.

from __future__ import print_function
import glob
import logging
import optparse
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import urllib

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

DEPS_DIR = os.path.join(os.path.dirname(__file__), "deps")
ENV_DIR = os.path.join(os.path.dirname(__file__), "env")

# Requirements file with packages we need for our build and tests.
REQS_PATH = os.path.join(DEPS_DIR, "requirements.txt")

# Second stage of requirements which cannot be installed together with their dependencies
# in requirements.txt.
REQS2_PATH = os.path.join(DEPS_DIR, "stage2-requirements.txt")

# Requirements for the next bootstrapping step that builds compiled requirements
# with toolchain gcc.
COMPILED_REQS_PATH = os.path.join(DEPS_DIR, "compiled-requirements.txt")

# Requirements for the Kudu bootstrapping step, which depends on Cython being installed
# by the compiled requirements step.
KUDU_REQS_PATH = os.path.join(DEPS_DIR, "kudu-requirements.txt")

# Requirements for the ADLS test client step, which depends on Cffi (C Foreign Function
# Interface) being installed by the compiled requirements step.
ADLS_REQS_PATH = os.path.join(DEPS_DIR, "adls-requirements.txt")

def delete_virtualenv_if_exist():
  if os.path.exists(ENV_DIR):
    shutil.rmtree(ENV_DIR)


def create_virtualenv():
  LOG.info("Creating python virtualenv")
  build_dir = tempfile.mkdtemp()
  file = tarfile.open(find_file(DEPS_DIR, "virtualenv*.tar.gz"), "r:gz")
  for member in file.getmembers():
    file.extract(member, build_dir)
  file.close()
  python_cmd = detect_python_cmd()
  exec_cmd([python_cmd, find_file(build_dir, "virtualenv*", "virtualenv.py"), "--quiet",
      "--python", python_cmd, ENV_DIR])
  shutil.rmtree(build_dir)


def exec_cmd(args, **kwargs):
  '''Executes a command and waits for it to finish, raises an exception if the return
     status is not zero. The command output is returned.

     'args' and 'kwargs' use the same format as subprocess.Popen().
  '''
  process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
      **kwargs)
  output = process.communicate()[0]
  if process.returncode != 0:
    raise Exception("Command returned non-zero status\nCommand: %s\nOutput: %s"
        % (args, output))
  return output

def use_ccache():
  '''Returns true if ccache is available and should be used'''
  if 'DISABLE_CCACHE' in os.environ: return False
  try:
    exec_cmd(['ccache', '-V'])
    return True
  except:
    return False

def select_cc():
  '''Return the C compiler command that should be used as a string or None if the
  compiler is not available '''
  # Use toolchain gcc for ABI compatibility with other toolchain packages, e.g.
  # Kudu/kudu-python
  if not have_toolchain(): return None
  toolchain_gcc_dir = toolchain_pkg_dir("gcc")
  cc = os.path.join(toolchain_gcc_dir, "bin/gcc")
  if not os.path.exists(cc): return None
  if use_ccache(): cc = "ccache %s" % cc
  return cc

def exec_pip_install(args, cc="no-cc-available", env=None):
  '''Executes "pip install" with the provided command line arguments. If 'cc' is set,
  it is used as the C compiler. Otherwise compilation of C/C++ code is disabled by
  setting the CC environment variable to a bogus value.
  Other environment vars can optionally be set with the 'env' argument. By default the
  current process's command line arguments are inherited.'''
  if not env: env = dict(os.environ)
  env["CC"] = cc

  # Parallelize the slow numpy build.
  # Use getconf instead of nproc because it is supported more widely, e.g. on older
  # linux distributions.
  env["NPY_NUM_BUILD_JOBS"] = exec_cmd(["getconf", "_NPROCESSORS_ONLN"]).strip()

  # Don't call the virtualenv pip directly, it uses a hashbang to to call the python
  # virtualenv using an absolute path. If the path to the virtualenv is very long, the
  # hashbang won't work.
  impala_pip_base_cmd = [os.path.join(ENV_DIR, "bin", "python"),
                         os.path.join(ENV_DIR, "bin", "pip"), "install", "-v"]

  # Passes --no-binary for IMPALA-3767: without this, Cython (and
  # several other packages) fail download.
  #
  # --no-cache-dir is used to prevent caching of compiled artifacts, which may be built
  # with different compilers or settings.
  third_party_pkg_install_cmd = \
      impala_pip_base_cmd[:] + ["--no-binary", ":all:", "--no-cache-dir"]

  # When using a custom mirror, we also must use the index of that mirror.
  if "PYPI_MIRROR" in os.environ:
    third_party_pkg_install_cmd.extend(["--index-url",
                                        "%s/simple" % os.environ["PYPI_MIRROR"]])
  else:
    # Prevent fetching additional packages from the index. If we forget to add a package
    # to one of the requirements.txt files, this should trigger an error. However, we will
    # still access the index for version/dependency resolution, hence we need to change it
    # when using a private mirror.
    third_party_pkg_install_cmd.append("--no-index")

  third_party_pkg_install_cmd.extend(["--find-links",
      "file://%s" % urllib.pathname2url(os.path.abspath(DEPS_DIR))])
  third_party_pkg_install_cmd.extend(args)
  exec_cmd(third_party_pkg_install_cmd, env=env)

  # Finally, we want to install the packages from our own internal python lib
  local_package_install_cmd = impala_pip_base_cmd + \
      ['-e', os.path.join(os.getenv('IMPALA_HOME'), 'lib', 'python')]
  exec_cmd(local_package_install_cmd)


def find_file(*paths):
  '''Returns the path specified by the glob 'paths', raises an exception if no file is
     found.

     Ex: find_file('/etc', 'h*sts') --> /etc/hosts
  '''
  path = os.path.join(*paths)
  files = glob.glob(path)
  if len(files) > 1:
    raise Exception("Found too many files at %s: %s" % (path, files))
  if len(files) == 0:
    raise Exception("No file found at %s" % path)
  return files[0]


def detect_python_cmd():
  '''Returns the system command that provides python 2.6 or greater.'''
  paths = os.getenv("PATH").split(os.path.pathsep)
  for cmd in ("python", "python27", "python2.7", "python-27", "python-2.7", "python26",
      "python2.6", "python-26", "python-2.6"):
    for path in paths:
      cmd_path = os.path.join(path, cmd)
      if not os.path.exists(cmd_path) or not os.access(cmd_path, os.X_OK):
        continue
      exit = subprocess.call([cmd_path, "-c", textwrap.dedent("""
          import sys
          sys.exit(int(sys.version_info[:2] < (2, 6)))""")])
      if exit == 0:
        return cmd_path
  raise Exception("Could not find minimum required python version 2.6")


def install_deps():
  LOG.info("Installing packages into the virtualenv")
  exec_pip_install(["-r", REQS_PATH])
  mark_reqs_installed(REQS_PATH)
  LOG.info("Installing stage 2 packages into the virtualenv")
  exec_pip_install(["-r", REQS2_PATH])
  mark_reqs_installed(REQS2_PATH)

def have_toolchain():
  '''Return true if the Impala toolchain is available'''
  return "IMPALA_TOOLCHAIN" in os.environ

def toolchain_pkg_dir(pkg_name):
  '''Return the path to the toolchain package'''
  pkg_version = os.environ["IMPALA_" + pkg_name.upper() + "_VERSION"]
  return os.path.join(os.environ["IMPALA_TOOLCHAIN"], pkg_name + "-" + pkg_version)

def install_compiled_deps_if_possible():
  '''Install dependencies that require compilation with toolchain GCC, if the toolchain
  is available. Returns true if the deps are installed'''
  if reqs_are_installed(COMPILED_REQS_PATH):
    LOG.debug("Skipping compiled deps: matching compiled-installed-requirements.txt found")
    return True
  cc = select_cc()
  if cc is None:
    LOG.debug("Skipping compiled deps: cc not available yet")
    return False

  env = dict(os.environ)

  # Compilation of pycrypto fails on CentOS 5 with newer GCC versions because of a
  # problem with inline declarations in older libc headers. Setting -fgnu89-inline is a
  # workaround.
  distro_version = ''.join(exec_cmd(["lsb_release", "-irs"]).lower().split())
  print(distro_version)
  if distro_version.startswith("centos5."):
    env["CFLAGS"] = "-fgnu89-inline"

  LOG.info("Installing compiled requirements into the virtualenv")
  exec_pip_install(["-r", COMPILED_REQS_PATH], cc=cc, env=env)
  mark_reqs_installed(COMPILED_REQS_PATH)
  return True

def install_adls_deps():
  # The ADLS dependencies require that the OS is at least CentOS 6.7 or above,
  # which is why we break this into a seperate step. If the target filesystem is
  # ADLS, the expectation is that the dev environment is running at least CentOS 6.7.
  if os.environ.get('TARGET_FILESYSTEM') == "adls":
    if reqs_are_installed(ADLS_REQS_PATH):
      LOG.debug("Skipping ADLS deps: matching adls-installed-requirements.txt found")
      return True
    cc = select_cc()
    assert cc is not None
    LOG.info("Installing ADLS packages into the virtualenv")
    exec_pip_install(["-r", ADLS_REQS_PATH], cc=cc)
    mark_reqs_installed(ADLS_REQS_PATH)

def install_kudu_client_if_possible():
  '''Installs the Kudu python module if possible, which depends on the toolchain and
  the compiled requirements in compiled-requirements.txt. If the toolchain isn't
  available, nothing will be done. Also nothing will be done if the Kudu client lib
  required by the module isn't available (as determined by KUDU_IS_SUPPORTED)'''
  if reqs_are_installed(KUDU_REQS_PATH):
    LOG.debug("Skipping Kudu: matching kudu-installed-requirements.txt found")
    return
  if os.environ["KUDU_IS_SUPPORTED"] != "true":
    LOG.debug("Skipping Kudu: Kudu is not supported")
    return
  kudu_base_dir = os.environ["IMPALA_KUDU_HOME"]
  if not os.path.exists(kudu_base_dir):
    LOG.debug("Skipping Kudu: %s doesn't exist" % kudu_base_dir)
    return

  LOG.info("Installing Kudu into the virtualenv")
  # The installation requires that KUDU_HOME/build/latest exists. An empty directory
  # structure will be made to satisfy that. The Kudu client headers and lib will be made
  # available through GCC environment variables.
  fake_kudu_build_dir = os.path.join(tempfile.gettempdir(), "virtualenv-kudu")
  try:
    artifact_dir = os.path.join(fake_kudu_build_dir, "build", "latest")
    if not os.path.exists(artifact_dir):
      os.makedirs(artifact_dir)
    cc = select_cc()
    assert cc is not None
    env = dict(os.environ)
    env["KUDU_HOME"] = fake_kudu_build_dir
    kudu_client_dir = find_kudu_client_install_dir()
    env["CPLUS_INCLUDE_PATH"] = os.path.join(kudu_client_dir, "include")
    env["LIBRARY_PATH"] = os.path.pathsep.join([os.path.join(kudu_client_dir, 'lib'),
                                                os.path.join(kudu_client_dir, 'lib64')])
    exec_pip_install(["-r", KUDU_REQS_PATH], cc=cc, env=env)
    mark_reqs_installed(KUDU_REQS_PATH)
  finally:
    try:
      shutil.rmtree(fake_kudu_build_dir)
    except Exception:
      LOG.debug("Error removing temp Kudu build dir", exc_info=True)


def find_kudu_client_install_dir():
  custom_client_dir = os.environ["KUDU_CLIENT_DIR"]
  if custom_client_dir:
    install_dir = os.path.join(custom_client_dir, "usr", "local")
    error_if_kudu_client_not_found(install_dir)
  else:
    # If the toolchain appears to have been setup already, then the Kudu client is
    # required to exist. It's possible that the toolchain won't be setup yet though
    # since the toolchain bootstrap script depends on the virtualenv.
    kudu_base_dir = os.environ["IMPALA_KUDU_HOME"]
    install_dir = os.path.join(kudu_base_dir, "debug")
    if os.path.exists(kudu_base_dir):
      error_if_kudu_client_not_found(install_dir)
  return install_dir


def error_if_kudu_client_not_found(install_dir):
  header_path = os.path.join(install_dir, "include", "kudu", "client", "client.h")
  if not os.path.exists(header_path):
    raise Exception("Kudu client header not found at %s" % header_path)

  kudu_client_lib = "libkudu_client.so"
  lib_dir = os.path.join(install_dir, "lib64")
  if not os.path.exists(lib_dir):
    lib_dir = os.path.join(install_dir, "lib")
  for _, _, files in os.walk(lib_dir):
    for file in files:
      if file == kudu_client_lib:
        return
  raise Exception("%s not found at %s" % (kudu_client_lib, lib_dir))

def mark_reqs_installed(reqs_path):
  '''Mark that the requirements from the given file are installed by copying it into the root
  directory of the virtualenv.'''
  installed_reqs_path = os.path.join(ENV_DIR, os.path.basename(reqs_path))
  shutil.copyfile(reqs_path, installed_reqs_path)

def reqs_are_installed(reqs_path):
  '''Check if the requirements from the given file are installed in the virtualenv by
  looking for a matching requirements file in the root directory of the virtualenv.'''
  installed_reqs_path = os.path.join(ENV_DIR, os.path.basename(reqs_path))
  if not os.path.exists(installed_reqs_path):
    return False
  installed_reqs_file = open(installed_reqs_path)
  try:
    reqs_file = open(reqs_path)
    try:
      if reqs_file.read() == installed_reqs_file.read():
        return True
      else:
        LOG.debug("Virtualenv upgrade needed")
        return False
    finally:
      reqs_file.close()
  finally:
    installed_reqs_file.close()

def setup_virtualenv_if_not_exists():
  if not (reqs_are_installed(REQS_PATH) and reqs_are_installed(REQS2_PATH)):
    delete_virtualenv_if_exist()
    create_virtualenv()
    install_deps()
    LOG.debug("Virtualenv setup complete")


if __name__ == "__main__":
  parser = optparse.OptionParser()
  parser.add_option("-l", "--log-level", default="INFO",
      choices=("DEBUG", "INFO", "WARN", "ERROR"))
  parser.add_option("-r", "--rebuild", action="store_true", help="Force a rebuild of"
      " the virtualenv even if it exists and appears to be completely up-to-date.")
  parser.add_option("--print-ld-library-path", action="store_true", help="Print the"
      " LD_LIBRARY_PATH that should be used when running python from the virtualenv.")
  options, args = parser.parse_args()

  if options.print_ld_library_path:
    kudu_client_dir = find_kudu_client_install_dir()
    print(os.path.pathsep.join([os.path.join(kudu_client_dir, 'lib'),
                                    os.path.join(kudu_client_dir, 'lib64')]))
    sys.exit()

  logging.basicConfig(level=getattr(logging, options.log_level))
  if options.rebuild:
    delete_virtualenv_if_exist()

  # Complete as many bootstrap steps as possible (see file comment for the steps).
  setup_virtualenv_if_not_exists()
  if install_compiled_deps_if_possible():
    install_kudu_client_if_possible()
    install_adls_deps()
