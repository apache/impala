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
# It is expected that bootstrap_toolchain.py already ran prior to running this
# (and thus the toolchain GCC compiler is in place).
#
# The virtualenv creation process involves multiple rounds of pip installs, but
# this script expects to complete all rounds in a single invocation. The steps are:
# 1. Install setuptools and its depenencies. These are used by the setup.py scripts
#    that run during pip install.
# 2. Install most packages (including ones that require C/C++ compilation)
# 3. Install Kudu package (which uses the toolchain GCC and the installed Cython)
# 4. Install ADLS packages if applicable
#
# This module can be run with python >= 2.7. It makes no guarantees about usage on
# python < 2.7.

from __future__ import absolute_import, division, print_function
import glob
import logging
import optparse
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
try:
  from urllib.request import pathname2url
except ImportError:
  from urllib import pathname2url
from bootstrap_toolchain import ToolchainPackage

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

SKIP_TOOLCHAIN_BOOTSTRAP = "SKIP_TOOLCHAIN_BOOTSTRAP"

GCC_VERSION = os.environ["IMPALA_GCC_VERSION"]

DEPS_DIR = os.path.join(os.path.dirname(__file__), "deps")
ENV_DIR_PY2 = os.path.join(os.path.dirname(__file__),
                           "env-gcc{0}".format(GCC_VERSION))
ENV_DIR_PY3 = os.path.join(os.path.dirname(__file__),
                           "env-gcc{0}-py3".format(GCC_VERSION))

# Setuptools requirements file. Setuptools is required during pip install for
# some packages. Newer setuptools dropped python 2 support, and some python
# install tools don't understand that they need to get a version that works
# with the current python version. This can cause them to try to install the newer
# setuptools that won't work on python 2. Doing this as a separate step makes it
# easy to pin the version of setuptools to a Python 2 compatible version.
SETUPTOOLS_REQS_PATH = os.path.join(DEPS_DIR, "setuptools-requirements.txt")

# Requirements file with packages we need for our build and tests, which depends
# on setuptools being installed by the setuptools requirements step.
REQS_PATH = os.path.join(DEPS_DIR, "requirements.txt")

# Requirements for the Kudu bootstrapping step, which depends on Cython being installed
# by the requirements step.
KUDU_REQS_PATH = os.path.join(DEPS_DIR, "kudu-requirements.txt")

# Requirements for the ADLS test client step, which depends on Cffi (C Foreign Function
# Interface) being installed by the requirements step.
ADLS_REQS_PATH = os.path.join(DEPS_DIR, "adls-requirements.txt")

# Extra packages specific to python 3
PY3_REQS_PATH = os.path.join(DEPS_DIR, "py3-requirements.txt")

# Extra packages specific to python 2
PY2_REQS_PATH = os.path.join(DEPS_DIR, "py2-requirements.txt")


def delete_virtualenv_if_exist(venv_dir):
  if os.path.exists(venv_dir):
    shutil.rmtree(venv_dir)


def detect_virtualenv_version():
  with open(REQS_PATH, "r") as reqs_file:
    for line in reqs_file:
      line = line.strip()
      # Ignore blank lines and comments
      if len(line) == 0 or line[0] == '#':
        continue
      if line.find("virtualenv") != -1 and line.find("==") != -1:
        packagestring, version = [a.strip() for a in line.split("==")]
        if packagestring == "virtualenv":
          LOG.debug("Detected virtualenv version {0}".format(version))
          return version
  # If the parsing didn't work, don't raise an exception.
  return None


def create_virtualenv(venv_dir, is_py3):
  if is_py3:
    # Python 3 is much simpler, because there is a builtin venv command
    LOG.info("Creating python3 virtualenv")
    python_cmd = download_toolchain_python(is_py3)
    exec_cmd([python_cmd, "-m" "venv", venv_dir])
    return

  # Python 2
  LOG.info("Creating python2 virtualenv")
  build_dir = tempfile.mkdtemp()
  # Try to find the virtualenv version by parsing the requirements file
  # Default to "*" if we can't figure it out.
  virtualenv_version = detect_virtualenv_version()
  if virtualenv_version is None:
    virtualenv_version = "*"
  # Open the virtualenv tarball
  virtualenv_tarball = \
      find_file(DEPS_DIR, "virtualenv-{0}.tar.gz".format(virtualenv_version))
  file = tarfile.open(virtualenv_tarball, "r:gz")
  for member in file.getmembers():
    file.extract(member, build_dir)
  file.close()
  python_cmd = download_toolchain_python(is_py3)
  exec_cmd([python_cmd, find_file(build_dir, "virtualenv*", "virtualenv.py"), "--quiet",
      "--python", python_cmd, venv_dir])
  shutil.rmtree(build_dir)


def exec_cmd(args, **kwargs):
  '''Executes a command and waits for it to finish, raises an exception if the return
     status is not zero. The command output is returned.

     'args' and 'kwargs' use the same format as subprocess.Popen().
  '''
  process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
      universal_newlines=True, **kwargs)
  output = process.communicate()[0]
  if process.returncode != 0:
    raise Exception("Command returned non-zero status\nCommand: %s\nOutput: %s"
        % (args, output))
  return output


def select_cc():
  '''Return the C compiler command that should be used as a string or None if the
  compiler is not available '''
  # Use toolchain gcc for ABI compatibility with other toolchain packages, e.g.
  # Kudu/kudu-python
  if not have_toolchain(): return None
  toolchain_gcc_dir = toolchain_pkg_dir("gcc")
  cc = os.path.join(toolchain_gcc_dir, "bin/gcc")
  if not os.path.exists(cc): return None
  return cc


def exec_pip_install(venv_dir, is_py3, args, cc="no-cc-available", env=None):
  '''Executes "pip install" with the provided command line arguments. If 'cc' is set,
  it is used as the C compiler. Otherwise compilation of C/C++ code is disabled by
  setting the CC environment variable to a bogus value.
  Other environment vars can optionally be set with the 'env' argument. By default the
  current process's command line arguments are inherited.'''
  if not env: env = dict(os.environ)
  env["CC"] = cc
  # Since gcc is now built with toolchain binutils which may be newer than the
  # system binutils, we need to include the toolchain binutils on the PATH.
  toolchain_binutils_dir = toolchain_pkg_dir("binutils")
  binutils_bin_dir = os.path.join(toolchain_binutils_dir, "bin")
  env["PATH"] = "{0}:{1}".format(binutils_bin_dir, env["PATH"])
  # Sometimes pip install invokes gcc directly without using the CC environment
  # variable. If system GCC is too new, then it will fail, because it needs symbols
  # that are not in Impala's libstdc++. To avoid this, we add GCC to the PATH,
  # so any direct reference will use our GCC rather than the system GCC.
  toolchain_gcc_dir = toolchain_pkg_dir("gcc")
  gcc_bin_dir = os.path.join(toolchain_gcc_dir, "bin")
  env["PATH"] = "{0}:{1}".format(gcc_bin_dir, env["PATH"])

  # Parallelize the slow numpy build.
  # Use getconf instead of nproc because it is supported more widely, e.g. on older
  # linux distributions.
  env["NPY_NUM_BUILD_JOBS"] = exec_cmd(["getconf", "_NPROCESSORS_ONLN"]).strip()

  # Don't call the virtualenv pip directly, it uses a hashbang to to call the python
  # virtualenv using an absolute path. If the path to the virtualenv is very long, the
  # hashbang won't work.
  if is_py3:
    impala_pip_base_cmd = [os.path.join(venv_dir, "bin", "python3"),
                           os.path.join(venv_dir, "bin", "pip3"), "install", "-v"]
  else:
    impala_pip_base_cmd = [os.path.join(venv_dir, "bin", "python"),
                           os.path.join(venv_dir, "bin", "pip"), "install", "-v"]

  # Passes --no-binary for IMPALA-3767: without this, Cython (and
  # several other packages) fail download.
  #
  # --no-cache-dir is used to prevent caching of compiled artifacts, which may be built
  # with different compilers or settings.
  third_party_pkg_install_cmd = \
      impala_pip_base_cmd[:] + ["--no-binary", ":all:", "--no-cache-dir"]

  # When using a custom mirror, we also must use the index of that mirror.
  # The python 3 virtualenv has trouble with using --index-url with PYPI_MIRROR,
  # so it falls back to --no-index, which works fine.
  if "PYPI_MIRROR" in os.environ and not is_py3:
    third_party_pkg_install_cmd.extend(["--index-url",
                                        "%s/simple" % os.environ["PYPI_MIRROR"]])
  else:
    # Prevent fetching additional packages from the index. If we forget to add a package
    # to one of the requirements.txt files, this should trigger an error. However, we will
    # still access the index for version/dependency resolution, hence we need to change it
    # when using a private mirror.
    third_party_pkg_install_cmd.append("--no-index")

  third_party_pkg_install_cmd.extend(["--find-links",
      "file://%s" % pathname2url(os.path.abspath(DEPS_DIR))])
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


def download_toolchain_python(is_py3):
  '''Grabs the Python implementation from the Impala toolchain, using the machinery from
     bin/bootstrap_toolchain.py.
     Skip the download if SKIP_TOOLCHAIN_BOOTSTRAP=true in the environment. In that case
     only the presence of the Python executable is checked in the toolchain location.
  '''

  toolchain_packages_home = os.environ.get("IMPALA_TOOLCHAIN_PACKAGES_HOME")
  if not toolchain_packages_home:
    raise Exception("Impala environment not set up correctly, make sure "
        "$IMPALA_TOOLCHAIN_PACKAGES_HOME is set.")

  if is_py3:
    package = ToolchainPackage("python",
                               explicit_version=os.environ["IMPALA_PYTHON3_VERSION"])
  else:
    package = ToolchainPackage("python")
  if package.needs_download() and \
     not (os.environ.get(SKIP_TOOLCHAIN_BOOTSTRAP) == 'true'):
    package.download()
  if is_py3:
    python_cmd = os.path.join(package.pkg_directory(), "bin/python3")
  else:
    python_cmd = os.path.join(package.pkg_directory(), "bin/python")
  if not os.path.exists(python_cmd):
    raise Exception("Unexpected error bootstrapping python from toolchain: {0} does not "
                    "exist".format(python_cmd))
  return python_cmd


def install_deps(venv_dir, is_py3):
  py_str = "3" if is_py3 else "2"
  LOG.info("Installing setuptools into the python{0} virtualenv".format(py_str))
  exec_pip_install(venv_dir, is_py3, ["-r", SETUPTOOLS_REQS_PATH])
  cc = select_cc()
  if cc is None:
    raise Exception("CC not available")
  env = dict(os.environ)
  LOG.info("Installing packages into the python{0} virtualenv".format(py_str))
  exec_pip_install(venv_dir, is_py3, ["-r", REQS_PATH], cc=cc, env=env)
  mark_reqs_installed(venv_dir, REQS_PATH)


def have_toolchain():
  '''Return true if the Impala toolchain is available'''
  return "IMPALA_TOOLCHAIN_PACKAGES_HOME" in os.environ


def toolchain_pkg_dir(pkg_name):
  '''Return the path to the toolchain package'''
  pkg_version = os.environ["IMPALA_" + pkg_name.upper() + "_VERSION"]
  return os.path.join(os.environ["IMPALA_TOOLCHAIN_PACKAGES_HOME"],
      pkg_name + "-" + pkg_version)


def install_adls_deps(venv_dir, is_py3):
  # The ADLS dependencies require that the OS is at least CentOS 6.7 or above,
  # which is why we break this into a seperate step. If the target filesystem is
  # ADLS, the expectation is that the dev environment is running at least CentOS 6.7.
  if os.environ.get('TARGET_FILESYSTEM') == "adls":
    if reqs_are_installed(venv_dir, ADLS_REQS_PATH):
      LOG.debug("Skipping ADLS deps: matching adls-installed-requirements.txt found")
      return True
    cc = select_cc()
    assert cc is not None
    py_str = "3" if is_py3 else "2"
    LOG.info("Installing ADLS packages into the python{0} virtualenv".format(py_str))
    exec_pip_install(venv_dir, is_py3, ["-r", ADLS_REQS_PATH], cc=cc)
    mark_reqs_installed(venv_dir, ADLS_REQS_PATH)


def install_py_version_deps(venv_dir, is_py3):
  cc = select_cc()
  assert cc is not None
  if not is_py3:
    if not reqs_are_installed(venv_dir, PY2_REQS_PATH):
      # These are extra python2-only packages
      LOG.info("Installing python2 packages into the virtualenv")
      exec_pip_install(venv_dir, is_py3, ["-r", PY2_REQS_PATH], cc=cc)
      mark_reqs_installed(venv_dir, PY2_REQS_PATH)
  else:
    if not reqs_are_installed(venv_dir, PY3_REQS_PATH):
      # These are extra python3-only packages
      LOG.info("Installing python3 packages into the virtualenv")
      exec_pip_install(venv_dir, is_py3, ["-r", PY3_REQS_PATH], cc=cc)
      mark_reqs_installed(venv_dir, PY3_REQS_PATH)


def install_kudu_client_if_possible(venv_dir, is_py3):
  '''Installs the Kudu python module if possible, which depends on the toolchain and
  the compiled requirements in requirements.txt. If the toolchain isn't
  available, nothing will be done.'''
  if reqs_are_installed(venv_dir, KUDU_REQS_PATH):
    LOG.debug("Skipping Kudu: matching kudu-installed-requirements.txt found")
    return
  kudu_base_dir = os.environ["IMPALA_KUDU_HOME"]
  if not os.path.exists(kudu_base_dir):
    LOG.debug("Skipping Kudu: %s doesn't exist" % kudu_base_dir)
    return

  py_str = "3" if is_py3 else "2"
  LOG.info("Installing Kudu into the python{0} virtualenv".format(py_str))
  # The installation requires that KUDU_HOME/build/latest exists. An empty directory
  # structure will be made to satisfy that. The Kudu client headers and lib will be made
  # available through GCC environment variables.
  fake_kudu_build_dir = os.path.join(tempfile.gettempdir(),
                                     "virtualenv-kudu{0}".format(py_str))
  try:
    artifact_dir = os.path.join(fake_kudu_build_dir, "build", "latest")
    if not os.path.exists(artifact_dir):
      os.makedirs(artifact_dir)
    cc = select_cc()
    assert cc is not None
    env = dict(os.environ)
    env["KUDU_HOME"] = fake_kudu_build_dir
    kudu_client_dir = find_kudu_client_install_dir()
    # Copy the include directory to the fake build directory
    kudu_include_dir = os.path.join(kudu_client_dir, "include")
    shutil.copytree(kudu_include_dir,
                    os.path.join(fake_kudu_build_dir, "build", "latest", "src"))
    env["CPLUS_INCLUDE_PATH"] = os.path.join(kudu_client_dir, "include")
    env["LIBRARY_PATH"] = os.path.pathsep.join([os.path.join(kudu_client_dir, 'lib'),
                                                os.path.join(kudu_client_dir, 'lib64')])
    exec_pip_install(venv_dir, is_py3, ["-r", KUDU_REQS_PATH], cc=cc, env=env)
    mark_reqs_installed(venv_dir, KUDU_REQS_PATH)
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


def mark_reqs_installed(venv_dir, reqs_path):
  '''Mark that the requirements from the given file are installed by copying it into
  the root directory of the virtualenv.'''
  installed_reqs_path = os.path.join(venv_dir, os.path.basename(reqs_path))
  shutil.copyfile(reqs_path, installed_reqs_path)


def reqs_are_installed(venv_dir, reqs_path):
  '''Check if the requirements from the given file are installed in the virtualenv by
  looking for a matching requirements file in the root directory of the virtualenv.'''
  installed_reqs_path = os.path.join(venv_dir, os.path.basename(reqs_path))
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


def setup_virtualenv_if_not_exists(venv_dir, is_py3):
  if not (reqs_are_installed(venv_dir, REQS_PATH)):
    delete_virtualenv_if_exist(venv_dir)
    create_virtualenv(venv_dir, is_py3)
    install_deps(venv_dir, is_py3)
    LOG.debug("Virtualenv setup complete")


if __name__ == "__main__":
  parser = optparse.OptionParser()
  parser.add_option("-l", "--log-level", default="INFO",
      choices=("DEBUG", "INFO", "WARN", "ERROR"))
  parser.add_option("-r", "--rebuild", action="store_true", help="Force a rebuild of"
      " the virtualenv even if it exists and appears to be completely up-to-date.")
  parser.add_option("--print-ld-library-path", action="store_true", help="Print the"
      " LD_LIBRARY_PATH that should be used when running python from the virtualenv.")
  parser.add_option("--python3", action="store_true", help="Generate the python3"
      " virtualenv")
  options, args = parser.parse_args()

  if options.print_ld_library_path:
    # Some python packages have native code that is compiled with the toolchain
    # compiler, so that code needs to dynamically link against matching library
    # versions.
    ld_library_dirs = [os.path.join(toolchain_pkg_dir("gcc"), 'lib64')]
    kudu_client_dir = find_kudu_client_install_dir()
    ld_library_dirs.append(os.path.join(kudu_client_dir, 'lib'))
    ld_library_dirs.append(os.path.join(kudu_client_dir, 'lib64'))
    print(os.path.pathsep.join(ld_library_dirs))
    sys.exit()

  logging.basicConfig(level=getattr(logging, options.log_level))

  if options.python3:
    venv_dir = ENV_DIR_PY3
  else:
    venv_dir = ENV_DIR_PY2

  if options.rebuild:
    delete_virtualenv_if_exist(venv_dir)

  # Complete as many bootstrap steps as possible (see file comment for the steps).
  setup_virtualenv_if_not_exists(venv_dir, options.python3)
  install_kudu_client_if_possible(venv_dir, options.python3)
  install_adls_deps(venv_dir, options.python3)
  install_py_version_deps(venv_dir, options.python3)
