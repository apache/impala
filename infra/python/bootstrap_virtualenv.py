# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
#
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

# This module will create a python virtual env and install external dependencies. If
# the virtualenv already exists and the list of dependencies matches the list of
# installed dependencies, nothing will be done.
#
# This module can be run with python >= 2.4 but python >= 2.6 must be installed on the
# system. If the default 'python' command refers to < 2.6, python 2.6 will be used
# instead.

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

# Generated using "pip install --download <DIR> -r requirements.txt"
REQS_PATH = os.path.join(DEPS_DIR, "requirements.txt")

# After installing, the requirements.txt will be copied into the virtualenv to
# record what was installed.
INSTALLED_REQS_PATH = os.path.join(ENV_DIR, "installed-requirements.txt")


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


def exec_pip_install(args, **popen_kwargs):
  # Don't call the virtualenv pip directly, it uses a hashbang to to call the python
  # virtualenv using an absolute path. If the path to the virtualenv is very long, the
  # hashbang won't work.
  #
  # Passes --no-binary for IMPALA-3767: without this, Cython (and
  # several other packages) fail download.
  exec_cmd([os.path.join(ENV_DIR, "bin", "python"), os.path.join(ENV_DIR, "bin", "pip"),
    "install", "--no-binary", "--no-index", "--find-links",
    "file://%s" % urllib.pathname2url(os.path.abspath(DEPS_DIR))] + args, **popen_kwargs)


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
  shutil.copyfile(REQS_PATH, INSTALLED_REQS_PATH)


def install_kudu_client_if_possible():
  """Installs the Kudu python module if possible. The Kudu module is the only one that
     requires the toolchain. If the toolchain isn't in use or hasn't been populated
     yet, nothing will be done. Also nothing will be done if the Kudu client lib required
     by the module isn't available (as determined by KUDU_IS_SUPPORTED).
  """
  if os.environ["KUDU_IS_SUPPORTED"] != "true":
    LOG.debug("Skipping Kudu: Kudu is not supported")
    return
  impala_toolchain_dir = os.environ.get("IMPALA_TOOLCHAIN")
  if not impala_toolchain_dir:
    LOG.debug("Skipping Kudu: IMPALA_TOOLCHAIN not set")
    return
  toolchain_kudu_dir = os.path.join(
      impala_toolchain_dir, "kudu-" + os.environ["IMPALA_KUDU_VERSION"])
  if not os.path.exists(toolchain_kudu_dir):
    LOG.debug("Skipping Kudu: %s doesn't exist" % toolchain_kudu_dir)
    return

  # The "pip" command could be used to provide the version of Kudu installed (if any)
  # but it's a little too slow. Running the virtualenv python to detect the installed
  # version is faster.
  actual_version_string = exec_cmd([os.path.join(ENV_DIR, "bin", "python"), "-c",
      textwrap.dedent("""
      try:
        import kudu
        print kudu.__version__
      except ImportError:
        pass""")]).strip()
  actual_version = [int(v) for v in actual_version_string.split(".") if v]

  reqs_file = open(REQS_PATH)
  try:
    for line in reqs_file:
      if not line.startswith("# kudu-python=="):
        continue
      expected_version_string = line.split()[1].split("==")[1]
      break
    else:
      raise Exception("Unable to find kudu-python version in requirements file")
  finally:
    reqs_file.close()
  expected_version = [int(v) for v in expected_version_string.split(".")]

  if actual_version and actual_version == expected_version:
    LOG.debug("Skipping Kudu: Installed %s == required %s"
        % (actual_version_string, expected_version_string))
    return
  LOG.debug("Kudu installation required. Actual version %s. Required version %s.",
      actual_version, expected_version)

  LOG.info("Installing Kudu into the virtualenv")
  # The installation requires that KUDU_HOME/build/latest exists. An empty directory
  # structure will be made to satisfy that. The Kudu client headers and lib will be made
  # available through GCC environment variables.
  fake_kudu_build_dir = os.path.join(tempfile.gettempdir(), "virtualenv-kudu")
  try:
    artifact_dir = os.path.join(fake_kudu_build_dir, "build", "latest")
    if not os.path.exists(artifact_dir):
      os.makedirs(artifact_dir)
    env = dict(os.environ)
    env["KUDU_HOME"] = fake_kudu_build_dir
    kudu_client_dir = find_kudu_client_install_dir()
    env["CPLUS_INCLUDE_PATH"] = os.path.join(kudu_client_dir, "include")
    env["LIBRARY_PATH"] = os.path.pathsep.join([os.path.join(kudu_client_dir, 'lib'),
                                                os.path.join(kudu_client_dir, 'lib64')])

    exec_pip_install(["kudu-python==" + expected_version_string], env=env)
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
    kudu_base_dir = os.path.join(os.environ["IMPALA_TOOLCHAIN"],
        "kudu-%s" % os.environ["IMPALA_KUDU_VERSION"])
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


def deps_are_installed():
  if not os.path.exists(INSTALLED_REQS_PATH):
    return False
  installed_reqs_file = open(INSTALLED_REQS_PATH)
  try:
    reqs_file = open(REQS_PATH)
    try:
      if reqs_file.read() == installed_reqs_file.read():
        return True
      else:
        LOG.info("Virtualenv upgrade needed")
        return False
    finally:
      reqs_file.close()
  finally:
    installed_reqs_file.close()


def setup_virtualenv_if_not_exists():
  if not deps_are_installed():
    delete_virtualenv_if_exist()
    create_virtualenv()
    install_deps()
    LOG.info("Virtualenv setup complete")


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
    print os.path.pathsep.join([os.path.join(kudu_client_dir, 'lib'),
                                os.path.join(kudu_client_dir, 'lib64')])
    sys.exit()

  logging.basicConfig(level=getattr(logging, options.log_level))
  if options.rebuild:
    delete_virtualenv_if_exist()
  setup_virtualenv_if_not_exists()
  install_kudu_client_if_possible()
