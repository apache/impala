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


def exec_cmd(args):
  '''Executes a command and waits for it to finish, raises an exception if the return
     status is not zero.

     'args' uses the same format as subprocess.Popen().
  '''
  process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  output = process.communicate()[0]
  if process.returncode != 0:
    raise Exception("Command returned non-zero status\nCommand: %s\nOutput: %s"
        % (args, output))


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
  LOG.info("Installing packages into virtualenv")
  # Don't call the virtualenv pip directly, it uses a hashbang to to call the python
  # virtualenv using an absolute path. If the path to the virtualenv is very long, the
  # hashbang won't work.
  # --no-cache-dir is used because the dev version of Impyla may be the same even though
  # the contents are different. Since the version doesn't change, pip may use its cached
  # build.
  exec_cmd([os.path.join(ENV_DIR, "bin", "python"), os.path.join(ENV_DIR, "bin", "pip"),
    "install", "--no-cache-dir", "--no-index", "--find-links",
    "file://%s" % urllib.pathname2url(os.path.abspath(DEPS_DIR)), "-r", REQS_PATH])
  shutil.copyfile(REQS_PATH, INSTALLED_REQS_PATH)


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
  logging.basicConfig(level=logging.INFO)
  parser = optparse.OptionParser()
  parser.add_option("-r", "--rebuild", action="store_true", help="Force a rebuild of"
      " the virtualenv even if it exists and appears to be completely up-to-date.")
  options, args = parser.parse_args()
  if options.rebuild:
    delete_virtualenv_if_exist()
  setup_virtualenv_if_not_exists()
