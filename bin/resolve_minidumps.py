#!/usr/bin/env ambari-python-wrap
#
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
# This script automates symbol resolution for Breakpad minidumps
# under ideal circumstances. Specifically, it expects all the
# binaries to be in the same locations as when the minidump
# was taken. This is often true for minidumps on a developer
# workstation or at the end of an Impala test job. It finds Breakpad
# using environment variables from the Impala dev environment,
# so it must run inside the Impala dev environment.
# TODO: It may be possible to extend this to Docker images.
#
# Within this simple context, this script aims for complete
# symbol resolution. It uses Breakpad's minidump_dump utility
# to dump the minidump, then it parses the list of libraries
# that were used by the binary. It gets the symbols for all
# those libraries and resolves the minidump.
#
# Usage: resolve_minidumps.py --minidump_file [file] --output_file [file]
# (optional -v or --verbose for more output)

import errno
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback

from argparse import ArgumentParser


class ModuleInfo:
  def __init__(self, code_file, code_id, debug_file, debug_id):
    self.code_file = code_file
    self.code_id = code_id
    self.debug_file = debug_file
    self.debug_id = debug_id


def read_module_info(minidump_dump_contents):
  """Read the module information out of the minidump_dump raw contents.
  This is expecting 'minidump_dump_contents' to be the minidump_dump
  contents for the minidump split by newlines.
  This will return a list of ModuleInfo objects.
  """
  # Find the module_count
  for idx, line in enumerate(minidump_dump_contents):
    if line.strip().startswith("module_count"):
      module_count = int(line.split("=")[1].strip())
      break

  # The minidump has a MDRawModule per module and it will have
  # the same number of MDRawModule dumps as module_count.
  module_boundaries = []
  for idx, line in enumerate(minidump_dump_contents):
    if line.startswith("MDRawModule"):
      module_boundaries.append(idx)

  if len(module_boundaries) != module_count:
    logging.error("Failed to parse modules, mismatch in module count "
                  "({0} != {1})".format(len(module_boundaries), module_count))
    return None

  # Add one more entry to module_boundaries that is the end of the file
  # That makes this more of a list of boundaries than the list of
  # start locations.
  module_boundaries.append(len(minidump_dump_contents))

  modules = []
  for module_idx in range(module_count):
    module_start = module_boundaries[module_idx]
    module_end = module_boundaries[module_idx + 1]

    # Find the code_file
    code_file = None
    code_identifier = None
    debug_file = None
    debug_identifier = None
    for line in minidump_dump_contents[module_start:module_end]:
      if line.find("code_file") != -1:
        code_file = line.split("=")[1].strip().strip('"')
      elif line.find("code_identifier") != -1:
        code_identifier = line.split("=")[1].strip().strip('"')
      elif line.find("debug_file") != -1:
        debug_file = line.split("=")[1].strip().strip('"')
      elif line.find("debug_identifier") != -1:
        debug_identifier = line.split("=")[1].strip().strip('"')

    # Important: it is ok for the fields to be the zero-length string.
    # We just care that they are non-None (i.e. the loop above encountered
    # them and parsed a value).
    if code_file is None or code_identifier is None or debug_file is None or \
       debug_identifier is None:
      logging.error("Failed to parse dump output, missing fields for MDRawModule "
                    "{0}".format(module_idx))
      return None

    # Jars and other files show up in this list, but they have
    # code identifiers or debug identifiers as all zeros. Skip those,
    # as there are no symbols to find.
    if re.fullmatch("[0]+", code_identifier) or re.fullmatch("[0]+", debug_identifier):
      continue

    # Skip cases where the code identifier or debug identifier are null
    if len(code_identifier) == 0 or len(debug_identifier) == 0:
      continue

    # linux-gate.so is a special case, and it is not an actual file on disk.
    if code_file.startswith("linux-gate.so"):
      continue

    modules.append(ModuleInfo(code_file, code_identifier, debug_file, debug_identifier))

  return modules


def filter_shared_library_modules(module_list, lib_allow_list):
  """Filter the list of modules by eliminating any shared libaries that do not match
  one of the prefixes in the allow list. This keeps all non-shared libaries
  (such as the main binary).
  """
  filtered_module_list = []
  for module in module_list:
    code_file_basename = os.path.basename(module.code_file)
    # Keep anything that is not a shared library (e.g. the main binary)
    if ".so" not in code_file_basename:
      filtered_module_list.append(module)
      continue
    # Only keep shared libraries that match an entry on the allow list.
    for allow_lib in lib_allow_list:
      if code_file_basename.startswith(allow_lib):
        filtered_module_list.append(module)
        break
  return filtered_module_list


def find_breakpad_home():
  """Locate the Breakpad home directory.

  We try to locate the package in the Impala toolchain folder.
  """
  toolchain_packages_home = os.environ.get('IMPALA_TOOLCHAIN_PACKAGES_HOME')
  if not toolchain_packages_home:
    logging.error("IMPALA_TOOLCHAIN_PACKAGES_HOME is not set")
    return None

  if not os.path.isdir(toolchain_packages_home):
    logging.error("Could not find toolchain packages directory")
    return None
  breakpad_version = os.environ.get('IMPALA_BREAKPAD_VERSION')
  if not breakpad_version:
    logging.error("Could not determine breakpad version from toolchain")
    return None
  breakpad_dir = '{0}/breakpad-{1}'.format(toolchain_packages_home, breakpad_version)
  if not os.path.isdir(breakpad_dir):
    logging.error("Could not find breakpad directory")
    return None

  return breakpad_dir


def find_breakpad_binary(binary_name):
  """Locate the specified Breadpad binary"""
  breakpad_home = find_breakpad_home()
  if not breakpad_home:
    return None

  binary_path = os.path.join(breakpad_home, 'bin', binary_name)
  if not os.path.isfile(binary_path):
    logging.error("Could not find {0} executable at {1}".format(binary_name, binary_path))
    return None

  return binary_path


def find_objcopy_binary():
  """Locate the 'objcopy' binary from Binutils.

  We try to locate the package in the Impala toolchain folder.
  TODO: Fall back to finding objcopy in the system path.
  """
  toolchain_packages_home = os.environ.get('IMPALA_TOOLCHAIN_PACKAGES_HOME')
  if not toolchain_packages_home:
    logging.error("IMPALA_TOOLCHAIN_PACKAGES_HOME is not set")
    return None

  if not os.path.isdir(toolchain_packages_home):
    logging.error("Could not find toolchain packages directory")
    return None
  binutils_version = os.environ.get('IMPALA_BINUTILS_VERSION')
  if not binutils_version:
    logging.error("Could not determine binutils version from toolchain")
    return None
  binutils_dir = "binutils-{0}".format(binutils_version)
  objcopy = os.path.join(toolchain_packages_home, binutils_dir, 'bin', 'objcopy')
  if not os.path.isfile(objcopy):
    logging.error("Could not find objcopy executable at {0}".format(objcopy))
    return None
  return objcopy


def ensure_dir_exists(path):
  """Make sure the directory 'path' exists in a thread-safe way."""
  try:
    os.makedirs(path)
  except OSError as e:
    if e.errno != errno.EEXIST or not os.path.isdir(path):
      raise e


def dump_symbols_for_binary(dump_syms, objcopy, binary, out_dir):
  """Dump symbols of a single binary file and move the result.

  Symbols will be extracted to a temporary file and moved into place afterwards. Required
  directories will be created if necessary.
  """
  logging.info("Processing binary file: {0}".format(binary))
  ensure_dir_exists(out_dir)
  # tmp_fd will be closed when the file object created by os.fdopen() below gets
  # destroyed.
  tmp_fd, tmp_file = tempfile.mkstemp(dir=out_dir, suffix='.sym')
  try:
    # Create a temporary directory used for decompressing debug info
    tempdir = tempfile.mkdtemp()

    # Binaries can contain compressed debug symbols. Breakpad currently
    # does not support dumping symbols for binaries with compressed debug
    # symbols.
    #
    # As a workaround, this uses objcopy to create a copy of the binary with
    # the debug symbols decompressed. If the debug symbols are not compressed
    # in the original binary, objcopy simply makes a copy of the binary.
    # Breakpad is able to read symbols from the decompressed binary, and
    # those symbols work correctly in resolving a minidump from the original
    # compressed binary.
    # TODO: In theory, this could work with the binary.debug_path.
    binary_basename = os.path.basename(binary)
    decompressed_binary = os.path.join(tempdir, binary_basename)
    objcopy_retcode = subprocess.call([objcopy, "--decompress-debug-sections",
                                       binary, decompressed_binary])

    # Run dump_syms on the binary
    # If objcopy failed for some reason, fall back to running dump_syms
    # directly on the original binary. This is unlikely to work, but it is a way of
    # guaranteeing that objcopy is not the problem.
    args = [dump_syms, decompressed_binary]
    if objcopy_retcode != 0:
      sys.stderr.write('objcopy failed. Trying to run dump_sym directly.\n')
      args = [dump_syms, binary]

    # Run dump_syms on the binary.
    proc = subprocess.Popen(args, stdout=os.fdopen(tmp_fd, 'wb'), stderr=subprocess.PIPE)
    _, stderr = proc.communicate()
    if proc.returncode != 0:
      sys.stderr.write('Failed to dump symbols from %s, return code %s\n' %
          (binary, proc.returncode))
      sys.stderr.write(stderr.decode('utf-8'))
      os.remove(tmp_file)
      return False
    # Parse the temporary file to determine the full target path.
    with open(tmp_file, 'r') as f:
      header = f.readline().strip()
      # Format of header is: MODULE os arch binary_id binary
      _, _, _, binary_id, binary = header.split(' ')
      out_path = os.path.join(out_dir, binary, binary_id)
      ensure_dir_exists(out_path)
    # Move the temporary file to its final destination.
    shutil.move(tmp_file, os.path.join(out_path, '%s.sym' % binary))
  except Exception as e:
    # Only need to clean up in case of errors.
    try:
      os.remove(tmp_file)
    except EnvironmentError:
      pass
    raise e
  finally:
    # Cleanup temporary directory
    shutil.rmtree(tempdir)
  return True


def dump_symbols_for_all_modules(dump_syms, objcopy, module_list, out_dir):
  """Given a list of modules (ModuleInfo objects), dump symbols for
  each library listed.
  """
  for module in module_list:
    success = dump_symbols_for_binary(dump_syms, objcopy, module.code_file, out_dir)
    if not success:
      logging.warning("Failed to dump symbols for {0}".format(module.code_file))


def resolve_minidump(minidump_stackwalk, minidump_path, symbol_dir, verbose, out_file):
  minidump_stackwalk_cmd = [minidump_stackwalk, minidump_path, symbol_dir]
  # There are circumstances where the minidump_stackwalk can go wrong and become
  # a runaway process capable of using all system memory. If the prlimit utility
  # is present, we use it to apply a limit on the memory consumption.
  #
  # See if we have the prlimit utility
  check_prlimit = subprocess.run(["prlimit", "-V"], stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL)
  if check_prlimit.returncode == 0:
    # The prlimit utility is available, so wrap the minidump_stackwalk command
    # to apply a 4GB limit on virtual memory. In normal operations, 4G is plenty.
    prlimit_wrapper = ["prlimit", "--as={0}".format(4 * 1024 * 1024 * 1024)]
    minidump_stackwalk_cmd = prlimit_wrapper + minidump_stackwalk_cmd
  with open(out_file, "w") as out_f:
    stderr_output = None if verbose else subprocess.DEVNULL
    subprocess.run(minidump_stackwalk_cmd, stdout=out_f,
                   stderr=stderr_output, check=True)


def raw_dump_for_minidump(minidump_dump, minidump_path):
  """Run minidump_dump on the specified minidump and split the output into lines"""
  # minidump_dump sometimes returns an error code even though it produced usable output.
  # So, this doesn't check the error code, and it relies on read_module_info() doing
  # validation.
  #
  # Python 3.6 adjustments:
  # 'capture_output=True' not supported: set stdout/stderr to subprocess.PIPE instead
  # 'text=True' not supported: set 'universal_newlines=True' (the two are the same thing)
  output = subprocess.run([minidump_dump, minidump_path], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, universal_newlines=True)
  return output.stdout.split('\n')


def parse_args():
  """Parse command line arguments and perform sanity checks."""
  # TODO:
  #  - Add ability to specify Breakpad home
  #  - Add ability to specify the symbol directory location (for reuse)
  #  - Add ability to specify Binutils home
  parser = ArgumentParser()
  parser.add_argument('--minidump_file', required=True)
  parser.add_argument('--output_file', required=True)
  parser.add_argument('-v', '--verbose', action='store_true')
  parser.add_argument('--safe_library_list',
      default="libstdc++.so,libc.so,libjvm.so",
      help="Comma-separate list of prefixes for allowed system libraries")
  args = parser.parse_args()
  return args


def dump_syms_and_resolve_stack(modules, minidump_file, output_file, verbose):
  """Dump the symbols for the listed modules and use them to resolve the minidump."""
  # Create a temporary directory to store the symbols
  # This automatically gets cleaned up
  with tempfile.TemporaryDirectory() as tmp_dir:
    # Dump symbols for all the modules into this temporary directory.
    # Need both dump_syms and objcopy
    dump_syms_bin = find_breakpad_binary("dump_syms")
    if not dump_syms_bin:
      logging.error("Could not find Breakpad dump_syms binary")
      sys.exit(1)
    objcopy_bin = find_objcopy_binary()
    if not objcopy_bin:
      logging.error("Could not find Binutils objcopy binary")
      sys.exit(1)
    dump_symbols_for_all_modules(dump_syms_bin, objcopy_bin, modules, tmp_dir)

    # Resolve the minidump with the temporary symbol directory
    minidump_stackwalk_bin = find_breakpad_binary("minidump_stackwalk")
    if not minidump_stackwalk_bin:
      logging.error("Could not find Breakpad minidump_stackwalk binary")
      sys.exit(1)
    resolve_minidump(find_breakpad_binary("minidump_stackwalk"), minidump_file,
                     tmp_dir, verbose, output_file)


def main():
  args = parse_args()

  if args.verbose:
    logging.basicConfig(level=logging.INFO)
  else:
    logging.basicConfig(level=logging.WARNING)

  # Step 1: Get the raw dump for the specified minidump
  minidump_dump_bin = find_breakpad_binary("minidump_dump")
  if not minidump_dump_bin:
    logging.error("Could not find Breakpad minidump_dump binary")
    sys.exit(1)
  contents = raw_dump_for_minidump(minidump_dump_bin, args.minidump_file)
  if not contents:
    logging.error(
      "minidump_dump could not get the contents of {0}".format(args.minidump_file))
    sys.exit(1)

  # Step 2: Parse the raw dump to get the list of code modules
  # This is the list of things that have symbols we need to dump.
  modules = read_module_info(contents)
  if not modules:
    logging.error("Failed to read modules for {0}".format(args.minidump_file))
    sys.exit(1)

  # Step 3: Dump the symbols and use them to resolve the minidump
  # Sometimes there are libraries with corrupt/problematic symbols
  # that can cause minidump_stackwalk to go haywire and use excessive
  # memory. First, we try using symbols from all of the shared libraries.
  # If that fails, we fallback to using a "safe" list of shared libraries.
  try:
    # Dump the symbols and use them to resolve the minidump
    dump_syms_and_resolve_stack(modules, args.minidump_file, args.output_file,
                                args.verbose)
    return
  except Exception:
    logging.warning("Encountered error: {0}".format(traceback.format_exc()))
    logging.warning("Falling back to resolution using the safe library list")
    logging.warning("Safe library list: {0}".format(args.safe_library_list))

  # Limit the shared libraries to the "safe" list of shared libraries and
  # try again.
  if len(args.safe_library_list) == 0:
    safe_library_list = []
  else:
    safe_library_list = args.safe_library_list.split(",")
  safe_modules = filter_shared_library_modules(modules, safe_library_list)
  dump_syms_and_resolve_stack(safe_modules, args.minidump_file, args.output_file,
                              args.verbose)


if __name__ == "__main__":
  main()
