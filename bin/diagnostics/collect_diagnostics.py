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

from __future__ import absolute_import, division, print_function
import argparse
import datetime
import errno
import getpass
import glob
import logging
import math
import os
import shutil
import subprocess
import sys
import tarfile
import time
import tempfile

from collections import namedtuple
from contextlib import closing
from struct import Struct
from threading import Timer

# This script is for automating the collection of following diagnostics from a host
# running an Impala service daemon (catalogd/statestored/impalad). Following diagnostics
# are supported.
#
# 1. Native core dump (+ shared libs)
# 2. GDB/Java thread dump (pstack + jstack)
# 3. Java heap dump (jmap)
# 4. Minidumps (using breakpad)
# 5. Profiles
#
# Dependencies:
# 1. gdb package should be installed to collect native thread stacks/coredump. The binary
#    location is picked up from the system path. In case of pstacks, the script falls back
#    to the breakpad minidumps if the 'pstack' binary is not in system path.
# 2. jstack/jmap from a JRE/JDK. Default location is picked up from system path but can be
#    overriden with --java_home PATH_TO_JAVA_HOME.
# 3. Mindumps are collected by sending a SIGUSR1 signal to the Impala process. Impala
#    versions without full breakpad support (<= release 2.6) will reliably crash if
#    we attempt to do that since those versions do not have the corresponding signal
#    handler. Hence it is suggested to run this script only on releases 2.7 and later.
# 4. python >= 2.6
#
# Usage: python collect_diagnostics.py --help
#
# Few example usages:
#
# Collect 3 jstacks, pstacks from an impalad process 3s apart.
#  python collect_diagnostics.py --pid $(pidof impalad) --stacks 3 3
#
# Collect core dump and a Java heapdump from the catalogd process
#  python collect_diagnostics.py --pid $(pidof impalad) --jmap --gcore
#
# Collect 5 breakpad minidumps from a statestored process 5s apart.
#  python collect_diagnostics.py --pid $(pidof statestored) --minidumps 5 5
#      --minidumps_dir /var/log/impala-minidumps
#
#
class Command(object):
  """Wrapper around subprocess.Popen() that is canceled after a configurable timeout."""
  def __init__(self, cmd, timeout=30):
    self.cmd = cmd
    self.timeout = timeout
    self.child_killed_by_timeout = False

  def run(self, cmd_stdin=None, cmd_stdout=subprocess.PIPE):
    """Runs the command 'cmd' by setting the appropriate stdin/out. The command is killed
    if hits a timeout (controlled by self.timeout)."""
    cmd_string = " ".join(self.cmd)
    logging.info("Starting command %s with a timeout of %s"
        % (cmd_string, str(self.timeout)))
    self.child = subprocess.Popen(self.cmd, stdin=cmd_stdin, stdout=cmd_stdout,
        universal_newlines=True)
    timer = Timer(self.timeout, self.kill_child)
    try:
      timer.start()
      # self.stdout is set to None if cmd_stdout is anything other than PIPE. The actual
      # stdout is written to the file corresponding to cmd_stdout.
      self.stdout = self.child.communicate()[0]
      if self.child.returncode == 0:
        logging.info("Command finished successfully: " + cmd_string)
      else:
        cmd_status = "timed out" if self.child_killed_by_timeout else "failed"
        logging.error("Command %s: %s" % (cmd_status, cmd_string))
      return self.child.returncode
    finally:
      timer.cancel()
    return -1

  def kill_child(self):
    """Kills the running command (self.child)."""
    self.child_killed_by_timeout = True
    self.child.kill()

class ImpalaDiagnosticsHandler(object):
  IMPALA_PROCESSES = ["impalad", "catalogd", "statestored"]
  OUTPUT_DIRS_TO_CREATE = ["stacks", "gcores", "jmaps", "profiles",
      "shared_libs", "minidumps"]
  MINIDUMP_HEADER = namedtuple("MDRawHeader", "signature version stream_count \
      stream_directory_rva checksum time_date_stamp flags")

  def __init__(self, args):
    """Initializes the state by setting the paths of required executables."""
    self.args = args
    if args.pid <= 0:
      return

    self.script_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    # Name of the Impala process for which diagnostics should be collected.
    self.target_process_name = self.get_target_process_name()

    self.minidump_search_path = os.path.join(self.args.minidumps_dir,
        self.target_process_name)

    self.java_home = self.get_java_home_from_env()
    if not self.java_home and args.java_home:
      self.java_home = os.path.abspath(args.java_home)
    self.jstack_cmd = os.path.join(self.java_home, "bin/jstack")
    self.java_cmd = os.path.join(self.java_home, "bin/java")
    self.jmap_cmd = os.path.join(self.java_home, "bin/jmap")

    self.gdb_cmd = self.get_command_from_path("gdb")
    self.gcore_cmd = self.get_command_from_path("gcore")
    self.pstack_cmd = self.get_command_from_path("pstack")

  def create_output_dir_structure(self):
    """Creates the skeleton directory structure for the diagnostics output collection."""
    self.collection_root_dir = tempfile.mkdtemp(prefix="impala-diagnostics-%s" %
        datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-"),
        dir=os.path.abspath(self.args.output_dir))
    for dirname in self.OUTPUT_DIRS_TO_CREATE:
      os.mkdir(os.path.join(self.collection_root_dir, dirname))

  def get_command_from_path(self, cmd):
    """Returns the path to a given command executable, if one exists in the
    system PATH."""
    for path in os.environ["PATH"].split(os.pathsep):
      cmd_path = os.path.join(path, cmd)
      if os.access(cmd_path, os.X_OK):
        return cmd_path
    return ""

  def get_target_process_name(self):
    """Returns the process name of the target process for which diagnostics
    should be collected."""
    try:
      return open("/proc/%s/comm" % self.args.pid).read().strip()
    except Exception:
      logging.exception("Failed to get target process name.")
      return ""

  def get_num_child_proc(self, name):
    """Returns number of processes with the given name and target Impala pid
    as parent."""
    # Not all pgrep versions support -c parameter. So fetch the stdout and
    # count the number of items in the list.
    cmd = Command(["pgrep", "-P", str(self.args.pid), name])
    cmd.run()
    return len(cmd.stdout.split("\n")) - 1

  def get_java_home_from_env(self):
    """Returns JAVA_HOME set in the env of the target process."""
    try:
      envs = open("/proc/%s/environ" % self.args.pid).read().split("\0")
      for s in envs:
        k, v = s.split("=", 1)
        if k == "JAVA_HOME":
          return v
    except Exception:
      logging.exception("Failed to determine JAVA_HOME from proc env.")
      return ""

  def get_free_disk_space_gbs(self, path):
    """Returns free disk space (in GBs) of the partition hosting the given path."""
    s = os.statvfs(path)
    return (s.f_bsize * s.f_bavail)/(1024.0 * 1024.0 * 1024.0)

  def get_minidump_create_timestamp(self, minidump_path):
    """Returns the unix timestamp of the minidump create time. It is extracted from
    the minidump header."""
    # Read the minidump's header to extract the create time stamp. More information about
    # the mindump header format can be found here: https://goo.gl/uxKZVe
    #
    # typedef struct {
    #   uint32_t  signature;
    #   uint32_t  version;
    #   uint32_t  stream_count;
    #   MDRVA     stream_directory_rva;  /* A |stream_count|-sized array of
    #                                     * MDRawDirectory structures. */
    #   uint32_t  checksum;              /* Can be 0.  In fact, that's all that's
    #                                     * been found in minidump files. */
    #   uint32_t  time_date_stamp;       /* time_t */
    #   uint64_t  flags;
    # } MDRawHeader;  /* MINIDUMP_HEADER */
    s = Struct("IIIiIIQ")
    data = open(minidump_path, "rb").read(s.size)
    header = self.MINIDUMP_HEADER(*s.unpack_from(data))
    return header.time_date_stamp

  def wait_for_minidump(self):
    """Minidump collection is async after sending the SIGUSR1 signal. So this method
    waits till it is written to the disk. Since minidump forks off a new process from
    the parent Impala process we need to wait till the forked process exits.
    Returns after 30s to prevent infinite waiting. Should be called after sending the
    SIGUSR1 signal to the Impala process."""
    MAX_WAIT_TIME_S = 30
    start_time = time.time()
    while time.time() < start_time + MAX_WAIT_TIME_S:
      # Sleep for a bit to ensure that the process fork to write minidump has started.
      # Otherwise the subsequent check on the process count could pass even when the
      # fork didn't succeed. This sleep reduces the likelihood of such race.
      time.sleep(1)
      if self.get_num_child_proc(self.target_process_name) == 0:
        break
    return

  def validate_args(self):
    """Returns True if self.args are valid, false otherwise"""
    if self.args.pid <= 0:
      logging.critical("Invalid PID provided.")
      return False

    if self.target_process_name not in self.IMPALA_PROCESSES:
      logging.critical("No valid Impala process with the given PID %s" % str(self.args.pid))
      return False

    if not self.java_home:
      logging.critical("JAVA_HOME could not be inferred from process env.\
          Please specify --java_home.")
      return False

    if self.args.jmap and not os.path.exists(self.jmap_cmd):
      logging.critical("jmap binary not found, required to collect a Java heap dump.")
      return False

    if self.args.gcore and not os.path.exists(self.gcore_cmd):
      logging.critical("gcore binary not found, required to collect a core dump.")
      return False

    if self.args.profiles_dir and not os.path.isdir(self.args.profiles_dir):
      logging.critical("No valid profiles directory at path: %s" % self.args.profiles_dir)
      return False

    return True

  def collect_thread_stacks(self):
    """Collects jstack/jstack-m/pstack for the given pid in that order. pstack collection
    falls back to minidumps if pstack binary is missing from the system path. Minidumps
    are collected by sending a SIGUSR1 to the Impala process and then archiving the
    contents of the minidump directory. The number of times stacks are collected and the
    sleep time between the collections are controlled by --stacks argument."""
    stacks_count, stacks_interval_secs = self.args.stacks
    if stacks_count <= 0 or stacks_interval_secs < 0:
      return

    # Skip jstack collection if the jstack binary does not exist.
    skip_jstacks = not os.path.exists(self.jstack_cmd)
    if skip_jstacks:
      logging.info("Skipping jstack collection since jstack binary couldn't be located.")

    # Fallback to breakpad minidump collection if pstack binaries are missing.
    fallback_to_minidump = False
    if not self.pstack_cmd:
      # Fall back to collecting a minidump if pstack is not installed.
      if not os.path.exists(self.minidump_search_path):
        logging.info("Skipping pstacks since pstack binary couldn't be located. Provide "
            + "--minidumps_dir for collecting minidumps instead.")
        # At this point, we can't proceed since we have nothing to collect.
        if skip_jstacks:
          return
      else:
        fallback_to_minidump = True;
        logging.info("Collecting breakpad minidumps since pstack/gdb binaries are " +
            "missing.")

    stacks_dir = os.path.join(self.collection_root_dir, "stacks")
    # Populate the commands to run in 'cmds_to_run' depending on what kinds of thread
    # stacks to collect. Each entry is a tuple of form
    # (Command, stdout_prefix, is_minidump). 'is_minidump' tells whether the command
    # is trying to trigger a minidump collection.
    cmds_to_run = []
    if not skip_jstacks:
      cmd_args = [self.jstack_cmd, str(self.args.pid)]
      cmds_to_run.append((Command(cmd_args, self.args.timeout), "jstack", False))
      # Collect mixed-mode jstack, contains native stack frames.
      cmd_args_mixed_mode = [self.jstack_cmd, "-m", str(self.args.pid)]
      cmds_to_run.append(
          (Command(cmd_args_mixed_mode, self.args.timeout), "jstack-m", False))

    if fallback_to_minidump:
      cmd_args = ["kill", "-SIGUSR1", str(self.args.pid)]
      cmds_to_run.append((Command(cmd_args, self.args.timeout), None, True))
    elif self.pstack_cmd:
      cmd_args = [self.pstack_cmd, str(self.args.pid)]
      cmds_to_run.append((Command(cmd_args, self.args.timeout), "pstack", False))

    collection_start_ts = time.time()
    for i in range(stacks_count):
      for cmd, file_prefix, is_minidump in cmds_to_run:
        if file_prefix:
          stdout_file = os.path.join(stacks_dir, file_prefix + "-" + str(i) + ".txt")
          with open(stdout_file, "w") as output:
            cmd.run(cmd_stdout=output)
        else:
          cmd.run()
          # Incase of minidump collection, wait for it to be written.
          if is_minidump:
            self.wait_for_minidump()
      time.sleep(stacks_interval_secs)

    # Copy minidumps if required.
    if fallback_to_minidump:
      minidump_out_dir =  os.path.join(self.collection_root_dir, "minidumps")
      self.copy_minidumps(minidump_out_dir, collection_start_ts);

  def collect_minidumps(self):
    """Collects minidumps on the Impala process based on argument --minidumps. The
    minidumps are collected by sending a SIGUSR1 signal to the Impala process and then
    the resulting minidumps are copied to the target directory."""
    minidump_count, minidump_interval_secs = self.args.minidumps
    if minidump_count <= 0 or minidump_interval_secs < 0:
      return
    # Impala process writes a minidump when it encounters a SIGUSR1.
    cmd_args = ["kill", "-SIGUSR1", str(self.args.pid)]
    cmd = Command(cmd_args, self.args.timeout)
    collection_start_ts = time.time()
    for i in range(minidump_count):
      cmd.run()
      self.wait_for_minidump()
      time.sleep(minidump_interval_secs)
    out_dir = os.path.join(self.collection_root_dir, "minidumps")
    self.copy_minidumps(out_dir, collection_start_ts);

  def copy_minidumps(self, target, start_ts):
    """Copies mindumps with create time >= start_ts to 'target' directory."""
    logging.info("Copying minidumps from %s to %s with ctime >= %s"
        % (self.minidump_search_path, target, start_ts))
    for filename in glob.glob(os.path.join(self.minidump_search_path, "*.dmp")):
      try:
        minidump_ctime = self.get_minidump_create_timestamp(filename)
        if minidump_ctime >= math.floor(start_ts):
          shutil.copy2(filename, target)
        else:
          logging.info("Ignored mindump: %s ctime: %s" % (filename, minidump_ctime))
      except Exception:
        logging.exception("Error processing minidump at path: %s. Skipping it." % filename)

  def collect_java_heapdump(self):
    """Generates the Java heap dump of the Impala process using the 'jmap' command."""
    if not self.args.jmap:
      return
    jmap_dir = os.path.join(self.collection_root_dir, "jmaps")
    out_file = os.path.join(jmap_dir, self.target_process_name + "_heap.bin")
    # jmap command requires it to be run as the process owner.
    # Command: jmap -dump:format=b,file=<outfile> <pid>
    cmd_args = [self.jmap_cmd, "-dump:format=b,file=" + out_file, str(self.args.pid)]
    Command(cmd_args, self.args.timeout).run()

  def collect_native_coredump(self):
    """Generates the core dump of the Impala process using the 'gcore' command"""
    if not self.args.gcore:
      return
    # Command: gcore -o <outfile> <pid>
    gcore_dir = os.path.join(self.collection_root_dir, "gcores")
    out_file_name = self.target_process_name + "-" +\
        datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + ".core"
    out_file = os.path.join(gcore_dir, out_file_name)
    cmd_args = [self.gcore_cmd, "-o", out_file, str(self.args.pid)]
    Command(cmd_args, self.args.timeout).run()

  def collect_query_profiles(self):
    """Collects Impala query profiles from --profiles_dir. Enforces an uncompressed limit
    of --profiles_max_size_limit bytes on the copied profile logs."""
    if not self.args.profiles_dir:
      return
    out_dir = os.path.join(self.collection_root_dir, "profiles")
    # Hardcoded in Impala
    PROFILE_LOG_FILE_PATTERN = "impala_profile_log_1.1-*";
    logging.info("Collecting profile data, limiting size to %f GB" %
        (self.args.profiles_max_size_limit/(1024 * 1024 * 1024)))

    profiles_path = os.path.join(self.args.profiles_dir, PROFILE_LOG_FILE_PATTERN)
    # Sort the profiles by creation time and copy the most recent ones in that order.
    sorted_profiles =\
        sorted(glob.iglob(profiles_path), key=os.path.getctime, reverse=True)
    profile_size_included_so_far = 0
    for profile_path in sorted_profiles:
      try:
        file_size = os.path.getsize(profile_path)
        if file_size == 0:
          continue
        if profile_size_included_so_far + file_size > self.args.profiles_max_size_limit:
          # Copying the whole file violates profiles_max_size_limit. Copy a part of it.
          # Profile logs are newline delimited with a single profile per line.
          num_bytes_to_copy =\
              self.args.profiles_max_size_limit - profile_size_included_so_far
          file_name = os.path.basename(profile_path)
          copied_bytes = 0
          with open(profile_path, "rb") as in_file:
            with open(os.path.join(out_dir, file_name), "wb") as out_file:
              for line in in_file.readlines():
                if copied_bytes + len(line) > num_bytes_to_copy:
                  break
                out_file.write(line)
                copied_bytes += len(line)
          return
        profile_size_included_so_far += file_size
        shutil.copy2(profile_path, out_dir)
      except:
        logging.exception("Encountered an error while collecting profile %s. Skipping it."
            % profile_path)

  def collect_shared_libs(self):
    """Collects shared libraries loaded by the target Impala process."""
    # Shared libs are collected if either of core dump or minidumps are enabled.
    if not (self.args.gcore or self.args.minidumps_dir):
      return
    # If gdb binary is missing, we cannot extract the shared library list
    if not self.gdb_cmd:
      logging.info("'gdb' executable missing. Skipping shared library collection.")
      return

    out_dir = os.path.join(self.collection_root_dir, "shared_libs")

    script_path = os.path.join(self.script_dir, "collect_shared_libs.sh")
    cmd_args = [script_path, self.gdb_cmd, str(self.args.pid), out_dir]
    Command(cmd_args, self.args.timeout).run()

  def archive_diagnostics(self):
    """Creates a gztar of the collected diagnostics and cleans up the original
    directory. Returns True if successful, False otherwise."""
    try:
      # tarfile does not support context managers in python 2.6. We use closing() to work
      # around that.
      with closing(tarfile.open(self.collection_root_dir + '.tar.gz', mode='w:gz')) as\
          archive:
        # collection_root_dir is an absoulte path. There is no point in preserving its
        # entire directory structure in the archive, so set the arcname accordingly.
        archive.add(self.collection_root_dir,
            arcname=os.path.basename(self.collection_root_dir))
      return True
    except Exception:
      logging.exception("Encountered an exception archiving diagnostics, cleaning up.")
      return False
    finally:
      self.cleanup()

  def cleanup(self):
    """Cleans up the directory to which diagnostics were written."""
    shutil.rmtree(self.collection_root_dir, ignore_errors=True)

  def get_diagnostics(self):
    """Calls all collect_*() methods to collect diagnostics. Returns True if no errors
    were encountered during diagnostics collection, False otherwise."""
    if not self.validate_args():
      return False
    logging.info("Using JAVA_HOME: %s" % self.java_home)
    self.create_output_dir_structure()
    logging.info("Free disk space: %.2fGB" %
        self.get_free_disk_space_gbs(self.collection_root_dir))
    os.chdir(self.args.output_dir)
    collection_methods = [self.collect_shared_libs, self.collect_query_profiles,
        self.collect_native_coredump, self.collect_java_heapdump, self.collect_minidumps,
        self.collect_thread_stacks]
    exception_encountered = False
    for method in collection_methods:
      try:
        method()
      except IOError as e:
        if e.errno == errno.ENOSPC:
          # Clean up and abort if we are low on disk space. Other IOErrors are logged and
          # ignored.
          logging.exception("Disk space low, aborting.")
          self.cleanup()
          return False
        logging.exception("Encountered an IOError calling: %s" % method.__name__)
        exception_encountered = True
      except Exception:
        exception_encountered = True
        logging.exception("Encountered an exception calling: %s" % method.__name__)
    if exception_encountered:
      logging.error("Encountered an exception collecting diagnostics. Final output " +
          "could be partial.\n")
    # Archive the directory, even if it is partial.
    archive_path = self.collection_root_dir + ".tar.gz"
    logging.info("Archiving diagnostics to path: %s" % archive_path)
    if self.archive_diagnostics():
      logging.info("Diagnostics collected at path: %s" % archive_path)
    return not exception_encountered

def get_args_parser():
  """Creates the argument parser and adds the flags"""
  parser = argparse.ArgumentParser(
      description="Impala diagnostics collection",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("--pid", required=True, action="store", dest="pid", type=int,
      default=0, help="PID of the Impala process for which to collect diagnostics.")
  parser.add_argument("--java_home", action="store", dest="java_home", default="",
      help="If not set, it is set to the JAVA_HOME from the pid's environment.")
  parser.add_argument("--timeout", action="store", dest="timeout", default=300,
      type=int, help="Timeout (in seconds) for each of the diagnostics commands")
  parser.add_argument("--stacks", action="store", dest="stacks", nargs=2, type=int,
      default=[0, 0], metavar=("COUNT", "INTERVAL (in seconds)"),
      help="Collect jstack, mixed-mode jstack and pstacks of the Impala process.\
      Breakpad minidumps are collected in case of missing pstack binaries.")
  parser.add_argument("--jmap", action="store_true", dest="jmap", default=False,
      help="Collect heap dump of the Java process")
  parser.add_argument("--gcore", action="store_true", dest="gcore", default=False,
      help="Collect the native core dump using gdb. Requires gdb to be installed.")
  parser.add_argument("--minidumps", action="store", dest="minidumps", type=int,
      nargs=2, default=[0, 0], metavar=("COUNT", "INTERVAL (in seconds)"),
      help="Collect breakpad minidumps for the Impala process. Requires --minidumps_dir\
      be set.")
  parser.add_argument("--minidumps_dir", action="store", dest="minidumps_dir", default="",
      help="Path of the directory to which Impala process' minidumps are written. Looks\
      for minidumps in this path's subdirectory that is named after the target process\
      name.")
  parser.add_argument("--profiles_dir", action="store", dest="profiles_dir", default="",
      help="Path of the profiles directory to be included in the diagnostics output.")
  parser.add_argument("--profiles_max_size_limit", action="store",
      dest="profiles_max_size_limit", default=3 * 1024 * 1024 * 1024, type=float,
      help="Uncompressed limit (in Bytes) on profile logs collected from --profiles_dir.")
  parser.add_argument("--output_dir", action="store", dest="output_dir",
      default = tempfile.gettempdir(), help="Output directory that contains the final "
      "diagnostics data. Defaults to %s" % tempfile.gettempdir())
  return parser

if __name__ == "__main__":
  parser = get_args_parser()
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, datefmt="%Y-%m-%d %H:%M:%S",
      format="%(asctime)s %(levelname)-8s %(message)s")
  diagnostics_handler = ImpalaDiagnosticsHandler(parser.parse_args())
  logging.info("Running as user: %s" % getpass.getuser())
  logging.info("Input args: %s" % " ".join(sys.argv))
  sys.exit(0 if diagnostics_handler.get_diagnostics() else 1)
