// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/security/test/mini_kdc.h"

#include <csignal>
#include <stdlib.h>

#include <limits>
#include <map>
#include <memory>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"

// test_util.h has a dependancy on gmock which Impala doesn't depend on, so we rewrite
// parts of this code that use test_util members.
//#include "kudu/util/test_util.h"

using std::map;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {

string MiniKdcOptions::ToString() const {
  return strings::Substitute("{ realm: $0, port: $1, data_root: $2 }", realm, port, data_root);
}

MiniKdc::MiniKdc()
    : MiniKdc(MiniKdcOptions()) {
}

MiniKdc::MiniKdc(const MiniKdcOptions& options)
    : options_(options) {
  if (options_.realm.empty()) {
    options_.realm = "KRBTEST.COM";
  }
  if (options_.data_root.empty()) {
    // We hardcode "/tmp" here since the original function which initializes a random test
    // directory (GetTestDataDirectory()), depends on gmock.
    options_.data_root = JoinPathSegments("/tmp", "krb5kdc");
  }
  if (options_.renew_lifetime.empty()) {
    options_.renew_lifetime = "7d";
  }
  if (options_.ticket_lifetime.empty()) {
    options_.ticket_lifetime = "24h";
  }
}

MiniKdc::~MiniKdc() {
  if (kdc_process_) {
    WARN_NOT_OK(Stop(), "Unable to stop MiniKdc");
  }
}

map<string, string> MiniKdc::GetEnvVars() const {
  return {
    {"KRB5_CONFIG", JoinPathSegments(options_.data_root, "krb5.conf")},
    {"KRB5_KDC_PROFILE", JoinPathSegments(options_.data_root, "kdc.conf")},
    {"KRB5CCNAME", JoinPathSegments(options_.data_root, "krb5cc")}
  };
}

vector<string> MiniKdc::MakeArgv(const vector<string>& in_argv) {
  vector<string> real_argv = { "env" };
  for (const auto& p : GetEnvVars()) {
    real_argv.push_back(Substitute("$0=$1", p.first, p.second));
  }
  for (const string& a : in_argv) {
    real_argv.push_back(a);
  }
  return real_argv;
}

namespace {
// Attempts to find the path to the specified Kerberos binary, storing it in 'path'.
Status GetBinaryPath(const string& binary,
                     const vector<string>& search,
                     string* path) {
  string p;

  // First, check specified locations which are sometimes not on the PATH.
  // This is necessary to check first so that the system Heimdal kerberos
  // binaries won't be found first on OS X.
  for (const auto& location : search) {
    p = JoinPathSegments(location, binary);
    if (Env::Default()->FileExists(p)) {
      *path = p;
      return Status::OK();
    }
  }

  // Next check if the binary is on the PATH.
  Status s = Subprocess::Call({ "which", binary }, "", &p);
  if (s.ok()) {
    StripTrailingNewline(&p);
    *path = p;
    return Status::OK();
  }

  return Status::NotFound("Unable to find binary", binary);
}

// Attempts to find the path to the specified Kerberos binary, storing it in 'path'.
Status GetBinaryPath(const string& binary, string* path) {
  static const vector<string> kCommonLocations = {
    "/usr/local/opt/krb5/sbin", // Homebrew
    "/usr/local/opt/krb5/bin", // Homebrew
    "/opt/local/sbin", // Macports
    "/opt/local/bin", // Macports
    "/usr/lib/mit/sbin", // SLES
    "/usr/sbin", // Linux
  };
  return GetBinaryPath(binary, kCommonLocations, path);
}
} // namespace


Status MiniKdc::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, "starting KDC");
  CHECK(!kdc_process_);
  VLOG(1) << "Starting Kerberos KDC: " << options_.ToString();

  if (!Env::Default()->FileExists(options_.data_root)) {
    VLOG(1) << "Creating KDC database and configuration files";
    RETURN_NOT_OK(Env::Default()->CreateDir(options_.data_root));

    RETURN_NOT_OK(CreateKdcConf());
    RETURN_NOT_OK(CreateKrb5Conf());

    // Create the KDC database using the kdb5_util tool.
    string kdb5_util_bin;
    RETURN_NOT_OK(GetBinaryPath("kdb5_util", &kdb5_util_bin));

    RETURN_NOT_OK(Subprocess::Call(MakeArgv({
        kdb5_util_bin, "create",
        "-s", // Stash the master password.
        "-P", "masterpw", // Set a password.
        "-W", // Use weak entropy (since we don't need real security).
    })));
  }

  // Start the Kerberos KDC.
  string krb5kdc_bin;
  RETURN_NOT_OK(GetBinaryPath("krb5kdc", &krb5kdc_bin));

  kdc_process_.reset(new Subprocess(
      MakeArgv({
      krb5kdc_bin,
      "-n", // Do not daemonize.
  })));

  RETURN_NOT_OK(kdc_process_->Start());

  const bool need_config_update = (options_.port == 0);
  // Wait for KDC to start listening on its ports and commencing operation.
  RETURN_NOT_OK(WaitForKdcPorts());

  if (need_config_update) {
    // If we asked for an ephemeral port, grab the actual ports and
    // rewrite the configuration so that clients can connect.
    RETURN_NOT_OK(CreateKrb5Conf());
    RETURN_NOT_OK(CreateKdcConf());
  }

  return Status::OK();
}

Status MiniKdc::Stop() {
  CHECK(kdc_process_);
  VLOG(1) << "Stopping KDC";
  unique_ptr<Subprocess> proc(kdc_process_.release());
  RETURN_NOT_OK(proc->Kill(SIGKILL));
  RETURN_NOT_OK(proc->Wait());

  return Status::OK();
}

// Creates a kdc.conf file according to the provided options.
Status MiniKdc::CreateKdcConf() const {
  static const string kFileTemplate = R"(
[kdcdefaults]
kdc_ports = $2
kdc_tcp_ports = ""

[realms]
$1 = {
        acl_file = $0/kadm5.acl
        admin_keytab = $0/kadm5.keytab
        database_name = $0/principal
        key_stash_file = $0/.k5.$1
        max_renewable_life = 7d 0h 0m 0s
}
  )";
  string file_contents = strings::Substitute(kFileTemplate, options_.data_root,
                                             options_.realm, options_.port);
  return WriteStringToFile(Env::Default(), file_contents,
                           JoinPathSegments(options_.data_root, "kdc.conf"));
}

// Creates a krb5.conf file according to the provided options.
Status MiniKdc::CreateKrb5Conf() const {
  static const string kFileTemplate = R"(
[logging]
    kdc = FILE:/dev/stderr

[libdefaults]
    default_realm = $1
    dns_lookup_kdc = false
    dns_lookup_realm = false
    forwardable = true
    renew_lifetime = $2
    ticket_lifetime = $3

    # In miniclusters, we start daemons on local loopback IPs that
    # have no reverse DNS entries. So, disable reverse DNS.
    rdns = false

    # The server side will start its GSSAPI server using the local FQDN.
    # However, in tests, we connect to it via a non-matching loopback IP.
    # This enables us to connect despite that mismatch.
    ignore_acceptor_hostname = true

[realms]
    $1 = {
        kdc = 127.0.0.1:$0
        # This super-arcane syntax can be found documented in various Hadoop
        # vendors' security guides and very briefly in the MIT krb5 docs.
        # Basically, this one says to map anyone coming in as foo@OTHERREALM.COM
        # and map them to a local user 'other-foo'
        auth_to_local = RULE:[1:other-$$1@$$0](.*@OTHERREALM.COM$$)s/@.*//
    }
  )";
  string file_contents = strings::Substitute(kFileTemplate, options_.port, options_.realm,
                                             options_.renew_lifetime, options_.ticket_lifetime);
  return WriteStringToFile(Env::Default(), file_contents,
                           JoinPathSegments(options_.data_root, "krb5.conf"));
}

Status MiniKdc::WaitForKdcPorts() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, Substitute("waiting for KDC ports"));
  // We have to use 'lsof' to figure out which ports the KDC bound to if we
  // requested ephemeral ones. The KDC doesn't log the bound port or expose it
  // in any other fashion, and re-implementing lsof involves parsing a lot of
  // files in /proc/. So, requiring lsof for tests and parsing its output seems
  // more straight-forward. We call lsof in a loop in case the kdc is slow to
  // bind to the ports.

  string lsof;
  RETURN_NOT_OK(GetBinaryPath("lsof", {"/sbin", "/usr/sbin"}, &lsof));

  const vector<string> cmd = {
    lsof, "-wbnP", "-Ffn",
    "-p", std::to_string(kdc_process_->pid()),
    "-a", "-i", "4UDP"};

  string lsof_out;
  for (int i = 1; ; i++) {
    lsof_out.clear();
    Status s = Subprocess::Call(cmd, "", &lsof_out);

    if (s.ok()) {
      StripTrailingNewline(&lsof_out);
      break;
    } else if (i > 10) {
      return s;
    }

    SleepFor(MonoDelta::FromMilliseconds(i * i));
  }

  // The '-Ffn' flag gets lsof to output something like:
  //   p19730
  //   f123
  //   n*:41254
  // The first line is the pid. We ignore it.
  // The second line is the file descriptor number. We ignore it.
  // The third line has the bind address and port.
  vector<string> lines = strings::Split(lsof_out, "\n");
  int32_t port = -1;
  if (lines.size() != 3 ||
      lines[2].substr(0, 3) != "n*:" ||
      !safe_strto32(lines[2].substr(3), &port) ||
      port <= 0) {
    return Status::RuntimeError("unexpected lsof output", lsof_out);
  }
  CHECK(port > 0 && port < std::numeric_limits<uint16_t>::max())
      << "parsed invalid port: " << port;
  VLOG(1) << "Determined bound KDC port: " << port;
  if (options_.port == 0) {
    options_.port = port;
  } else {
    // Sanity check: if KDC's port is already established, it's supposed to be
    // written into the configuration files, so the process must bind to the
    // already established port.
    CHECK(options_.port == port);
  }
  return Status::OK();
}

Status MiniKdc::CreateUserPrincipal(const string& username) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, Substitute("creating user principal $0", username));
  string kadmin;
  RETURN_NOT_OK(GetBinaryPath("kadmin.local", &kadmin));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({
          kadmin, "-q", Substitute("add_principal -pw $0 $0", username)})));
  return Status::OK();
}

Status MiniKdc::CreateServiceKeytab(const string& spn,
                                    string* path) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, Substitute("creating service keytab for $0", spn));
  string kt_path = spn;
  StripString(&kt_path, "/", '_');
  kt_path = JoinPathSegments(options_.data_root, kt_path) + ".keytab";

  string kadmin;
  RETURN_NOT_OK(GetBinaryPath("kadmin.local", &kadmin));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({
          kadmin, "-q", Substitute("add_principal -randkey $0", spn)})));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({
          kadmin, "-q", Substitute("ktadd -k $0 $1", kt_path, spn)})));
  *path = kt_path;
  return Status::OK();
}

Status MiniKdc::Kinit(const string& username) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, Substitute("kinit for $0", username));
  string kinit;
  RETURN_NOT_OK(GetBinaryPath("kinit", &kinit));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({ kinit, username }), username));
  return Status::OK();
}

Status MiniKdc::Kdestroy() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, "kdestroy");
  string kdestroy;
  RETURN_NOT_OK(GetBinaryPath("kdestroy", &kdestroy));
  return Subprocess::Call(MakeArgv({ kdestroy, "-A" }));
}

Status MiniKdc::Klist(string* output) {
  string klist;
  RETURN_NOT_OK(GetBinaryPath("klist", &klist));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({ klist, "-A" }), "", output));
  return Status::OK();
}

Status MiniKdc::KlistKeytab(const string& keytab_path, string* output) {
  string klist;
  RETURN_NOT_OK(GetBinaryPath("klist", &klist));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({ klist, "-k", keytab_path }), "", output));
  return Status::OK();
}

Status MiniKdc::SetKrb5Environment() const {
  if (!kdc_process_) {
    return Status::IllegalState("KDC not started");
  }
  for (const auto& p : GetEnvVars()) {
    CHECK_ERR(setenv(p.first.c_str(), p.second.c_str(), 1 /*overwrite*/));
  }

  return Status::OK();
}

} // namespace kudu
