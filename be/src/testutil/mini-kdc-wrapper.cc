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

#include "testutil/mini-kdc-wrapper.h"

#include <string>

#include "common/names.h"
#include "exec/kudu-util.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "rpc/auth-provider.h"
#include "util/filesystem-util.h"

using namespace impala;
namespace filesystem = boost::filesystem;
using filesystem::path;

DECLARE_bool(use_kudu_kinit);

DECLARE_string(keytab_file);
DECLARE_string(principal);
DECLARE_string(be_principal);
DECLARE_string(krb5_conf);

Status MiniKdcWrapper::StartKdc(string keytab_dir) {
  kudu::MiniKdcOptions options;
  options.realm = realm_;
  options.data_root = keytab_dir;
  options.ticket_lifetime = ticket_lifetime_;
  options.renew_lifetime = renew_lifetime_;
  options.port = kdc_port_;

  DCHECK(kdc_.get() == nullptr);
  kdc_.reset(new kudu::MiniKdc(options));
  DCHECK(kdc_.get() != nullptr);
  KUDU_RETURN_IF_ERROR(kdc_->Start(), "Failed to start KDC.");
  KUDU_RETURN_IF_ERROR(kdc_->SetKrb5Environment(), "Failed to set Kerberos environment.");
  return Status::OK();
}

Status MiniKdcWrapper::StopKdc() {
  KUDU_RETURN_IF_ERROR(kdc_->Stop(), "Failed to stop KDC.");
  return Status::OK();
}

Status MiniKdcWrapper::CreateServiceKeytab(const string& spn, string* kt_path) {
  KUDU_RETURN_IF_ERROR(kdc_->CreateServiceKeytab(spn, kt_path),
      "Failed to create service keytab.");
  return Status::OK();
}

Status MiniKdcWrapper::SetupAndStartMiniKDC(KerberosSwitch k) {
  if (k != KERBEROS_OFF) {
    // Enable the workaround for MIT krb5 1.10 bugs from krb5_realm_override.cc.
    setenv("KUDU_ENABLE_KRB5_REALM_FIX", "true", 0);

    FLAGS_use_kudu_kinit = k == USE_KUDU_KERBEROS;

    // Check if the unique directory already exists, and create it if it doesn't.
    RETURN_IF_ERROR(FileSystemUtil::RemoveAndCreateDirectory(unique_test_dir_.string()));
    string keytab_dir = unique_test_dir_.string() + "/krb5kdc";

    RETURN_IF_ERROR(StartKdc(keytab_dir));

    string kt_path;
    RETURN_IF_ERROR(CreateServiceKeytab(spn_, &kt_path));

    // Set the appropriate flags based on how we've set up the kerberos environment.
    FLAGS_krb5_conf = strings::Substitute("$0/$1", keytab_dir, "krb5.conf");
    FLAGS_keytab_file = kt_path;

    // We explicitly set 'principal' and 'be_principal' even though 'principal' won't be
    // used to test IMPALA-6256.
    FLAGS_principal = "dummy-service/host@realm";
    FLAGS_be_principal = strings::Substitute("$0@$1", spn_, realm_);
  }
  return Status::OK();
}

Status MiniKdcWrapper::TearDownMiniKDC(KerberosSwitch k) {
  if (k != KERBEROS_OFF) {
    RETURN_IF_ERROR(StopKdc());

    // Clear the flags so we don't step on other tests that may run in the same process.
    FLAGS_keytab_file.clear();
    FLAGS_principal.clear();
    FLAGS_be_principal.clear();
    FLAGS_krb5_conf.clear();

    // Remove test directory.
    RETURN_IF_ERROR(FileSystemUtil::RemovePaths({unique_test_dir_.string()}));
  }
  return Status::OK();
}
