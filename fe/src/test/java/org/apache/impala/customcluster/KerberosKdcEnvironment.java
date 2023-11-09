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

package org.apache.impala.customcluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.KrbClient;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.kerberos.kerb.type.ticket.TgtTicket;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Package protected helper class to encapsulate simple
 * Kerberos KDC server used by junit tests.
 */
class KerberosKdcEnvironment extends ExternalResource {

  private final Logger LOG =
          Logger.getLogger(KerberosKdcEnvironment.class);

  private final static String realm = "myorg.com";
  private final static String servicePrincipal =
          String.format("impala/localhost@%s", realm);

  private final TemporaryFolder testFolder;
  private SimpleKdcServer kerbyServer;

  public KerberosKdcEnvironment(TemporaryFolder testFolder) {
    this.testFolder = testFolder;
  }

  @Override
  protected void before() throws Throwable {
    testFolder.create();

    kerbyServer = new SimpleKdcServer();
    kerbyServer.setKdcRealm(realm);
    kerbyServer.setAllowUdp(false);
    kerbyServer.setWorkDir(testFolder.getRoot());
    kerbyServer.init();

    // Create service principal and keytab file for impala components
    kerbyServer.createPrincipal(servicePrincipal, "password");
    File keytabFile = new File(getServiceKeytabFilePath());
    kerbyServer.exportPrincipal(servicePrincipal, keytabFile);

    kerbyServer.start();
  }

  @Override
  protected void after() {
    try {
      kerbyServer.stop();
    } catch (KrbException e) {
      LOG.error("An exception received while stopping KDC server", e);
    }
  }

  public String getServicePrincipal() {
    return servicePrincipal;
  }

  public String getServicePrincipal(String service, String hostname) {
    return String.format("%s/%s@%s", service, hostname, realm);
  }

  public String getServiceKeytabFilePath() throws IOException {
    return new File(kerbyServer.getWorkDir().getCanonicalPath() + "/impala.keytab")
            .getCanonicalPath();
  }

  public String getKeytabFilePathWithServicePrincipal(String service, String hostname)
          throws IOException, KrbException {
    String servicePrincipal = getServicePrincipal(service, hostname);

    String keytabFileName = String.format("%s.%s.keytab", service, hostname);
    File keytabFile = new File(kerbyServer.getWorkDir().getCanonicalPath() +
            "/" + keytabFileName);

    kerbyServer.createPrincipal(servicePrincipal, "password");
    kerbyServer.exportPrincipal(servicePrincipal, keytabFile);

    return keytabFile.getCanonicalPath();
  }

  public String getKrb5ConfigPath() throws IOException {
    return kerbyServer.getWorkDir().getCanonicalPath() + "/krb5.conf";
  }

  public String getUserPrincipal(String user) {
    return String.format("%s@%s", user, realm);
  }

  public String createUserPrincipalAndCredentialsCache(String username)
          throws KrbException, IOException {
    String userPrincipal = getUserPrincipal(username);
    deleteUserPrincipalIfExists(userPrincipal);
    kerbyServer.createPrincipal(userPrincipal, "password");

    File credentialsCache = testFolder.newFile();

    KrbClient krbClient = new KrbClient(kerbyServer.getWorkDir());
    krbClient.init();
    TgtTicket tgt = krbClient.requestTgt(username, "password" );
    krbClient.storeTicket(tgt, credentialsCache);

    return credentialsCache.getCanonicalPath();
  }

  private void deleteUserPrincipalIfExists(String userPrincipal )
          throws KrbException {
    if (kerbyServer.getIdentityService().getIdentity(userPrincipal) != null) {
      kerbyServer.deletePrincipal(userPrincipal);
    }
  }

  public Map<String, String> getKerberosAuthFlags()
          throws IOException {
    return ImmutableMap.of(
            "principal", getServicePrincipal(), // enables Kerberos auth
            "keytab_file", getServiceKeytabFilePath()
    );
  }

  public Map<String, String> getKerberosAuthFlagsWithCustomServicePrincipal(
          String service, String hostname) throws IOException, KrbException {
    return ImmutableMap.of(
            "principal", getServicePrincipal(service, hostname), // enables Kerberos auth
            "keytab_file", getKeytabFilePathWithServicePrincipal(service, hostname),

            // To use the internal Kerberos authentication, the impala daemons must be
            // started with a service principal that has a valid hostname,
            // which in this test environment can be practically only "localhost",
            // but here we want to specify a unique hostname,
            // so we need to disable internal Kerberos authentication.
            // Another, more complicated option would be to put more service principals
            // in the keytab file above and specify the "be_principal" flag accordingly.
            "skip_internal_kerberos_auth", "true"
    );
  }

  private Map<String, String> getClusterEnv() throws IOException {
    String krb5Config = getKrb5ConfigPath();
    return ImmutableMap.of(
            "KRB5_CONFIG", krb5Config,
            "KRB5_KDC_PROFILE", krb5Config
    );
  }

  private String overrideKrbCcNameFlag(String args) throws IOException {
    // in order to use a unique credentials cache file we need to
    // override krb5_ccname flag
    return String.format("%s --krb5_ccname=%s", args,
            testFolder.newFile().getCanonicalPath());
  }

  public int startImpalaClusterWithArgs(String args)
          throws IOException, InterruptedException {
    // Note: To avoid race conditions between impala components each daemon uses
    // its own unique krb credentials cache file when creating krb cc at initial kinit.
    // For the same reason, the cluster size is limited to 1 instance of each daemon type.
    return CustomClusterRunner.StartImpalaCluster(
            overrideKrbCcNameFlag(args), // impalad args
            overrideKrbCcNameFlag(args), // catalogd args
            overrideKrbCcNameFlag(args), // statestored args
            getClusterEnv(),
            "--cluster_size=1");
  }

  public String[] getImpalaShellEnv(String credentialsCacheFilePath) throws IOException {
    List<String> envList =
            System.getenv().entrySet().stream()
                    .map(entry -> String.format("%s=%s", entry.getKey(),
                            entry.getValue()))
                    .collect(Collectors.toList());

    // Kerberos environment variables defined for impala-shell process:
    // KRB5_CONFIG and KRB5_KDC_PROFILE env variables refer to the Kerberos configuration,
    // KRB5CCNAME env variable refers to the credentials cache file of the provided user.
    String krb5ConfigPath = getKrb5ConfigPath();
    envList.addAll(
            ImmutableList.of(
                    String.format("KRB5_CONFIG=%s", krb5ConfigPath),
                    String.format("KRB5_KDC_PROFILE=%s", krb5ConfigPath),
                    String.format("KRB5CCNAME=FILE:%s", credentialsCacheFilePath)));
    return envList.toArray(new String[0]);
  }

}
