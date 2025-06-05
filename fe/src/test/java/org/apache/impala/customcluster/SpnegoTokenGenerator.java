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

import org.ietf.jgss.*;

import java.io.FileOutputStream;
import java.io.IOException;

public class SpnegoTokenGenerator {
  public static void main(String[] args) {
    try {
      if (args.length < 1) {
        System.err.println("Missing argument: <output-token-file>");
        System.exit(1);
      }
      String outputPath = args[0];
      // OID for Kerberos V5
      Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");

      // Create GSSManager
      GSSManager manager = GSSManager.getInstance();

      // This OID corresponds to NT_KRB5_PRINCIPAL
      Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");

      // Full service principal with realm
      String servicePrincipal = "impala/localhost@myorg.com";

      // Create GSSName with full principal name
      GSSName serverName = manager.createName(servicePrincipal, krb5PrincipalOid);

      // Create security context
      GSSContext context = manager.createContext(
          serverName,
          krb5Oid,
          null, // use default credentials from ccache
          GSSContext.DEFAULT_LIFETIME
      );

      // Initiate the context, which triggers ticket acquisition
      context.requestMutualAuth(true);
      context.requestCredDeleg(false);

      byte[] token = context.initSecContext(new byte[0], 0, 0);
      if (token != null) {
        try {
          FileOutputStream fos = new FileOutputStream(outputPath);
          fos.write(token);
          System.out.println("Token written to " + outputPath);
        } catch (IOException e) {
          System.err.println("Failed to write token to file: " + e.getMessage());
          e.printStackTrace();
          System.exit(1);
        }
      } else {
        System.err.println("Failed to obtain SPNEGO token.");
        System.exit(1);
      }
      context.dispose();
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
