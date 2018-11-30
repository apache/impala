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

package org.apache.impala.testutil;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.impala.authorization.sentry.SentryConfig;
import org.apache.impala.authorization.User;
import org.apache.impala.authorization.sentry.SentryPolicyService;
import org.apache.log4j.Level;
import org.apache.sentry.core.common.transport.SentryTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class that issues a read-only RPC to the Sentry Service to check if it
 * is online. Attempts to ping the Sentry Service a user specified number of times
 * and returns a non-zero error code if the RPC never succeeds, otherwise returns
 * 0.
 */
public class SentryServicePinger {
  private final static Logger LOG =
      LoggerFactory.getLogger(SentryServicePinger.class);

  // Suppress warnings from OptionBuilder.
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    // Programmatically disable Sentry Thrift logging since Sentry error logging can be
    // pretty noisy and verbose.
    org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getLogger(
        SentryTransportFactory.class.getPackage().getName());
    logger4j.setLevel(Level.OFF);

    // Parse command line options to get config file path.
    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt("config_file")
        .withDescription("Absolute path to a sentry-site.xml config file (string)")
        .hasArg()
        .withArgName("CONFIG_FILE")
        .isRequired()
        .create('c'));
    options.addOption(OptionBuilder.withLongOpt("num_pings")
        .withDescription("Max number of pings to try before failing (int)")
        .hasArg()
        .isRequired()
        .withArgName("NUM_PINGS")
        .create('n'));
    options.addOption(OptionBuilder.withLongOpt("sleep_secs")
        .withDescription("Time (s) to sleep between pings (int)")
        .hasArg()
        .withArgName("SLEEP_SECS")
        .create('s'));
    BasicParser optionParser = new BasicParser();
    CommandLine cmdArgs = optionParser.parse(options, args);

    SentryConfig sentryConfig = new SentryConfig(cmdArgs.getOptionValue("config_file"));
    int numPings = Integer.parseInt(cmdArgs.getOptionValue("num_pings"));
    int maxPings = numPings;
    int sleepSecs = Integer.parseInt(cmdArgs.getOptionValue("sleep_secs"));

    sentryConfig.loadConfig();
    Exception exception = null;
    while (numPings > 0) {
      SentryPolicyService policyService = new SentryPolicyService(sentryConfig);
      try {
        policyService.listAllRoles(new User(System.getProperty("user.name")));
        LOG.info("Sentry Service ping succeeded.");
        System.exit(0);
      } catch (Exception e) {
        exception = e;
        LOG.error(String.format("Error issuing RPC to Sentry Service (attempt %d/%d)",
            maxPings - numPings + 1, maxPings));
        Thread.sleep(sleepSecs * 1000);
      }
      --numPings;
    }
    if (exception != null) {
      LOG.error("Error starting Sentry Service: ", exception);
    }
    System.exit(1);
  }
}
