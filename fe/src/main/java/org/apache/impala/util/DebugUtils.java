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

package org.apache.impala.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the DebugAction equivalent from the backend (see DebugActionImpl in
 * debug-util.cc). This is useful to execute certain debug actions (like Sleep, Jitter)
 * which can be executed from the code. The debug actions are passed to the CatalogService
 * using a query option (debug_action).
 */
public class DebugUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DebugUtils.class);
  private static final Random random = new Random();

  // debug action label for introducing HDFS listing delay during listFiles or statuses.
  public static final String REFRESH_HDFS_LISTING_DELAY
      = "catalogd_refresh_hdfs_listing_delay";

  // debug action label for introducing delay in alter table recover partitions command.
  public static final String RECOVER_PARTITIONS_DELAY = "catalogd_table_recover_delay";

  // debug action label for introducing delay in update stats command.
  public static final String UPDATE_STATS_DELAY = "catalogd_update_stats_delay";

  /**
   * Given list of debug actions, execute the debug action pertaining to the given label.
   * The debugActions string is of the format specified for the query_option/configuration
   * debug_actions. It is generally of the format
   * LABEL:ACTION@ACTION_PARAMS|LABEL:ACTION@ACTION_PARAMS.
   * For example, if the debug action configuration is:
   * CATALOGD_HDFS_LISTING_DELAY:SLEEP@100|CATALOGD_HMS_RPC_DELAY:JITTER@100@0.2
   * Then a when a label "CATALOGD_HDFS_LISTING_DELAY" is provided, this method will sleep
   * for 100 milli-seconds. If the label CATALOGD_HMS_RPC_DELAY is provided, this method
   * will sleep for a random value between 1-100 milli-seconds with a probability of 0.2.
   *
   * @param debugActions the debug actions with the format given in the description
   *                     above.
   * @param label        the label of action which needs to be executed.
   */
  public static void executeDebugAction(String debugActions, String label) {
    if (Strings.isNullOrEmpty(debugActions)) {
      return;
    }
    List<String> actions = Splitter.on('|').splitToList(debugActions);
    for (String action : actions) {
      List<String> components = Splitter.on(':').splitToList(action);
      if (components.isEmpty()) continue;
      if (!components.get(0).equalsIgnoreCase(label)) continue;
      // found the debug action for the given label
      // get the debug action params
      Preconditions.checkState(components.size() > 1,
          "Invalid debug action " + action);
      List<String> actionParams = Splitter.on('@').splitToList(components.get(1));
      Preconditions.checkState(actionParams.size() > 1,
          "Illegal debug action format found in " + debugActions + " for label"
              + label);
      switch (actionParams.get(0)) {
        case "SLEEP":
          // the debug action params should be of the format SLEEP@<millis>
          Preconditions.checkState(actionParams.size() == 2);
          try {
            int timeToSleepMs = Integer.parseInt(actionParams.get(1).trim());
            LOG.trace("Sleeping for {} msec to execute debug action {}",
                timeToSleepMs, label);
            Thread.sleep(timeToSleepMs);
          } catch (NumberFormatException ex) {
            LOG.error("Invalid number format in debug action {}", action);
          } catch (InterruptedException e) {
            LOG.warn("Sleep interrupted for the debug action {}", label);
          }
          break;
        case "JITTER":
          // the JITTER debug action is of format JITTER@<millis>[@<probability>}
          Preconditions.checkState(actionParams.size() <= 3);
          try {
            int maxTimeToSleepMs = Integer.parseInt(actionParams.get(1).trim());
            boolean shouldExecute = true;
            if (actionParams.size() == 3) {
              shouldExecute = parseProbability(actionParams.get(2));
            }
            if (!shouldExecute) {
              continue;
            }
            long timeToSleepMs = random.nextInt(maxTimeToSleepMs);
            LOG.trace("Sleeping for {} msec to execute debug action {}",
                timeToSleepMs, action);
            Thread.sleep(timeToSleepMs);
          } catch (NumberFormatException ex) {
            LOG.error("Invalid number format in debug action {}", action);
          } catch (InterruptedException ex) {
            LOG.warn("Sleep interrupted for the debug action {}", label);
          }
          break;
        default:
          LOG.error("Debug action {} is not implemented", actionParams.get(0));
      }
    }
  }


  /**
   * Parses the probability action parameter of a debug action.
   *
   * @return true if the action should be executed, else false.
   */
  private static boolean parseProbability(String probability) {
    double p = Double.parseDouble(probability.trim());
    if (p <= 0 || p > 1.0) {
      return false;
    }
    return random.nextDouble() < p;
  }
}
