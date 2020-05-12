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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggingEvent;

import org.apache.impala.common.InternalException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TGetJavaLogLevelsResult;
import org.apache.impala.thrift.TSetJavaLogLevelParams;
import org.apache.impala.thrift.TLogLevel;
import org.apache.thrift.protocol.TBinaryProtocol;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * log4j appender which calls into C++ code to log messages at their correct severities
 * via glog.
 */
public class GlogAppender extends AppenderSkeleton {
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();

  // GLOG takes care of formatting, so we don't require a layout
  public boolean requiresLayout() { return false; }

  // Required implementation by superclass.
  public void ActivateOptions() { }

  // Required implementation by superclass
  public void close() { }

  private TLogLevel levelToSeverity(Level level) {
    Preconditions.checkState(!level.equals(Level.OFF));
    // TODO: Level does not work well in a HashMap or switch statement due to some
    // strangeness with equality testing.
    if (level.equals(Level.TRACE)) return TLogLevel.VLOG_3;
    if (level.equals(Level.ALL)) return TLogLevel.VLOG_3;
    if (level.equals(Level.DEBUG)) return TLogLevel.VLOG;
    if (level.equals(Level.ERROR)) return TLogLevel.ERROR;
    if (level.equals(Level.FATAL)) return TLogLevel.FATAL;
    if (level.equals(Level.INFO)) return TLogLevel.INFO;
    if (level.equals(Level.WARN)) return TLogLevel.WARN;

    throw new IllegalStateException("Unknown log level: " + level.toString());
  }

  @Override
  public void append(LoggingEvent event) {
    Level level = event.getLevel();
    if (level.equals(Level.OFF)) return;

    String msg = event.getRenderedMessage();
    if (event.getThrowableInformation() != null) {
      msg = msg + "\nJava exception follows:\n" +
          Joiner.on("\n").join(event.getThrowableStrRep());
    }
    int lineNumber = Integer.parseInt(event.getLocationInformation().getLineNumber());
    String fileName = event.getLocationInformation().getFileName();
    NativeLogger.LogToGlog(
        levelToSeverity(level).getValue(), msg, fileName, lineNumber);
  }

  /**
   * Returns a log4j level string corresponding to the Glog log level
   */
  private static String log4jLevelForTLogLevel(TLogLevel logLevel)
      throws InternalException {
    switch (logLevel) {
      case INFO: return "INFO";
      case WARN: return "WARN";
      case ERROR: return "ERROR";
      case FATAL: return "FATAL";
      case VLOG:
      case VLOG_2: return "DEBUG";
      case VLOG_3: return "TRACE";
      default: throw new InternalException("Unknown log level:" + logLevel);
    }
  }

  /**
   * Manually override Log4j root logger configuration. Any values in log4j.properties
   * not overridden (that is, anything but the root logger and its default level) will
   * continue to have effect.
   *  - impalaLogLevel - the maximum log level for org.apache.impala.* classes
   *  - otherLogLevel - the maximum log level for all other classes
   */
  public static void Install(TLogLevel impalaLogLevel, TLogLevel otherLogLevel)
      throws InternalException {
    Properties properties = new Properties();
    properties.setProperty("log4j.appender.glog", GlogAppender.class.getName());

    // These settings are relatively subtle. log4j provides many ways to filter log
    // messages, and configuring them in the right order is a bit of black magic.
    //
    // The 'Threshold' property supercedes everything, so must be set to its most
    // permissive and applies to any message sent to the glog appender.
    //
    // The 'rootLogger' property controls the default maximum logging level (where more
    // verbose->larger logging level) for the entire space of classes. This will apply to
    // all non-Impala classes, so is set to otherLogLevel.
    //
    // Finally we can configure per-package logging which overrides the rootLogger
    // setting. In order to control Impala's logging independently of the rest of the
    // world, we set the log level for org.apache.impala.
    properties.setProperty("log4j.rootLogger",
        log4jLevelForTLogLevel(otherLogLevel) + ",glog");
    properties.setProperty("log4j.appender.glog.Threshold", "TRACE");
    properties.setProperty("log4j.logger.org.apache.impala",
        log4jLevelForTLogLevel(impalaLogLevel));
    PropertyConfigurator.configure(properties);
    Logger.getLogger(GlogAppender.class).info(String.format("Logging (re)initialized. " +
        "Impala: %s, All other: %s", impalaLogLevel, otherLogLevel));
  }

  /**
   * Sets the logging level of a class as per serialized TSetJavaLogLevelParams.
   */
  public static String setLogLevel(byte[] serializedParams) throws ImpalaException {
    TSetJavaLogLevelParams thriftParams = new TSetJavaLogLevelParams();
    JniUtil.deserializeThrift(protocolFactory_, thriftParams, serializedParams);
    String className = thriftParams.getClass_name();
    String logLevel = thriftParams.getLog_level();
    if (Strings.isNullOrEmpty(className) || Strings.isNullOrEmpty(logLevel)) return null;
    // Level.toLevel() returns DEBUG for an incorrect logLevel input.
    Logger.getLogger(className).setLevel(Level.toLevel(logLevel));
    return Logger.getLogger(className).getEffectiveLevel().toString();
  }

  /**
   * Re-initializes the Java log4j logging levels.
   */
  public static void resetLogLevels() throws ImpalaException {
    LogManager.resetConfiguration();
    Install(TLogLevel.values()[BackendConfig.INSTANCE.getImpalaLogLevel()],
        TLogLevel.values()[BackendConfig.INSTANCE.getNonImpalaJavaVlogLevel()]);
  }

  /**
   * Get all the previously set log4j log levels.
   */
  public static byte[] getLogLevels() throws ImpalaException {
    Enumeration<Logger> allLoggers = LogManager.getCurrentLoggers();
    List<String> logLevels = new ArrayList<String>();

    while (allLoggers.hasMoreElements()) {
      Logger logger = allLoggers.nextElement();
      if (logger.getLevel() != null) {
        logLevels.add(logger.getName() + " : " + logger.getLevel());
      }
    }

    TGetJavaLogLevelsResult result = new TGetJavaLogLevelsResult();
    result.setLog_levels(logLevels);

    return JniUtil.serializeToThrift(result, protocolFactory_);
  }
};
