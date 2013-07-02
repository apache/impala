// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.service;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.Level;

import java.util.Map;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.base.Preconditions;

import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TLogSeverity;

/**
 * log4j appender which calls into C++ code to log messages at their correct severities
 * via glog.
 */
public class GlogAppender extends AppenderSkeleton {
  // GLOG takes care of formatting, so we don't require a layout
  public boolean requiresLayout() { return false; }

  // Required implementation by superclass.
  public void ActivateOptions() { }

  // Required implementation by superclass
  public void close() { }

  private TLogSeverity levelToSeverity(Level level) {
    Preconditions.checkState(!level.equals(Level.OFF));
    // TODO: Level does not work well in a HashMap or switch statement due to some
    // strangeness with equality testing.
    if (level.equals(Level.TRACE)) return TLogSeverity.VERBOSE;
    if (level.equals(Level.DEBUG)) return TLogSeverity.VERBOSE;
    if (level.equals(Level.ALL)) return TLogSeverity.VERBOSE;
    if (level.equals(Level.ERROR)) return TLogSeverity.ERROR;
    if (level.equals(Level.FATAL)) return TLogSeverity.FATAL;
    if (level.equals(Level.INFO)) return TLogSeverity.INFO;
    if (level.equals(Level.WARN)) return TLogSeverity.WARN;

    throw new IllegalStateException("Unknown log level: " + level.toString());
  }

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
    FeSupport.LogToGlog(levelToSeverity(level).getValue(), msg, fileName, lineNumber);
  }
};
