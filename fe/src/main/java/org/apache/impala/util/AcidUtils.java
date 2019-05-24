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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.google.errorprone.annotations.Immutable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.catalog.FileMetadataLoader.LoadStats;
import org.apache.impala.common.FileSystemUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Contains utility functions for working with Acid tables.
 * <p>
 * The code is mostly copy pasted from Hive. Ideally we should use the
 * the code directly from Hive.
 * </p>
 */
public class AcidUtils {
  // Constant also defined in TransactionalValidationListener
  public static final String INSERTONLY_TRANSACTIONAL_PROPERTY = "insert_only";
  // Constant also defined in hive_metastoreConstants
  public static final String TABLE_IS_TRANSACTIONAL = "transactional";
  public static final String TABLE_TRANSACTIONAL_PROPERTIES = "transactional_properties";

  private static final Pattern BASE_PATTERN = Pattern.compile(
      "base_" +
      "(?<writeId>\\d+)" +
      "(?:_v(?<visibilityTxnId>\\d+))?" +
      "(?:/.*)?");
  private static final Pattern DELTA_PATTERN = Pattern.compile(
      "delta_" +
       "(?<minWriteId>\\d+)_" +
       "(?<maxWriteId>\\d+)" +
       "(?:_(?<optionalStatementId>\\d+))?" +
       // Optional path suffix.
       "(?:/.*)?");

  @VisibleForTesting
  static final long SENTINEL_BASE_WRITE_ID = Long.MIN_VALUE;

  // The code is same as what exists in AcidUtils.java in hive-exec.
  // Ideally we should move the AcidUtils code from hive-exec into
  // hive-standalone-metastore or some other jar and use it here.
  private static boolean isInsertOnlyTable(Map<String, String> props) {
    Preconditions.checkNotNull(props);
    if (!isTransactionalTable(props)) {
      return false;
    }
    String transactionalProp = props.get(TABLE_TRANSACTIONAL_PROPERTIES);
    return transactionalProp != null && INSERTONLY_TRANSACTIONAL_PROPERTY.
        equalsIgnoreCase(transactionalProp);
  }

  public static boolean isTransactionalTable(Map<String, String> props) {
    Preconditions.checkNotNull(props);
    String tableIsTransactional = props.get(TABLE_IS_TRANSACTIONAL);
    if (tableIsTransactional == null) {
      tableIsTransactional = props.get(TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
  }

  public static boolean isFullAcidTable(Map<String, String> props) {
    return isTransactionalTable(props) && !isInsertOnlyTable(props);
  }

  /**
   * Predicate that checks if the file or directory is relevant for a given WriteId list.
   * <p>
   *  <b>Must be called only for ACID table.</b>
   *  Checks that the path conforms to ACID table dir structure, and includes only
   *  directories that correspond to valid committed transactions.
   * </p>
   */
  private static class WriteListBasedPredicate implements Predicate<String> {

    private final ValidWriteIdList writeIdList;

    WriteListBasedPredicate(ValidWriteIdList writeIdList) {
      this.writeIdList = Preconditions.checkNotNull(writeIdList);
    }

    public boolean test(String dirPath) {
      long baseWriteId = getBaseWriteId(dirPath);
      if (baseWriteId != SENTINEL_BASE_WRITE_ID) {
        return writeIdList.isValidBase(baseWriteId);
      } else {
        ParsedDelta pd = parseDelta(dirPath);
        if (pd != null) {
          ValidWriteIdList.RangeResponse rr =
              writeIdList.isWriteIdRangeValid(pd.minWriteId, pd.maxWriteId);
          return rr.equals(ValidWriteIdList.RangeResponse.ALL);
        }
      }
      // If it wasn't in a base or a delta directory, we should include it.
      // This allows post-upgrade tables to be read.
      // TODO(todd) add an e2e test for this.
      return true;
    }
  }

  @VisibleForTesting
  static long getBaseWriteId(String relPath) {
    Matcher baseMatcher = BASE_PATTERN.matcher(relPath);
    if (baseMatcher.matches()) {
      return Long.valueOf(baseMatcher.group("writeId"));
    }
    return SENTINEL_BASE_WRITE_ID;
  }

  @Immutable
  private static final class ParsedDelta {
    final long minWriteId;
    final long maxWriteId;
    /**
     * Negative value indicates there was no statement id.
     */
    final long statementId;

    ParsedDelta(long minWriteId, long maxWriteId, long statementId) {
      this.minWriteId = minWriteId;
      this.maxWriteId = maxWriteId;
      this.statementId = statementId;
    }
  }

  private static ParsedDelta parseDelta(String dirPath) {
    Matcher deltaMatcher = DELTA_PATTERN.matcher(dirPath);
    if (!deltaMatcher.matches()) {
      return null;
    }
    long minWriteId = Long.valueOf(deltaMatcher.group("minWriteId"));
    long maxWriteId = Long.valueOf(deltaMatcher.group("maxWriteId"));
    String statementIdStr = deltaMatcher.group("optionalStatementId");
    long statementId = statementIdStr != null ? Long.valueOf(statementIdStr) : -1;
    return new ParsedDelta(minWriteId, maxWriteId, statementId);
  }

  /**
   * Filters the files based on Acid state.
   * @param stats the FileStatuses obtained from recursively listing the directory
   * @param baseDir the base directory for the partition (or table, in the case of
   *   unpartitioned tables)
   * @param writeIds the valid write IDs for the table
   * @param loadStats stats to add counts of skipped files to. May be null.
   * @return the FileStatuses that is a subset of passed in descriptors that
   *    must be used.
   */
  public static List<FileStatus> filterFilesForAcidState(List<FileStatus> stats,
      Path baseDir, ValidWriteIdList writeIds, @Nullable LoadStats loadStats) {
    List<FileStatus> validStats = new ArrayList<>(stats);

    // First filter out any paths that are not considered valid write IDs.
    // At the same time, calculate the max valid base write ID.
    Predicate<String> pred = new WriteListBasedPredicate(writeIds);
    long maxBaseWriteId = Long.MIN_VALUE;
    for (Iterator<FileStatus> it = validStats.iterator(); it.hasNext(); ) {
      FileStatus stat = it.next();
      String relPath = FileSystemUtil.relativizePath(stat.getPath(), baseDir);
      if (!pred.test(relPath)) {
        it.remove();
        if (loadStats != null) loadStats.uncommittedAcidFilesSkipped++;
        continue;
      }

      maxBaseWriteId = Math.max(getBaseWriteId(relPath), maxBaseWriteId);
    }

    // Filter out any files that are superceded by the latest valid base,
    // as well as any directories.
    for (Iterator<FileStatus> it = validStats.iterator(); it.hasNext(); ) {
      FileStatus stat = it.next();

      if (stat.isDirectory()) {
        it.remove();
        continue;
      }

      String relPath = FileSystemUtil.relativizePath(stat.getPath(), baseDir);
      long baseWriteId = getBaseWriteId(relPath);
      if (baseWriteId != SENTINEL_BASE_WRITE_ID) {
        if (baseWriteId < maxBaseWriteId) {
          it.remove();
          if (loadStats != null) loadStats.filesSupercededByNewerBase++;
        }
        continue;
      }
      ParsedDelta parsedDelta = parseDelta(relPath);
      if (parsedDelta != null) {
        if (parsedDelta.minWriteId <= maxBaseWriteId) {
          it.remove();
          if (loadStats != null) loadStats.filesSupercededByNewerBase++;
        }
        continue;
      }

      // Not in a base or a delta directory. In that case, it's probably a post-upgrade
      // file.
      // If there is no valid base: we should read the file (assuming that
      // hive.mm.allow.originals == true)
      // If there is a valid base: the file should be merged to the base by the
      // compaction, so we can assume that the file is no longer valid and just
      // waits to be deleted.
      if (maxBaseWriteId != SENTINEL_BASE_WRITE_ID) it.remove();
    }
    return validStats;
  }
}
