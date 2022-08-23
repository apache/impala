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
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.Immutable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList.RangeResponse;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FileMetadataLoader.LoadStats;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TTransactionalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utility functions for working with Acid tables.
 * <p>
 * The code is mostly copy pasted from Hive. Ideally we should use the
 * code directly from Hive.
 * </p>
 */
public class AcidUtils {
  private final static Logger LOG = LoggerFactory.getLogger(AcidUtils.class);
  // Constant also defined in TransactionalValidationListener
  public static final String INSERTONLY_TRANSACTIONAL_PROPERTY = "insert_only";
  // Constant also defined in hive_metastoreConstants
  public static final String TABLE_IS_TRANSACTIONAL = "transactional";
  public static final String TABLE_TRANSACTIONAL_PROPERTIES = "transactional_properties";

  /**
   * Transaction parameters needed for single table operations.
   */
  public static class TblTransaction {
    public long txnId;
    public boolean ownsTxn;
    public long writeId;
    public String validWriteIds;
  }

  // Regex pattern for files in base directories. The pattern matches strings like
  // "base_0000005/abc.txt",
  // "base_0000005/0000/abc.txt",
  // "base_0000003_v0003217/000000_0"
  private static final Pattern BASE_PATTERN = Pattern.compile(
      "base_" +
      "(?<writeId>\\d+)" +
      "(?:_v(?<visibilityTxnId>\\d+))?" +
      "(?:/.*)?");

  // Regex pattern for files in delta directories. The pattern matches strings like
  // "delta_0000006_0000006/000000_0",
  // "delta_0000009_0000009_0000/0000/def.txt"
  private static final String DELTA_STR =
      "delta_" +
      "(?<minWriteId>\\d+)_" +
      "(?<maxWriteId>\\d+)" +
      // Statement id, or visiblityTxnId
      "(?:_(?<statementId>\\d+)|_v(?<visibilityTxnId>\\d+))?" +
      // Optional path suffix.
      "(?:/.*)?";

  private static final Pattern DELTA_PATTERN = Pattern.compile(DELTA_STR);

  // Regex pattern for files in delete delta directories. The pattern is similar to
  // the 'DELTA_PATTERN', but starts with "delete_".
  private static final Pattern DELETE_DELTA_PATTERN = Pattern.compile(
    "delete_" + DELTA_STR);

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

  public static Column getRowIdColumnType(int position) {
    StructType row__id = new StructType();
    row__id.addField(new StructField("operation", ScalarType.INT, ""));
    row__id.addField(new StructField("originaltransaction", ScalarType.BIGINT, ""));
    row__id.addField(new StructField("bucket", ScalarType.INT, ""));
    row__id.addField(new StructField("rowid", ScalarType.BIGINT, ""));
    row__id.addField(new StructField("currenttransaction", ScalarType.BIGINT, ""));
    return new Column("row__id", row__id, "", position);
  }

  // Sets transaction related table properties for new tables based on manually
  // set table properties and default transactional type.
  public static void setTransactionalProperties(Map<String, String> props,
      TTransactionalType defaultTransactionalType) {
    Preconditions.checkNotNull(props);
    if (props.get(TABLE_IS_TRANSACTIONAL) != null
        || props.get(TABLE_TRANSACTIONAL_PROPERTIES) != null) {
      // Table properties are set manually, ignore default.
      return;
    }

    switch (defaultTransactionalType) {
      case NONE: break;
      case INSERT_ONLY:
        props.put(TABLE_IS_TRANSACTIONAL, "true");
        props.put(TABLE_TRANSACTIONAL_PROPERTIES, INSERTONLY_TRANSACTIONAL_PROPERTY);
        break;
    }
  }

  /**
   * This method is copied from Hive's org.apache.hadoop.hive.ql.io.AcidUtils.java
   * (commit hash 17ac1d9f230b8d663c09c22016753012a9b91edf). It is used to generate
   * the ACID directory names to be added to the insert events on the transactional
   * tables.
   */
  //Get the first level acid directory (if any) from a given path
  public static String getFirstLevelAcidDirPath(Path dataPath, FileSystem fileSystem)
      throws IOException {
    if (dataPath == null) {
      return null;
    }
    String firstLevelAcidDir = getAcidSubDir(dataPath);
    if (firstLevelAcidDir != null) {
      return firstLevelAcidDir;
    }

    String acidDirPath = getFirstLevelAcidDirPath(dataPath.getParent(), fileSystem);
    if (acidDirPath == null) {
      return null;
    }

    // We need the path for directory so no need to append file name
    if (fileSystem.isDirectory(dataPath)) {
      return acidDirPath + Path.SEPARATOR + dataPath.getName();
    }
    return acidDirPath;
  }

  private static String getAcidSubDir(Path dataPath) {
    String dataDir = dataPath.getName();
    if (dataDir.startsWith("base_")
        || dataDir.startsWith("delta_")
        || dataDir.startsWith("delete_delta_")) {
      return dataDir;
    }
    return null;
  }

  /**
   * Predicate that checks if the file or directory is relevant for a given WriteId list.
   * The class does not implement a Predicate interface since we want to support a strict
   * mode which throw exception in certain cases.
   * <p>
   *  <b>Must be called only for ACID table.</b>
   *  Checks that the path conforms to ACID table dir structure, and includes only
   *  directories that correspond to valid committed transactions.
   * </p>
   */
  private static class WriteListBasedPredicate {

    @Nullable
    private final ValidTxnList validTxnList;
    private final ValidWriteIdList writeIdList;
    // when strict mode is turned on, it throws exceptions when a given base file
    // is invalid or a compacted delta file has some open writeIds.
    private final boolean doStrictCheck;

    /**
     * Creates a Predicate just based on WriteIdList. This is used to filter out
     * already cached filedescriptors where it is guaranteed that files related to
     * invalid transactions are not loaded.
     * @param writeIdList
     */
    WriteListBasedPredicate(ValidWriteIdList writeIdList, boolean strictMode) {
      this.validTxnList = null;
      this.writeIdList = Preconditions.checkNotNull(writeIdList);
      this.doStrictCheck = strictMode;
    }

    /**
     * Creates a Predicate based on a ValidTxnList and ValidWriteIdList. Useful when we
     * are filtering out the file listing directly from fileSystem which may included
     * compacted directories.
     *
     * @param validTxnList
     * @param writeIdList
     */
    WriteListBasedPredicate(ValidTxnList validTxnList, ValidWriteIdList writeIdList) {
      this.validTxnList = Preconditions.checkNotNull(validTxnList);
      this.writeIdList = Preconditions.checkNotNull(writeIdList);
      this.doStrictCheck = false;
    }

    public boolean check(String dirPath) throws CatalogException {
      ParsedBase parsedBase = parseBase(dirPath);
      if (parsedBase.writeId != SENTINEL_BASE_WRITE_ID) {
        boolean isValid = writeIdList.isValidBase(parsedBase.writeId) &&
               isTxnValid(parsedBase.visibilityTxnId);
        if (doStrictCheck && !isValid) {
          throw new CatalogException("Invalid base file found " + dirPath);
        }
        return isValid;
      } else {
        ParsedDelta pd = parseDelta(dirPath);
        if (pd == null) pd = parseDeleteDelta(dirPath);
        if (pd != null) {
          if (!isTxnValid(pd.visibilityTxnId)) return false;
          ValidWriteIdList.RangeResponse rr =
              writeIdList.isWriteIdRangeValid(pd.minWriteId, pd.maxWriteId);
          if (rr == RangeResponse.ALL) return true;
          if (rr == RangeResponse.NONE) return false;
          // either this is compacted delta file whose visibility transaction id is
          // valid or a delta file generated by Hive Streaming engine.
          // We allow the delta files for streaming engine which have open writeIds since
          // backend code handles such writeIds appropriately.
          if (!pd.isCompactedDeltaFile()) return true;
          // This is a compacted delta file and has some writeIds which are not valid.
          // We allow only aborted and committed writeIds in compacted files (no open
          // writeIds)
          for (long writeId = pd.minWriteId; writeId<=pd.maxWriteId; writeId++) {
            if (!writeIdList.isWriteIdValid(writeId) && !writeIdList
                .isWriteIdAborted(writeId)) {
              if (doStrictCheck) {
                throw new CatalogException(
                    "Open writeId " + writeId + " found in compacted delta file "
                        + dirPath);
              }
              return false;
            }
          }
          return true;
        }
      }
      // If it wasn't in a base or a delta directory, we should include it.
      // This allows post-upgrade tables to be read.
      // TODO(todd) add an e2e test for this.
      return true;
    }

    /**
     * The ACID compactor process does not change the writeIds of the compacted files (eg.
     * delta_0001 and delta_0002 will be compacted to delta_0001_0002_v0123 where v0123 is
     * the visibility txn id for the compaction itself). While this compaction is in
     * progress, we need to make sure that we ignore all such files. This is where
     * the TxnList is useful. We use the validTxnList to determine if the compaction
     * process is committed or not. If its not, we ignore the files which
     * are being compacted.
     *
     * @param visibilityTxnId TransactionID derived from the directory name on the
     *                        filesystem.
     * @return true if the given visibilityTxnId is valid. In case either the
     * visibilityTxnId is -1 or validTxnList is null, we return true. False otherwise.
     */
    private boolean isTxnValid(long visibilityTxnId) {
      return validTxnList == null ||
        visibilityTxnId == -1 || validTxnList.isTxnValid(visibilityTxnId);
    }
  }

  @Immutable
  private static final class ParsedBase {
    final long writeId;
    final long visibilityTxnId;

    ParsedBase(long writeId, long visibilityTxnId) {
      this.writeId = writeId;
      this.visibilityTxnId = visibilityTxnId;
    }
  }

  @VisibleForTesting
  static ParsedBase parseBase(String relPath) {
    Matcher baseMatcher = BASE_PATTERN.matcher(relPath);
    if (baseMatcher.matches()) {
      long writeId = Long.valueOf(baseMatcher.group("writeId"));
      long visibilityTxnId = -1;
      String visibilityTxnIdStr = baseMatcher.group("visibilityTxnId");
      if (visibilityTxnIdStr != null) {
        visibilityTxnId = Long.valueOf(visibilityTxnIdStr);
      }
      return new ParsedBase(writeId, visibilityTxnId);
    }
    return new ParsedBase(SENTINEL_BASE_WRITE_ID, -1);
  }

  @VisibleForTesting
  static long getBaseWriteId(String relPath) {
    return parseBase(relPath).writeId;
  }

  @Immutable
  private static final class ParsedDelta {
    final long minWriteId;
    final long maxWriteId;
    /// Value -1 means there is no statement id.
    final long statementId;
    /// Value -1 means there is no visibility txn id.
    final long visibilityTxnId;

    ParsedDelta(long minWriteId, long maxWriteId, long statementId,
        long visibilityTxnId) {
      this.minWriteId = minWriteId;
      this.maxWriteId = maxWriteId;
      this.statementId = statementId;
      this.visibilityTxnId = visibilityTxnId;
    }

    private boolean isCompactedDeltaFile() {
      return visibilityTxnId != -1;
    }
  }

  private static ParsedDelta matcherToParsedDelta(Matcher deltaMatcher) {
    if (!deltaMatcher.matches()) {
      return null;
    }
    long minWriteId = Long.valueOf(deltaMatcher.group("minWriteId"));
    long maxWriteId = Long.valueOf(deltaMatcher.group("maxWriteId"));
    String statementIdStr = deltaMatcher.group("statementId");
    long statementId = statementIdStr != null ? Long.valueOf(statementIdStr) : -1;
    String visibilityTxnIdStr = deltaMatcher.group("visibilityTxnId");
    long visibilityTxnId = visibilityTxnIdStr != null ?
        Long.valueOf(visibilityTxnIdStr) : -1;
    return new ParsedDelta(minWriteId, maxWriteId, statementId, visibilityTxnId);
  }

  private static ParsedDelta parseDelta(String dirPath) {
    return matcherToParsedDelta(DELTA_PATTERN.matcher(dirPath));
  }

  private static ParsedDelta parseDeleteDelta(String dirPath) {
    return matcherToParsedDelta(DELETE_DELTA_PATTERN.matcher(dirPath));
  }

  private static String getFirstDirName(String relPath) {
    int slashIdx = relPath.indexOf("/");
    if (slashIdx != -1) {
      return relPath.substring(0, slashIdx);
    } else {
      return null;
    }
  }

  /**
   * Returns true if 'fd' refers to a delete delta file.
   */
  public static boolean isDeleteDeltaFd(FileDescriptor fd) {
    return fd.getPath().startsWith("delete_delta_");
  }

  /**
   * This method is similar to {@link AcidUtils#filterFilesForAcidState} with the
   * difference that it expects input to be valid file descriptors from a loaded table.
   * This means that file descriptors are already pre-vetted and are consistent with
   * respect to some ValidWriteIdList and ValidTxnList. All this method does is to try to
   * filter such file descriptors for a different ValidWriteIdList.
   *
   * @param fds Input list of File descriptors to be filtered in-place.
   * @param validWriteIdList The ValidWriteIdList for which we filter the fds.
   * @return The number of file descriptors which were filtered out.
   * @throws CatalogException if any of the provided FileDescriptor could be included or
   * excluded since it contains some writeIds which are invalid for the given
   * ValidWriteIdList.
   */
  public static int filterFdsForAcidState(List<FileDescriptor> fds,
      ValidWriteIdList validWriteIdList) throws CatalogException {
    Preconditions.checkNotNull(fds);

    if (validWriteIdList == null) return 0;

    WriteListBasedPredicate writeListBasedPredicate = new WriteListBasedPredicate(
        validWriteIdList, true);
    Iterator<FileDescriptor> it = fds.iterator();
    int numRemoved = 0;
    while (it.hasNext()) {
      if (!writeListBasedPredicate.check(it.next().getPath())) {
        it.remove();
        numRemoved++;
      }
    }
    return numRemoved;
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
   * @throws CatalogException on ACID error. TODO: Remove throws clause once IMPALA-9042
   * is resolved.
   */
  public static List<FileStatus> filterFilesForAcidState(List<FileStatus> stats,
      Path baseDir, ValidTxnList validTxnList, ValidWriteIdList writeIds,
      @Nullable LoadStats loadStats) throws CatalogException {
    // First filter out any paths that are not considered valid write IDs.
    // At the same time, calculate the max valid base write ID and collect the names of
    // the delta directories.
    WriteListBasedPredicate pred = new WriteListBasedPredicate(validTxnList, writeIds);
    long maxBaseWriteId = Long.MIN_VALUE;
    Set<String> deltaDirNames = new HashSet<>();
    for (Iterator<FileStatus> it = stats.iterator(); it.hasNext();) {
      FileStatus stat = it.next();
      String relPath = FileSystemUtil.relativizePath(stat.getPath(), baseDir);
      if (!pred.check(relPath)) {
        it.remove();
        if (loadStats != null) loadStats.uncommittedAcidFilesSkipped++;
        continue;
      }
      maxBaseWriteId = Math.max(getBaseWriteId(relPath), maxBaseWriteId);
      String dirName = getFirstDirName(relPath);
      if (dirName != null && (dirName.startsWith("delta_") ||
          dirName.startsWith("delete_delta_"))) {
        deltaDirNames.add(dirName);
      }
    }
    // Get a list of all valid delta directories.
    List<Pair<String, ParsedDelta>> deltas =
        getValidDeltaDirsOrdered(deltaDirNames, maxBaseWriteId);
    // Filter out delta directories superceded by major/minor compactions.
    Set<String> filteredDeltaDirs =
        getFilteredDeltaDirs(deltas, maxBaseWriteId, writeIds);
    // Filter out any files that are superceded by the latest valid base or not located
    // in 'filteredDeltaDirs'.
    return filterFilesForAcidState(stats, baseDir, maxBaseWriteId, filteredDeltaDirs,
        loadStats);
  }

  private static List<FileStatus> filterFilesForAcidState(List<FileStatus> stats,
      Path baseDir, long maxBaseWriteId, Set<String> deltaDirs,
      @Nullable LoadStats loadStats) throws CatalogException {
    List<FileStatus> validStats = new ArrayList<>(stats);
    for (Iterator<FileStatus> it = validStats.iterator(); it.hasNext();) {
      FileStatus stat = it.next();

      if (stat.isDirectory()) {
        it.remove();
        continue;
      }

      String relPath = FileSystemUtil.relativizePath(stat.getPath(), baseDir);
      if (relPath.startsWith("delta_") ||
          relPath.startsWith("delete_delta_")) {
        String dirName = getFirstDirName(relPath);
        if (dirName != null && !deltaDirs.contains(dirName)) {
          it.remove();
          if (loadStats != null) loadStats.filesSupersededByAcidState++;
        }
        if (relPath.endsWith("_flush_length")) {
          throw new CatalogException("Found Hive Streaming side-file: " +
              stat.getPath() + " It means that the contents of the directory are " +
              "currently being written, therefore Impala is not able to read it. " +
              "Please try to load the table again once Hive Streaming commits " +
              "the transaction.");
        }
        continue;
      }
      long baseWriteId = getBaseWriteId(relPath);
      if (baseWriteId != SENTINEL_BASE_WRITE_ID) {
        if (baseWriteId < maxBaseWriteId) {
          it.remove();
          if (loadStats != null) loadStats.filesSupersededByAcidState++;
        }
        continue;
      }

      // Not in a base or a delta directory. In that case, it's probably a
      // post-upgrade file.
      // If there is no valid base: we should read the file (assuming that
      // hive.mm.allow.originals == true)
      // If there is a valid base: the file should be merged to the base by the
      // compaction, so we can assume that the file is no longer valid and just
      // waits to be deleted.
      if (maxBaseWriteId != SENTINEL_BASE_WRITE_ID) it.remove();
    }
    return validStats;
  }

  private static List<Pair<String, ParsedDelta>> getValidDeltaDirsOrdered(
      Set<String> deltaDirNames, long baseWriteId)
      throws CatalogException {
    List <Pair<String, ParsedDelta>> deltas = new ArrayList<>();
    for (Iterator<String> it = deltaDirNames.iterator(); it.hasNext();) {
      String dirname = it.next();
      ParsedDelta parsedDelta = parseDelta(dirname);
      if (parsedDelta == null) parsedDelta = parseDeleteDelta(dirname);
      if (parsedDelta != null) {
        if (parsedDelta.minWriteId <= baseWriteId) {
          Preconditions.checkState(parsedDelta.maxWriteId <= baseWriteId);
          it.remove();
          continue;
        }
        deltas.add(new Pair<String, ParsedDelta>(dirname, parsedDelta));
        continue;
      }
    }

    deltas.sort(new Comparator<Pair<String, ParsedDelta>>() {
      // This compare method is based on Hive (d6ad73c3615)
      // AcidUtils.ParsedDeltaLight.compareTo()
      // One additon to it is to take the visbilityTxnId into consideration. Hence if
      // there's delta_N_M and delta_N_M_v001234 then delta_N_M_v001234 must be ordered
      // before.
      @Override
      public int compare(Pair<String, ParsedDelta> o1, Pair<String, ParsedDelta> o2) {
        ParsedDelta pd1 = o1.second;
        ParsedDelta pd2 = o2.second;
        if (pd1.minWriteId != pd2.minWriteId) {
          if (pd1.minWriteId < pd2.minWriteId) {
            return -1;
          } else {
            return 1;
          }
        } else if (pd1.maxWriteId != pd2.maxWriteId) {
          if (pd1.maxWriteId < pd2.maxWriteId) {
            return 1;
          } else {
            return -1;
          }
        } else if (pd1.statementId != pd2.statementId) {
          /**
           * We want deltas after minor compaction (w/o statementId) to sort earlier so
           * that getAcidState() considers compacted files (into larger ones) obsolete
           * Before compaction, include deltas with all statementIds for a given writeId.
           */
          if (pd1.statementId < pd2.statementId) {
            return -1;
          } else {
            return 1;
          }
        } else if (pd1.visibilityTxnId != pd2.visibilityTxnId) {
          // This is an alteration from Hive's algorithm. If everything is the same then
          // the higher visibilityTxnId wins (since no visibiltyTxnId is -1).
          // Currently this cannot happen since Hive doesn't minor compact standalone
          // delta directories of streaming ingestion, i.e. the following cannot happen:
          // delta_1_5 => delta_1_5_v01234
          // However, it'd make sense because streaming ingested ORC files doesn't use
          // advanced features like dictionary encoding or statistics. Hence Hive might
          // do that in the future and that'd make Impala seeing duplicate rows.
          // So I'd be cautious here in case they forget to tell us.
          if (pd1.visibilityTxnId < pd2.visibilityTxnId) {
            return 1;
          } else {
            return -1;
          }
        } else {
          return o1.first.compareTo(o2.first);
        }
      }
    });
    return deltas;
  }

  /**
   * The algorithm is copied from Hive's (d6ad73c3615)
   * org.apache.hadoop.hive.ql.io.AcidUtils.getAcidState()
   * One additon to it is to take the visbilityTxnId into consideration. Hence if
   * there's delta_N_M_v001234 and delta_N_M then it ignores delta_N_M.
   */
  private static Set<String> getFilteredDeltaDirs(List<Pair<String, ParsedDelta>> deltas,
      long baseWriteId, ValidWriteIdList writeIds) {
    long current = baseWriteId;
    long lastStmtId = -1;
    ParsedDelta prev = null;
    Set<String> filteredDeltaDirs = new HashSet<>();
    for (Pair<String, ParsedDelta> pathDelta : deltas) {
      ParsedDelta next = pathDelta.second;
      if (next.maxWriteId > current) {
        // are any of the new transactions ones that we care about?
        if (writeIds.isWriteIdRangeValid(current + 1, next.maxWriteId) !=
            ValidWriteIdList.RangeResponse.NONE) {
          filteredDeltaDirs.add(pathDelta.first);
          current = next.maxWriteId;
          lastStmtId = next.statementId;
          prev = next;
        }
      } else if (next.maxWriteId == current && lastStmtId >= 0) {
        // make sure to get all deltas within a single transaction; multi-statement txn
        // generate multiple delta files with the same txnId range
        // of course, if maxWriteId has already been minor compacted, all per statement
        // deltas are obsolete
        filteredDeltaDirs.add(pathDelta.first);
        prev = next;
      } else if (prev != null && next.maxWriteId == prev.maxWriteId &&
          next.minWriteId == prev.minWriteId &&
          next.statementId == prev.statementId &&
          // If visibilityTxnId differs, then 'pathDelta' is probably a streaming ingested
          // delta directory and 'prev' is the compacted version of it.
          next.visibilityTxnId == prev.visibilityTxnId) {
        // The 'next' parsedDelta may have everything equal to the 'prev' parsedDelta,
        // except
        // the path. This may happen when we have split update and we have two types of
        // delta
        // directories- 'delta_x_y' and 'delete_delta_x_y' for the SAME txn range.

        // Also note that any delete_deltas in between a given delta_x_y range would be
        // made
        // obsolete. For example, a delta_30_50 would make delete_delta_40_40 obsolete.
        // This is valid because minor compaction always compacts the normal deltas and
        // the delete deltas for the same range. That is, if we had 3 directories,
        // delta_30_30, delete_delta_40_40 and delta_50_50, then running minor compaction
        // would produce delta_30_50 and delete_delta_30_50.
        filteredDeltaDirs.add(pathDelta.first);
        prev = next;
      }
    }
    return filteredDeltaDirs;
  }

  /**
   * This method compares the writeIdList of the given table if it is loaded and is a
   * transactional table with the given ValidWriteIdList. If the tbl metadata is a
   * superset of the metadata view represented by the given validWriteIdList this
   * method returns a value greater than 0. If they are an exact match of each other,
   * it returns 0 and if the table ValidWriteIdList is behind the provided
   * validWriteIdList this return -1. This information useful to determine if the
   * cached table can be used to construct a consistent snapshot corresponding to the
   * given validWriteIdList. The ValidWriteIdList is compared only if the table id
   * matches with the given tableId.
   */
  public static int compare(HdfsTable tbl, ValidWriteIdList validWriteIdList,
      long tableId) {
    Preconditions.checkState(tbl != null && tbl.getMetaStoreTable() != null);
    // if tbl is not a transactional, there is nothing to compare against and we return 0
    if (!isTransactionalTable(tbl.getMetaStoreTable().getParameters())) return 0;
    Preconditions.checkNotNull(tbl.getValidWriteIds());
    // if the provided table id does not match with what CatalogService has we return
    // -1 indicating that cached table is stale.
    if (tableId != CatalogServiceCatalog.TABLE_ID_UNAVAILABLE
        && MetastoreShim.getTableId(tbl.getMetaStoreTable()) != tableId) {
      return -1;
    }
    return compare(tbl.getValidWriteIds(), validWriteIdList);
  }

  /*** This method is mostly copied from {@link org.apache.hive.common.util.TxnIdUtils}
   * (e649562) with the exception that the table names for both the input writeIdList
   * must be the same to have a valid comparison.
   * //TODO source this directly from hive-exec so that future changes to this are
   * automatically imported.
   *
   * @param a
   * @param b
   * @return 0, if a and b are equivalent
   * 1, if a is more recent
   * -1, if b is more recent
   ***/
  @VisibleForTesting
  public static int compare(ValidWriteIdList a, ValidWriteIdList b) {
    Preconditions.checkState(a.getTableName().equalsIgnoreCase(b.getTableName()));
    // The algorithm assumes invalidWriteIds are sorted and values are less or equal than
    // hwm, here is how the algorithm works:
    // 1. Compare two invalidWriteIds until one the list ends, difference means the
    // mismatch writeid is committed in one ValidWriteIdList but not the other, the
    // comparison end
    // 2. Every writeid from the last writeid in the short invalidWriteIds till its
    // hwm should be committed in the other ValidWriteIdList, otherwise the comparison
    // end
    // 3. Every writeid from lower hwm to higher hwm should be invalid, otherwise, the
    // comparison end
    int minLen = Math.min(a.getInvalidWriteIds().length, b.getInvalidWriteIds().length);
    for (int i = 0; i < minLen; i++) {
      if (a.getInvalidWriteIds()[i] == b.getInvalidWriteIds()[i]) {
        continue;
      }
      return a.getInvalidWriteIds()[i] > b.getInvalidWriteIds()[i] ? 1 : -1;
    }
    if (a.getInvalidWriteIds().length == b.getInvalidWriteIds().length) {
      return Long.signum(a.getHighWatermark() - b.getHighWatermark());
    }
    if (a.getInvalidWriteIds().length == minLen) {
      if (a.getHighWatermark() != b.getInvalidWriteIds()[minLen] - 1) {
        return Long.signum(a.getHighWatermark() - (b.getInvalidWriteIds()[minLen] - 1));
      }
      if (allInvalidFrom(b.getInvalidWriteIds(), minLen, b.getHighWatermark())) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (b.getHighWatermark() != a.getInvalidWriteIds()[minLen] - 1) {
        return Long.signum((a.getInvalidWriteIds()[minLen] - 1) - b.getHighWatermark());
      }
      if (allInvalidFrom(a.getInvalidWriteIds(), minLen, a.getHighWatermark())) {
        return 0;
      } else {
        return 1;
      }
    }
  }
  private static boolean allInvalidFrom(long[] invalidIds, int start, long hwm) {
    for (int i=start+1;i<invalidIds.length;i++) {
      if (invalidIds[i] != (invalidIds[i-1]+1)) {
        return false;
      }
    }
    return invalidIds[invalidIds.length-1] == hwm;
  }

  /**
   * This method fetches latest compaction id from HMS and compares it with the cached
   * compaction id. It returns list of partitions that need to refresh file metadata due
   * to compactions. If none of the partitions need to refresh, it returns empty list.
   */
  public static List<HdfsPartition.Builder> getPartitionsForRefreshingFileMetadata(
      CatalogServiceCatalog catalog, HdfsTable hdfsTable) throws CatalogException {
    Stopwatch sw = Stopwatch.createStarted();
    Preconditions.checkState(hdfsTable.isReadLockedByCurrentThread());

    List<HdfsPartition.Builder> partBuilders = MetastoreShim
        .getPartitionsForRefreshingFileMetadata(catalog, hdfsTable);
    LOG.debug("Checked the latest compaction id for {}. Time taken: {}",
        hdfsTable.getFullName(),
        PrintUtils.printTimeMs(sw.stop().elapsed(TimeUnit.MILLISECONDS)));
    return partBuilders;
  }
}
