/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.planner.paimon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.function.FunctionDefinition;
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Exercises the shaded impala-paimon-runtime jar: file-format read/write,
 * the Table and Catalog APIs, and every non-JDK class the jar references.
 */
public class PaimonRuntimeJarCoverageTest {
  private static final String DB = "db";
  private static final String SERVICE =
      "META-INF/services/org.apache.paimon.format.FileFormatFactory";

  @Test
  public void testRequiredFormatRoundTrips() throws Exception {
    for (String format : requiredFormats()) {
      try (Env env = Env.open()) {
        Table table = env.createAppend("round_" + format, format);
        write(table, row(1, "a", 10L), row(2, null, 20L));
        assertEquals(Arrays.asList("1,a,10", "2,null,20"), scan(table));
        FileFormatDiscover.of(new CoreOptions(table.options())).discover(format);
      }
    }
  }

  @Test
  public void testParquetPrimaryKeyMerge() throws Exception {
    try (Env env = Env.open()) {
      Table table = env.create(pkSchema(CoreOptions.FILE_FORMAT_PARQUET, false), "pk");
      write(table, row(1, "a", 10L));
      write(table, row(1, "b", 11L));
      assertEquals(Collections.singletonList("1,b,11"), scan(table));
    }
  }

  @Test
  public void testParquetDeletionVector() throws Exception {
    try (Env env = Env.open()) {
      Table table = env.create(pkSchema(CoreOptions.FILE_FORMAT_PARQUET, true), "dv");
      write(table, row(1, "a", 10L), row(2, "b", 20L));
      write(table, GenericRow.ofKind(RowKind.DELETE, 1,
          BinaryString.fromString("a"), 10L));
      assertEquals(Collections.singletonList("2,b,20"), scan(table));
    }
  }

  @Test
  public void testParquetPredicate() throws Exception {
    try (Env env = Env.open()) {
      Table table = env.createAppend("pred", CoreOptions.FILE_FORMAT_PARQUET);
      write(table, row(1, "a", 10L), row(2, "b", 20L));
      PredicateBuilder builder = new PredicateBuilder(table.rowType());
      ReadBuilder read = table.newReadBuilder().withFilter(builder.equal(0, 1));
      assertEquals(Collections.singletonList("1,a,10"), scan(read, true));
    }
  }

  @Test
  public void testParquetPartition() throws Exception {
    try (Env env = Env.open()) {
      Schema schema = Schema.newBuilder()
          .column("id", DataTypes.INT())
          .column("name", DataTypes.STRING())
          .column("val", DataTypes.BIGINT())
          .column("pt", DataTypes.STRING())
          .partitionKeys("pt")
          .option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET)
          .build();
      Table table = env.create(schema, "part");
      write(table,
          GenericRow.of(1, BinaryString.fromString("a"), 10L,
              BinaryString.fromString("a")),
          GenericRow.of(2, BinaryString.fromString("b"), 20L,
              BinaryString.fromString("b")));
      ReadBuilder read = table.newReadBuilder()
          .withPartitionFilter(Collections.singletonMap("pt", "a"));
      assertEquals(Collections.singletonList("1,a,10"), scan(read));
    }
  }

  @Test
  public void testOtherFileFormats() throws Exception {
    List<String> executed = new ArrayList<>();
    List<String> skipped = new ArrayList<>();
    Set<String> required = new HashSet<>(requiredFormats());
    for (String format : serviceFormats()) {
      if (required.contains(format)) {
        continue;
      }
      try (Env env = Env.open()) {
        Table table = env.createAppend("opt_" + format, format);
        write(table, row(1, "a", 10L));
        scan(table);
        executed.add(format);
      } catch (Throwable thrown) {
        if (isMissingClass(thrown)) {
          throw new AssertionError("missing class for format " + format, thrown);
        }
        skipped.add(format + ": " + thrown.getClass().getSimpleName());
      }
    }
    System.out.println("Optional formats executed: " + executed);
    System.out.println("Optional formats not executed: " + skipped);
  }

  @Test
  public void testTableApis() throws Exception {
    try (Env env = Env.open()) {
      Table table = env.createAppend("apis", CoreOptions.FILE_FORMAT_PARQUET);
      write(table, row(1, "a", 10L));
      write(table, row(2, "b", 20L));
      assertTrue(table instanceof FileStoreTable);
      FileStoreTable store = (FileStoreTable) table;
      coverMetadata(table, store);
      coverManifests(table);
      coverTagsAndBranches(table, store);
      coverBuilders(table, store);
      call(() -> store.setManifestCache(null));
      call(() -> store.getManifestCache());
      call(() -> store.setManifestSidecarCache(null));
      call(() -> store.getManifestSidecarCache());
      call(() -> store.setSnapshotCache(null));
      call(() -> store.setStatsCache(null));
      call(() -> store.setDVMetaCache(null));
      call(() -> store.purgeFiles());
    }
  }

  @Test
  public void testCatalogApis() throws Exception {
    try (Env env = Env.open()) {
      Identifier id = Identifier.create(DB, "cat");
      env.catalog.createTable(id, appendSchema(CoreOptions.FILE_FORMAT_PARQUET),
          false);
      Table table = env.catalog.getTable(id);
      write(table, row(1, "a", 10L));
      coverCatalog(env.catalog, id, table);
    }
  }

  @Test
  public void testTransitiveReferences() throws Exception {
    File jar = runtimeJar();
    Set<String> names = new HashSet<>();
    try (ZipFile zip = new ZipFile(jar)) {
      Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        readClassNames(zip, entries.nextElement(), names);
      }
    }
    ClassLoader loader = Table.class.getClassLoader();
    Map<String, int[]> counts = new TreeMap<>();
    List<String> failures = new ArrayList<>();
    Map<String, List<String>> uncovered = new TreeMap<>();
    for (String internal : names) {
      String binary = internal.replace('/', '.');
      String key = rootOf(binary);
      int[] row = counts.computeIfAbsent(key, ignored -> new int[4]);
      row[0]++;
      try {
        Class<?> cls = Class.forName(binary, false, loader);
        if (fromRuntimeJar(cls)) {
          row[1]++;
        } else {
          row[2]++;
        }
      } catch (ClassNotFoundException | LinkageError thrown) {
        row[3]++;
        if (mustResolve(binary)) {
          failures.add(binary);
        } else {
          uncovered.computeIfAbsent(key, ignored -> new ArrayList<>()).add(binary);
        }
      }
    }
    String summary = formatSummary(counts, uncovered);
    System.out.println(summary);
    if (!failures.isEmpty()) {
      fail(summary + "\nUnresolved required classes (" + failures.size() + "): "
          + failures.subList(0, Math.min(30, failures.size())));
    }
  }

  private static void coverMetadata(Table table, FileStoreTable store) {
    call(table::name);
    call(table::fullName);
    call(table::uuid);
    call(table::rowType);
    call(table::partitionKeys);
    call(table::primaryKeys);
    call(table::options);
    call(table::comment);
    call(table::statistics);
    call(table::fileIO);
    call(() -> table.copy(Collections.emptyMap()));
    call(table::latestSnapshot);
    call(() -> table.snapshot(table.latestSnapshot().get().id()));
    call(store::schema);
    call(store::store);
    call(store::coreOptions);
    call(store::location);
    call(store::bucketSpec);
    call(store::bucketMode);
    call(store::catalogEnvironment);
    call(() -> store.copy(Collections.emptyMap()));
    call(() -> store.copy(store.schema()));
    call(() -> store.copyWithoutTimeTravel(Collections.emptyMap()));
    call(store::copyWithLatestSchema);
    call(store::snapshotManager);
    call(store::changelogManager);
    call(store::schemaManager);
    call(store::tagManager);
    call(store::branchManager);
    call(store::consumerManager);
    call(store::newSnapshotReader);
    call(() -> store.newScan());
    call(() -> table.fileIO().listStatus(store.location()));
    call(() -> table.fileIO().getFileStatus(store.location()));
    call(() -> table.fileIO().exists(store.location()));
    call(() -> table.fileIO().isObjectStore());
  }

  private static void coverManifests(Table table) {
    call(() -> {
      Snapshot snapshot = table.latestSnapshot().orElse(null);
      if (snapshot == null) {
        return;
      }
      List<ManifestFileMeta> metas =
          table.manifestListReader().read(snapshot.deltaManifestList());
      table.manifestListReader().read(snapshot.baseManifestList());
      if (snapshot.changelogManifestList() != null) {
        table.manifestListReader().read(snapshot.changelogManifestList());
      }
      if (!metas.isEmpty()) {
        table.manifestFileReader().read(metas.get(0).fileName());
      }
      if (snapshot.indexManifest() != null) {
        table.indexManifestFileReader().read(snapshot.indexManifest());
      }
    });
  }

  private static void coverTagsAndBranches(Table table, FileStoreTable store) {
    long snapshotId = table.latestSnapshot().get().id();
    call(() -> table.createTag("t1"));
    call(() -> table.createTag("t2", snapshotId));
    call(() -> table.createTag("t3", Duration.ofHours(1)));
    call(() -> table.createTag("t4", snapshotId, Duration.ofHours(1)));
    call(() -> table.renameTag("t1", "t1b"));
    call(() -> table.replaceTag("t2", snapshotId, Duration.ofHours(2)));
    call(() -> table.rollbackTo("t2"));
    call(() -> table.deleteTag("t1b"));
    call(() -> table.deleteTags("t3"));
    call(store::newTagAutoManager);
    call(() -> table.createBranch("b1"));
    call(() -> table.createBranch("b2", "t2"));
    call(() -> table.createBranch("b3", true));
    call(() -> table.createBranch("b4", "t2", true));
    call(() -> table.renameBranch("b1", "b1b"));
    call(() -> table.fastForward("b1b"));
    call(() -> table.mergeBranch("b2", "main"));
    call(() -> store.switchToBranch("b2"));
    call(() -> table.deleteBranch("b3"));
    call(() -> table.deleteBranches("b4"));
    call(() -> table.rollbackTo(snapshotId));
    call(() -> table.rollbackSchema(0L));
    call(() -> table.newExpireSnapshots().expire());
    call(() -> table.newExpireChangelog().expire());
  }

  private static void coverBuilders(Table table, FileStoreTable store) {
    call(table::newReadBuilder);
    call(table::newBatchWriteBuilder);
    call(table::newVectorSearchBuilder);
    call(table::newHybridSearchBuilder);
    call(table::newBatchVectorSearchBuilder);
    call(table::newFullTextSearchBuilder);
    call(() -> {
      StreamWriteBuilder builder =
          table.newStreamWriteBuilder().withCommitUser("coverage");
      try (StreamTableWrite write = builder.newWrite();
           StreamTableCommit commit = builder.newCommit()) {
        write.write(row(3, "c", 30L));
        commit.commit(1L, write.prepareCommit(false, 1L));
      }
    });
    call(() -> store.newWrite("coverage").close());
    call(() -> store.newWrite("coverage", 0).close());
    call(() -> store.newCommit("coverage").close());
    call(() -> store.newLocalTableQuery().close());
    call(store::supportStreamingReadOverwrite);
    call(store::createRowKeyExtractor);
    call(store::newPostponeFixedBucketWriteBuilder);
  }

  private static void coverCatalog(Catalog catalog, Identifier id, Table table)
      throws Exception {
    Snapshot snapshot = table.latestSnapshot().orElse(null);
    call(catalog::listDatabases);
    call(() -> catalog.listDatabasesPaged(10, null, null));
    call(() -> catalog.createDatabase("other", true));
    call(() -> catalog.createDatabase("other2", true, Collections.emptyMap()));
    call(() -> catalog.getDatabase(DB));
    call(() -> catalog.alterDatabase(DB,
        Collections.singletonList(PropertyChange.setProperty("owner", "impala")),
        false));
    call(() -> catalog.getTable(id));
    call(() -> catalog.getTableById(table.uuid()));
    call(() -> catalog.listTables(DB));
    call(() -> catalog.listTablesPaged(DB, 10, null, null, null));
    call(() -> catalog.listTableDetailsPaged(DB, 10, null, null, null));
    call(() -> catalog.listTableDetails(DB));
    call(() -> catalog.listTablesPagedGlobally(null, null, 10, null));
    Identifier renamed = Identifier.create(DB, "cat_renamed");
    call(() -> catalog.renameTable(id, renamed, false));
    Identifier current = catalog.listTables(DB).contains("cat_renamed")
        ? renamed : id;
    call(() -> catalog.alterTable(current,
        SchemaChange.addColumn("extra", DataTypes.INT()), false));
    call(() -> catalog.alterTable(current,
        Collections.singletonList(SchemaChange.setOption("comment", "c")), false));
    call(() -> catalog.replaceTable(current,
        appendSchema(CoreOptions.FILE_FORMAT_PARQUET), false));
    call(() -> catalog.invalidateTable(current));
    call(() -> catalog.registerTable(Identifier.create(DB, "registered"),
        ((FileStoreTable) table).location().toString()));
    call(catalog::repairCatalog);
    call(() -> catalog.repairDatabase(DB));
    call(() -> catalog.repairTable(current));
    coverPartitions(catalog, current);
    coverVersioning(catalog, current, snapshot);
    coverCatalogBranches(catalog, current, snapshot);
    coverViewsAndFunctions(catalog);
    call(catalog::supportsListObjectsPaged);
    call(catalog::supportsListByPattern);
    call(catalog::supportsListTableByType);
    call(catalog::supportsVersionManagement);
    call(catalog::supportsPartitionModification);
    call(catalog::options);
    call(() -> {
      Catalog loaded = catalog.catalogLoader().load();
      try {
        loaded.listDatabases();
      } finally {
        loaded.close();
      }
    });
    call(catalog::caseSensitive);
    call(() -> catalog.dropTable(current, true));
    call(() -> catalog.dropDatabase("other", true, true));
    call(() -> PropertyChange.removeProperty("owner"));
    call(() -> SchemaChange.removeOption("comment"));
    call(() -> SchemaChange.updateComment("n"));
    call(() -> ViewChange.addDialect("impala", "select 1"));
  }

  private static void coverPartitions(Catalog catalog, Identifier id) {
    List<Map<String, String>> specs =
        Collections.singletonList(Collections.singletonMap("pt", "a"));
    call(() -> catalog.listPartitions(id));
    call(() -> catalog.listPartitionsPaged(id, 10, null, null));
    call(() -> catalog.listPartitionsByNames(id, specs));
    call(() -> catalog.markDonePartitions(id, specs));
    call(() -> catalog.createPartitions(id, specs));
    call(() -> catalog.dropPartitions(id, specs));
    call(() -> catalog.alterPartitions(id,
        Collections.singletonList(PartitionStatistics.unknown(
            Collections.singletonMap("pt", "a")))));
    call(() -> catalog.createPartitions(id, specs, false,
        Collections.emptyList(), false, Collections.emptyList()));
  }

  private static void coverVersioning(
      Catalog catalog, Identifier id, Snapshot snapshot) {
    call(() -> catalog.loadSnapshot(id));
    call(() -> catalog.loadSnapshot(id, "1"));
    call(() -> catalog.listSnapshotsPaged(id, 10, null));
    call(() -> catalog.listSchemasPaged(id, 10, null));
    call(() -> catalog.loadSchema(id, "0"));
    call(() -> catalog.rollbackTo(id, Instant.snapshot(1L)));
    call(() -> catalog.rollbackTo(id, Instant.tag("missing"), null));
    call(() -> catalog.rollbackSchema(id, 0L));
    call(() -> catalog.commitSnapshot(id, "main",
        snapshot == null ? null : snapshot.uuid(), snapshot,
        Collections.emptyList()));
    call(() -> catalog.listConsumersPaged(id, 10, null));
    call(() -> catalog.resetConsumer(id, "reader", 1L));
    call(() -> Instant.snapshot(1L));
    call(() -> Instant.tag("t"));
  }

  private static void coverCatalogBranches(
      Catalog catalog, Identifier id, Snapshot snapshot) {
    Long snapshotId = snapshot == null ? 1L : snapshot.id();
    call(() -> catalog.createTag(id, "ct", snapshotId, "c", true));
    call(() -> catalog.getTag(id, "ct"));
    call(() -> catalog.listTagsPaged(id, 10, null, null));
    call(() -> catalog.deleteTag(id, "ct"));
    call(() -> catalog.createBranch(id, "cb", null));
    call(() -> catalog.createBranch(id, "cb2", "ct", true));
    call(() -> catalog.listBranches(id));
    call(() -> catalog.renameBranch(id, "cb", "cb3"));
    call(() -> catalog.fastForward(id, "cb3"));
    call(() -> catalog.dropBranch(id, "cb2"));
  }

  private static void coverViewsAndFunctions(Catalog catalog) {
    Identifier viewId = Identifier.create(DB, "v");
    call(() -> catalog.createView(viewId, new ViewImpl(viewId,
        Collections.singletonList(new DataField(0, "id", DataTypes.INT())),
        "SELECT 1", Collections.emptyMap(), "c", Collections.emptyMap()), false));
    call(() -> catalog.getView(viewId));
    call(() -> catalog.listViews(DB));
    call(() -> catalog.listViewsPaged(DB, 10, null, null));
    call(() -> catalog.listViewDetailsPaged(DB, 10, null, null));
    call(() -> catalog.listViewsPagedGlobally(null, null, 10, null));
    Identifier view2 = Identifier.create(DB, "v2");
    call(() -> catalog.renameView(viewId, view2, false));
    call(() -> catalog.alterView(view2,
        Collections.singletonList(ViewChange.updateComment("d")), false));
    call(() -> catalog.dropView(view2, true));
    Identifier fnId = Identifier.create(DB, "fn");
    call(() -> catalog.createFunction(fnId, new FunctionImpl(fnId,
        Collections.singletonMap("sql", FunctionDefinition.sql("1"))), false));
    call(() -> catalog.getFunction(fnId));
    call(() -> catalog.listFunctions(DB));
    call(() -> catalog.listFunctionsPaged(DB, 10, null, null));
    call(() -> catalog.listFunctionDetailsPaged(DB, 10, null, null));
    call(() -> catalog.listFunctionsPagedGlobally(null, null, 10, null));
    call(() -> catalog.alterFunction(fnId,
        Collections.singletonList(org.apache.paimon.function.FunctionChange
            .updateComment("d")), false));
    call(() -> catalog.dropFunction(fnId, true));
    call(() -> catalog.authTableQuery(Identifier.create(DB, "cat"),
        Collections.singletonList("id")));
  }

  private static List<String> requiredFormats() {
    return Arrays.asList(CoreOptions.FILE_FORMAT_PARQUET, CoreOptions.FILE_FORMAT_ORC,
        CoreOptions.FILE_FORMAT_AVRO);
  }

  private static List<String> serviceFormats() throws Exception {
    List<String> formats = new ArrayList<>();
    try (InputStream in = Table.class.getClassLoader().getResourceAsStream(SERVICE);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        FileFormatFactory factory = (FileFormatFactory)
            Class.forName(line).getDeclaredConstructor().newInstance();
        formats.add(factory.identifier());
      }
    }
    return formats;
  }

  private static Schema appendSchema(String format) {
    return Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .column("val", DataTypes.BIGINT())
        .option(CoreOptions.FILE_FORMAT.key(), format)
        .build();
  }

  private static Schema pkSchema(String format, boolean deletionVectors) {
    Schema.Builder builder = Schema.newBuilder()
        .column("id", DataTypes.INT().notNull())
        .column("name", DataTypes.STRING())
        .column("val", DataTypes.BIGINT())
        .primaryKey("id")
        .option(CoreOptions.FILE_FORMAT.key(), format)
        .option(CoreOptions.BUCKET.key(), "1");
    if (deletionVectors) {
      builder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
    }
    return builder.build();
  }

  private static GenericRow row(int id, String name, long val) {
    return GenericRow.of(id, name == null ? null : BinaryString.fromString(name), val);
  }

  private static void write(Table table, GenericRow... rows) throws Exception {
    File temp = Files.createTempDirectory("paimon-io-").toFile();
    IOManager ioManager = IOManager.create(temp.getAbsolutePath());
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    BatchTableWrite write = builder.newWrite().withIOManager(ioManager);
    BatchTableCommit commit = builder.newCommit();
    try {
      for (GenericRow one : rows) {
        write.write(one);
      }
      commit.commit(write.prepareCommit());
    } finally {
      write.close();
      commit.close();
      ioManager.close();
      Env.delete(temp);
    }
  }

  private static List<String> scan(Table table) throws Exception {
    return scan(table.newReadBuilder());
  }

  private static List<String> scan(ReadBuilder readBuilder) throws Exception {
    return scan(readBuilder, false);
  }

  private static List<String> scan(ReadBuilder readBuilder, boolean executeFilter)
      throws Exception {
    List<Split> splits = readBuilder.newScan().plan().splits();
    TableRead read = readBuilder.newRead();
    if (executeFilter) {
      read = read.executeFilter();
    }
    List<String> rows = new ArrayList<>();
    try (RecordReader<InternalRow> reader = read.createReader(splits)) {
      reader.forEachRemaining(row -> rows.add(formatRow(row)));
    }
    Collections.sort(rows);
    return rows;
  }

  private static String formatRow(InternalRow row) {
    String name = row.isNullAt(1) ? "null" : row.getString(1).toString();
    return row.getInt(0) + "," + name + "," + row.getLong(2);
  }

  private interface Action {
    void run() throws Exception;
  }

  private static void call(Action action) {
    try {
      action.run();
    } catch (Throwable thrown) {
      if (isMissingClass(thrown) || thrown instanceof AssertionError) {
        throw new AssertionError(thrown.getMessage(), thrown);
      }
      if (thrown instanceof Error) {
        throw (Error) thrown;
      }
    }
  }

  private static boolean isMissingClass(Throwable thrown) {
    for (Throwable current = thrown; current != null; current = current.getCause()) {
      if (current instanceof ClassNotFoundException
          || current instanceof NoClassDefFoundError) {
        return true;
      }
    }
    return false;
  }

  private static File runtimeJar() throws Exception {
    URL location = Table.class.getProtectionDomain().getCodeSource().getLocation();
    File jar = new File(location.toURI());
    assertTrue("impala-paimon-runtime jar not found at " + location, jar.isFile());
    return jar;
  }

  private static void readClassNames(ZipFile zip, ZipEntry entry, Set<String> names) {
    if (!entry.getName().endsWith(".class")) {
      return;
    }
    try (DataInputStream in = new DataInputStream(zip.getInputStream(entry))) {
      if (in.readInt() != 0xCAFEBABE) {
        return;
      }
      in.readUnsignedShort();
      in.readUnsignedShort();
      int count = in.readUnsignedShort();
      String[] utf = new String[count];
      List<Integer> classIndexes = new ArrayList<>();
      for (int i = 1; i < count; i++) {
        int tag = in.readUnsignedByte();
        if (tag == 1) {
          int len = in.readUnsignedShort();
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          utf[i] = new String(bytes, StandardCharsets.UTF_8);
        } else if (tag == 7) {
          classIndexes.add(in.readUnsignedShort());
        } else if (tag == 8 || tag == 16 || tag == 19 || tag == 20) {
          in.readUnsignedShort();
        } else if (tag == 15) {
          in.readUnsignedByte();
          in.readUnsignedShort();
        } else if (tag == 5 || tag == 6) {
          in.readLong();
          i++;
        } else if (tag == 3 || tag == 4 || tag == 9 || tag == 10 || tag == 11
            || tag == 12 || tag == 17 || tag == 18) {
          in.readInt();
        } else {
          return;
        }
      }
      for (int index : classIndexes) {
        String name = utf[index];
        if (name == null) {
          continue;
        }
        String internal = unwrap(name);
        if (internal == null || isJdk(internal)) {
          continue;
        }
        names.add(internal);
      }
    } catch (IOException ignored) {
      // A truncated entry is reported as missing only if a later resolve fails.
    }
  }

  private static String unwrap(String descriptor) {
    String name = descriptor;
    while (name.startsWith("[")) {
      name = name.substring(1);
    }
    if (name.startsWith("L") && name.endsWith(";")) {
      name = name.substring(1, name.length() - 1);
    }
    if (name.isEmpty() || name.indexOf('/') < 0 && name.indexOf('.') < 0) {
      return null;
    }
    return name;
  }

  private static boolean isJdk(String internal) {
    return internal.startsWith("java/") || internal.startsWith("javax/")
        || internal.startsWith("jdk/") || internal.startsWith("sun/")
        || internal.startsWith("org/w3c/") || internal.startsWith("org/xml/");
  }

  private static boolean fromRuntimeJar(Class<?> cls) {
    if (cls.getProtectionDomain() == null
        || cls.getProtectionDomain().getCodeSource() == null
        || cls.getProtectionDomain().getCodeSource().getLocation() == null) {
      return false;
    }
    return cls.getProtectionDomain().getCodeSource().getLocation().toString()
        .contains("impala-paimon-runtime");
  }

  /**
   * Referenced, but not shipped and not used by the filesystem read/write path.
   * The summary still lists them. A missing class anywhere else fails the test.
   */
  private static boolean knownGap(String name) {
    return name.startsWith("scala.")
        || name.startsWith("org.tukaani.xz.")
        || name.startsWith("org.apache.paimon.codegen.")
        || name.startsWith("org.apache.hadoop.hive.ql.")
        || name.startsWith("org.apache.paimon.hive.mapred.")
        || name.startsWith("org.apache.paimon.shade.org.apache.commons.codec.")
        || name.equals("org.apache.paimon.shade.org.apache.commons.compress."
            + "compressors.lz4.XXHash32");
  }

  private static boolean mustResolve(String binary) {
    String name = binary.replace('/', '.');
    if (knownGap(name)) {
      return false;
    }
    if (name.startsWith("org.apache.paimon.")) {
      return true;
    }
    return name.startsWith("com.fasterxml.jackson.")
        || name.startsWith("com.google.common.")
        || name.startsWith("com.google.protobuf.")
        || name.startsWith("org.apache.hadoop.")
        || name.startsWith("org.apache.hc.")
        || name.startsWith("org.apache.arrow.")
        || name.startsWith("io.airlift.compress.")
        || name.startsWith("com.github.luben.")
        || name.startsWith("org.apache.datasketches.")
        || name.startsWith("org.apache.commons.codec.");
  }

  private static String rootOf(String binary) {
    String name = binary.replace('/', '.');
    if (name.startsWith("scala.")) {
      return "scala";
    }
    String shade = "org.apache.paimon.shade.";
    if (name.startsWith(shade)) {
      String[] rest = name.substring(shade.length()).split("\\.");
      int take = Math.min(3, rest.length);
      if (rest.length > 1 && "org".equals(rest[0]) && "apache".equals(rest[1])) {
        take = Math.min(4, rest.length);
      }
      while (take > 1 && Character.isUpperCase(rest[take - 1].charAt(0))) {
        take--;
      }
      StringBuilder root = new StringBuilder("shade");
      for (int i = 0; i < take; i++) {
        root.append('.').append(rest[i]);
      }
      return root.toString();
    }
    String[] parts = name.split("\\.");
    int take = Math.min(3, parts.length);
    StringBuilder root = new StringBuilder();
    for (int i = 0; i < take; i++) {
      if (i > 0) {
        root.append('.');
      }
      root.append(parts[i]);
    }
    return root.toString();
  }

  private static String formatSummary(
      Map<String, int[]> counts, Map<String, List<String>> uncovered) {
    StringBuilder out = new StringBuilder();
    out.append("Transitive reference coverage (referenced, shaded jar, ")
        .append("frontend classpath, unresolved)\n");
    int referenced = 0;
    int shaded = 0;
    int classpath = 0;
    int unresolved = 0;
    for (Map.Entry<String, int[]> entry : counts.entrySet()) {
      int[] row = entry.getValue();
      referenced += row[0];
      shaded += row[1];
      classpath += row[2];
      unresolved += row[3];
      out.append(String.format("%-40s %6d %6d %6d %6d%n",
          entry.getKey(), row[0], row[1], row[2], row[3]));
    }
    out.append(String.format("%-40s %6d %6d %6d %6d%n",
        "TOTAL", referenced, shaded, classpath, unresolved));
    out.append("Uncovered roots (not required to resolve):\n");
    for (Map.Entry<String, List<String>> entry : uncovered.entrySet()) {
      List<String> samples = entry.getValue();
      out.append("  ").append(entry.getKey()).append(" ")
          .append(samples.size());
      int limit = Math.min(3, samples.size());
      if (limit > 0) {
        out.append(" e.g. ").append(samples.subList(0, limit));
      }
      out.append('\n');
    }
    return out.toString();
  }

  /** Local filesystem catalog in a temp warehouse. */
  private static final class Env implements AutoCloseable {
    final File warehouse;
    final Catalog catalog;

    private Env(File warehouse, Catalog catalog) {
      this.warehouse = warehouse;
      this.catalog = catalog;
    }

    static Env open() throws Exception {
      File warehouse = Files.createTempDirectory("paimon-runtime-").toFile();
      Options options = new Options();
      options.set(CatalogOptions.WAREHOUSE, warehouse.getAbsolutePath());
      Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
      catalog.createDatabase(DB, true);
      return new Env(warehouse, catalog);
    }

    Table createAppend(String name, String format) throws Exception {
      return create(appendSchema(format), name);
    }

    Table create(Schema schema, String name) throws Exception {
      Identifier id = Identifier.create(DB, name);
      catalog.createTable(id, schema, false);
      return catalog.getTable(id);
    }

    @Override
    public void close() throws Exception {
      catalog.close();
      delete(warehouse);
    }

    private static void delete(File file) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          delete(child);
        }
      }
      file.delete();
    }
  }
}
