# Scale Test Metadata

This README.md explain how to setup 1k_col_tbl table, a wide table with 1000+ columns,
long partition key, and huge partiton count. This table is intended to scale test metadata
operation limit against such table. This experiment/test is only documented here because
the time to load data and execute such metadata operation query can be prohibitively long
if written as a custom_cluster test run on single machine. This doc will use IMPALA-11669
as a case study.

## IMPALA-11669: Make Thrift max message size configuration

With the upgrade to Thrift 0.16, Thrift now has a protection against malicious message in
the form of a maximum size for messages. This is currently set to 100MB by default. Impala
should add the ability to override this default value. In particular, it seems like
communication between coordinators and the catalogd may need a larger value.

To test this, we will setup 1k_col_tbl with 150k partitons and run a metadata query to
test that coordinator-to-catalogd RPC works well. The steps are follow:

1. Run create-wide-table.sql with impala-shell to create 1k_col_tbl table.
   ```
   impala-shell.sh -f create-wide-table.sql
   ```

2. Populate 1k_col_tbl with 150k partitons by running load-1k_col_tbl.sh. HDFS must be
   running.
   ```
   ./load-1k_col_tbl.sh
   ```

3. With impala-shell, recover partition of table 1k_col_tbl.
   ```
   ALTER TABLE 1k_col_tbl RECOVER PARTITIONS;
   ```
   Run it multiple times if impalad/catalogd hits OOM until all partitions registered with
   HMS.

4. Restart impala cluster with `--thrift_rpc_max_message_size=0` (will set it to 100MB,
   the default max message size from Thrift).
   ```
   # kill cluster
   ./bin/start-impala-cluster.py --kill

   # start cluster
   ./bin/start-impala-cluster.py -s 1 \
     --state_store_args="--thrift_rpc_max_message_size=0" \
     --impalad_args="--thrift_rpc_max_message_size=0 --use_local_catalog=true" \
     --catalogd_args="--catalog_topic_mode=minimal"

   # Restart catalogd with additional args and jvm args
   ./bin/start-impala-cluster.py -s 1 --restart_catalogd_only --jvm_args=-Xmx12g \
     --catalogd_args=" --catalog_topic_mode=minimal --thrift_rpc_max_message_size=0 --warn_catalog_response_size_mb=1"
   ```

5. Run the following EXPLAIN query with impala-shell.
   ```
   impala-shell.sh -q 'EXPLAIN SELECT id FROM 1k_col_tbl'
   ```

   This will fail with "MaxMessageSize reached".
   ```
   Starting Impala Shell with no authentication using Python 2.7.16
   Warning: live_progress only applies to interactive shell sessions, and is being skipped for now.
   Opened TCP connection to localhost:21050
   Connected to localhost:21050
   Server version: impalad version 4.2.0-SNAPSHOT DEBUG (build e081348e02848f3e7dd904f44e43b9da63a93594)
   Query: EXPLAIN SELECT id FROM 1k_col_tbl
   ERROR: LocalCatalogException: Could not load partition names for table default.1k_col_tbl
   CAUSED BY: TException: TGetPartialCatalogObjectResponse(status:TStatus(status_code:GENERAL, error_msgs:[couldn't deserialize thrift msg:
   MaxMessageSize reached]), lookup_status:OK, object_version_number:1649)
   ```

6. Restart impala-shell again, but without passing `--thrift_rpc_max_message_size` argument.
   ```
   # kill cluster
   ./bin/start-impala-cluster.py --kill

   # start cluster
   ./bin/start-impala-cluster.py -s 1 \
     --impalad_args="--use_local_catalog=true" \
     --catalogd_args="--catalog_topic_mode=minimal"

   # Restart catalogd with additional args and jvm args
   ./bin/start-impala-cluster.py -s 1 --restart_catalogd_only --jvm_args=-Xmx12g \
     --catalogd_args="--catalog_topic_mode=minimal --warn_catalog_response_size_mb=1"
   ```

7. Run the same EXPLAIN query again. This should run successfully, because the default
   `thrift_rpc_max_message_size` is 64GB (see IMPALA-13020).
   ```
   impala-shell.sh -q 'EXPLAIN SELECT id FROM 1k_col_tbl'
   ```

To exercise Impala with SSL, add the following args in each daemon start up args.
```
--ssl_client_ca_certificate=$IMPALA_HOME/be/src/testutil/server-cert.pem --ssl_server_certificate=$IMPALA_HOME/be/src/testutil/server-cert.pem --ssl_private_key=$IMPALA_HOME/be/src/testutil/server-key.pem --hostname=localhost
```

And use the following impala-shell command.
```
impala-shell.sh --ssl --ca_cert=$IMPALA_HOME/be/src/testutil/server-cert.pem -q 'EXPLAIN SELECT id FROM 1k_col_tbl'
```
