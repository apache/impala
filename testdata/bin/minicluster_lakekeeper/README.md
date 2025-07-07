## About Lakekeeper
Lakekeeper is an Apache-Licensed implementation of the Apache Iceberg REST Catalog specification. See more at https://github.com/lakekeeper/lakekeeper

## Prerequisites to use Lakekeeper
You need docker compose (Compose V2) in your environment. This usually means you just need a recent docker version. Sometimes you need to install the docker compose plugin.

## Run and stop Lakekeeper in dev environment
Via the following scripts you can run/stop Lakekeeper. Be aware that each restart resets the warehouse contents.
```
${IMPALA_HOME}/testdata/bin/run-lakekeeper.sh
${IMPALA_HOME}/testdata/bin/stop-lakekeeper.sh
```

## Ingesting data
Until Impala can write Iceberg tables in the REST Catalog, you can use Trino to create tables.
Let's rebuild our Trino image for this:
```
docker stop impala-minicluster-trino
docker rm impala-minicluster-trino
./testdata/bin/build-trino-docker-image.sh
./testdata/bin/run-trino.sh
```

Let's connect to Trino via its CLI:
```
testdata/bin/trino-cli.sh
```

Now we can execute the following commands:
```
show catalogs;
create schema iceberg_lakekeeper.trino_db;
use iceberg_lakekeeper.trino_db;
create table trino_t (i int);
insert into trino_t values (35);
```

## Query via Impala
After this, you should be able to query the table via Impala:
```
mkdir /tmp/iceberg_lakekeeper
cp testdata/bin/minicluster_trino/iceberg_lakekeeper.properties /tmp/iceberg_lakekeeper

bin/start-impala-cluster.py --no_catalogd \
    --impalad_args="--catalogd_deployed=false --use_local_catalog=true \
    --catalog_config_dir=/tmp/iceberg_lakekeeper/"

bin/impala-shell.sh
```
