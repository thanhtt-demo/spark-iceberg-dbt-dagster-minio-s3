# dbt-spark-demo

Use GitHub Codespaces / VS Code Remote Dev Toolkit

Or

Local setup steps:
1. Install requirements (Linux)
2. Deploy spark server
3. Run dagster web ui

## Requirement

Debian

```sh
apt-get update
# For thrift
apt-get -y install gcc libsasl2-modules libsasl2-dev libsasl2-modules-gssapi-heimdal
```

## Spark Server

services

- minio
- spark thrift server (STS)


```sh
docker-compose up -d
```

## Dagster + DBT

```sh
cd my-dagster-project
pip install -e ".[dev]"
dagster dev
```

Open Web UI: http://127.0.0.1:3000

## Misc

### Disable auth

Disable auth by removing `hive-site.xml` mount of `sts` services.


## Setting DDL

# To dbt debug successs
%%sql
CREATE DATABASE IF NOT EXISTS default;

%%sql
CREATE DATABASE IF NOT EXISTS egn;

%%sql
CREATE DATABASE mart__s2;

%%sql
CREATE DATABASE IF NOT EXISTS raw;

%%sql
CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE raw.t24_account__s2(
  userId bigint,
  id bigint,
  title string,
  body string,
  partition_date date
)
PARTITIONED BY (partition_date)

CREATE TABLE raw.posts
(
  userId bigint,
  id bigint,
  title string,
  body string,
  partition_date date
)
PARTITIONED BY (partition_date)

CREATE TABLE raw.comments
(
  postId bigint,
  id bigint,
  name string,
  email string,
  body string,
  partition_date date
)
PARTITIONED BY (partition_date);

## show create table
CREATE TABLE raw.posts (
    userId STRING,
    id STRING,
    title STRING,
    body STRING,
    partition_date DATE)
USING iceberg
PARTITIONED BY (partition_date)
LOCATION 's3://warehouse/raw/posts'
TBLPROPERTIES (
'current-snapshot-id' = 'none',
'format' = 'iceberg/parquet',
'format-version' = '2',
'write.parquet.compression-codec' = 'zstd')

#
%%sql
show table extended in default like '*'

# update external tables
curl -X POST localhost:3000/report_asset_materialization/t24_account__s2?partition=2024-10-26