#!/usr/bin/env ruby
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# This script generates the "CREATE TABLE" and "INSERT" statements for loading
# benchmark data and writes them to create-benchmark*-generated.sql and
# load-benchmark*-generated.sql respectively.
#
# The statements that are generated are based on statements based on an input test vector
# (read from a file) that describes the coverage desired. For example, currently
# we want to run benchmarks with different data sets, across different file types, and
# with different compression algorithms set.

# The input test vectors are generated via the 'generate_test_vectors.rb' so
# ensure that script has been run (or the test vector files already exist) before
# running this script.
#
# Note: This statement generation is assuming the following data loading workflow:
# 1) Load all the data in its orginal format (text)
# 2) Create tables for the new file formats and compression types
# 3) Run INSERT OVERWRITE TABLE SELECT * from the original table into the new tables
#
# TODO: Convert this script to python

### CONSTANTS
# Used by the HTML workload
RANKINGS_CREATE_STATEMENT =
"DROP TABLE IF EXISTS Rankings_%s;
CREATE TABLE Rankings_%s (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|' %s;"

# Used by the HTML workload
USER_VISITS_CREATE_STATEMENT =
"DROP TABLE IF EXISTS UserVisits_%s;
CREATE TABLE UserVisits_%s (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'  %s;"

USER_VISTS_INSERT_STATEMENT =
    "FROM UserVisits_web_text_none INSERT OVERWRITE TABLE UserVisits_%s SELECT *;"
RANKINGS_INSERT_STATEMENT =
    "FROM Rankings_web_text_none INSERT OVERWRITE TABLE Rankings_%s SELECT *;"

GREP_CREATE_TABLE_STATEMENT =
"DROP TABLE IF EXISTS %s;
CREATE TABLE %s (field string) partitioned by (chunk int) %s;"

GREP_INSERT_STATEMENT =
    "FROM %s_text_none INSERT OVERWRITE TABLE %s PARTITION(chunk) SELECT *;"

# Compression type will be another dimension in the future.
COMPRESSION_TYPE = "set mapreduce.output.compression.type=BLOCK;"
COMPRESSION_ENABLED = "SET hive.exec.compress.output=%s;"
COMPRESSION_CODEC = "SET mapreduce.output.compression.codec=com.hadoop.compression.%s;"
SET_DYNAMIC_PARTITION_STATEMENT = "SET hive.exec.dynamic.partition=true;"
SET_PARTITION_MODE_NONSTRICT_STATEMENT = "SET hive.exec.dynamic.partition.mode=nonstrict;"
###

def generate_file_format(file_format)
    case file_format
        when "text" then "stored as textfile"
        when "rc_file" then "stored as rcfile"
        when "sequence_file" then "stored as sequencefile"
        else raise "Unknown file format #{file_format}"
    end
end

# Source tables are used to load data initial data in. Currently with in text format
# with no compression. These are then copied to other data formats. Because of this
# we skip creating of insert and create statements for these tables.
def is_source_table(file_format, compression)
    file_format == 'text' and compression == 'none'
end

def generate_create_statement(table_name, data_set, file_format)
    case data_set
        when "grep1GB", "grep10GB" then GREP_CREATE_TABLE_STATEMENT %
            [table_name, table_name, generate_file_format(file_format)]
        when "web" then
            create = RANKINGS_CREATE_STATEMENT %
                [table_name, table_name, generate_file_format(file_format)] + "\n\n"
            create += USER_VISITS_CREATE_STATEMENT %
                [table_name, table_name, generate_file_format(file_format)]
        else raise "Unknown data set: #{data_set}"
    end
end

def generate_compression_codec_statement(compression)
    if compression == "none" then return "" end

    codec = case compression
                when 'lzip' then "lzo.LzoCodec"
                when 'gzip' then "GzipCodec"
                when 'bzip2' then "BZip2Codec"
                when 'snappy' then "SnappyCodec"
                else raise "Unknown compression format: #{compression}"
            end
    "%s\n%s" % [COMPRESSION_TYPE, COMPRESSION_CODEC % codec]
end

def generate_compression_enabled_statement(compression)
    compression == "none" ?  COMPRESSION_ENABLED % "false" : COMPRESSION_ENABLED % "true"
end

def generate_insert_statement(data_set, table_name)
    statement = SET_PARTITION_MODE_NONSTRICT_STATEMENT + "\n"
    statement += SET_DYNAMIC_PARTITION_STATEMENT + "\n"
    statement += case data_set
        when "grep1GB", "grep10GB" then GREP_INSERT_STATEMENT % [data_set, table_name]
        when "web" then
            insert = USER_VISTS_INSERT_STATEMENT % [table_name] + "\n"
            insert += RANKINGS_INSERT_STATEMENT % [table_name]
        else raise "Unknown data set: #{data_set}"
    end
end

def generate_statements(table_name, file_format, data_set, compression)
    output = generate_compression_enabled_statement(compression) + "\n"
    output += generate_compression_codec_statement(compression) + "\n"
    output += generate_insert_statement(data_set, table_name) + "\n"
end

def build_table_name(file_format, data_set, compression)
    "#{data_set}_#{file_format}_#{compression}"
end

def write_statements_to_file_based_on_input_vector(output_name, input_file_name)
    output_create =
        "-- Generated file - It is not recommended to edit this file directly.\n"
    output_load = "--Generated file - It is not recommended to edit this file directly.\n"

    # Expected Input Format: <file format> <data set> <compression>
    File.open(input_file_name, 'r') do |file|
        while line = file.gets
            file_format, data_set, compression = line.split

            # Don't want to generate create/insert statements for sources
            # tables because they are already created.
            if is_source_table(file_format, compression) then next end

            table_name = build_table_name(file_format, data_set, compression)
            output_create +=
                generate_create_statement(table_name, data_set, file_format) + "\n\n"
            output_load +=
                generate_statements(table_name, file_format, data_set, compression) + "\n"
        end
    end

    File.open("create-benchmark-#{output_name}-generated.sql", 'w') do |file|
        file.puts output_create
    end
    File.open("load-benchmark-#{output_name}-generated.sql", 'w') do |file|
        file.puts output_load
    end
end

write_statements_to_file_based_on_input_vector(
                                           "exhaustive", "benchmark_exhaustive.vector")
write_statements_to_file_based_on_input_vector("pairwise", "benchmark_pairwise.vector")
write_statements_to_file_based_on_input_vector("core", "benchmark_core.vector")
