#!/usr/bin/env ruby
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script is used to generate test "vectors" based on a dimension input file.
# A vector in this context is simply a permutation of the values in the the
# dimension input file.  For example, in this case the script is generating test vectors
# for the Impala / Hive benchmark suite so interesting dimensions are data set,
# file format, and compression algorithm. More can be added later.
# The output of running this script is a list of vectors. Currently two different vector
# outputs are generated - an "exhaustive" vector which contains all permutations and a
# "pairwise" vector that contains a subset of the vectors by chosing all combinations of
# pairs (the pairwise strategy). More information about pairwise can be found at
# http://www.pairwise.org.
#
# The end goal is to have a reduced set of test vectors to provide coverage but don't take
# as long to run as the exhaustive set of vectors along with a set of vectors that provide
# full coverage. This is especially important for benchmarks which work on very large data
# sets.
#
# The output files output can then be read in by other tests by other scripts,tools,tests.
# In the benchmark case the vector files are used by generate_benchmark_statements.rb to
# dynamically build the schema and data for running benchmarks.
#
# TODO: Convert this script to python

require 'rubygems'
require 'pairwise' #from the 'pairwise' ruby gem
require 'yaml'

class VectorGenerator
    def initialize(input_vectors)
        @input_vectors = input_vectors
    end

    def generate_pairwise_matrix
        Pairwise.combinations(*@input_vectors)
    end

    def generate_exhaustive_matrix
        @input_vectors[0].product(*@input_vectors[1..-1])
    end

end

class BenchmarkVector

    attr_accessor :compression, :data_set, :file_format

    def initialize(input_vector)
        # This is assuming a specific sort order. TODO: Improve this
        @compression, @data_set, @file_format = input_vector
    end

    def to_a
        [@compression, @data_set, @file_format]
    end

    def to_s
        "#{@file_format} #{@data_set} #{@compression}"
    end
end

def generate_matrix(exploration_strategy, input_vectors)
    generator = VectorGenerator.new(input_vectors)

    vectors = Array.new()
    if exploration_strategy == 'pairwise'
       vectors = generator.generate_pairwise_matrix.collect {|item| BenchmarkVector.new(item)}
    elsif exploration_strategy == 'exhaustive'
        vectors = generator.generate_exhaustive_matrix.collect {|item| BenchmarkVector.new(item)}
    else
        raise "Unsupported option";
    end

    apply_constraints(vectors)
end

def write_matrix_to_file(file_name, matrix)
    file = File.open(file_name, 'w')

    # This is assuming a specific sort order. TODO: Improve this
    matrix.each {|vector|
        file.puts("#{vector.file_format} #{vector.data_set} #{vector.compression}")}
    file.close
end

def apply_constraints(input_vectors)
    # rc_files don't currently work with large data sets
    input_vectors = input_vectors.select {|vector| vector.file_format != 'rc_file'}
    # text file format does not support compression
    input_vectors = input_vectors.select {|vector|
        !(vector.file_format == 'text' && vector.compression != 'none') }
end

if ARGV.length != 1 then raise "usage: generate_test_vectors.rb <dimension file>" end

dimension_file = ARGV[0]

input_hash = YAML.load_file(dimension_file)

# this is a hacky way to ensure fixed ordering.
input_vectors = Array.new()
input_hash.keys.sort.each {|k| input_vectors.push((input_hash[k]))}

write_matrix_to_file('benchmark_pairwise.vector',
    generate_matrix('pairwise', input_vectors.to_a))
write_matrix_to_file('benchmark_exhaustive.vector',
    generate_matrix('exhaustive', input_vectors.to_a))
