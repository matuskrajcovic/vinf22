#!/bin/sh

# Usage: ./parse.sh [input_file] [output_file]
# parse.py has to be present in this directory

if [ $# -eq 2 ]
then
    hadoop fs -rmr parse_output
    hadoop fs -put $1
    spark-submit --packages com.databricks:spark-xml_2.12:0.15.0 parse.py $1 parse_output
    rm $2
    hadoop fs -getmerge -skip-empty-file parse_output $2
fi
