#!/usr/bin/python

import re
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession


# parse links and map them with their corresponding page
def my_mapper(row):
    title = str(row['title'].encode('utf8')) 
    text = row['revision']['text'].encode('utf8')
    regex = r"\[\[((?:(?!(?:\]\])|(?:\[\[)|:).)+)\]\]"
    matches = re.findall(regex, text, re.MULTILINE)
    links = []
    for match in matches:
        if(match):
            # remove \t, get the alternative anchor text
            link = match.replace('\t', '\\t')
            link = re.split(r"\|", link)
            if(len(link) > 1):
                link = link[1]
            else:
                link = link[0]
            link = re.split(r"#", link)[0].strip()
            links.append((link, [title]))
    return links


# reduce all pages links was a part of
def my_reducer(arr1, arr2):
    return arr1 + arr2


# count document and collection frequency
def frequency_counter(elem):
    return (elem[0], (len(elem[1]), len(set(elem[1]))))


# output link and frequencies divided by \t
def my_outputter(elem):
    return elem[0] + '\t' + str(elem[1][0]) + '\t' + str(elem[1][1])


# main parse function, load the XML file and returns text output
def my_parser(context, input_file, output_file, compress=False):
    my_schema = StructType([ 
        StructField("title", StringType(), True),
        StructField("revision", StructType([
            StructField("text", StringType(), True)    
        ]), True)
    ])

    # init dataframe
    df = context.read.format("com.databricks.spark.xml").options(rowTag="page", rootTag="mediawiki").load(input_file, schema=my_schema)

    # select, map and reduce, then save
    sel = df.select(['title', 'revision']).rdd.flatMap(my_mapper).reduceByKey(my_reducer).map(frequency_counter).map(my_outputter)
    if(compress):
        sel.saveAsTextFile(output_file, 'org.apache.hadoop.io.compress.BZip2Codec')
    else:
        sel.saveAsTextFile(output_file)


# argument parsing and launching the XML file parsing
if(len(sys.argv) > 2):
    spark = SparkSession.builder.getOrCreate()
    my_parser(spark, sys.argv[1], sys.argv[2])
else:
    print('Not enough arguments')
