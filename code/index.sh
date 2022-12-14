#!/bin/sh

# Usage: ./parse.sh [input_file] [output_index_file]
# index.py has to be present in this directory

if [ $# -eq 2 ]
then
    python index.py $1 $2
fi
