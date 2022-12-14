#!/bin/sh

# Usage: ./search.sh [input_index_file]
# search.py has to be present in this directory

if [ $# -eq 1 ]
then
    python search.py $1
fi
