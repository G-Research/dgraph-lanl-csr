#!/bin/bash

if [[ $# -lt 4 ]]
then
	echo "Provide path to the data, where to bulk load the files, the path for temporary data,"
	echo "and the rdf files to load, relative to the given data path."
	echo "The first rdf file should be the schema file."
	echo "Paths must be absolute."
	exit 1
fi

data=$(cd "$1"; pwd)
bulk=$(cd "$2"; pwd)
tmp=$(cd "$3"; pwd)
schema=$4

mkdir -p $bulk
cp dgraph.bulk.load.sh $bulk/
docker run --rm -it -v "$data:/data" -v "$bulk:/dgraph" -v "$tmp:/tmp" dgraph/dgraph:v20.11.0-rc1-5-g2d2aabe9c /dgraph/dgraph.bulk.load.sh "$schema" "${@:4}"
