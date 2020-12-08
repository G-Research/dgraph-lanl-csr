#!/bin/bash

set -euo pipefail

if [ $# -ne 1 ]
then
	echo "Please provide the path to the RDF files, e.g. rdf"
	exit 1
fi

echo -e "Generated with https://github.com/G-Research/dgraph-lanl-csr/tree/v1.0,\nderived from https://csr.lanl.gov/data/cyber1/ and\nlicenced under https://creativecommons.org/publicdomain/zero/1.0/." > LICENCE.txt
zip="dgraph-lanl-csr-v1.0.zip"
echo -n "$zip: "
zip -r -0 $zip LICENCE.txt *.rdf | while read line; do echo -n .; done
echo
