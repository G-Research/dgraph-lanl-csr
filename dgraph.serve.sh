#!/bin/bash

if [[ $# -ne 1 ]]
then
	echo "Provide path where to the bulk location"
	exit 1
fi

bulk=$(cd "$1"; pwd)

docker run --rm -it -p 8080:8080 -p 9080:9080 -p 8000:8000 -p 6080:6080 -v "$bulk:/dgraph" dgraph/dgraph:v20.11.0-rc1-10-g9de8f6677 /bin/bash -c "dgraph-ratel > /dgraph/ratel.log 2>&1 < /dev/null & dgraph zero --enable_sentry=false > /dgraph/zero.log 2>&1 < /dev/null & sleep 5; dgraph alpha --whitelist 0.0.0.0/0 --cwd /dgraph/out/0 2>&1 < /dev/null | tee -a /dgraph/alpha.log"
