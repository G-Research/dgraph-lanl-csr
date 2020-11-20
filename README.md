./dgraph.bulk.sh $(pwd)/data $(pwd)/bulk /data/dgraph.schema.rdf "/data/*.rdf/*.txt"

./dgraph.serve.sh $(pwd)/bulk
