# Dgraph LANL CSR cyber1 dataset

This project helps to load the ["Comprehensive, Multi-Source Cyber-Security Events"](https://csr.lanl.gov/data/cyber1/) dataset published
by [Advanced Research in Cyber Systems](https://csr.lanl.gov/) into a [Dgraph cluster](https://dgraph.io/docs/get-started#dgraph).

This comprises the following steps:

- [Download dataset](#download-dataset)
- [Transform the dataset into RDF](#transform-the-dataset-into-rdf)
- [Bulk-load the RDF into Dgraph](#loading-rdf-into-dgraph)
- [Spin-up Dgraph cluster](#serve-the-graph)
- [Example queries for Dgraph](#querying-dgraph)

The graph has the following schema.

![...](schema.png)

The graph model mimics the original dataset model as much as possible and adds the `User`, `Computer`
and `ComputerUser` entities. Those have no `time` property, in contrast to the dataset entities that
have either `time` (event types) or `start`, `end` and `duration` (duration types) properties.

## Statistics

The dataset and the derived graph have the following properties:

|Table           |Rows         |Node Type        |Properties<br/>/ Edges|Nodes        |Triples       |
|:--------------:|:-----------:|:---------------:|:--------------------:|:-----------:|:------------:|
|*all files*     |             |`User`           | 3 / 0                |      100,162|       400,648|
|*all files*     |             |`Computer`       | 1 / 0                |       17,684|        35,368|
|*all files*     |             |`ComputerUser`   | 0 / 2                |      900,983|     2,702,949|
|`auth.txt.gz`   |1,051,430,459|`AuthEvent`      | 6 / 2                |1,051,430,459| 7,680,842,814|
|`proc.txt.gz`   |  426,045,096|`ProcessEvent`   | 4 / 1                |  426,045,096| 2,130,225,480|
|`flow.txt.gz`   |  129,977,412|`FlowDuration`   | 9 / 2                |  107,968,032| 1,048,963,354|
|`dns.txt.gz`    |   40,821,591|`DnsEvent`       | 2 / 2                |   40,821,591|   163,286,364|
|`redteam.txt.gz`|          749|`CompromiseEvent`| 2 / 2                |          715|         2,872|
|||||||
|**sum**         |1,648,275,307|                 |27 / 11               |1,627,284,722|11,026,459,849|

The dataset contains some null values in four columns:
authentication type (55%) and logon type (14%) in `auth.txt.gz` as well as
source (71%) and destination port (64%) in `flow.txt.gz`.
All other columns have values in all rows.

## Download dataset

First, download the dataset from https://csr.lanl.gov/data/cyber1/.
The compressed `.txt.gz` files should be decompressed to allow for scalability of the next step.

## Transform the dataset into RDF

This project provides a Spark application that lets you transform the dataset CSV files into RDF
that can be processed by Dgraph live and bulk loaders.

The following commands read the dataset from `./data` and write RDF files to `./rdf`.
Use appropriate paths accessible to the Spark workers if you run on a Spark cluster.

Run the Spark application locally on your machine with

    MAVEN_OPTS=-Xmx2g mvn test-compile exec:java -Dexec.classpathScope="test" -Dexec.cleanupDaemonThreads=false \
        -Dexec.mainClass="uk.co.gresearch.dgraph.lanl.csr.RunSparkApp" -Dexec.args="data/ rdf/"

Or via Spark submit on your Spark cluster:

    mvn package
    spark-submit --master "…" --class uk.co.gresearch.dgraph.lanl.csr.CsrDgraphSparkApp \
        target/dgraph-lanl-csr-1.0-SNAPSHOT.jar data/ rdf/

The application takes 3 hours on 8 CPUs with 4 GB RAM and 100 GB SSD disk.
On a cluster with more CPUs the time reduces proportionally.

## Loading RDF into Dgraph

Load the RDF files by running

    cp dgraph.schema.rdf rdf/
    ./dgraph.bulk.sh $(pwd)/rdf $(pwd)/bulk /data/dgraph.schema.rdf "/data/*.rdf/*.txt.gz"

The `dgraph.schema.rdf` schema file defines all predicates and types and adds indices to all predicates.

## Serve the graph

After bulk loading the RDF files into `bulk/out/0` we can serve that graph by running

    ./dgraph.serve.sh $(pwd)/bulk

## Querying Dgraph

Ten users (`User`), their logins (`ComputerLogin`) and destinations of `AuthEvent`s from those logins:

    {
      user(func: eq(<dgraph.type>, "User"), first: 10) {
        uid
        id
        login
        domain
        logins: ~user {
          uid
          computer { uid id }
          logsOnto: ~sourceComputerUser @filter(eq(<dgraph.type>, "AuthEvent")) {
            destinationComputerUser {
              uid
              computer { uid id }
              user { uid id }
            }
          }
        }
      }
    }

![...](dgraph-ratel-query-graph.png)


## Fine-tuning

The Spark application `CsrDgraphSparkApp` lets you customize the RDF generation part of this pipeline.

User ids are split on the `@` characters. If your dataset uses a different separator between login and domain, set this here:

    // user ids are split on this pattern to extract login and domain
    val userIdSplitPattern = "@"

The Spark application prints some statistics of the dataset. Computing these is expensive and only needed once.
You should run this at least once to see if assumption of the code hold for the particular dataset.

    // prints statistics of the dataset, this is expensive so only really needed once
    val doStatistics = false

The RDF files will be a multiple in size of the input files. Compressing them saves disk space at the extra cost of CPU.

    // written RDF files will be compressed if true
    val compressRdf = true

Some input files are known to have duplicate rows. These are duplicated by the Spark application by adding
an optional `occurrences` predicate to those events that occur multiple times in the input files.
Computing these extra predicates is expensive and only needs to be done for files when duplicate rows are known to exist.
The statistics provide such information for all input files.

    // tables with duplicate rows need to be de-duplicated
    // deduplication is expensive, so only set to true if there are duplicate rows
    // you can set doStatistics = true to find out
    val deduplicateAuth = false
    val deduplicateProc = false
    val deduplicateFlow = true
    val deduplicateDns = false
    val deduplicateRed = true

In `uk/co/gresearch/dgraph/lanl/package.scala` you can switch from [int](https://dgraph.io/docs/query-language/schema/#scalar-types) time
to proper Dgraph [datetime](https://dgraph.io/docs/query-language/schema/#scalar-types) timestamps.
Instead of

```scala
def timeLiteral(time: Int): String = literal(time, integerType)
```

use this `timeLiteral` implementation:

```scala
def timeLiteral(time: Int): String =
  literal(Instant.ofEpochSecond(time).atOffset(ZoneOffset.UTC).toString, datetimeType)
```

Here you could also offset the `int` time to any time epoch other than `1970-01-01`.

Switching to `datetime` timestamps requires you to also modify the `dgraph.schema.rdf`. Instead of

    <time>: int @index(int) .
    <start>: int @index(int) .
    <end>: int @index(int) .

you should now use

    <time>: dateTime @index(hour) .
    <start>: dateTime @index(hour) .
    <end>: dateTime @index(hour) .

All this allows you to benefit from [datetime indices](https://dgraph.io/docs/query-language/schema/#datetime-indices)
rather than [integer index](https://dgraph.io/docs/query-language/schema/#indexing).
