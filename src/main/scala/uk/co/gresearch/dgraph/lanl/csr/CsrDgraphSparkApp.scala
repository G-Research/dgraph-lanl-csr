package uk.co.gresearch.dgraph.lanl.csr

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import uk.co.gresearch._
import uk.co.gresearch.dgraph.lanl.csr.Schema.Predicates._
import uk.co.gresearch.dgraph.lanl.csr.Schema.Types

// Data model of the input files
case class Auth(time: Int, srcUser: String, dstUser: String, srcComputer: String, dstComputer: String, authType: Option[String], logonType: Option[String], authOrient: Option[String], outcome: Option[String])
case class Proc(time: Int, user: String, computer: String, processName: String, eventType: String)
case class Flow(time: Int, duration: Int, srcComputer: String, srcPort: Option[Int], dstComputer: String, dstPort: Option[Int], protocol: Option[String], packets: Option[Int], bytes: Option[Long])
case class Dns(time: Int, srcComputer: String, resolvedComputer: String)
case class Red(time: Int, user: String, srcComputer: String, dstComputer: String)

// Entities of the graph (have no time dimension)
case class User(blankId: Long, id: String, login: Option[String], domain: Option[String])
case class Computer(blankId: Long, id: String)
// Event entities (events have time: Int)
case class AuthEvent(blankId: Long, srcUserId: Long, dstUserId: Long, srcComputerId: Long, dstComputerId: Long, authType: Option[String], logonType: Option[String], authOrient: Option[String], outcome: Option[String], time: Int, occurrences: Option[Int])
case class ProcessEvent(blankId: Long, userId: Long, computerId: Long, processName: String, eventType: String, time: Int, occurrences: Option[Int])
case class DnsEvent(blankId: Long, srcComputerId: Long, resolvedComputerId: Long, time: Int, occurrences: Option[Int])
case class CompromiseEvent(blankId: Long, userId: Long, srcComputerId: Long, dstComputerId: Long, time: Int, occurrences: Option[Int])
// Duration entities (durations have Int start, end, duration)
case class ProcessDuration(blankId: Long, userId: Long, computerId: Long, processName: String, start: Option[Int], end: Option[Int], duration: Option[Int])
case class FlowDuration(blankId: Long, srcComputerId: Long, dstComputerId: Long, srcPort: Option[Int], dstPort: Option[Int], protocol: Option[String], packets: Option[Int], bytes: Option[Long], start: Int, end: Int, duration: Int, occurrences: Option[Int])

// Triple for written to RDF files
case class Triple(s: String, p: String, o: String)

object CsrDgraphSparkApp {

  // user ids are split on this pattern to extract login and domain
  val userIdSplitPattern = "@"

  // prints statistics of the dataset, this is expensive so only really needed once
  val doStatistics = false

  // caching the input files improves performance, but requires a lot of RAM
  // should only be done when running on a Spark cluster with sufficient memory (64GB mem storage)
  val doCache = false

  // written RDF files will be compressed if true
  val compressRdf = true

  // tables with duplicate rows need to be de-duplicated
  // deduplication is expensive, so only set to true if there are duplicate rows
  // you can set doStatistics = true to find out
  val deduplicateAuth = false
  val deduplicateProc = false
  val deduplicateFlow = true
  val deduplicateDns = false
  val deduplicateRed = true

  // xml schema datatypes used in the RDF files
  // set to None if you do not want any
  val dateType: Option[String] = None  // Some("<http://www.w3.org/2001/XMLSchema#date>")
  val doubleType: Option[String] = None  // Some("<http://www.w3.org/2001/XMLSchema#double>")
  val integerType: Option[String] = None  // Some("<http://www.w3.org/2001/XMLSchema#integer>")
  val longType: Option[String] = None  // Some("<http://www.w3.org/2001/XMLSchema#long>")
  val stringType: Option[String] = None  // Some("<http://www.w3.org/2001/XMLSchema#string>")
  val datetimeType: Option[String] = None  // Some("<http://www.w3.org/2001/XMLSchema#dateTime>")
  val noType: Option[String] = None

  // read an input table file
  def read[T](path: String)(implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] =
    spark.read.option("nullValue", "?").schema(encoder.schema).csv(path).as[T](encoder)

  def main(args: Array[String]): Unit = {
    println("This tool models the LANL CSR dataset as a graph and writes it")
    println("in a simple RDF NQuad triple format that can be loaded into a Dgraph cluster.")
    println()

    if (args.length != 2) {
      println("Please provide the path to the LANL CSR dataset. All files should exist uncompressed in that directory.")
      println("Also provide the output directory.")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val startTime = System.nanoTime()

    // start a local Spark session
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Dgraph LANL CSR Spark App")
        .config("spark.local.dir", ".")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    import spark.implicits._

    val auth = read[Auth](s"$inputPath/auth.txt").when(doCache).call(_.cache())
    val proc = read[Proc](s"$inputPath/proc.txt").when(doCache).call(_.cache())
    val flow = read[Flow](s"$inputPath/flows.txt").when(doCache).call(_.cache())
    val dns = read[Dns](s"$inputPath/dns.txt").when(doCache).call(_.cache())
    val red = read[Red](s"$inputPath/redteam.txt").when(doCache).call(_.cache())

    val (authCount, procCount, flowCount, dnsCount, redCount) = if (doStatistics) {
      (auth.count, proc.count, flow.count, dns.count, red.count)
    } else {
      (-1L, -1L, -1L, -1L, -1L)
    }

    if (doStatistics) {
      println(s"Auth nulls: ${countNulls(auth)}")
      println(s"Proc nulls: ${countNulls(proc)}")
      println(s"Flow nulls: ${countNulls(flow)}")
      println(s"DNS nulls: ${countNulls(dns)}")
      println(s"Red nulls: ${countNulls(red)}")
      println()

      println(f"Row counts: auth=$authCount%,d proc=$procCount%,d flow=$flowCount%,d dns=$dnsCount%,d red=$redCount%,d " +
        f"all=${authCount+procCount+flowCount+dnsCount+redCount}%,d")

      println(s"Duplicate rows: " +
        f"auth=${countDuplicates(auth)}%,d " +
        f"proc=${countDuplicates(proc)}%,d " +
        f"flow=${countDuplicates(flow)}%,d " +
        f"dns=${countDuplicates(dns)}%,d " +
        f"red=${countDuplicates(red)}%,d")

      val procWithUnexpectedType = proc.where(!$"eventType".isin("Start", "End")).count
      println(s"Rows with Proc.eventType other than 'Start' and 'End': " +
        f"$procWithUnexpectedType%,d (${(procWithUnexpectedType * 1000 / procCount) / 10.0}%%)")
    }

    // TODO: filter out rows breaking assumptions

    val (userMapping, computerMapping) = {
      // generate all occurring users
      val users = Seq(
        auth.select(explode(array($"srcUser", $"dstUser")).as("id")),
        proc.select($"user".as("id")),
        red.select($"user".as("id")),
      )
        .reduce(_.unionByName(_))
        .distinct()
        .call(addLoginAndDomain(_, $"id"))
        .call(addBlankId)
        .as[User]
        .cache()

      // generate all occurring computers
      val computers = Seq(
        auth.select(explode(array($"srcComputer", $"dstComputer")).as("id")),
        proc.select($"computer".as("id")),
        flow.select(explode(array($"srcComputer", $"dstComputer")).as("id")),
        dns.select(explode(array($"srcComputer", $"resolvedComputer")).as("id")),
        red.select(explode(array($"srcComputer", $"dstComputer")).as("id")),
      )
        .reduce(_.unionByName(_))
        .distinct()
        .call(addBlankId)
        .as[Computer]
        .cache()

      // write users and computers
      users
        .flatMap { user =>
          val userId = blank("user", user.blankId)
          Seq(
            Some(Triple(userId, predicate(isType), literal(Types.User))),
            Some(Triple(userId, predicate(id), literal(user.id, stringType))),
            user.login.map(v => Triple(userId, predicate(login), literal(v, stringType))),
            user.domain.map(v => Triple(userId, predicate(domain), literal(v, stringType))),
          ).flatten
        }
      .call(writeRdf(s"$outputPath/users.rdf", compressRdf))

      computers
        .flatMap { computer =>
          val computerId = blank("comp", computer.blankId)
          Seq(
            Some(Triple(computerId, predicate(isType), literal(Types.Computer))),
            Some(Triple(computerId, predicate(id), literal(computer.id, stringType))),
          ).flatten
        }
      .call(writeRdf(s"$outputPath/computers.rdf", compressRdf))

      // derive user and computer mappings and uncache unused datasets
      val userMapping = users.select($"id", $"blankId").as[(String, Long)].cache
      userMapping.count()  // materialize user mapping
      users.unpersist()    // unpersist users dataset
      val computerMapping = computers.select($"id", $"blankId").as[(String, Long)].cache
      computerMapping.count()  // materialize computer  mapping
      computers.unpersist()    // unpersist computer dataset

      (userMapping, computerMapping)
    }

    // turn Auth into AuthEvent
    val authEvents =
      auth
        .call(mapIdToBlankId("srcUser", "srcUserId", userMapping))
        .call(mapIdToBlankId("dstUser", "dstUserId", userMapping))
        .call(mapIdToBlankId("srcComputer", "srcComputerId", computerMapping))
        .call(mapIdToBlankId("dstComputer", "dstComputerId", computerMapping))
        .when(deduplicateAuth).call(addOccurrences)
        .when(!deduplicateAuth).call(addNoOccurrences)
        .call(addId)
        .as[AuthEvent]

    // turn Proc into ProcessEvent
    val processEvent =
      proc
        .call(mapIdToBlankId("user", "userId", userMapping))
        .call(mapIdToBlankId("computer", "computerId", computerMapping))
        .when(deduplicateProc).call(addOccurrences)
        .when(!deduplicateProc).call(addNoOccurrences)
        .call(addId)
        .as[ProcessEvent]

    // turn DNS into DnsEvent
    val dnsEvent =
      dns
        .call(mapIdToBlankId("srcComputer", "srcComputerId", computerMapping))
        .call(mapIdToBlankId("resolvedComputer", "resolvedComputerId", computerMapping))
        .when(deduplicateDns).call(addOccurrences)
        .when(!deduplicateDns).call(addNoOccurrences)
        .call(addId)
        .as[DnsEvent]

    // turn Red into CompromiseEvent
    val compromiseEvent =
      red
        .call(mapIdToBlankId("user", "userId", userMapping))
        .call(mapIdToBlankId("srcComputer", "srcComputerId", computerMapping))
        .call(mapIdToBlankId("dstComputer", "dstComputerId", computerMapping))
        .when(deduplicateRed).call(addOccurrences)
        .when(!deduplicateRed).call(addNoOccurrences)
        .call(addId)
        .as[CompromiseEvent]

    // turn Flow into FlowDuration
    val flowDuration =
      flow
        .call(mapIdToBlankId("srcComputer", "srcComputerId", computerMapping))
        .call(mapIdToBlankId("dstComputer", "dstComputerId", computerMapping))
        .withColumnRenamed("time", "start")
        .withColumn("end", $"start" + $"duration")
        .when(deduplicateFlow).call(addOccurrences)
        .when(!deduplicateFlow).call(addNoOccurrences)
        .call(addId)
        .as[FlowDuration]

    // turn entities and durations into triples and write them to RDF
    authEvents
      .flatMap { event =>
        val eventId = blank("auth", event.blankId)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.AuthEvent))),
          Some(Triple(eventId, predicate(sourceUser), blank("user", event.srcUserId))),
          Some(Triple(eventId, predicate(destinationUser), blank("user", event.dstUserId))),
          Some(Triple(eventId, predicate(sourceComputer), blank("comp", event.srcComputerId))),
          Some(Triple(eventId, predicate(destinationComputer), blank("comp", event.dstComputerId))),
          event.authType.map(v => Triple(eventId, predicate(authType), literal(v, stringType))),
          event.logonType.map(v => Triple(eventId, predicate(logonType), literal(v, stringType))),
          event.authOrient.map(v => Triple(eventId, predicate(authOrient), literal(v, stringType))),
          event.outcome.map(v => Triple(eventId, predicate(outcome), literal(v, stringType))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf(s"$outputPath/auth-events.rdf", compressRdf))

    processEvent
      .flatMap { event =>
        val eventId = blank("proc", event.blankId)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.ProcessEvent))),
          Some(Triple(eventId, predicate(user), blank("user", event.userId))),
          Some(Triple(eventId, predicate(computer), blank("comp", event.computerId))),
          Some(Triple(eventId, predicate(processName), literal(event.processName, stringType))),
          Some(Triple(eventId, predicate(eventType), literal(event.eventType, stringType))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf(s"$outputPath/proc-events.rdf", compressRdf))

    dnsEvent
      .flatMap { event =>
        val eventId = blank("dns", event.blankId)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.DnsEvent))),
          Some(Triple(eventId, predicate(sourceComputer), blank("comp", event.srcComputerId))),
          Some(Triple(eventId, predicate(resolvedComputer), blank("comp", event.resolvedComputerId))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf(s"$outputPath/dns-events.rdf", compressRdf))

    compromiseEvent
      .flatMap { event =>
        val eventId = blank("red", event.blankId)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.CompromiseEvent))),
          Some(Triple(eventId, predicate(user), blank("user", event.userId))),
          Some(Triple(eventId, predicate(sourceComputer), blank("comp", event.srcComputerId))),
          Some(Triple(eventId, predicate(destinationComputer), blank("comp", event.dstComputerId))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf(s"$outputPath/compromise-events.rdf", compressRdf))

    flowDuration
      .flatMap { event =>
        val eventId = blank("flow", event.blankId)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.FlowDuration))),
          Some(Triple(eventId, predicate(sourceComputer), blank("comp", event.srcComputerId))),
          Some(Triple(eventId, predicate(destinationComputer), blank("comp", event.dstComputerId))),
          event.srcPort.map(v => Triple(eventId, predicate(sourcePort), literal(v, integerType))),
          event.dstPort.map(v => Triple(eventId, predicate(destinationPort), literal(v, integerType))),
          event.protocol.map(v => Triple(eventId, predicate(protocol), literal(v, integerType))),
          event.packets.map(v => Triple(eventId, predicate(packets), literal(v, integerType))),
          event.bytes.map(v => Triple(eventId, predicate(bytes), literal(v, longType))),
          Some(Triple(eventId, predicate(start), timeLiteral(event.start))),
          Some(Triple(eventId, predicate(`end`), timeLiteral(event.`end`))),
          Some(Triple(eventId, predicate(duration), literal(event.duration, integerType))),
        ).flatten
      }
      .call(writeRdf(s"$outputPath/flow-durations.rdf", compressRdf))

    println()
    val seconds = (System.nanoTime() - startTime) / 1000000000
    println(s"Finished in ${seconds / 3600}h ${seconds / 60 % 60}m ${seconds % 60}s")
    Console.readLine()
  }

  def countNulls[T](dataset: Dataset[T]): String = {
    val counts = countColumns((c: Column) => c.isNull, dataset).head()
    val fields = counts.schema.fields.map(_.name)
    val values = counts.getValuesMap[Long](fields)
    val nulls = values.flatMap { case (c, v) => if (v > 0) Some(f"$v%,d ($c)") else None }.mkString(" ")
    if (nulls.isEmpty) "None" else nulls
  }

  def countColumns[T](condition: Column => Column, dataset: Dataset[T]): DataFrame = {
    val ones = dataset.columns.map(c => when(condition(col(c)), 1).otherwise(0).as(c))
    val sums = dataset.columns.toSeq.map(c => sum(c).as(c))
    dataset
      .select(ones: _*)
      .groupBy()
      .agg(sums.head, sums.tail: _*)
  }

  def countDuplicates[T](dataset: Dataset[T]): Long = {
    import dataset.sqlContext.implicits._
    dataset
      .groupBy(dataset.columns.map(col): _*)
      .agg(count(lit(1)).as("count"))
      .where($"count" > 1)
      .count
  }

  def addOccurrences[T](dataset: Dataset[T]): DataFrame = {
    import dataset.sqlContext.implicits._
    dataset
      .groupBy(dataset.columns.map(col): _*)
      .agg(count(lit(1)).cast(IntegerType).as("occurrences"))
      // only keep occurences > 1
      .withColumn("occurrences", when($"occurrences" > 1, $"occurrences"))
  }

  def addNoOccurrences[T](dataset: Dataset[T]): DataFrame = {
    dataset.withColumn("occurrences", lit(null))
  }

  def addLoginAndDomain[T](dataset: Dataset[T], userIdColumn: Column): DataFrame = {
    import dataset.sqlContext.implicits._
    dataset
      .withColumn("split", split(userIdColumn, userIdSplitPattern, 2))
      // if we cant split the user id, then both login and domain should be null
      .withColumn("login", when(size($"split") === 2, $"split"(0)))
      .withColumn("domain", when(size($"split") === 2, $"split"(1)))
      .drop("split")
  }

  def addBlankId[T](dataset: Dataset[T]): DataFrame =
    dataset.withColumn("blankId", row_number() over Window.partitionBy().orderBy("id"))

  def addId[T](dataset: Dataset[T]): DataFrame =
    dataset.withColumn("blankId", monotonically_increasing_id())

  def mapIdToBlankId[T](idColumnName: String, mappedIdColumnName: String, mapping: Dataset[(String, Long)])
                       (dataset: Dataset[T]): DataFrame = {
    dataset
      .join(
        mapping.withColumnRenamed("blankId", mappedIdColumnName),
        col(idColumnName) === col("id")
      )
      .drop(idColumnName, "id")
  }

  def writeRdf(path: String, compressed: Boolean)(triples: Dataset[Triple]): Unit = {
    import triples.sqlContext.implicits._
    println(s"Writing $path")
    triples
      .select(concat($"s", lit(" "), $"p", lit(" "), $"o", lit(" .")))
      .write
      .mode(SaveMode.Overwrite)
      .when(compressed).call(_.option("compression", "gzip"))
      .text(path)
  }

}
