package uk.co.gresearch.dgraph.lanl.csr

import org.apache.spark.sql._
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
case class User(id: String, login: Option[String], domain: Option[String])
case class Computer(id: String)
// Event entities (events have time: Int)
case class AuthEvent(id: String, srcUserId: String, dstUserId: String, srcComputerId: String, dstComputerId: String, authType: Option[String], logonType: Option[String], authOrient: Option[String], outcome: Option[String], time: Int, occurrences: Option[Int])
case class ProcessEvent(id: String, userId: String, computerId: String, processName: String, eventType: String, time: Int, occurrences: Option[Int])
case class DnsEvent(id: String, srcComputerId: String, resolvedComputerId: String, time: Int, occurrences: Option[Int])
case class CompromiseEvent(id: String, userId: String, srcComputerId: String, dstComputerId: String, time: Int, occurrences: Option[Int])
// Duration entities (durations have Int start, end, duration)
case class ProcessDuration(id: String, userId: String, computerId: String, processName: String, start: Option[Int], end: Option[Int], duration: Option[Int])
case class FlowDuration(id: String, srcComputerId: String, dstComputerId: String, srcPort: Option[Int], dstPort: Option[Int], protocol: Option[String], packets: Option[Int], bytes: Option[Long], start: Int, end: Int, duration: Int, occurrences: Option[Int])

// Triple for written to RDF files
case class Triple(s: String, p: String, o: String)

object CsrDgraphSparkApp {

  // user ids are split on this pattern to extract login and domain:
  val userIdSplitPattern = "@"

  val dateType: Option[String] = Some("<http://www.w3.org/2001/XMLSchema#date>")
  val doubleType: Option[String] = Some("<http://www.w3.org/2001/XMLSchema#double>")
  val integerType: Option[String] = Some("<http://www.w3.org/2001/XMLSchema#integer>")
  val longType: Option[String] = Some("<http://www.w3.org/2001/XMLSchema#long>")
  val stringType: Option[String] = Some("<http://www.w3.org/2001/XMLSchema#string>")
  val datetimeType: Option[String] = Some("<http://www.w3.org/2001/XMLSchema#dateTime>")
  val noType: Option[String] = None

  def read[T](path: String)(implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] =
    spark.read.option("nullValue", "?").schema(encoder.schema).csv(path).as[T](encoder)

  def main(args: Array[String]): Unit = {

    println("This tool models the LANL CSR dataset as a graph and writes it")
    println("in a simple RDF NQuad triple format that can be loaded into a Dgraph cluster.")
    println()

    if (args.length != 1) {
      println("Please provide the path to the LANL CSR dataset. All files should exist uncompressed in that directory.")
      System.exit(1)
    }

    val path = args(0)

    val startTime = System.nanoTime()

    // start a local Spark session
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Dgraph LANL CSR Spark App")
        .config("spark.local.dir", ".")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    import spark.implicits._

    val auth = read[Auth](s"$path/auth.txt").cache()
    val proc = read[Proc](s"$path/proc.txt").cache()
    val flow = read[Flow](s"$path/flows.txt").cache()
    val dns = read[Dns](s"$path/dns.txt").cache()
    val red = read[Red](s"$path/redteam.txt").cache()

    val doAssertions = false
    val (authCount, procCount, flowCount, dnsCount, redCount) = if (doAssertions) {
      (auth.count, proc.count, flow.count, dns.count, red.count)
    } else {
      (-1L, -1L, -1L, -1L, -1L)
    }

    if (doAssertions) {
      println("Auth nulls:"); countNulls(auth).show(false)
      println("Proc nulls:"); countNulls(proc).show(false)
      println("Flow nulls:"); countNulls(flow).show(false)
      println("DNS nulls:"); countNulls(dns).show(false)
      println("Red nulls:"); countNulls(red).show(false)

      println(s"Row counts: auth=$authCount proc=$procCount flow=$flowCount dns=$dnsCount red=$redCount " +
        s"all=${authCount+procCount+flowCount+dnsCount+redCount}")

      println(s"Duplicate rows: " +
        s"auth=${countDuplicates(auth)} " +
        s"proc=${countDuplicates(proc)} " +
        s"flow=${countDuplicates(flow)} " +
        s"dns=${countDuplicates(dns)} " +
        s"red=${countDuplicates(red)}")

      val procWithUnexpectedType = proc.where(!$"eventType".isin("Start", "End")).count
      println(s"Rows with Proc.eventType other than 'Start' and 'End': " +
        s"$procWithUnexpectedType (${(procWithUnexpectedType * 1000 / procCount) / 10.0}%)")
    }

    // TODO: filter out rows breaking assumptions
    // TODO: add xid

    val procStarts =
      proc
        .where($"eventType" === "Start")
        .withColumnRenamed("time", "start")
        .drop("eventType")

    val procEnds =
      proc
        .where($"eventType" === "End")
        .withColumnRenamed("time", "end")
        .drop("eventType")

    // TODO: connect end with earlier start, as there may be multiple starts and ends of the same process name
    val procDuration = procStarts
      .join(procEnds, Seq("user", "computer", "processName"), "fullouter")
      .withColumnRenamed("user", "userId")
      .withColumnRenamed("computer", "computerId")
      .withColumn("duration", $"end" - $"start")
      .call(addId)
      .as[ProcessDuration]

    if (doAssertions) {
      val procWithoutDuration = procDuration.where($"duration".isNull).count
      println(s"Rows in Proc with either Start or End but not both: " +
        s"$procWithoutDuration (${(procWithoutDuration * 1000 / procCount) / 10.0}%)")
    }

    // generate all occurring users
    val users = Seq(
      auth.select($"srcUser".as("id")),
      auth.select($"dstUser".as("id")),
      proc.select($"user".as("id")),
      red.select($"user".as("id")),
    )
      .reduce(_.unionByName(_))
      .distinct()
      .call(addLoginAndDomain(_, $"id"))
      .as[User]

    // generate all occurring computers
    val computers = Seq(
      auth.select($"srcComputer".as("id")),
      auth.select($"dstComputer".as("id")),
      proc.select($"computer".as("id")),
      flow.select($"srcComputer".as("id")),
      flow.select($"dstComputer".as("id")),
      dns.select($"srcComputer".as("id")),
      dns.select($"resolvedComputer".as("id")),
      red.select($"srcComputer".as("id")),
      red.select($"dstComputer".as("id")),
    )
      .reduce(_.unionByName(_))
      .distinct()
      .as[Computer]

    val authEvents =
      auth
        .withColumnRenamed("srcUser", "srcUserId")
        .withColumnRenamed("dstUser", "dstUserId")
        .withColumnRenamed("srcComputer", "srcComputerId")
        .withColumnRenamed("dstComputer", "dstComputerId")
        .call(addOccurrences)
        .call(addId)
        .as[AuthEvent]

    val processEvent =
      proc
        .withColumnRenamed("user", "userId")
        .withColumnRenamed("computer", "computerId")
        .call(addOccurrences)
        .call(addId)
        .as[ProcessEvent]

    val dnsEvent =
      dns
        .withColumnRenamed("srcComputer", "srcComputerId")
        .withColumnRenamed("resolvedComputer", "resolvedComputerId")
        .call(addOccurrences)
        .call(addId)
        .as[DnsEvent]

    val compromiseEvent =
      red
        .withColumnRenamed("user", "userId")
        .withColumnRenamed("srcComputer", "srcComputerId")
        .withColumnRenamed("dstComputer", "dstComputerId")
        .call(addOccurrences)
        .call(addId)
        .as[CompromiseEvent]

    val processDuration =
      procDuration
        .withColumnRenamed("user", "userId")
        .withColumnRenamed("computer", "computerId")
        // no occurrences here
        .call(addId)
        .as[ProcessDuration]

    val flowDuration =
      flow
        .withColumnRenamed("srcComputer", "srcComputerId")
        .withColumnRenamed("dstComputer", "dstComputerId")
        .withColumnRenamed("time", "start")
        .withColumn("end", $"start" + $"duration")
        .call(addOccurrences)
        .call(addId)
        .as[FlowDuration]

    // turn entities and durations into triples and write them to RDF
    users
      .flatMap { user =>
        val userId = blank(user.id)
        Seq(
          Some(Triple(userId, predicate(isType), literal(Types.User))),
          Some(Triple(userId, predicate(id), literal(user.id, stringType))),
          user.login.map(v => Triple(userId, predicate(login), literal(v, stringType))),
          user.domain.map(v => Triple(userId, predicate(domain), literal(v, stringType))),
        ).flatten
      }
      .call(writeRdf("users.rdf"))

    computers
      .flatMap { computer =>
        val computerId = blank(computer.id)
        Seq(
          Some(Triple(computerId, predicate(isType), literal(Types.Computer))),
          Some(Triple(computerId, predicate(id), literal(computer.id, stringType))),
        ).flatten
      }
      .call(writeRdf("computers.rdf"))

    authEvents
      .flatMap { event =>
        val eventId = blank(event.id)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.AuthEvent))),
          Some(Triple(eventId, predicate(sourceUser), blank(event.srcUserId))),
          Some(Triple(eventId, predicate(destinationUser), blank(event.dstUserId))),
          Some(Triple(eventId, predicate(sourceComputer), blank(event.srcComputerId))),
          Some(Triple(eventId, predicate(destinationComputer), blank(event.dstComputerId))),
          event.authType.map(v => Triple(eventId, predicate(authType), literal(v, stringType))),
          event.logonType.map(v => Triple(eventId, predicate(logonType), literal(v, stringType))),
          event.authOrient.map(v => Triple(eventId, predicate(authOrient), literal(v, stringType))),
          event.outcome.map(v => Triple(eventId, predicate(outcome), literal(v, stringType))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf("auth-events.rdf"))

    processEvent
      .flatMap { event =>
        val eventId = blank(event.id)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.ProcessEvent))),
          Some(Triple(eventId, predicate(user), blank(event.userId))),
          Some(Triple(eventId, predicate(computer), blank(event.computerId))),
          Some(Triple(eventId, predicate(processName), literal(event.processName, stringType))),
          Some(Triple(eventId, predicate(eventType), literal(event.eventType, stringType))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf("proc-events.rdf"))

    dnsEvent
      .flatMap { event =>
        val eventId = blank(event.id)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.DnsEvent))),
          Some(Triple(eventId, predicate(sourceComputer), blank(event.srcComputerId))),
          Some(Triple(eventId, predicate(resolvedComputer), blank(event.resolvedComputerId))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf("dns-events.rdf"))

    compromiseEvent
      .flatMap { event =>
        val eventId = blank(event.id)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.CompromiseEvent))),
          Some(Triple(eventId, predicate(user), blank(event.userId))),
          Some(Triple(eventId, predicate(sourceComputer), blank(event.srcComputerId))),
          Some(Triple(eventId, predicate(destinationComputer), blank(event.dstComputerId))),
          Some(Triple(eventId, predicate(time), timeLiteral(event.time))),
          event.occurrences.map(v => Triple(eventId, predicate(occurrences), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf("compromise-events.rdf"))

    procDuration
      .flatMap { event =>
        val eventId = blank(event.id)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.ProcessDuration))),
          Some(Triple(eventId, predicate(user), blank(event.userId))),
          Some(Triple(eventId, predicate(computer), blank(event.computerId))),
          Some(Triple(eventId, predicate(processName), literal(event.processName, stringType))),
          event.start.map(v => Triple(eventId, predicate(start), timeLiteral(v))),
          event.`end`.map(v => Triple(eventId, predicate(`end`), timeLiteral(v))),
          event.duration.map(v => Triple(eventId, predicate(duration), literal(v, integerType))),
        ).flatten
      }
      .call(writeRdf("proc-durations.rdf"))

    flowDuration
      .flatMap { event =>
        val eventId = blank(event.id)
        Seq(
          Some(Triple(eventId, predicate(isType), literal(Types.FlowDuration))),
          Some(Triple(eventId, predicate(sourceComputer), blank(event.srcComputerId))),
          Some(Triple(eventId, predicate(destinationComputer), blank(event.dstComputerId))),
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
      .call(writeRdf("flow-durations.rdf"))
  }

  def countNulls[T](dataset: Dataset[T]): DataFrame = countColumns((c: Column) => c.isNull, dataset)

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

  def addLoginAndDomain[T](dataset: Dataset[T], userIdColumn: Column): DataFrame = {
    import dataset.sqlContext.implicits._
    dataset
      .withColumn("split", split(userIdColumn, userIdSplitPattern, 2))
      // if we cant split the user id, then both login and domain should be null
      .withColumn("login", when(size($"split") === 2, $"split"(0)))
      .withColumn("domain", when(size($"split") === 2, $"split"(1)))
      .drop("split")
  }

  def addId[T](dataset: Dataset[T]): DataFrame = {
    val hashableColumnNames = dataset.columns.diff(Seq("occurrences"))
    val columnHash = (c: String) => concat(lit(c), lit("="), col(c).cast(StringType))
    val columnHashes = array(hashableColumnNames.map(columnHash): _*)
    val sortedColumnHashes = array_sort(columnHashes)
    val rowId = sortedColumnHashes.cast(StringType)
    dataset.withColumn("id", rowId)
  }

//  def uri(column: Column, blankNode: Boolean): Column =
//    if (blankNode) concat(lit("_:"), md5(column)) else concat(lit("<"), column, lit(">"))

//  def literal(value: Column, dataType: Column): Column =
//    concat(lit("\""), value, lit("\"^^"), dataType)

  def writeRdf(path: String)(triples: Dataset[Triple]): Unit = {
    import triples.sqlContext.implicits._
    triples
      .select(concat($"s", lit(" "), $"p", lit(" "), $"o", lit(" .")))
      .write
      .mode(SaveMode.Overwrite)
      .text(path)
  }

  // model in two variants: lifetime (connecting start and end event) and events
  // write into separate files, can be loaded individually or all
}
