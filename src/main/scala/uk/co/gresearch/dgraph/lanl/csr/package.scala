package uk.co.gresearch.dgraph.lanl

import java.security.MessageDigest
import java.time.{Instant, ZoneOffset}

import uk.co.gresearch.dgraph.lanl.csr.CsrDgraphSparkApp.{datetimeType, integerType}

package object csr {

  def md5sum(s: String): String = MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02x".format(_)).mkString

  def blank(uri: String): String = s"_:${md5sum(uri)}"

  def predicate(predicate: String): String = s"<$predicate>"

  def literal(value: Any, dataType: Option[String]=None): String =
    "\"" + value + "\"" + dataType.map(dt => "^^" + dt).getOrElse("")

  // time as xs:integer
  //def timeLiteral(time: Int): String = literal(time, integerType)

  // time as xs:dateTime
  def timeLiteral(time: Int): String =
    literal(Instant.ofEpochSecond(time).atOffset(ZoneOffset.UTC).toString, datetimeType)

}
