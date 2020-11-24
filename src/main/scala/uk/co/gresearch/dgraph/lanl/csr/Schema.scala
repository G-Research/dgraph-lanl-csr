package uk.co.gresearch.dgraph.lanl.csr

object Schema {

  object Types {
    val User = "User"
    val Computer = "Computer"
    val ComputerUser = "ComputerUser"

    val AuthEvent = "AuthEvent"
    val ProcessEvent = "ProcessEvent"
    val DnsEvent = "DnsEvent"
    val CompromiseEvent = "CompromiseEvent"

    val ProcessDuration = "ProcessDuration"
    val FlowDuration = "FlowDuration"
  }

  object Predicates {
    val isType = "dgraph.type"

    val id = "id"
    val time = "time"
    val start = "start"
    val end = "end"
    val duration = "duration"
    val occurrences = "occurrences"

    val user = "user"

    val computer = "computer"
    val sourceComputer = "sourceComputer"
    val destinationComputer = "destinationComputer"
    val resolvedComputer = "resolvedComputer"

    val computerUser = "computerUser"
    val sourceComputerUser = "sourceComputerUser"
    val destinationComputerUser = "destinationComputerUser"

    val login = "login"
    val domain = "domain"

    val authType = "authType"
    val logonType = "logonType"
    val authOrient = "authOrientation"
    val outcome = "outcome"

    val processName = "processName"
    val eventType = "eventType"

    val sourcePort = "sourcePort"
    val destinationPort = "destinationPort"
    val protocol = "protocol"
    val packets = "packets"
    val bytes = "bytes"
  }

}
