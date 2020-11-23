package uk.co.gresearch.dgraph.lanl.csr

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object RunSparkApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .getOrCreate()

    CsrDgraphSparkApp.main(args)
  }

}
