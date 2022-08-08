package com.sparkbyexamples.spark.bonies

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 *
 * dataset jdbc
 */
object DataSetJdbc {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("bonies-spark-sql")
      .getOrCreate()
    import session.implicits._
    val rs = session.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.122.128:1521/helowin")
      .option("dbtable", "test_01")
      .option("user", "orcl")
      .option("password", "123456")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
      .as[Dto]
    rs.printSchema
    rs.show
    rs.createTempView("test")
    session.sql("select * from test").show()

  }
}

case class Dto(key: String, cnt: Int)
