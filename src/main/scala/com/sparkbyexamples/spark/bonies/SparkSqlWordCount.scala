package com.sparkbyexamples.spark.bonies

import org.apache.spark.sql.{SaveMode, SparkSession}
case class WordCase(key:String,cnt:Int)
/**
 *
 * spark sql work count
 *
 *
 */
object SparkSqlWordCount {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("bonies-spark-sql")
      .getOrCreate()
     val path="D:\\alice.txt"
    //dataset
     val ds= session.read.textFile(path)
    ds.printSchema()
    ds.show()
    //隐式转换
    import  session.implicits._
    val words=ds.flatMap(_.split(" ")).map(work=>WordCase(work,1))
    words.printSchema()
    words.show()
    words.createOrReplaceTempView("t_works")
    val sql="select key,count(*) as cnt from  t_works group by key order by count(*) desc"
    session.sql(sql).show()
    words.write.format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.122.128:1521/helowin")
      .option("dbtable", "test_01")
      .option("user", "orcl")
      .option("password", "123456")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .mode(SaveMode.Append).save()
    session.close()





  }

}
