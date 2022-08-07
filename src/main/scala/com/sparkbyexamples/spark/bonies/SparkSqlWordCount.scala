package com.sparkbyexamples.spark.bonies

import org.apache.spark.sql.SparkSession
case class WordCase(value:String,cnt:Int)
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
    val works=ds.flatMap(_.split(" ")).map(work=>WordCase(work,1))
    works.printSchema()
    works.show()
    val table=works.createOrReplaceTempView("t_works")
    val sql="select value,count(*) as cnt from  t_works group by value order by count(*) desc"
    session.sql(sql).show()
    works.groupBy('value).count().orderBy('count.desc).show()
    session.close()





  }

}
