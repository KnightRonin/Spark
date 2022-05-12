package com.ithgf.spark_core.WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * [key,value]对换,排序,[key,value]互换
 * [key,value] ==> [value,key] ==> [sortByKey] ==>[key,value]
 */
object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://LianTian:9000/test/")
    // map(_._n)表示任意元组，n代表取第几个
    val wordCount = file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .map(a => (a._2, a._1)).sortByKey(false).map(b => (b._2, b._1)).coalesce(1, true).saveAsTextFile("hdfs://LianTian:9000/saveAsTextFile")
    //    wordCount.foreach(println)
    sc.stop()
  }

}
