package com.ithgf.spark_core.WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author HuangGuoFu
 * @Date 2022/4/24 15:40
 * */
object WordCount {
  def main(args: Array[String]): Unit = {
//    val inputFile = "/home/adolph/SparkWordCount.txt"
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val testFile = sc.textFile(args(0))
    val wordCount = testFile.flatMap(line => line.split(" ")).map(word =>(word, 1)).reduceByKey((a,b)=>a +b)
    wordCount.foreach(println)
    sc.stop()
  }
}

