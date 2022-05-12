package com.ithgf.spark_core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // TODO 建立和Spark框架的连接
    // JDBC : Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    // TODO 执行业务操作

    // 1.读取文件，获取一行一行思维数据
    val lines: RDD[String] = sc.textFile(path = "datas")
    // 2.将一行数据进行拆分，拆成一个一个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 3。将数据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    // 4.对分组后的数据进行聚合转换
    val wordToCount = wordGroup.map {
      case  (word, list) => {
        (word, list.size)
    }
    }
    // 将采集的数据打印到控制台
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    // TODO 关闭连接
    sc.stop()
  }
}
