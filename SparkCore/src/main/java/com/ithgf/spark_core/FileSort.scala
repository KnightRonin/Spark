package com.ithgf.spark_core
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
/**
 * @Author HuangGuoFu
 * @Date 2022/5/12 15:19
 * */
object FileSort {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FileSort").setMaster("local")
    val sc = new SparkContext(conf)
    val dataFile = "hdfs://LianTian:9000/sparkdata/FileSort"
    val data= sc.textFile(dataFile,3)
    var index = 0
    val result = data.filter(_.trim().length>0).map(n=>(n.trim.toInt,"")).partitionBy(new HashPartitioner(1)).sortByKey().map(t => {
      index += 1
      (index,t._1)
    })
    result.saveAsTextFile("hdfs://LianTian:9000/saveAsTextFile/FileSort")
  }
// 测试
}
