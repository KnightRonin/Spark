package com.ithgf.spark_core.RWHbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author HuangGuoFu
 * @Date 2022/5/15 18:42
 * */
object SparkRH {


  /**
   * @Author HuangGuoFu
   * @Date 2022/5/15 16:24
   * */

  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "adolph")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","LianTian")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val sc = new SparkContext(new SparkConf().setAppName("SparkOperationHbase").setMaster("local"))
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "hgf:student")
    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    stuRDD.cache()
    //遍历输出
    stuRDD.foreach({ case (_, result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes, "gender".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes, "age".getBytes))
      println("Row key:" + key + " Name:" + name + " Gender:" + gender + " Age:" + age)
    })
  }

}


