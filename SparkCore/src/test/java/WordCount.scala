import java.io.File
import collection.mutable.Map
import scala.io.Source

/**
 * @Author HuangGuoFu
 * @Date 2022/4/19 12:03
 * */
object WordCount {
  def main(args: Array[String]): Unit = {
    val dirFile = new File("D:\\大数据技术与应用专业\\Spark\\SparkProject\\datas")
    val files = dirFile.listFiles
    val result = Map.empty[String, Int]
    for (file <- files) {
      val data = Source.fromFile(file)
      val strings = data.getLines().flatMap{s => s.split(" ")}
      strings foreach {
        word => if (result.contains(word)) result(word) += 1 else result(word) = 1
      }
    }
    result foreach {case (k,v) => println(s"$k=>$v")}
  }
}
