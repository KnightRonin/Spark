/**
 * @Author HuangGuoFu
 * @Date 2022/5/9 21:25
 * */


import scala.io.Source

object score {
  def main(arg: Array[String]) {
//    val inputFile = Source.fromFile("D:\\大数据技术与应用专业\\Spark\\SparkProject\\SparkCore\\src\\data\\score.txt")
    val inputFile = Source.fromFile("/usr/local/score.txt")
    val originalData = inputFile.getLines.map {
      _.split(" ")
    }.toList
    val courseNames = originalData.head.drop(2)
    val allStudents = originalData.tail
    val courseNum = courseNames.length
    def statistic(lines: List[Array[String]]) = {
      (for (i <- 2 to courseNum + 1) yield {
        val temp = lines map {
          elem => elem(i).toDouble
        }
        (temp.sum, temp.min, temp.max)
      }) map { case (total, min, max) => (total / lines.length, min, max) }
    }

    def printResult(theresult: Seq[(Double, Double, Double)]) {
      (courseNames zip theresult) foreach {
        case (course, result) => println(f"${course + ":"}%-10s${result._1}%5.2f${
          result._2
        }%8.2f${result._3}%8.2f")
      }
    }
    val allResult = statistic(allStudents)
    println("course  average  min  max")
    printResult(allResult)
    val (maleLines, femaleLines) = allStudents partition {
      _ (1) == "male"
    }
    val maleResult = statistic(maleLines)
    println("(male)course  average  min  max")
    printResult(maleResult)
    val femaleResult = statistic(femaleLines)
    println("(female)course  average  min  max")
    printResult(femaleResult)
  }
}

