import scala.io.Source

/**
 * @Author HuangGuoFu
 * @Date 2022/4/21 11:06
 * */
object Sum {
  def main(args: Array[String]): Unit = {
    val inputFile = Source.fromFile("D:\\大数据技术与应用专业\\Spark\\SparkProject\\datas\\score.txt");
    val line = inputFile.getLines().map(_.split("\t")).toList
    val noTail = line.tail

    val mathData = noTail.map(_(2).toInt)
    val englishData = noTail.map(_(3).toInt)
    val physicsData  = noTail.map(_(4).toInt)

    val maleMathData = noTail.filter(_(1) == "male").map(_(2).toInt)
    val maleEnglishData = noTail.filter(_(1) == "male").map(_(3).toInt)
    val malePhysicData = noTail.filter(_(1) == "male").map(_(4).toInt)

    val femaleMathData = noTail.filter(_(1) == "female").map(_(2).toInt)
    val femaleEnglishData = noTail.filter(_(1) == "female").map(_(3).toInt)
    val femalePhysicData = noTail.filter(_(1) == "female").map(_(4).toInt)

    val len = noTail.length
    println(s"math:最高成绩：${mathData.max} 分\t最低成绩：${mathData.min} 分\t平均成绩：${mathData.sum/len} 分")
    println(s"english:最高成绩：${englishData.max} 分\t最低成绩：${englishData.min} 分\t平均成绩：${englishData.sum/len} 分")
    println(s"physics:最高成绩：${physicsData.max} 分\t最低成绩：${physicsData.min} 分\t平均成绩：${physicsData.sum/len} 分")
    println()
    println(s"math:最高成绩：${maleMathData.max} 分\t最低成绩：${maleMathData.min} 分\t平均成绩：${maleMathData.sum/len} 分")
    println(s"english:最高成绩：${maleEnglishData.max} 分\t最低成绩：${maleEnglishData.min} 分\t平均成绩：${maleEnglishData.sum/len} 分")
    println(s"physics:最高成绩：${malePhysicData.max} 分\t最低成绩：${malePhysicData.min} 分\t平均成绩：${malePhysicData.sum/len} 分")
    println()
    println(s"math:最高成绩：${femaleMathData.max} 分\t最低成绩：${femaleMathData.min} 分\t平均成绩：${femaleMathData.sum/len} 分")
    println(s"english:最高成绩：${femaleEnglishData.max} 分\t最低成绩：${femaleEnglishData.min} 分\t平均成绩：${femaleEnglishData.sum/len} 分")
    println(s"physics:最高成绩：${femalePhysicData.max} 分\t最低成绩：${femalePhysicData.min} 分\t平均成绩：${femalePhysicData.sum/len} 分")




  }
}
