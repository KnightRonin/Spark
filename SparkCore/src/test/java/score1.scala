/**
 * @Author HuangGuoFu
 * @Date 2022/5/9 22:02
 * */
import scala.io.Source
object score1 {
  def main(args: Array[String]): Unit = {
    val inputfile = Source.fromFile("D:\\大数据技术与应用专业\\Spark\\SparkProject\\SparkCore\\src\\data\\score.txt") //测试样例文件的路径
    val lines = inputfile.getLines

    val Data = lines.map {
      _.split(" ")
    } //将每行按空白字符（包括空格/制表符）分开

    val originalData = Data.toList //转成List

    val header = originalData.head //获得第一行信息
    val courseNames = header.drop(2) //去掉前两列，获取第一行中的课程名
    val courseNum = courseNames.length //获取courseNames的长度

    val allStudents = originalData.tail //获取除第一行信息外， 剩下的信息
    val stuNum = allStudents.length //总的学生人数

    val (maleLines, femaleLines) = allStudents partition {
      _ (1) == "male"
    } //按性别划分为两个容器
    val maleNum = maleLines.length //男生人数
    val femaleNum = femaleLines.length //女生人数


    //全部同学
    for (i <- 2 to courseNum + 1) {
      val temp = allStudents map { elem => elem(i).toDouble }; println("for 循环结果:temp,avg,max,min"); println(temp.sum / stuNum, temp.min, temp.max)
    } //对每门课程生成一个三元组，分别表示总分，最低分和最高分

    val result1 =
      (for (i <- 2 to courseNum + 1) yield {
        val temp = allStudents map { elem => elem(i).toDouble }; (temp.sum / stuNum, temp.min, temp.max)
      }) //for推导式，将for的结果放在result中，result是一个向量，向量中的元素个数等于课程门数，每一个元素是一个三元组tuple，对每门课程生成一个三元组，分别表示平均分，最低分和最高分

    //输出结果
    for (i <- 0 to courseNum - 1) {
      println(courseNames(i)); println("avg,min,max:"); println(result1(i))
    }

    //男同学
    for (i <- 2 to courseNum + 1) {
      val temp = maleLines map { elem => elem(i).toDouble }; println("for 循环结果:male,avg,max,min"); println(temp.sum / maleNum, temp.min, temp.max)
    }


    val result2 =
      (for (i <- 2 to courseNum + 1) yield {
        val temp = maleLines map { elem => elem(i).toDouble }; (temp.sum / maleNum, temp.min, temp.max)
      })

    for (i <- 0 to courseNum - 1) {
      println(courseNames(i)); println("avg,min,max:(male)"); println(result2(i))
    }


    //女同学
    for (i <- 2 to courseNum + 1) {
      val temp = femaleLines map { elem => elem(i).toDouble }; println("for 循环结果:female,avg,max,min"); println(temp.sum / femaleNum, temp.min, temp.max)
    }

    val result3 =
      (for (i <- 2 to courseNum + 1) yield {
        val temp = femaleLines map { elem => elem(i).toDouble }; (temp.sum / femaleNum, temp.min, temp.max)
      })


    for (i <- 0 to courseNum - 1) {
      println(courseNames(i)); println("avg,min,max:（female）"); println(result3(i))
    }
  }


}
