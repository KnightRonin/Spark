import scala.io.StdIn._

/**
 * @Author HuangGuoFu
 * @Date 2022/4/12 14:03
 * */
object Scala {
  println("☹" * 20 + "   快乐加减乘除    " + "☹" * 20)
  println("说明：加法【+】 减法【-】 乘法【*】 除法【/]】 退出【exit】")

  def main(args: Array[String]): Unit = {
    while (true) {
      print("Please input firstNum: ")
      var firstNum = readDouble()
      print("Please input secondNum: ")
      var secondNum = readDouble()
      print("Please choice operator you want operation: ")
      var accept = readLine()

      accept match {
        case "+" => println("res=" + fun(add)); println("=" * 25 + "=" * 25)
        case "-" => println("res=" + fun(minus)); println("=" * 25 + "=" * 25)
        case "*" => println("res=" + fun(multiply)); println("=" * 25 + "=" * 25)
        case "/" => if (secondNum == 0) {
          println("Please input right number");
          var rightNum = readDouble()
          secondNum = rightNum
          println("res=" + fun(device))
        } else println("res=" + fun(device));
          println("=" * 25 + "=" * 25)
        case _ => println("Error operation.Please read the rule.")
      }

      // 加法
      def add(a: Double, b: Double): Double = a + b

      // 减法
      def minus(a: Double, b: Double): Double = a - b

      // 乘法
      def multiply(a: Double, b: Double): Double = a * b

      // 除法
      def device(a: Double, b: Double): Double = a / b

      // 高阶函数
      def fun(res: (Double, Double) => Double): Double = res(firstNum, secondNum)

    }
  }
}
