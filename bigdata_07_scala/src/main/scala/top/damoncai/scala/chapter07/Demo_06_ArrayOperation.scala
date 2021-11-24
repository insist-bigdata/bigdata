package top.damoncai.scala.chapter07

object Demo_06_ArrayOperation {

  def main(args: Array[String]): Unit = {
    def arrayOperation(array:Array[Int],opt:Int => Int):Array[Int] = {
      for(i <- array) yield opt(i)
    }

    // 1.数组每个元素乘以二
    def m2(x:Int):Int =  x * 2

    val resArray = arrayOperation(Array(1,2,3,4,5),m2)
    println(resArray.mkString(","))
  }
}
