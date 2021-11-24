package top.damoncai.scala.chapter07

object Demo_08_Stratum {

  def main(args: Array[String]): Unit = {

    // 递归
    def stratum(n: Int):Int = {
      if(n == 1) return 1
      n * stratum(n - 1)
    }

    println(stratum(5))

    // 尾递归
    def tailStratum(n: Int,result:Int):Int = {
      if(n == 1) return result
      tailStratum(n - 1,n * result)
    }

    println(tailStratum(5,1))

  }
}
