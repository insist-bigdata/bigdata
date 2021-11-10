package top.damoncai.scala.chapter06

object Demo_02_For {


  def main(args: Array[String]): Unit = {


    println("=======================  范围遍历 To  =======================")
    // 前后闭合
    for(i <- 1 to 10) {
      println(i)
    }

    println("=======================  范围遍历 Util  =======================")
    for(i <- 1 until 10) {
      println(i)
    }

    println("=======================  集合遍历  =======================")
    for(i <- Array(22,33,44)) {
      println(i)
    }

    println("=======================  循环守卫  =======================")
    for(i <- 1 until 5 if i != 3) {
      println(i)
    }

    println("=======================  步长  =======================")
    for(i <- 1 to 10 by 2) {
      println(i)
    }

    println("=======================  步长 - reverse  =======================")
    for(i <- 1 to 10 by 2 reverse) {
      println(i)
    }

    println("=======================  步长 - 负数  =======================")
    for(i <- 10 to 1 by -2) {
      println(i)
    }

    println("=======================  嵌套循环  =======================")
    for (i <- 1 to 4) {
      for (j <- 1 to 5) {
        println("i=" + i + ",j=" + j)
      }
    }

    println("=======================  嵌套循环  =======================")
    for (i <- 1 to 4; j <- 1 to 5) {
      println("i=" + i + ",j=" + j)
    }

    println("=======================  引入变量  =======================")
    for (i <- 1 to 4; j = 4 -i) {
      println("i=" + i + ",j=" + j)
    }

    println("=======================  引入变量 2  =======================")
    for {
      i <- 1 to 4;
      j = 4 -i
    }
    {
      println("i=" + i + ",j=" + j)
    }
  }
}
