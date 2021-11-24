package top.damoncai.scala.chapter10

object Demo_05_HighLevelFunction_Reduce {

  def main(args: Array[String]): Unit = {
    var list = List(1,2,3,4,5)

//    （1）简化（归约）
    var res1 = list.reduce(_ + _)
    println("res1 ==> " + res1)

    var res2 = list.reduce(_ - _)
    println("res2 ==> " + res2)

    var res3 = list.reduceLeft(_ - _) // 和reduce一样
    println("res3 ==> " + res3)

    var res4 = list.reduceRight(_ - _) // 3
    println("res4 ==> " + res4) // 1 - (2 - (3 - (4 - 5)))

//    （2）折叠

    var flod1 = list.fold(10)(_ - _); // 底层调动的是foldLeft
    println("flod1 ==> " + flod1)

    var flod2 = list.foldRight(10)(_ - _); // 1 - (2 - (3 - (4 - (5 - 10))))
    println("flod2 ==> " + flod2)

  }
}
