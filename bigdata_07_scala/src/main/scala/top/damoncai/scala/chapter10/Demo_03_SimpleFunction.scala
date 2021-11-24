package top.damoncai.scala.chapter10

object Demo_03_SimpleFunction {

  def main(args: Array[String]): Unit = {
    var list = List(3,4,2,7,5,8)
    var list2 = List(("a",5),("b",9),("c",7))

//    （1）求和
    println("sum ==> " + list.sum)

//    （2）求乘积
    println("product ==> " + list.product)

//    （3）最大值
    println("max ==> " + list.max)
    println("maxBy ==> " + list2.max)
    println("maxBy ==> " + list2.maxBy(_._2)) // println("maxBy ==> " + list2.maxBy((tuple:(String,Int)) => {tuple._2}))

//    （4）最小值
    println("min ==> " + list.min)
    println("minBy ==> " + list2.minBy(_._2))

//    （5）排序
    println("sorted ==> " + list.sorted)
    println("sorted ==> " + list.sorted(Ordering[Int].reverse))

    println("sortedBy ==> " + list2.sortBy(_._2))
    println("sortedBy ==> " + list2.sortBy(_._2)(Ordering[Int].reverse))

    println("sortedWith ==> " + list2.sortWith((a,b) =>a._2 < b._2))
    println("sortedWith ==> " + list2.sortWith(_._2 > _._2))
  }
}
