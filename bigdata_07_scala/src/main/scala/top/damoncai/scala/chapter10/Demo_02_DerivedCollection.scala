package top.damoncai.scala.chapter10

object Demo_02_DerivedCollection {

  def main(args: Array[String]): Unit = {
    // 1.获取集合长度和集合大小
    val list1 = List(1,2,3,4,5)
    var list2 = List(4,5,6,7,8)

//    （1）获取集合的头
    println("head ==> " + list1.head)

//    （2）获取集合的尾（不是头的就是尾）
    println("tail ==> " + list1.tail)

//    （3）集合最后一个数据
    println("last ==> " + list1.last)

//    （4）集合初始数据（不包含最后一个）
    println("init ==> " + list1.init)

//    （5）反转
    println("reverse ==> " + list1.reverse)

//    （6）取前（后）n 个元素
    println("take ==> " + list1.take(2))
    println("takeRight ==> " + list1.takeRight(2))

//    （7）去掉前（后）n 个元素
    println("drop ==> " + list1.drop(2))
    println("dropRight ==> " + list1.dropRight(2))

//    （8）并集
    println("union ==> " + list1.union(list2))
    println("union ==> " + (list1 ::: list2))

//    （9）交集
    println("intersect ==> " + list1.intersect(list2))

//    （10）差集
    println("diff ==> " + list1.diff(list2))
    println("diff ==> " + list2.diff(list1))

//    （11）拉链 (如果两个集合元素长度不一样，会去除多余的)
    println("zip ==> " + list1.zip(list2))
    println("zip ==> " + list2.zip(list1))

//    （12）滑窗
    var slide = list1.sliding(3);
    for (ele <- slide) println(ele)

    println("============")

    var slide2 = list1.sliding(3,2);
    for (ele <- slide2) println(ele)
  }
}
