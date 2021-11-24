package top.damoncai.scala.chapter09

object Demo_01_ImmutableArray {

  def main(args: Array[String]): Unit = {
    // 1.创建数组
    var arr:Array[Int] = new Array[Int](5);
    var arr2:Array[Int] = Array(0,1,2,3,4,5);

    // 2.访问和修改
    arr(0) = 999;
    println(arr(0));

    // 3.数组遍历
    // 3.1. for循环
    for (i <- 0 until arr2.length) println(arr2(i));

    println("============================")
    // 3.2
    for (i <- arr2.indices) println(arr2(i));

    println("============================")
    // 3.3遍历获取元素
    for(i <- arr2) println(i)

    println("============================")
    // 3.4迭代器
    var iter = arr2.iterator;
    while (iter.hasNext) println(iter.next())

    println("============================")
    // 3.5 foreach
    arr2.foreach(item => println(item));

    // 4.添加元素
    // 4.1后面添加
    val newArr = arr2.:+ (5 + 1)
    println("newArr：" + newArr.mkString(","))

    val newArr2 = arr2 :+ (5 + 1)
    println("newArr2：" + newArr2.mkString(","))

    // 4.2前面添加
    val newArr3 = arr2.+: (-1)
    println("newArr3：" + newArr3.mkString(","))

    val newArr4 = (-2) +: (-1) +: arr2 :+ (-1) :+ (-2)
    println("newArr4：" + newArr4.mkString(","))






  }
}
