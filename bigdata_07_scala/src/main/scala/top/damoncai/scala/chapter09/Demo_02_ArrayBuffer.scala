package top.damoncai.scala.chapter09

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Demo_02_ArrayBuffer {
  def main(args: Array[String]): Unit = {
    // 1.创建数组
    var arr:ArrayBuffer[Int] = new ArrayBuffer[Int]();

    var arr2 = ArrayBuffer.apply(1,2,3)

    // 2.修改元素
    arr2(1) = 88

    // 3.访问元素
    println(arr2(1))

    // 4.遍历元素
    // 和不可变数组一样

    // 5.添加元素
    // 5-1.后面添加
    arr2 += 4

    // 5-2.前面添加
    0 +=: arr2
    println(arr2)

    // 5-3.后面添加
    arr2.append(5,6,7)

    // 5-4前面添加
    arr2.prepend(-2,-1)
    println(arr2)

    // 5-5.插入数
    arr2.insert(0,-4,-3)
    println(arr2)

    // 5-6.添加数组
    arr2.appendAll(arr)

    // 5-7.插入数组
    arr2.insertAll(0,arr)

    // 6.可变数组转不可变数组
    var arr3:Array[Int] = arr2.toArray

    // 7.不可变数组转可变数组
    var arr4:mutable.Buffer[Int] = arr3.toBuffer

  }
}
