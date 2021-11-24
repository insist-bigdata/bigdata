package top.damoncai.scala.chapter10

object Demo_04_HighLevelFunction_Map {

  def main(args: Array[String]): Unit = {
    var list = List(1,2,3,4,5)
    val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
    val wordList: List[String] = List("hello world", "hello atguigu", "hello scala")

//    （1）过滤
    var list2 = list.filter(_ % 2 == 0); // 获取偶数
    println("list2 ==> " + list2)

//    （2）转化/映射（map）
    var list3 = list.map(_ * 2)
    println("list3 ==> " + list3)

//    （3）扁平化
    var list4 = nestedList.flatten
    println("list4 ==> " + list4)

//    （4）扁平化+映射 注：flatMap
    var list5 = wordList.map(_.split(" ")).flatten;
    println("list5 ==> " + list5)

    var list6 = wordList.flatMap(_.split(" "));
    println("list6 ==> " + list6)

//    （5）分组(group)
    var map7 = list.groupBy( _ % 2)
    println(map7)

  }
}
