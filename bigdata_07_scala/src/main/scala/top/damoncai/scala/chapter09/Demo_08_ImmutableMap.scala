package top.damoncai.scala.chapter09

object Demo_08_ImmutableMap {

  def main(args: Array[String]): Unit = {
    // 1.创建Map
    var map:Map[String,Int] = Map("a" -> 1, "b" -> 2);
    println(map)

    println("======================")
    // 2.遍历
    map.foreach(println)

    println("======================")
    // 3.获取key集合
    for (key <- map.keySet) {
      println(s"$key --> ${map.get(key)}")
    }

    println("======================")
    // 4.获取某个元素
    var numa = map.getOrElse("a",0);
    var numb = map.getOrElse("c",0);
    println(numa)
    println(numb)

    var num = map("a")
    println(num)

  }
}
