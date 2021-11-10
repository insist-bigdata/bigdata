package top.damoncai.scala.chapter03

object Demo_02_String_Print {

  var name = "张三"

  var age = 12

  var money = 2.855

  def main(args: Array[String]): Unit = {
    println(name + "年龄" + age)

    //printf 用法字符串，通过%传值。
    printf("%s年龄%d",name,age)
    println()

    // 字符串模板
    println(s"${name}年龄${age}")

    // 字符串模板 - 格式化
    println(f"The num is ${money}%2.2f")
    //多行字符串，在 Scala中，利用三个双引号包围多行字符串就可以实现。
    //输入的内容，带有空格、\t 之类，导致每一行的开始位置不能整洁对齐。
    //应用 scala 的 stripMargin 方法，在 scala 中 stripMargin 默认
    //是“|”作为连接符，//在多行换行的行头前面加一个“|”符号即可。

    val sql =
      s"""
        |select *
        |from
        |   user
        |where
        |   name = ${name} and age = ${age}
        |""".stripMargin

    println(sql)
  }
}
