package top.damoncai.scala.chapter09

object Demo_03_MulArray {

  def main(args: Array[String]): Unit = {
    var arr:Array[Array[String]] = Array.ofDim(2,3)

    arr(0)(0) = "00"
    arr(0)(1) = "01"
    arr(0)(2) = "02"

    arr(1)(0) = "10"
    arr(1)(1) = "11"
    arr(1)(2) = "12"

    for (i <- arr.indices; j <- arr(i).indices) {
      print(arr(i)(j) + "\t")
      if(j == arr(i).length -1) println()
    }
  }
}
