package cn.edu.pku.nggis.geosparksql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.random

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("geosparksql").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 20
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))
    sc.stop()
  }
}
