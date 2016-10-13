package cn.edu.pku.nggis.geosparksql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.RangePartitioner
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.RangePartitioner
import org.apache.spark.util.MutablePair
import org.apache.spark.sql.SparkSession

class HilbertIndexGenerator(val spark: SparkSession,
    val numPerPartition: Int,
    val table: Dataset[Row],
    val idColmName: String,
    val xColmName: String,
    val yColmName: String) {
  val partitions = (table.count / numPerPartition).toInt + 1

  val hilbrtidxColmName = "HilbertIndex"
  val hilbrtTable: Dataset[Row] = {
    def minx = min(xColmName)
    def maxx = max(xColmName)
    def miny = min(yColmName)
    def maxy = max(yColmName)
    val hilbrtidxcalc = udf((x: Int, y: Int) => HilbertCurveIndexCalc.xy2d(x, y))
    val t = table.select(idColmName, xColmName, yColmName)
    t.withColumn(hilbrtidxColmName, hilbrtidxcalc((t(xColmName) - minx) / (maxx - minx),
      (t(yColmName) - miny) / (maxy - miny))).select(idColmName, hilbrtidxColmName).persist
  }

  import spark.implicits._
  private val rangepartitoner = new RangePartitioner(partitions,
    hilbrtTable.select(hilbrtidxColmName).as[Long].rdd.mapPartitions({ iter =>
      val mutablePair = new MutablePair[Long, Null]()
      iter.map(idx => mutablePair.update(idx, null))
    }, false))

  def getPartition(hilbrtidx: Long): Int = rangepartitoner.getPartition(hilbrtidx)

}

object HilbertIndexGenerator {
  
  def main(args:Array[String]){
    
  }

}