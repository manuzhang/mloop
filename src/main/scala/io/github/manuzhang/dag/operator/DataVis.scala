package io.github.manuzhang.dag.operator

import io.github.manuzhang.dag.operator.DataVis.{ColumnVis, Histogram, Response, Stats}
import io.github.manuzhang.dag.utils.ParseUtils._
import io.github.manuzhang.dag.{Dag, Node, ParseContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.rdd.RDD._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.util.StatCounter
import org.json4s.jackson.Serialization

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

object DataVis {

  case class Request(dag: Dag, nodeId: String, outputPort: Int, cols: Array[String])

  case class Response(columns: Array[ColumnVis])

  case class ColumnVis(name: String, statistics: Array[Stats], histogram: Histogram)

  case class Stats(name: String, displayName: String, value: String, description: String = "")

  case class Histogram(x: Array[String], y: Array[Long])

}

class DataVis extends Transform {

  private val DEFAULT_BUCKET_COUNT = 10

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, "0")
    val columns = parseArray(node, "columns")

    val rows = columns.map { col =>
      val vis = Serialization.write(visColumn(inputDf, col))
      Row.fromTuple(col -> vis)
    }.toList

    val schema = StructType(List(
      StructField("column", StringType),
      StructField("vis", StringType)
    ))

    context.sparkSession.createDataFrame(rows.asJava, schema)
  }

  def visColumn(df: DataFrame, column: String,
      bucketCount: Int = DEFAULT_BUCKET_COUNT): ColumnVis = {
    val schema = df.schema
    val index = schema.fieldIndex(column)
    schema.fields(index).dataType match {
      case _: ByteType =>
        visNumeric[Byte](df, column, bucketCount)
      case _: IntegerType =>
        visNumeric[Int](df, column, bucketCount)
      case _: LongType =>
        visNumeric[Long](df, column, bucketCount)
      case _: FloatType =>
        visNumeric[Float](df, column, bucketCount)
      case _: DoubleType =>
        visNumeric[Double](df, column, bucketCount)
      case _: StringType =>
        visString(df, column, bucketCount)
      case _ =>
        ColumnVis(column, Array.empty[Stats], Histogram(Array.empty[String], Array.empty[Long]))
    }
  }


  def vis(df: DataFrame, bucketCount: Int = DEFAULT_BUCKET_COUNT): Response = {
    Response(df.columns.map { column =>
      visColumn(df, column, bucketCount)
    })
  }

  def visNumeric[T](df: DataFrame, column: String, bucketCount: Int)
    (implicit num: Numeric[T], tag: ClassTag[T]): ColumnVis = {
    val nonNullDf = df.filter(not(missingNumeric(column)))
    val rdd = getColumnRdd[T](nonNullDf, column)
    val stats = rdd.stats()
    val uniqueCount = rdd.countApproxDistinct()
    val missingCount = countMissing(df, missingNumeric(column))
    val histogram = if (uniqueCount <= bucketCount) {
      histogramItem(nonNullDf, column, uniqueCount.toInt)
    } else {
      histogramRange(rdd, stats, Math.min(uniqueCount, bucketCount.toLong).toInt)
    }
    ColumnVis(column, statsNumeric(stats, uniqueCount, missingCount, rdd), histogram)
  }


  def visString(df: DataFrame, column: String, bucketCount: Int): ColumnVis = {
    val stats = Array(
      Stats(name = "count", displayName = "Count",
        value = df.count().toString),
      Stats(name = "countdistinct", displayName = "Unique Values",
        value = df.agg(approx_count_distinct(df.col(column))).first.get(0).toString),
      Stats(name = "countmissing", displayName = "Missing Values",
        value = countMissingString(df, column).toString),
      Stats(name = "columntype", displayName = "Feature Type", value = "String Feature")
    )
    ColumnVis(column, stats, histogramItem(df.filter(not(missingString(column))), column, bucketCount))
  }


  def statsNumeric[T](stats: StatCounter, uniqueCount: Long, missingCount: Long, rdd: RDD[T])
    (implicit num: Numeric[T]): Array[Stats] = {
    Array(
      Stats(name = "count", displayName = "Count", value = stats.count.toString),
      Stats(name = "min", displayName = "Min", value = stats.min.toString),
      Stats(name = "max", displayName = "Max", value = stats.max.toString),
      Stats(name = "mean", displayName = "Mean", value = stats.mean.toString),
      Stats(name = "stddev", displayName = "Standard Deviation", value = stats.stdev.toString),
      Stats(name = "variance", displayName = "Variance", value = stats.variance.toString),
      Stats(name = "countdistinct", displayName = "Unique Values",
        value = uniqueCount.toString),
      Stats(name = "countmissing", displayName = "Missing Values",
        value = missingCount.toString),
      Stats(name = "columntype", displayName = "Feature Type", value = "Numeric Feature")
    )
  }

  private def histogramRange[T](rdd: RDD[T], stats: StatCounter, bucketCount: Int)
    (implicit num: Numeric[T]): Histogram = {
    val x = getBuckets(stats.min, stats.max, bucketCount)
    val y = rdd.histogram(x, evenBuckets = true)
    val xRange = (0 until x.length - 1).map(index => s"${x(index)}-${x(index + 1)}").toArray
    Histogram(xRange, y)
  }

  private def histogramItem(df: DataFrame, column: String, bucketCount: Int): Histogram = {
    val (x, y) = getTopN(df, column, bucketCount)
      .map(row => getX(row) -> getY(row)).unzip

    Histogram(x, y)
  }

  private def getTopN(df: DataFrame, column: String, bucketCount: Int): Array[Row] = {
    df.groupBy(column).count().sort(col("count").desc).take(bucketCount)
  }

  private def getX(row: Row): String = {
    Option(row.get(0)).map(_.toString).getOrElse("null")
  }

  private def getY(row: Row): Long = {
    row.getLong(1)
  }

  private def getColumnRdd[T](df: DataFrame, column: String)
    (implicit tag: ClassTag[T]): RDD[T] = {
    df.rdd.map(row => row.getAs[T](column))
  }

  private def countMissingString(df: DataFrame, column: String): Long = {
    df.filter(missingString(column)).count()
  }

  private def countMissing(df: DataFrame, expr: Column): Long = {
    df.filter(expr).count()
  }

  private def missingNumeric(column: String): Column = {
    expr(s"`$column` is null")
  }

  private def missingString(column: String): Column = {
    expr(s"`$column` is null or `$column` == ''")
  }

  /**
   *
   * Reuse min/max value from StatCounter to get buckets
   *
   * @see [[org.apache.spark.rdd.DoubleRDDFunctions#histogram]]
   */
  private def getBuckets(min: Double, max: Double, bucketCount: Int): Array[Double]  = {

    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
    }

    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
    }
    val range = if (min != max) {
      // Range.Double.inclusive(min, max, increment)
      // The above code doesn't always work. See Scala bug #SI-8782.
      // https://issues.scala-lang.org/browse/SI-8782
      customRange(min, max, bucketCount)
    } else {
      List(min, min)
    }
    range.toArray
  }
}
