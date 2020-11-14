package io.github.manuzhang.dag.utils

import org.apache.spark.ml.linalg.VectorUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable

object SparkUtils {

  def createSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.sql.parquet.binaryAsString","true")
      .appName(appName)
      .getOrCreate()

    registerUdf(spark)
    spark
  }

  private def registerUdf(spark: SparkSession): Unit = {
    spark.udf.register("dot", VectorUtils.dot _)
  }

  def serialize(row: Row): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.write(row.toSeq.map {
      case row: Row =>
        Serialization.write(row.getValuesMap(row.schema.fieldNames))
      case wrapped: mutable.WrappedArray[_] =>
        s"[${wrapped.mkString(",")}]"
      case v => s"$v"
    })
  }

  def deserialize(value: String): Seq[String] = {
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.read[Seq[String]](value)
  }
}
