package io.github.manuzhang.dag.utils

import io.github.manuzhang.dag.utils.EvaluateResultBuilder._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer

class EvaluateResultBuilder {
  import scala.collection.JavaConversions._
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private val charts = new ListBuffer[Chart]()
  private val metrics = new ListBuffer[Metrics]()
  private val valueAndFields = new ListBuffer[(Any, StructField)]()
  private var confusionMatrix: Option[ConfusionMatrix] = None

  def withMetric(value: Double, key: String, displayName: String, dataType: DataType): EvaluateResultBuilder = {
    metrics.append(Metrics(key, displayName, value))
    valueAndFields.append((value, StructField(key, dataType, nullable = false)))
    this
  }

  def withConfusionMatrix(
      metrics: MulticlassMetrics,
      labels: Option[Array[String]]): EvaluateResultBuilder = {
    val dm = metrics.confusionMatrix.asInstanceOf[DenseMatrix]
    val data = dm.rowIter.toList.map(_.toArray).toArray
    val classNames = labels.getOrElse((0 until dm.numRows).map(_.toString).toArray)
    confusionMatrix = Some(ConfusionMatrix(data, classNames))
    this
  }

  def withChart(curve: RDD[(Double, Double)],
                title: String,
                xName: String,
                yName: String,
                `type`: String = "line"): EvaluateResultBuilder = {
    val data = curve.collect()
    val xs = data.map(_._1)
    val ys = data.map(_._2)
    val chartData = ChartData("series 1", xs, ys)
    charts.append(Chart(title, `type`, xName, yName, List(chartData)))
    this
  }

  def build(): EvaluateResult = {
    EvaluateResult(metrics.toList, confusionMatrix, charts.toList)
  }

  def toDataFrame(sparkSession: SparkSession): DataFrame = {
    val jsonResult = Serialization.write(build())

    valueAndFields.append((jsonResult, StructField(EVALUATE_RESULT, StringType, nullable = false)))
    val row = valueAndFields.map(_._1).toList
    sparkSession.createDataFrame(List(Row(row: _*)), StructType(valueAndFields.map(_._2)))
  }
}

object EvaluateResultBuilder {
  val EVALUATE_RESULT = "evaluate_result"

  case class Metrics(name: String, displayName: String, value: Double)

  case class ChartData(name: String, x: Array[Double], y: Array[Double])

  case class Chart(title: String, `type`: String, xName: String, yName: String, data: List[ChartData])

  case class ConfusionMatrix(data: Array[Array[Double]], classNames: Array[String])

  case class EvaluateResult(metrics: List[Metrics], confusionMatrix: Option[ConfusionMatrix], charts: List[Chart])
}
