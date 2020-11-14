package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.UntrainedModel
import io.github.manuzhang.dag.{Node, ParseContext}
import io.github.manuzhang.dag.operator.Transform
import io.github.manuzhang.dag.utils.EvaluateResultBuilder
import io.github.manuzhang.dag.utils.EvaluateResultBuilder.EVALUATE_RESULT
import io.github.manuzhang.dag.utils.ParseUtils._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, RegressionMetrics}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.util.Try

class MLEvaluator extends Transform {
  //Todo: find a better way to choose evaluator
  def getEvaluator(node: Node, context: ParseContext): EvaluatorBase = {
    val evalType = node.getParamValue("evaluator_type")
    val predictionCol = parseHead(node, "predictionCol")
    val labelCol = parseHead(node, "labelCol")
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    evalType match {
      case "Regression" => new RegressionEvaluator(predictionCol, labelCol, inputDf)
      case "Binary Classification"  => new BinaryClassificationEvaluator(predictionCol, labelCol, inputDf)
      case "Multiclass Classification" => new MulticlassClassificationEvaluator(predictionCol, labelCol, inputDf)
      case _ => throw new IllegalArgumentException(s"Can not recognize evaluator type $evalType")
    }
  }

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val resultBuilder = getEvaluator(node, context).evaluate(node, context)
    getUpstreamModel(node, context).foreach { model =>
      context.trainingSession(model).foreach { session =>
        session.logParams(model.get())
        resultBuilder.build().metrics.foreach(m => session.logMetric(m.name, m.value.toFloat))
        session.logModel(model.transformerInfos)
        session.terminate()
      }
    }
    resultBuilder.toDataFrame(context.sparkSession)
  }

  override def inferSchema(node: Node, context: ParseContext): StructType = {
    getEvaluator(node, context).schema
  }

  private def getUpstreamModel(node: Node, context: ParseContext): Option[UntrainedModel] = {
    val parent = context.getParentNode(node, DEFAULT_INPORT)
    Try {
      // The upstream may not be a TrainOp
      context.getInput[UntrainedModel](parent, DEFAULT_INPORT)
    }.toOption
  }
}

abstract class EvaluatorBase {
  def schema: StructType

  def evaluate(node: Node, context: ParseContext): EvaluateResultBuilder
}

class RegressionEvaluator(predictionCol: String, labelCol: String, inputDf: DataFrame) extends EvaluatorBase {
  override def schema = StructType(Array(
    StructField("explainedVariance", DoubleType),
    StructField("meanAbsoluteError", DoubleType),
    StructField("meanSquaredError", DoubleType),
    StructField("rootMeanSquaredError", DoubleType),
    StructField("r2", DoubleType),
    StructField(EVALUATE_RESULT, StringType)
  ))

  override def evaluate(node: Node, context: ParseContext): EvaluateResultBuilder = {
    val predictionAndLabels = inputDf
      .select(col(predictionCol).cast(DoubleType), col(labelCol).cast(DoubleType))
      .rdd
      .map { case Row(prediction: Double, label: Double) => (prediction, label) }
    val metrics = new RegressionMetrics(predictionAndLabels)
    new EvaluateResultBuilder()
      .withMetric(metrics.explainedVariance, "explainedVariance", "explainedVariance", DoubleType)
      .withMetric(metrics.meanAbsoluteError, "meanAbsoluteError", "meanAbsoluteError", DoubleType)
      .withMetric(metrics.meanSquaredError, "meanSquaredError", "meanSquaredError", DoubleType)
      .withMetric(metrics.rootMeanSquaredError, "rootMeanSquaredError", "rootMeanSquaredError", DoubleType)
      .withMetric(metrics.r2, "r2", "r2", DoubleType)
  }
}

class BinaryClassificationEvaluator(predictionCol: String, labelCol: String, inputDf: DataFrame) extends EvaluatorBase {
  def schema = StructType(Array(
    StructField("areaUnderPR", DoubleType),
    StructField("areaUnderROC", DoubleType),
    StructField(EVALUATE_RESULT, StringType)
  ))

  override def evaluate(node: Node, context: ParseContext): EvaluateResultBuilder = {
    val scoreAndLabels =
      inputDf.select(col(predictionCol), col(labelCol).cast(DoubleType)).rdd.map {
        case Row(rawPrediction: Vector, label: Double) => (rawPrediction(1), label)
        case Row(rawPrediction: Double, label: Double) => (rawPrediction, label)
      }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels, 100)
    new EvaluateResultBuilder()
      .withMetric(metrics.areaUnderPR(), "areaUnderPR", "areaUnderPR", DoubleType)
      .withMetric(metrics.areaUnderROC(), "areaUnderROC", "areaUnderROC", DoubleType)
      .withChart(metrics.roc(), "ROC", "False positive rate", "True positive rate")
      .withChart(metrics.pr(), "PR", "Recall", "Precision")
      .withChart(metrics.fMeasureByThreshold(), "F1 Score by Threshold", "Threshold", "F1 Score")
      .withChart(metrics.precisionByThreshold(), "Precision by Threshold", "Threshold", "Precision")
      .withChart(metrics.recallByThreshold(), "Recall by Threshold", "Threshold", "Recall")
  }
}

class MulticlassClassificationEvaluator(predictionCol: String, labelCol: String, inputDf: DataFrame) extends EvaluatorBase {
  def schema = StructType(Array(
    StructField("accuracy", DoubleType),
    StructField("weightedFalsePositiveRate", DoubleType),
    StructField("weightedTruePositiveRate", DoubleType),
    StructField("weightedFMeasure", DoubleType),
    StructField("weightedPrecision", DoubleType),
    StructField("weightedRecall", DoubleType),
    StructField(EVALUATE_RESULT, StringType)
  ))

  override def evaluate(node: Node, context: ParseContext): EvaluateResultBuilder = {
    val predictionAndLabels =
      inputDf.select(col(predictionCol), col(labelCol).cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    new EvaluateResultBuilder()
      .withMetric(metrics.accuracy, "accuracy", "accuracy", DoubleType)
      .withMetric(metrics.weightedFalsePositiveRate, "weightedFalsePositiveRate", "weightedFalsePositiveRate", DoubleType)
      .withMetric(metrics.weightedTruePositiveRate, "weightedTruePositiveRate", "weightedTruePositiveRate", DoubleType)
      .withMetric(metrics.weightedFMeasure, "weightedFMeasure", "weightedFMeasure", DoubleType)
      .withMetric(metrics.weightedPrecision, "weightedPrecision", "weightedPrecision", DoubleType)
      .withMetric(metrics.weightedRecall, "weightedRecall", "weightedRecall", DoubleType)
      .withConfusionMatrix(metrics, None)
  }
}
