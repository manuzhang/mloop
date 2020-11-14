package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.{Node, ParseContext}
import io.github.manuzhang.dag.model.EstimatorOperator.{Estimator, EstimatorType, castVal}
import io.github.manuzhang.dag.utils.ParseUtils._
import org.apache.spark.ml.evaluation.{ClusteringEvaluator, Evaluator}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}

trait Tuning extends EstimatorOperator

abstract class BaseValidator extends Tuning {

  //Todo: array of array?
  def getParamMaps(estimator: EstimatorType, node: Node): Array[ParamMap] = {
    val paramGrid = new ParamGridBuilder()
    val params = parseMap(node, "model_params")
    params.filter(_._2.nonEmpty).foreach{ case (paramName, jsonValue) =>
      estimator.params.find(_.name == paramName).map { param =>
        val defaultVal = estimator.getOrDefault(param)
        if (defaultVal.isInstanceOf[Boolean]) {
          paramGrid.addGrid(param.asInstanceOf[BooleanParam])
        } else {
          val values = parseArrayFromJson(jsonValue).map(v => castVal(v, defaultVal))
          paramGrid.addGrid(param.asInstanceOf[Param[Any]], values)
        }
      }
    }
    paramGrid.build()
  }

  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    if (!context.hasInput(node, DEFAULT_INPORT)) {
      throw new IllegalArgumentException(
        s"${CrossValidator.getClass.getCanonicalName} must receive an Estimator")
    }
    val estimator = context.getInput[Estimator](node, DEFAULT_INPORT).estimator
    if (!node.hasParam("evaluator")) {
      throw new IllegalArgumentException(
        s"${CrossValidator.getClass.getCanonicalName} must receive an Evaluator")
    }
    val evaluatorType = node.getParamValue("evaluator")
    val labelCol = node.getParamValue("labelCol")
    val predictionCol = node.getParamValue("predictionCol")
    val evaluator = getEvaluator(evaluatorType, labelCol, predictionCol)
    val paramMaps = getParamMaps(estimator, node)
    createValidator(estimator, evaluator, paramMaps, node, context)
  }

  def getEvaluator(evaluatorType: String, labelCol: String, predictionCol: String): Evaluator = {
    import org.apache.spark.ml.evaluation.{RegressionEvaluator, MulticlassClassificationEvaluator, BinaryClassificationEvaluator}
    evaluatorType match {
      case "Regression" => new RegressionEvaluator().setLabelCol(labelCol).setPredictionCol(predictionCol)
      case "Clustering" => new ClusteringEvaluator().setFeaturesCol(labelCol).setPredictionCol(predictionCol)
      case "Binary Classification"  => new BinaryClassificationEvaluator().setLabelCol(labelCol).setRawPredictionCol(predictionCol)
      case "Multiclass Classification" => new MulticlassClassificationEvaluator().setLabelCol(labelCol).setPredictionCol(predictionCol)
      case _ => throw new IllegalArgumentException(s"Can not recognize evaluator type $evaluatorType")
    }
  }

  def createValidator(estimator: EstimatorType,
                      evaluator: Evaluator,
                      paramMaps: Array[ParamMap],
                      node: Node,
                      context: ParseContext): EstimatorType
}

class MLCrossValidator extends BaseValidator {
  override def createValidator(estimator: EstimatorType,
                               evaluator: Evaluator,
                               paramMaps: Array[ParamMap],
                               node: Node,
                               context: ParseContext): CrossValidator = {
    initEstimatorParams(new CrossValidator(), node)
      .setEstimator(estimator)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)
  }
}

class MLTrainValidationSplit extends BaseValidator {
  override def createValidator(estimator: EstimatorType,
                               evaluator: Evaluator,
                               paramMaps: Array[ParamMap],
                               node: Node,
                               context: ParseContext): TrainValidationSplit = {
    initEstimatorParams(new TrainValidationSplit(), node)
      .setEstimator(estimator)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)
  }
}
