package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.{Estimator, EstimatorType}
import io.github.manuzhang.dag.utils.ParseUtils._
import io.github.manuzhang.dag.{Node, ParseContext}
import io.github.manuzhang.mlp.dag.model.ParamsAnnotation
// import ml.dmlc.xgboost4j.scala.spark.TrackerConf
import org.apache.spark.ml.classification._
import org.apache.spark.ml.param.Params
// import org.apache.spark.ml.xgboost.XGBoostClassifierWrapper

import scala.util.Try

trait Classification extends EstimatorOperator

trait HasThresholds extends Classification {
  override def initEstimatorParams[T <: Params](estimator: T, node: Node, excludedParam: Array[String] = Array.empty): T = {
    val classifier = super
      .initEstimatorParams(estimator, node)
      .asInstanceOf[ProbabilisticClassifier[_, _, _]]

    if (node.hasParam("thresholds") && node.getParamValue("thresholds").nonEmpty) {
      classifier.setThresholds(node.getParamValue("thresholds").split(",").map(_.trim.toDouble))
    }
    classifier.asInstanceOf[T]
  }
}

@ParamsAnnotation(paramsType = classOf[OneVsRest], name = "OneVsRest", id = "OneVsRest", desc = "")
class MLOneVsRest extends Classification {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val oneVsRest = initEstimatorParams(new OneVsRest(), node)
    if (context.hasInput(node, DEFAULT_INPORT)) {
      val estimator = context.getInput[Estimator](node, DEFAULT_INPORT).estimator
      if (!estimator.isInstanceOf[Classifier[_, _, _]]) {
        throw new IllegalArgumentException(
          s"${OneVsRest.getClass.getCanonicalName} only accepts type Classifier," +
            s" passing ${estimator.getClass.getCanonicalName}")
      }
      oneVsRest.setClassifier(estimator.asInstanceOf[Classifier[_, _, _]])
    } else {
      throw new IllegalArgumentException(
        s"${OneVsRest.getClass.getCanonicalName} must be set a Classifier")
    }
    oneVsRest
  }
}

//Todo: set matrix and vector
class MLLogisticRegression extends HasThresholds with HasWeightCol {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val lr = initEstimatorParams(new LogisticRegression(), node)
    setWeightCol(node, lr)(lr.setWeightCol)
  }
}

class MLDecisionTreeClassifier extends HasThresholds {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new DecisionTreeClassifier(), node)
  }
}

class MLRandomForestClassifier extends HasThresholds {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new RandomForestClassifier(), node)
  }
}

class MLGBTClassifier extends Classification with HasThresholds {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new GBTClassifier(), node)
  }
}

//Todo: set initial weights
class MLMultilayerPerceptronClassifier extends Classification {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val layers = parseCommaSepList(node, "layers").map(_.toInt)
    initEstimatorParams(new MultilayerPerceptronClassifier(), node).setLayers(layers)
  }
}

class MLLinearSVC extends Classification with HasWeightCol {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val svc = initEstimatorParams(new LinearSVC(), node)
    setWeightCol(node, svc)(svc.setWeightCol)
  }
}

class MLNaiveBayes extends Classification with HasThresholds with HasWeightCol {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val nb = initEstimatorParams(new NaiveBayes(), node)
    setWeightCol(node, nb)(nb.setWeightCol)
  }
}

/*class MLXgboostClassification extends Classification {
  override def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val trackerConf = new TrackerConf(500 * 1000, "scala")
    val estimator = initEstimatorParams(new XGBoostClassifierWrapper(), node, Array("thresholds"))
    estimator.set(estimator.trackerConf, trackerConf)
    if (node.getParamValue("thresholds").nonEmpty) {
      estimator.setThresholds(parseCommaSepList(node, "thresholds").map(_.toDouble))
    }
    estimator
  }
}*/
