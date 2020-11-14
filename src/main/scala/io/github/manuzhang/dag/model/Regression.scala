package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.EstimatorType
import io.github.manuzhang.dag.{Node, ParseContext}
import org.apache.spark.ml.regression._

trait Regression extends EstimatorOperator

class MLLinearRegression extends Regression with HasWeightCol {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val lr = initEstimatorParams(new LinearRegression(), node)
    setWeightCol(node, lr)(lr.setWeightCol)
  }
}

class MLGeneralizedLinearRegression extends Regression with HasWeightCol {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val glr = initEstimatorParams(new GeneralizedLinearRegression(), node)
    Option(node.getParamValue("link")).filter(p => p.nonEmpty && !p.equals("default")).map(glr.setLink)
    Option(node.getParamValue("linkPower")).filter(_.nonEmpty).map(p => glr.setLinkPower(p.toDouble))
    Option(node.getParamValue("linkPredictionCol")).filter(_.nonEmpty).map(glr.setLinkPredictionCol)
    Option(node.getParamValue("offsetCol")).filter(_.nonEmpty).map(glr.setOffsetCol)
    setWeightCol(node, glr)(glr.setWeightCol)
  }
}

class MLDecisionTreeRegressor extends Regression {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val dtr = initEstimatorParams(new DecisionTreeRegressor(), node)
    Option(node.getParamValue("varianceCol")).filter(_.nonEmpty).map(dtr.setVarianceCol)
    dtr
  }
}

class MLRandomForestRegressor extends Regression {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new RandomForestRegressor(), node)
  }
}

class MLGBTRegressor extends Regression {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new GBTRegressor(), node)
  }
}

//Todo: add set setQuantileProbabilities
class MLAFTSurvivalRegression extends Regression {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val aft = initEstimatorParams(new AFTSurvivalRegression(), node)
    Option(node.getParamValue("quantilesCol")).filter(_.nonEmpty).map(aft.setQuantilesCol)
    aft
  }
}

class MLIsotonicRegression extends Regression with HasWeightCol {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val ir = initEstimatorParams(new IsotonicRegression(), node)
    setWeightCol(node, ir)(ir.setWeightCol)
  }
}
