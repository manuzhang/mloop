package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.EstimatorType
import io.github.manuzhang.dag.{Node, ParseContext}
import org.apache.spark.ml.recommendation.ALS

trait Recommendation extends EstimatorOperator

class MLALS extends Recommendation {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new ALS(), node)
  }
}
