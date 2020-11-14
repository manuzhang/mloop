package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.EstimatorType
import io.github.manuzhang.dag.{Node, ParseContext}
import org.apache.spark.ml.fpm.FPGrowth

trait FrequentPatternMining extends EstimatorOperator

class MLFPGrowth extends FrequentPatternMining {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val fPGrowth = initEstimatorParams(new FPGrowth(), node)
    Option(node.getParamValue("numPartitions")).filter(_.nonEmpty).map(p => fPGrowth.setNumPartitions(p.toInt))
    fPGrowth
  }
}
