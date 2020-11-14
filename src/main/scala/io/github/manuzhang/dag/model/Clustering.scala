package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.EstimatorType
import io.github.manuzhang.dag.{Node, ParseContext}
import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture, KMeans, LDA}

trait Clustering extends EstimatorOperator

class MLKMeans extends Clustering {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new KMeans(), node)
  }
}

//Todo: set array, set topicConcentration
class MLLDA extends Clustering {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new LDA(), node)
  }
}

class MLBisectingKMeans extends Clustering {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new BisectingKMeans(), node)
  }
}

class MLGaussianMixture extends Clustering {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    initEstimatorParams(new GaussianMixture(), node)
  }
}
