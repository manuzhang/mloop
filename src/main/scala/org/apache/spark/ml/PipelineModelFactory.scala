package org.apache.spark.ml

import org.apache.spark.ml.util.Identifiable

object PipelineModelFactory {
  def create(transformers: List[Transformer]): PipelineModel = {
    new PipelineModel(Identifiable.randomUID("pipeline"), transformers.toArray)
  }
}
