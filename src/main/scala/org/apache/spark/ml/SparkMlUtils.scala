package org.apache.spark.ml

import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols}

object SparkMlUtils {
  def getInputCols(pipelineStage: PipelineStage): Array[String] = {
    pipelineStage match {
      case hasInput: HasInputCol => Array(hasInput.getInputCol)
      case hasInputs: HasInputCols => hasInputs.getInputCols
      case _ => throw new IllegalArgumentException(s"$pipelineStage does not have any input column")
    }
  }
}
