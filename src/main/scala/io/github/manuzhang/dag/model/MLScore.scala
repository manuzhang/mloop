package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.MLModel
import io.github.manuzhang.dag.{Node, ParseContext, PipelineStageDescriptions}
import io.github.manuzhang.dag.operator.Transform
import io.github.manuzhang.dag.utils.ParseUtils.DEFAULT_INPORT
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

class MLScore extends Transform {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val untrainedModel = context.getInput[MLModel](node, DEFAULT_INPORT)
    val testDf = context.getInputDf(node, "1")
    untrainedModel.get().transform(testDf)
  }

  override def inferSchema(node: Node, context: ParseContext): StructType = {
    val untrainedModel = context.getInput[MLModel](node, DEFAULT_INPORT)
    val testDf = context.getInputDf(node, "1")
    untrainedModel.inferSchema(testDf.schema)
  }

  override def getPipelineStageDescriptions(node: Node, context: ParseContext): List[PipelineStageDescriptions] = {
    context.getUpstreamStageDescs(node, DEFAULT_INPORT)
  }
}
