package io.github.manuzhang.dag.operator

import io.github.manuzhang.dag.utils.ParseUtils.DEFAULT_INPORT
import io.github.manuzhang.dag.{Node, ParseContext, PipelineStageDescriptions}
import org.apache.spark.sql._

trait Operator[T] extends java.io.Serializable {
  def parseNode(node: Node, context: ParseContext): T

  def getPipelineStageDescriptions(node: Node, context: ParseContext): List[PipelineStageDescriptions] = {
    if (node.inPorts.length == 1) {
      context.getUpstreamStageDescs(node, DEFAULT_INPORT)
    } else {
      List.empty
    }
  }
}

trait DataFrameOperator extends Operator[List[DataFrame]] {
  def parseNode(node: Node, context: ParseContext): List[DataFrame]
}
