package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.{Estimator, MLModel, TrainedModel, TrainedModelType, UntrainedModel}
import io.github.manuzhang.dag.{Node, ParseContext, PipelineStageDescriptions}
import io.github.manuzhang.dag.operator.Operator
// import io.github.manuzhang.dag.utils.ModelAdjustment
import io.github.manuzhang.dag.utils.ParseUtils.DEFAULT_INPORT
import org.apache.spark.ml.util.MLReadable
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

trait ModelOperator extends Operator[MLModel] {
  def parseNode(node: Node, context: ParseContext): MLModel
}

class TrainOp extends ModelOperator {
  override def parseNode(node: Node, context: ParseContext): MLModel = {
    val mlEstimator = context.getInput[Estimator](node, DEFAULT_INPORT)
    val df = context.getInputDf(node, "1")
    // `getUpstreamTransformers` must be called after `getInputDf`
    val transformers = context.getUpstreamTransformerInfos(node, "1")
    UntrainedModel(mlEstimator, df, transformers)
  }

  override def getPipelineStageDescriptions(node: Node, context: ParseContext): List[PipelineStageDescriptions] = {
    context.getUpstreamStageDescs(node, "1") ++ context.getUpstreamStageDescs(node, DEFAULT_INPORT)
  }
}

/*class ImportModel extends ModelOperator {
  override def parseNode(node: Node, context: ParseContext): MLModel = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val path = node.getParamValue("path")
    val metaDataDir = path + "/metadata"
    val metadataStr = context.sparkSession.sparkContext.textFile(metaDataDir, 1).first()
    val metadata = parse(metadataStr)
    val className = (metadata \ "class").extract[String]
    val objectClass = getClass.getClassLoader.loadClass(className + "$")
    val constructor = objectClass.getDeclaredConstructor()
    constructor.setAccessible(true)
    val instance = constructor.newInstance().asInstanceOf[MLReadable[_]]
    val model = ModelAdjustment.adjust(instance.load(path).asInstanceOf[TrainedModelType])
    TrainedModel(model)
  }
}*/
