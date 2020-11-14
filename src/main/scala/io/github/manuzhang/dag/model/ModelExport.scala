package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.MLModel
import io.github.manuzhang.dag.{Node, ParseContext, TransformerInfo}
import io.github.manuzhang.dag.operator.DataSink
import io.github.manuzhang.dag.utils.MLeapUtils
import io.github.manuzhang.dag.utils.ParseUtils._
import org.apache.spark.ml.PipelineModelFactory
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession

class ModelExport extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val stageIdsToExport = parseArrayFromJson(node.getParamValue("models"))
    checkStages(node, context, stageIdsToExport)

    val outputPath = node.getParamValue("path")
    val untrainedModel = context.getInput[MLModel](node, DEFAULT_INPORT)

    val shouldOverwrite = parseBoolean(node, "overwrite")
    val exportMleap = parseBoolean(node, "exportMleap")
    val transformersToExport = untrainedModel.getAllTransformers.filter(info => stageIdsToExport.contains(info.stageId))

    ModelExport.saveModel(outputPath, transformersToExport, context.sparkSession, shouldOverwrite, exportMleap)

    context.trainingSession(untrainedModel).foreach { trainingSession =>
      trainingSession.logModel(transformersToExport, overWrite = true)
    }
  }

  def checkStages(node: Node, context: ParseContext, stageIdsToExport: Array[String]): Unit = {
    val upstreamStages = context.getUpstreamStageDescs(node, DEFAULT_INPORT).map(_.stageId)
    if (!stageIdsToExport.forall(upstreamStages.contains)) {
      throw new IllegalArgumentException(s"The selected stages to export are obsolete, please reselect the stages in Dageditor.")
    }
  }
}

object ModelExport {
  def saveModel(outputPath: String, transformers: List[TransformerInfo], sparkSession: SparkSession, overWrite: Boolean, exportMleap: Boolean): Unit = {
    if (transformers.isEmpty) {
      throw new IllegalArgumentException("Please choose at least one model to export.")
    }

    val modelToExport = if (transformers.length == 1) {
      transformers.head.transformer
    } else {
      PipelineModelFactory.create(transformers.map(_.transformer))
    }

    if (overWrite) {
      modelToExport.asInstanceOf[MLWritable].write.overwrite.save(outputPath)
    } else {
      modelToExport.asInstanceOf[MLWritable].write.save(outputPath)
    }

    if (exportMleap) {
      MLeapUtils.saveMleapModel(modelToExport, transformers, outputPath, sparkSession, overWrite)
    }
  }
}

