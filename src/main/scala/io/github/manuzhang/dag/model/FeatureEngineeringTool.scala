package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.model.EstimatorOperator.{EstimatorType, MLModel}
import io.github.manuzhang.dag.{Node, ParseContext}
import io.github.manuzhang.dag.operator.Transform
import io.github.manuzhang.dag.utils.ParseUtils._
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class MLOneColStringIndexer extends EstimatorOperator {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType = {
    val inputCol = node.getParamValue("inputCol")
    initEstimatorParams(new StringIndexer(), node).setInputCol(inputCol)
  }
}

class MLIndexToString extends Transform {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    if (context.hasInput(node, DEFAULT_INPORT)) {
      val trainedModel = context.getInput[MLModel](node, DEFAULT_INPORT).get()
      if (!trainedModel.isInstanceOf[StringIndexerModel]) {
        throw new IllegalArgumentException(
          s"${IndexToString.getClass.getCanonicalName} only accepts trained StringIndexerModel, " +
            s" passing ${trainedModel.getClass.getCanonicalName}")
      }
      val labels = trainedModel.asInstanceOf[StringIndexerModel].labels
      val inputCol = parseHead(node, "inputCols")
      val outputCol = node.getParamValue("outputCol")
      val indexToString = new IndexToString().setInputCol(inputCol).setOutputCol(outputCol).setLabels(labels)
      indexToString.transform(context.getInputDf(node, "1"))
    } else {
      throw new IllegalArgumentException(
        s"${IndexToString.getClass.getCanonicalName} must be set labels")
    }
  }

  override def inferSchema(node: Node, context: ParseContext): StructType = {
    val df = context.getInputDf(node, "1")
    val inputCol = parseHead(node, "inputCols")
    val outputCol = node.getParamValue("outputCol")
    val indexToString = new IndexToString().setInputCol(inputCol).setOutputCol(outputCol)
    indexToString.transformSchema(df.schema)
  }
}
