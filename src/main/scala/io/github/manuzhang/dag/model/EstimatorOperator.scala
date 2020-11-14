package io.github.manuzhang.dag.model

import io.github.manuzhang.dag.{Node, ParseContext, PipelineStageDescriptions, TransformerInfo}
import io.github.manuzhang.dag.model.EstimatorOperator.{Estimator, EstimatorType, castVal}
import io.github.manuzhang.dag.operator.Operator
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.{Estimator => SparkEstimator, Model => SparkModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

trait EstimatorOperator extends Operator[Estimator] with ParamsParser {
  def parseEstimator(node: Node, context: ParseContext): EstimatorType

  override def parseNode(node: Node, context: ParseContext): Estimator = {
    Estimator(parseEstimator(node, context), node.id)
  }

  override def getPipelineStageDescriptions(node: Node, context: ParseContext): List[PipelineStageDescriptions] = {
    List(PipelineStageDescriptions(node.name, node.id, ""))
  }
}

trait ParamsParser {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def initEstimatorParams[T <: Params](params: T, node: Node, excludedParam: Array[String] = Array.empty): T = {
    val defaultParams = params.extractParamMap().toSeq
    node.params.filter(param => param.value.nonEmpty && !excludedParam.contains(param.name)).foreach { p =>
      defaultParams.find(_.param.name == p.name).map { paramPair =>
        val value = castVal(p.value.replace("\"", "").trim, paramPair.value)
        params.set(paramPair.param.asInstanceOf[Param[Any]], value)
      }
    }
    val missedParam = defaultParams.map(_.param.name).toSet -- node.params.map(_.name).toSet
    if (missedParam.nonEmpty) {
      LOG.warn(s"Missing parameters from fronted: ${missedParam.mkString(",")}")
    }
    params
  }
}

trait HasWeightCol {
  def setWeightCol(node: Node, default: EstimatorType)(
      setFunction: String => EstimatorType): EstimatorType = {
    if (node.hasParam("weightCol") && !node.getParamValue("weightCol").isEmpty) {
      setFunction(node.getParamValue("weightCol"))
    } else {
      default
    }
  }
}

object EstimatorOperator {
  type TrainedModelType = T forSome {
    type T <: SparkModel[T]
  }

  type EstimatorType = SparkEstimator[T] forSome {
    type T <: SparkModel[T]
  }

  case class Estimator(estimator: EstimatorType, nodeId: String)

  trait MLModel {
    def get(): TrainedModelType

    def inferSchema(schema: StructType): StructType

    def getAllTransformers: List[TransformerInfo]
  }

  case class TrainedModel(model: TrainedModelType) extends MLModel {
    override def get(): TrainedModelType = {
      model
    }

    override def inferSchema(schema: StructType): StructType = {
      model.transformSchema(schema)
    }

    override def getAllTransformers: List[TransformerInfo] = List.empty
  }

  case class UntrainedModel(estimatorWrapper: Estimator, inputDf: DataFrame, transformerInfos: List[TransformerInfo]) extends MLModel {
    lazy val model: TrainedModelType = estimatorWrapper.estimator.fit(inputDf)

    override def get(): TrainedModelType = {
      model
    }

    override def inferSchema(schema: StructType): StructType = {
      estimatorWrapper.estimator.transformSchema(schema)
    }

    override def getAllTransformers: List[TransformerInfo] = {
      transformerInfos :+ TransformerInfo(model, inputDf, estimatorWrapper.nodeId)
    }
  }

  def castVal(value: String, defaultVal: Any): Any = {
    defaultVal.getClass.getSimpleName.toLowerCase match {
      case "double"  => value.toDouble
      case "integer" => value.toInt
      case "string"  => value
      case "boolean" => value.toBoolean
      case "long"    => value.toLong
      case "float"   => value.toFloat
      case _ =>
        throw new IllegalArgumentException(
          s"Got value $value with type ${defaultVal.getClass.getName}, currently parameter parser only accepts primitive types")
    }
  }
}
