/*
package io.github.manuzhang.dag.utils

import io.github.manuzhang.dag.model.EstimatorOperator
import io.github.manuzhang.dag.operator.Operator
import io.github.manuzhang.mlp.dag.model.ParamsAnnotation
import org.apache.spark.ml.param.Params
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

object ModelParameterParser extends App {
  implicit val formats = Serialization.formats(NoTypeHints)

  def getType(value: Any): String = {
    value.getClass.getSimpleName.toLowerCase match {
      case "boolean" => "boolean"
      case "double" => "double"
      case "integer" => "integer"
      case _         => "text"
    }
  }

  case class OperatorDescription(id: String,
                                 name: String,
                                 desc: String,
                                 outPorts: List[Port],
                                 params: List[ParameterDescription],
                                 inPorts: Option[List[Port]] = None)

  case class Port(name: String, `type`: String)

  case class ParameterDescription(name: String,
                                  value: Option[String],
                                  `type`: Option[String],
                                  desc: Option[String],
                                  displayName: Option[String] = None)

  def getParamDescs(params: Params): List[ParameterDescription] = {
    params
      .extractParamMap()
      .toSeq
      .filter(_.value != null)
      .map { pair =>
        ParameterDescription(pair.param.name,
                             Some(pair.value.toString),
                             Some(getType(pair.value)),
                             Some(pair.param.doc))
      }
      .toList
  }

  def parseParameters(paramClass: Class[_ <: Params]): Unit = {
    val params = paramClass.getConstructor(classOf[String]).newInstance("temp")
    val paramDescs = getParamDescs(params)
    val result = Serialization.writePretty(paramDescs)
    println(result)
  }

  def parseEstimator(operatorClass: Class[_ <: Operator[_]]): Unit = {
    val ea = operatorClass.getAnnotation(classOf[ParamsAnnotation])
    val classParam = ParameterDescription("class",
                                          Some(operatorClass.getName),
                                          None,
                                          None,
                                          Some(""))
    val paramList = List(classParam) ++ getParamDescs(
      ea.paramsType().getConstructor(classOf[String]).newInstance("temp"))
    val output = if (classOf[EstimatorOperator].isAssignableFrom(operatorClass)) "paramsType" else "data"
    val outPorts = List(Port("output", output))
    val estimatorDesc =
      OperatorDescription(ea.id(), ea.name(), ea.desc(), outPorts, paramList)
    println(Serialization.writePretty(estimatorDesc))
  }

  parseParameters(classOf[XGBoostEstimator])
}
*/
