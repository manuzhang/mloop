package io.github.manuzhang.dag

case class Dag(name: String, fileName: Option[String], createTime: Option[String], data: Data) {

  val nodesMap: Map[String, Node] = data.nodes.map(n => n.id -> n).toMap

  def getNode(id: String): Node = {
    nodesMap.getOrElse(id,
      throw new IllegalArgumentException(s"Node $id not found in ${data.nodes}")
    )
  }
}

case class Data(nodes: List[Node], links: List[Link])

case class Node(
    id: String, nodeDefId: String, name: String,
    inPorts: List[InPort], outPorts: List[OutPort],
    params: List[Param]) {

  val alias: String = params.collectFirst {
    case p if p.name == "alias" && p.value.nonEmpty => p.value
  }.getOrElse(id)

  val paramsMap: Map[String, Param] = params.map(p => p.name -> p).toMap

  def getParamValue(name: String): String =
    paramsMap.get(name).map(_.value)
      .getOrElse(throw new IllegalArgumentException(s"Required param $name not specified"))

  def getParamType(name: String): Option[String] = {
    paramsMap.get(name).flatMap(_.`type`)
  }

  def getParamValue(name: String, defaultValue: String): String = {
    paramsMap.get(name).map(_.value).getOrElse(defaultValue)
  }

  def hasParam(name: String): Boolean = {
    paramsMap.contains(name)
  }
}

object Link {
  case class Source(id: String, outputPort: String)
  case class Target(id: String, inputPort: String)
}

case class Link(
    source: String, outputPort: String,
    target: String, inputPort: String)

case class InPort(name: String, desc: Option[String], `type`: String)

case class OutPort(name: String, desc: Option[String], `type`: String)

case class Param(name: String, value: String,
    displayName: Option[String], desc: Option[String], `type`: Option[String])
