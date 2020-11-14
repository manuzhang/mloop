package io.github.manuzhang.dag

import io.github.manuzhang.dag.DagParser._
import io.github.manuzhang.dag.utils.RedisCollector
import io.github.manuzhang.dag.utils.DataFrameUtils.getSchemaAsJson
import io.github.manuzhang.dag.utils.ParseUtils.DEFAULT_INPORT
import io.github.manuzhang.dag.utils.SparkUtils.serialize
import io.github.manuzhang.dag.mlflow.{TrainingSession, TrainingSessionFactory}
import io.github.manuzhang.dag.model.EstimatorOperator.MLModel
import io.github.manuzhang.dag.operator.{DataFrameOperator, DataSource, Operator, Transform}
import io.github.manuzhang.dag.utils.{Collector, DataFrameUtils, SourceSchemaCache}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.Random

object DagParser {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[DagParser])

  case class Request(dag: Dag, nodeId: Option[String])

  case class Records(rows: Array[Map[String, Any]], records: Int, total: Int, page: Int)

  val SAMPLE_NUM_PER_PARTITION = 100

  /**
   * Takes a number of rows for later exploration.
   * The original DataFrame is returned unchanged
   */
  private def collect(localCollector: Collector, df: DataFrame, sampleNum: Int, appId: String,
      dagId: String, nodeId: String, outputPort: Int): DataFrame = {
    LOG.info(s"Sampling results for Application: $appId, Dag: $dagId, Node: $nodeId, " +
      s"OutputPort: $outputPort")
    val dataKey = Collector.getDataKey(appId, dagId, nodeId, outputPort)
    localCollector.remove(dataKey)

    val partitionNum = df.rdd.partitions.length
    val ratio = partitionNum / sampleNum

    localCollector.save(Collector.getSchemaKey(appId, dagId, nodeId, outputPort),
      getSchemaAsJson(df.schema))

    df.mapPartitions { rowIter: Iterator[Row] =>
      if (sample(ratio)) {
        var count = 0
        val remoteCollector = new RedisCollector

        val newIter = rowIter.map { row =>
          if (count < sampleNum) {
            remoteCollector.append(dataKey, serialize(row))
            count += 1
          }
          row
        }
        newIter
      } else {
        rowIter
      }
    }(RowEncoder(df.schema))
  }

  /**
   * When partitionNum is much larger than sampleNum, we don't need to sample for each partition
   *
   * @param ratio = partitionNum / sampleNum
   */
  private def sample(ratio: Int): Boolean = {
    ratio <= 1 || (new Random().nextInt(ratio) == 0)
  }

}

trait ParseContext {
  def dagName: String

  def sparkSession: SparkSession

  def getParentNode(self: Node, inPort: String): Node

  def trainingSession(model: MLModel): Option[TrainingSession]

  def hasInput(node: Node, inPort: String): Boolean

  def getInput[T](node: Node, inPort: String): T

  def getInputDf(node: Node, inPort: String): DataFrame

  def getUpstreamTransformerInfos(node: Node, inPort: String): List[TransformerInfo]

  def registerTransformerInfo(anchor: Node, tfWithInputDf: TransformerInfo): Unit

  def getUpstreamStageDescs(node: Node, inPort: String): List[PipelineStageDescriptions]
}

case class ParseResult(dfs: List[DataFrame], node: Node)

case class TransformerInfo(transformer: Transformer, inputDf: DataFrame, stageId: String)

case class PipelineStageDescriptions(name: String, stageId: String, inputCols: String)

class DagParser(
    override val sparkSession: SparkSession,
    dag: Dag,
    sample: Boolean = true,
    trainingSessionFactory: Option[TrainingSessionFactory] = None,
    collector: Option[Collector] = None,
    schemaCache: Option[SourceSchemaCache] = None) extends ParseContext {

  def dagName: String = dag.name

  private val targetToSource: Map[Link.Target, Link.Source] =
    dag.data.links.map { case Link(source, op, target, ip) =>
      Link.Target(target, ip) -> Link.Source(source, op)
    }.toMap
  private val sources: Set[String] = dag.data.links.map(_.source).toSet
  private val targets: Set[String] = dag.data.links.map(_.target).toSet
  private val appId = sparkSession.sparkContext.applicationId
  private val inferSchema = schemaCache.isDefined
  // The parseResultCache must be a mutable map because it will be operated recursively so we have to make sure
  // that the operated object is the same one.
  private val parseResultCache = mutable.Map.empty[String, Any]
  private val transformers = mutable.Map.empty[Node, List[TransformerInfo]]
  private val trainingSessions = mutable.Map.empty[MLModel, TrainingSession]

  sparkSession.sparkContext.setJobGroup(dagName, dagName)

  def run(): Unit = {
    parse().foreach { parseResult =>
      parseResult.dfs.zipWithIndex.foreach { case (df, outPort) =>
        runTo(df, parseResult.node.id, outPort, collector)
      }
    }
  }

  /**
   * Run a DAG that ends with the provided node.
   * A `take` operation is appended to trigger execution if the node is not a sink.
   *
   * @return schema of output data
   */
  def runTo(nodeId: String): Unit = {
    getDf(nodeId).foreach { case (df, outPort) =>
      runTo(df, nodeId, outPort, collector)
    }
  }

  def getSchema(nodeId: String, outputPort: Int): StructType = {
    getDf(nodeId, outputPort).schema
  }

  def inputNode(nodeId: String, inPort: String): Link.Source = {
    targetToSource(Link.Target(nodeId, inPort))
  }

  def getDf(nodeId: String, outputPort: Int): DataFrame = {
    getDf(nodeId).find(_._2 == outputPort).getOrElse(
      throw new RuntimeException(s"DataFrame not found for Node: $nodeId, OutputPort: $outputPort"))
      ._1
  }

  def getDf(nodeId: String): List[(DataFrame, Int)] = {
    val node = dag.getNode(nodeId)
    parseDataFrameOperator(node).zipWithIndex
  }

  def isDataSource(nodeId: String, outputPort: Int): Boolean = {
    val node = dag.getNode(nodeId)
    Class.forName(node.getParamValue("class")).newInstance().isInstanceOf[DataSource]
  }

  override def getInputDf(node: Node, inPort: String): DataFrame = {
    val source = inputNode(node.id, inPort)
    val id = source.id
    val outputPort = source.outputPort.toInt

    if (isDataSource(id, outputPort) && inferSchema) {
      val key = SourceSchemaCache.getKey(dagName, id, outputPort)
      schemaCache.map { cache =>
        if (!cache.contains(key)) {
          val schema = getSchema(id, outputPort)
          cache.cacheSchema(key, schema)
        }
        cache.getSchemaDataFrame(key)
      }.getOrElse {
        LOG.warn("SchemaCache is not defined")
        DataFrameUtils.createEmptyDF(sparkSession, getSchema(id, outputPort))
      }
    } else {
      val dfs = getInput[List[DataFrame]](node, inPort)
      if (dfs.isEmpty) {
        throw new IllegalArgumentException(
          s"Failed to get DataFrame for input node $id")
      } else {
        val df = dfs(outputPort)
        if (sample) {
          collect(collector.getOrElse(
            throw new IllegalArgumentException(
              "Collector should be defined for sampling intermediate results")),
            df, SAMPLE_NUM_PER_PARTITION, appId, dag.name, id, outputPort)
        } else {
          df
        }
      }
    }
  }

  private def parse(): List[ParseResult] = {
    // tails could be sinks of a complete DAG or last nodes of a partial DAG
    // we will fallback to runTo on partial DAG
    val tails = dag.data.nodes.filterNot(n => sources(n.id))
    tails.map(node => ParseResult(parseDataFrameOperator(node), node))
  }

  def runTo(df: DataFrame, nodeId: String, outPort: Int,
      collector: Option[Collector]): Unit = {
    runTo(df, getSchemaAsJson(df.schema), nodeId, outPort, collector)
  }

  private def runTo(df: DataFrame, schema: String,
      nodeId: String, outPort: Int, collector: Option[Collector]): Unit = {
    // We need to take more samples at the end such that
    // enough number of samples are collected for intermediate nodes.
    // Size of links is a rough estimate of DAG layers
    val layers = Math.max(1, dag.data.links.size)
    val rows = df
      .take(SAMPLE_NUM_PER_PARTITION * layers)
      .map(serialize)

    collector.foreach { ctor =>
      ctor.save(Collector.getSchemaKey(appId, dag.name, nodeId, outPort), schema)
      ctor.save(Collector.getDataKey(appId, dag.name, nodeId, outPort), rows.toIterator)
    }
  }

  /**
   * This will recursively be invoked through [[getInputDf]]
   */
  private def parseDataFrameOperator(node: Node): List[DataFrame] = {
    Class.forName(node.getParamValue("class"))
      .newInstance() match {
      case op: DataFrameOperator =>
        parseDataFrameOperator(node, op)
    }
  }

  private def parseDataFrameOperator(node: Node, df: DataFrameOperator): List[DataFrame] = {
    df match {
      case op: Transform if inferSchema =>
        op.schema(node, this)
      case op: DataFrameOperator =>
        op.parseNode(node, this)
    }
  }

  override def getInput[T](node: Node, inPort: String): T = {
    val source = inputNode(node.id, inPort)
    val sourceNode = dag.getNode(source.id)
    if (!parseResultCache.contains(sourceNode.id)) {
      parseResultCache += sourceNode.id -> parseNode(sourceNode)
    }
    parseResultCache(sourceNode.id).asInstanceOf[T]
  }

  private def parseNode[T](node: Node): T = {
    Class.forName(node.getParamValue("class"))
      .newInstance() match {
      case dfOp: DataFrameOperator =>
        parseDataFrameOperator(node, dfOp).asInstanceOf[T]
      case op: Operator[T] =>
        op.parseNode(node, this)
    }
  }

  override def hasInput(node: Node, inPort: String): Boolean = {
    targetToSource.contains(Link.Target(node.id, inPort))
  }

  override def registerTransformerInfo(anchor: Node, transformerInfo: TransformerInfo): Unit = {
    val tl = transformers.getOrElse(anchor, List.empty[TransformerInfo])
    transformers += anchor -> (tl :+ transformerInfo)
  }

  override def getUpstreamTransformerInfos(node: Node, inPort: String): List[TransformerInfo] = {
    def getTransformerInfo(node: Node): List[TransformerInfo] = {
      if (node.inPorts.length == 1) {
        val source = inputNode(node.id, DEFAULT_INPORT)
        val sourceNode = dag.getNode(source.id)
        getTransformerInfo(sourceNode) ++ transformers.getOrElse(node, List.empty)
      } else {
        List.empty
      }
    }

    val source = inputNode(node.id, inPort)
    val sourceNode = dag.getNode(source.id)
    getTransformerInfo(sourceNode)
  }

  override def getUpstreamStageDescs(node: Node, inPort: String): List[PipelineStageDescriptions] = {
    val source = inputNode(node.id, inPort)
    val sourceNode = dag.getNode(source.id)
    getStageDescs(sourceNode)
  }

  def getStageDescs(node: Node): List[PipelineStageDescriptions] = {
    Class.forName(node.getParamValue("class"))
      .newInstance().asInstanceOf[Operator[_]]
      .getPipelineStageDescriptions(node, this)
  }

  override def getParentNode(self: Node, inPort: String): Node = {
    val source = inputNode(self.id, inPort)
    dag.getNode(source.id)
  }

  override def trainingSession(model: MLModel): Option[TrainingSession] = {
    trainingSessionFactory.map { factory =>
      if (!trainingSessions.contains(model)) {
        trainingSessions += model -> factory.create(dag)
      }
      trainingSessions(model)
    }
  }
}
