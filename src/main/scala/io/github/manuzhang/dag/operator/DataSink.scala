package io.github.manuzhang.dag.operator

import io.github.manuzhang.dag.utils.ParseUtils.{DEFAULT_INPORT, parseCommaSepList, stringToProperties}
import io.github.manuzhang.dag.{Node, ParseContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, IntegerType, LongType, StringType}

trait DataSink extends DataFrameOperator {

  def parse(node: Node, context: ParseContext): Unit

  override def parseNode(node: Node, context: ParseContext): List[DataFrame] = {
    parse(node, context)
    List.empty[DataFrame]
  }
}

class ParquetSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val path = node.getParamValue("path")
    val mode = node.getParamValue("mode")

    val writer = inputDf.write.mode(mode)
    writer.parquet(path)
  }
}

class HiveSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val table = node.getParamValue("table")
    val mode = node.getParamValue("mode")
    val partitions = parseCommaSepList(node, "partitions")
    if (partitions.length > 0) {
      inputDf.write.partitionBy(partitions: _*).mode(mode).saveAsTable(table)
    } else {
      inputDf.write.mode(mode).saveAsTable(table)
    }
  }
}

/**
 * specific params:
 *   sep
 */
class CsvSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val path = node.getParamValue("path")
    val mode = node.getParamValue("mode")

    val writer = inputDf.write.mode(mode)
    val sep = node.getParamValue("sep")
    writer.option("sep", sep).csv(path)
  }
}


class JsonSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val path = node.getParamValue("path")
    val mode = node.getParamValue("mode")
    val writer = inputDf.write.mode(mode)
    writer.json(path)
  }
}


class TxtSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val path = node.getParamValue("path")
    val mode = node.getParamValue("mode")
    val writer = inputDf.write.mode(mode)
    writer.text(path)
  }
}

class JdbcSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val url = node.getParamValue("url")
    val table = node.getParamValue("table")
    val properties = stringToProperties(node.getParamValue("connectionProperties"))
    inputDf.write.jdbc(url, table, properties)
  }
}

class OrcSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val path = node.getParamValue("path")
    val mode = node.getParamValue("mode")

    val writer = inputDf.write.mode(mode)
    writer.orc(path)
  }
}

class TfRecordSink extends DataSink {
  override def parse(node: Node, context: ParseContext): Unit = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val path = node.getParamValue("path")
    val recordType = node.getParamValue("record_type")
    val mode = node.getParamValue("mode")

    inputDf.schema.fields.foreach(f => verify(f.dataType))

    inputDf.write.format("tfrecords")
      .option("recordType", recordType)
      .mode(mode)
      .save(path)
  }

  private def verify(dataType: DataType): Unit = {
    dataType match {
      case at: ArrayType =>
        verify(at.elementType)
      case _ : StringType | FloatType | IntegerType | LongType =>
      // these types are supported
      case dt =>
        throw new UnsupportedOperationException(
          s"$dt is not supported in TFRecord; please cast it first")
    }
  }
}
