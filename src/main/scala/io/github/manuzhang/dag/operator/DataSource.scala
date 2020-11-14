package io.github.manuzhang.dag.operator

import io.github.manuzhang.dag.utils.ParseUtils._
import io.github.manuzhang.dag.{Node, ParseContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame

/**
 * common params:
 *   path, days
 */

trait DataSource extends Transform

class ParquetSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val paths = parseCommaSepList(node, "path")
    spark.read.parquet(paths: _*)
  }
}

class OrcSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val paths = parseCommaSepList(node, "path")
    spark.read.orc(paths: _*)
  }
}

class HiveSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val table = node.getParamValue("table")
    val condition = node.getParamValue("condition")
    if (condition != "") {
      spark.sql(s"select * from $table where $condition")
    } else {
      spark.table(table)
    }
  }
}

class SqlSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val sql = node.getParamValue("sql")
    spark.sql(sql)
  }
}

class TxtSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val paths = parseCommaSepList(node, "path")
    spark.read.text(paths: _*)
  }
}

/**
 * specific params:
 *   sep
 */
class CsvSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val paths = parseCommaSepList(node, "path")
    val sep = node.getParamValue("sep")
    val hasHeader = parseBoolean(node, "has_header")
    spark.read.option("sep", sep).
      option("header", hasHeader).
      option("ignoreLeadingWhiteSpace", value = true).
      option("ignoreTrailingWhiteSpace", value = true).
      option("inferSchema", value = true).
      csv(paths: _*)
  }
}

class JsonSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val paths = parseCommaSepList(node, "path")
    spark.read.option("multiline", parseBoolean(node, "multiline"))
      .json(paths: _*)
  }
}

class JdbcSource extends DataSource {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val url = node.getParamValue("url")
    val table = node.getParamValue("table")
    val properties = stringToProperties(node.getParamValue("connectionProperties"))
    val spark = context.sparkSession
    spark.read.jdbc(url, table, properties)
  }
}

class LibSVMSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    import org.apache.spark.ml.feature.LabeledPoint
    import spark.sqlContext.implicits._
    val path = node.getParamValue("path")
    MLUtils
      .loadLibSVMFile(context.sparkSession.sparkContext, path)
      .map(old => LabeledPoint(old.label, old.features.asML))
      .toDF()
  }
}

class TfRecordSource extends DataSource {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val path = node.getParamValue("path")
    val recordType = node.getParamValue("record_type")
    spark.read
      .option("path", path)
      .option("recordType", recordType)
      .format("tfrecords").load()
  }
}
