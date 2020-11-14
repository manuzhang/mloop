package io.github.manuzhang.dag.source

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import io.github.manuzhang.dag.{Node, ParseContext}
import io.github.manuzhang.dag.operator.DataSource
import io.github.manuzhang.dag.utils.ParseUtils.renderPath
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}

/**
 * {
 *   "id": "image_source",
 *   "name": "Image",
 *   "icon": "icon moonshot-sjy",
 *   "outPorts": [
 *     {
 *       "name": "output",
 *       "desc": "输出参数",
 *       "type": "data"
 *     }
 *   ],
 *   "params": [
 *     {
 *       "name": "class",
 *       "desc": "",
 *       "value": "com.vip.mlp.dag.source.ImageSource"
 *     },
 *     {
 *       "name": "first_category",
 *       "desc": "一级品类",
 *       "value": "*"
 *     },
 *     {
 *       "name": "second_category",
 *       "desc": "二级品类",
 *       "value": "*"
 *     },
 *     {
 *       "name": "third_category",
 *       "desc": "三级品类",
 *       "value": "*"
 *     },
 *     {
 *       "name": "dt",
 *       "desc": "读入哪天的数据",
 *       "value": "{\"options\":[\"current\", \"history\", \"all\",], \"value\": \"\"}",
 *       "type": "select"
 *     }
 *   ]
 * }
 */
class ImageSource extends DataSource {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val spark = context.sparkSession
    val firstCategory = node.getParamValue("first_category")
    val secondCategory = node.getParamValue("second_category")
    val thirdCategory = node.getParamValue("third_category")
    val dt = node.getParamValue("dt") match {
      case "current" =>
        val now = LocalDate.now()
        now.minus(1, ChronoUnit.DAYS).format(DateTimeFormatter.BASIC_ISO_DATE)
      case "history" => "history"
      case "all" => "*"
    }

    val template = Map(
      "first_category" -> firstCategory,
      "second_category" -> secondCategory,
      "third_category" -> thirdCategory,
      "dt" -> dt
    )

    val path = renderPath(node.getParamValue("path"), template)

    val rdd = spark.sparkContext.sequenceFile[String, Array[Byte]](path)
      .map(kv => Row(kv._1, kv._2))
    val schema = StructType(Array(
      StructField("key", StringType),
      StructField("value", BinaryType)
    ))
    val pathPattern = "hdfs://hdfs-server/user/mlp/image_source/image_data/(\\d+)/(\\d+)/(\\d+)/.+"
    val keyPattern = "([^_]+)_(.+)"


    spark.createDataFrame(rdd, schema)
      .withColumn("path", input_file_name())
      .withColumn("spu_id", regexp_extract(col("key"), keyPattern, 1))
      .withColumn("goods_no", regexp_extract(col("key"), keyPattern, 2))
      .withColumn("first_category", regexp_extract(col("path"), pathPattern, 1))
      .withColumn("second_category", regexp_extract(col("path"), pathPattern, 2))
      .withColumn("third_category", regexp_extract(col("path"), pathPattern, 3))
  }
}
