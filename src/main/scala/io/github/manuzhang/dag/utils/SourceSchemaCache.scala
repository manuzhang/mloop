package io.github.manuzhang.dag.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SourceSchemaCache {

  def getKey(dagName: String, nodeId: String, outputPort: Int): String = {
    s"$dagName:$nodeId:$outputPort"
  }
}

class SourceSchemaCache(spark: SparkSession) {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[SourceSchemaCache])
  private var cacheMap = Map.empty[String, DataFrame]

  def contains(key: String): Boolean = {
    cacheMap.contains(key)
  }

  def getSchemaDataFrame(key: String): DataFrame = {
    LOG.info(s"Getting schema DataFrame for $key")
    cacheMap(key)
  }

  def cacheSchema(key: String, schema: StructType): Unit = {
    LOG.info(s"Caching schema for node $key")
    cacheMap += key -> spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

}
