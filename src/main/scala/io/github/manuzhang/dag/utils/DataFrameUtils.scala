package io.github.manuzhang.dag.utils

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.Serialization
import io.github.manuzhang.dag.utils.ParseUtils.formats

object DataFrameUtils {

  case class Schema(fields: Array[Field])

  case class Field(name: String, `type`: String, nullable: Boolean)

  def toExpr(name: String): Column = {
    col(escapeSpecialChars(name))
  }

  def selectAndRename(df: DataFrame, fieldMapping: Map[String, String]): DataFrame = {
    val inputColumns = df.columns.filter(fieldMapping.contains).map(toExpr)
    val inputDf = df.select(inputColumns: _*)
    rename(inputDf, fieldMapping)
  }

  def rename(inputDf: DataFrame, fieldMapping: Map[String, String]): DataFrame = {
    val columns = inputDf.schema.map { field =>
      val fieldName = field.name
      if (fieldMapping.contains(fieldName)) {
        col(fieldName).as(fieldMapping(fieldName))
      } else {
        col(fieldName)
      }
    }
    inputDf.select(columns: _*)
  }

  def getSchemaAsJson(schema: StructType): String = {
    Serialization.write(Schema(schema.fields.map { field =>
      Field(field.name, field.dataType.typeName, field.nullable)
    }))
  }

  def getSchemaFromJson(schema: String): Schema = {
    Serialization.read[Schema](schema)
  }

  def createEmptyDF(spark: SparkSession, schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  /**
   * Escape special chars in column name, like a.b, a:b
   */
  def escapeSpecialChars(colName: String): String = {
    s"`$colName`"
  }
}
