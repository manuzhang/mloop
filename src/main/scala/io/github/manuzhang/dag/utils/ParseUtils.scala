package io.github.manuzhang.dag.utils

import java.util.Properties

import io.github.manuzhang.dag.Node
import io.github.manuzhang.dag.utils.DataFrameUtils.{escapeSpecialChars, toExpr}
import org.apache.spark.sql.{Column, DataFrame}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.stringtemplate.v4.ST

import scala.util.{Failure, Success, Try}

object ParseUtils {

  val DEFAULT_INPORT = "0"
  val DEFAULT_COLUMNS = "columns"

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def parseBoolean(node: Node, name: String): Boolean = {
    tryParse(node, name)(_.toBoolean)
  }

  def parseInt(node: Node, name: String): Int = {
    tryParse(node, name)(_.toInt)
  }

  def parseLong(node: Node, name: String): Long = {
    tryParse(node, name)(_.toLong)
  }

  def parseDouble(node: Node, name: String): Double = {
    tryParse(node, name)(_.toDouble)
  }

  def parseShort(node: Node, name: String): Short = {
    tryParse(node, name)(_.toShort)
  }

  def parseColumnNames: (String, Option[String]) => Array[String] =
    (columns, paramType) => {
      if (columns.isEmpty) {
        Array.empty[String]
      } else {
        paramType match {
          case Some("list") =>
            Serialization.read[Array[String]](columns)
          case None =>
            columns.split(",")
              .map(s => s.trim)
          case Some(t) =>
            throw new IllegalArgumentException(s"Params with type $t cannot be parsed for columns")
        }
      }
    }

  def parseColumns(node: Node, name: String): Array[Column] = {
    parseArray(node, name).map(toExpr)
  }


  def parseCommaSepList(node: Node, name: String, escape: Boolean = false): Array[String] = {
    maybeEscape(tryParse(node, name)(parseCommaSepList), escape)
  }

  /**
   * parse comma separated string to array
   */
  def parseCommaSepList(values: String): Array[String] = {
    values.split(",").filter(!_.isEmpty).map(_.trim)
  }

  def parseArray(node: Node, name: String, escape: Boolean = false): Array[String] = {
    maybeEscape(tryParse(node, name)(parseArrayFromJson), escape)
  }

  def parseArrayFromJson(values: String): Array[String] = {
    Serialization.read[Array[String]](values)
  }

  def parseHead(node: Node, name: String, escape: Boolean = false): String = {
    val array = parseArray(node, name, escape)
    if (array.length == 1) {
      array.head
    } else {
      throw new IllegalArgumentException("Only one element is allowed")
    }
  }

  def parseMap(node: Node, name: String): Map[String, String] = {
    tryParse(node, name)(parseMapFromJson)
  }

  def parseMapFromJson(values: String): Map[String, String] = {
    Serialization.read[Map[String, String]](values)
  }

  def parseJoinCondition(
      node: Node,
      paramName: String,
      leftDf: DataFrame, rightDf: DataFrame): Column = {

    tryParse(node, paramName){ paramValue =>
      val conditions = parseArrayFromJson(paramValue)
      conditions.map { con =>
        val (lc, rc) = parseJoinCols(con)
        leftDf(escapeSpecialChars(lc)) === rightDf(escapeSpecialChars(rc))
      }.reduce(_ and _)
    }
  }

  def parseJoinCols(values: String): (String, String) = {
    val parts = values.split("==").map(_.trim)
    if (parts.length != 2) {
      throw new IllegalArgumentException(s"Invalid join expressions $values")
    } else {
      parts(0) -> parts(1)
    }
  }

  def maybeEscape(array: Array[String], escape: Boolean): Array[String] = {
    if (escape) {
      array.map(escapeSpecialChars)
    } else {
      array
    }
  }

  def getColumnCount(df: DataFrame, columns: Array[String]): Int = {
    if (columns.length == 1 && columns(0) == "*") {
      df.schema.fields.length
    } else {
      columns.length
    }
  }


  def renderPath(path: String, template: Map[String, String]): String = {
    val st = new ST(path)
    template.foreach { case (k, v) =>
      st.add(k, v)
    }
    st.render()
  }

  def stringToProperties(str: String): Properties = {
    val properties = new Properties()
    if (str != null && str != "") {
      str.split(";").foreach { kv =>
        val pair = kv.split("=")
        properties.setProperty(pair(0), pair(1))
      }
    }
    properties
  }

  private def tryParse[T](node: Node, name: String)(parse: String => T): T = {
    val value = node.getParamValue(name)
    Try(parse(value)) match {
      case Success(result) => result
      case Failure(e) =>
        val param = Serialization.write(node.paramsMap(name))
        throw new IllegalArgumentException(s"Illegal param $param  ${e.getMessage}", e)
    }
  }
}
