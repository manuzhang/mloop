package io.github.manuzhang.dag.operator

import io.github.manuzhang.dag.utils.DataFrameUtils
import io.github.manuzhang.dag.utils.DataFrameUtils._
import io.github.manuzhang.dag.utils.ParseUtils._
import io.github.manuzhang.dag.{Node, ParseContext, PipelineStageDescriptions, TransformerInfo}
import io.github.manuzhang.recommend.utils.HashEncoder
import org.apache.spark.ml.feature.FillnaTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

trait Transform extends DataFrameOperator {

  def parse(node: Node, context: ParseContext): DataFrame

  def parseMultiOutputs(node: Node, context: ParseContext): List[DataFrame] = {
    List(parse(node, context))
  }

  def inferSchema(node: Node, context: ParseContext): StructType = {
    parse(node, context).schema
  }

  def inferSchemaMultiOutputs(node: Node, context: ParseContext): List[StructType] = {
    List(inferSchema(node, context))
  }

  def schema(node: Node, context: ParseContext): List[DataFrame] = {
    inferSchemaMultiOutputs(node, context)
      .map(DataFrameUtils.createEmptyDF(context.sparkSession, _))
  }

  override def parseNode(node: Node, context: ParseContext): List[DataFrame] = {
    parseMultiOutputs(node, context)
  }

  protected def getSelectedDataFrame(node: Node, context: ParseContext,
      inPort: String, keyword: String): DataFrame = {
    val inputDF = context.getInputDf(node, inPort)
    val columns = parseColumns(node, keyword)
    inputDF.select(columns: _*)
  }

}

/**
 * params:
 *   type, condition, left_columns, right_columns
 */
class Join extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val leftColumns = parseMap(node, "left_columns")
    val leftDf = rename(context.getInputDf(node, DEFAULT_INPORT), leftColumns)

    val rightColumns = parseMap(node, "right_columns")
    val rightDf = rename(context.getInputDf(node, "1"), rightColumns)

    val joinType = node.getParamValue("type")
    val joinCondition = parseJoinCondition(node, "condition", leftDf, rightDf)
    val outColumns = (joinType match {
      case "left_semi" | "left_anti" =>
        leftColumns.values.toSeq
      case _ =>
        leftColumns.values.toSeq ++ rightColumns.values.toSeq
    }).map(escapeSpecialChars)

    leftDf.join(rightDf, joinCondition, joinType).selectExpr(outColumns: _*)
  }
}

/**
 * params:
 *   columns
 */
class Distinct extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val columns = parseArray(node, "columns")

    inputDf.dropDuplicates(columns)
  }
}

/**
 * params:
 *   columns
 */
class Select extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val columns = parseMap(node, "columns")
    val outDf = selectAndRename(context.getInputDf(node, DEFAULT_INPORT), columns)
    outDf
  }
}

/**
 * params:
 *   columns
 */
class Rename extends Transform {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val columnsNames = parseMap(node, "columns")
    DataFrameUtils.rename(inputDf, columnsNames)
  }
}

/**
 * params:
 *   condition
 */
class Filter extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val conditions = parseArray(node, "condition")

    conditions.foldLeft(inputDf) { (df, cond) =>
      df.filter(cond)
    }
  }
}

/**
 * params:
 *   with_replacement, fraction, seed
 */
class Sample extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val replacement = parseBoolean(node, "with_replacement")
    val fraction = parseDouble(node, "fraction")
    val seed = parseLong(node, "seed")
    inputDf.sample(replacement, fraction, seed)
  }
}

/**
 * params:
 *   column, fractions, seed
 */
class SampleBy extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val column = parseHead(node, "column")
    val fractions = parseMap(node, "fractions")
      .map{ case (k, v)=> k -> v.toDouble }

    val seed = parseLong(node, "seed")

    val c = toExpr(column)
    val r = rand(seed)
    val f = udf { (stratum: Any, x: Double) =>
      stratum != null && x < fractions.getOrElse(stratum.toString, 0.0)
    }
    inputDf.filter(f(c, r))
  }
}

/**
 * params:
 *   columns, type
 */
class Cast extends Transform {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val columnAndTypes = parseMap(node, "columns")
    val columns = inputDf.schema.map { field =>
      if (columnAndTypes.contains(field.name)) {
        toExpr(field.name).cast(columnAndTypes(field.name))
      } else {
        col(field.name)
      }
    }
    inputDf.select(columns: _*)
  }
}

/**
 * params:
 *   columns
 */
class Fillna extends Transform {
  import FillnaTransformer.fillCol

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val columnAndValues = parseMap(node, "columns")
    val projections = inputDf.schema.fields.map { f =>
      columnAndValues.find { case (k, _) => k.equals(f.name) }.map {
        case (_, v) =>
          f.dataType match {
            case BooleanType => fillCol(inputDf, f, v.toBoolean)
            case IntegerType => fillCol(inputDf, f, v.toInt)
            case LongType => fillCol(inputDf, f, v.toLong)
            case FloatType => fillCol(inputDf, f, v.toFloat)
            case DoubleType => fillCol(inputDf, f, v.toDouble)
            case StringType => fillCol(inputDf, f, v)
            case d: DecimalType => fillCol(inputDf, f, Decimal(v))
            case _ => throw new IllegalArgumentException(
              s"The column must be of the following type: `int`, `long`, `float`, " +
                s"`double`, `string`, `boolean`, `decimal`; ${f.dataType.typeName} is found instead")
          }
      }.getOrElse(inputDf.col(escapeSpecialChars(f.name)))
    }

    context.registerTransformerInfo(node, TransformerInfo(new FillnaTransformer(columnAndValues), inputDf, node.id))

    inputDf.select(projections : _*)
  }

  override def getPipelineStageDescriptions(node: Node, context: ParseContext): List[PipelineStageDescriptions] = {
    val columns = parseMap(node, "columns").keys.mkString(",")
    context.getUpstreamStageDescs(node, DEFAULT_INPORT) :+ PipelineStageDescriptions(node.name, node.id, columns)
  }
}

/**
 * params:
 *   condition, columns
 */
class Dropna extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val columns = parseArray(node, "columns", escape = true)
    val condition = node.getParamValue("condition")
    val naf = inputDf.na
    condition match {
      case "any" => naf.drop("any", columns)
      case "all" => naf.drop("all", columns)
      // TODO: this is not used yet in practice
      case num => Try(num.toInt).map { n =>
        // if there are more than n null values
        naf.drop(getColumnCount(inputDf, columns) + 1 - n, columns)
      }.getOrElse(
        throw new IllegalArgumentException(s"The drop condition must be any, how or an Integer;" +
          s"$condition is found instead")
      )
    }
  }
}

/**
 * params:
 *   left_columns, right_columns, allow_empty_right
 */
class UnionRows extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val leftColumns = parseMap(node, "left_columns")
    val leftDf = selectAndRename(context.getInputDf(node, DEFAULT_INPORT), leftColumns)

    val rightColumns = parseMap(node, "right_columns")
    val rightDf = selectAndRename(context.getInputDf(node, "1"), rightColumns)

    if (leftDf.columns.length >= rightDf.columns.length) {
      union(leftDf, rightDf)
    } else {
      union(rightDf, leftDf)
    }
  }

  /**
   * df1 has not less columns than df2
   *
   * @return the aligned dataframes
   */
  private def union(df1: DataFrame, df2: DataFrame): DataFrame = {
    val l1 = df1.columns.length
    val l2 = df2.columns.length
    assert(l1 >= l2,
      "the first DataFrame should not have less columns than the seconds DataFrame")
    df1.union(
      df1.columns.slice(l2, l1).foldLeft(df2) {
        case (df, col) =>
          df.withColumn(col, lit(null))
      })
  }
}

/**
 * params:
 *   left_columns, right_columns
 */
class UnionCols extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val leftColumns = parseMap(node, "left_columns")
    val leftDf = selectAndRename(context.getInputDf(node, DEFAULT_INPORT), leftColumns)

    val rightColumns = parseMap(node, "right_columns")
    val rightDf = selectAndRename(context.getInputDf(node, "1"), rightColumns)

    val id = "_id"
    val joinType = if (leftDf.count() > rightDf.count()) "left" else "right"
    leftDf.withColumn(id, monotonically_increasing_id())
      .join(rightDf.withColumn(id, monotonically_increasing_id()), Seq(id), joinType)
      .drop("_id")
  }
}

/**
 * params:
 *   column_name
 */
class WithId extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val colName = node.getParamValue("column_name")
    inputDf.withColumn(colName, monotonically_increasing_id())
  }
}

/**
 * params:
 *   column_name, condition, values
 */
class WithLit extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val colName = node.getParamValue("column_name")
    val value = node.getParamValue("values")
    inputDf.withColumn(colName, lit(value))
  }
}

/**
 * params:
 *   ratio, seed
 */
class RandomSplit extends Transform {

  val LOG: Logger = LoggerFactory.getLogger(classOf[RandomSplit])

  override def parse(node: Node, context: ParseContext): DataFrame = {
    throw new UnsupportedOperationException("Parse should not be invoked on RandomSplit operator")
  }

  override def parseMultiOutputs(node: Node, context: ParseContext): List[DataFrame] = {
    val inPort = DEFAULT_INPORT
    val inputDf = context.getInputDf(node, inPort)

    val ratio = parseDouble(node, "ratio")
    val seed = parseLong(node, "seed")
    inputDf.randomSplit(Array(ratio, 1 - ratio), seed).toList
  }

  override def inferSchemaMultiOutputs(node: Node, context: ParseContext): List[StructType] = {
    parseMultiOutputs(node, context).map(_.schema)
  }
}

/**
 * params:
 *   column, values
 */
class SplitBy extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    throw new UnsupportedOperationException("Parse should not be invoked on SplitBy operator")
  }

  override def parseMultiOutputs(node: Node, context: ParseContext): List[DataFrame] = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val column = parseHead(node, "column", escape = true)
    val values = parseCommaSepList(node, "values")
    values.map(v => inputDf.filter(s"$column == $v")).toList
  }

  override def inferSchemaMultiOutputs(node: Node, context: ParseContext): List[StructType] = {
    parseMultiOutputs(node, context).map(_.schema)
  }
}

/**
 * params:
 *   input_columns, keep_columns, sep, output_column
 */
class MergeCols extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val inputCols = parseColumns(node, "input_columns")
    val keepCols = parseBoolean(node, "keep_columns")
    val sep  = node.getParamValue("sep")
    val outColName = node.getParamValue("output_column")
    val outputDf = inputDf.withColumn(outColName, concat_ws(sep, inputCols: _*))
    if (keepCols) {
      outputDf
    } else {
      inputCols.foldLeft(outputDf){case (df, col) => df.drop(col)}
    }
  }
}



/**
 * params:
 *   input_column, keep_column, num, sep, multi_output_columns
 */
class SplitCol extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val inputCol = parseHead(node, "input_column")
    val keepCol = parseBoolean(node, "keep_column")
    val num = parseInt(node, "num")
    val sep = node.getParamValue("sep")
    val multiOutputCols = parseBoolean(node, "multi_output_columns")


    val fields = split(toExpr(inputCol), sep)

    val outputDf =
      if (multiOutputCols) {
        0.until(num).foldLeft(inputDf) { case (d, i) =>
          d.withColumn(s"${inputCol}_$i", fields.getItem(i))
        }
      } else {
        inputDf.withColumn(s"${inputCol}_split", fields)
      }

    if (keepCol) {
      outputDf
    } else {
      outputDf.drop(inputCol)
    }
  }
}

/**
 * params:
 *   column
 */
class Explode extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val column = parseHead(node, "column")
    inputDf.withColumn(column, explode(toExpr(column)))
  }
}

/**
 * params:
 *   groupby_columns, agg_columns
 *
 */
class GroupByAgg extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val gbCols = parseColumns(node, "groupby_columns")
    val aggColFns = parseMap(node, "agg_columns")
    val outputDf = inputDf.groupBy(gbCols: _*).agg(
      aggColFns.map { case (k, v) => escapeSpecialChars(k) -> v })
    aggColFns.foldLeft(outputDf) { case (df, (col, fn)) =>
      df.withColumnRenamed(s"$fn($col)", s"${col}_$fn")
    }
  }
}

/**
 * params:
 *   groupby_columns, pivot_column, pivot_values, agg
 */
class GroupByPivotAgg extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val gbCols = parseColumns(node, "groupby_columns")
    val pivotCol = parseHead(node, "pivot_column")
    val pivotVals = parseArray(node, "pivot_values")
    val aggCol = parseHead(node, "aggregate_column")
    val agg = node.getParamValue("agg")

    inputDf.groupBy(gbCols: _*)
      .pivot(escapeSpecialChars(pivotCol), pivotVals.toSeq)
      .agg(Map(escapeSpecialChars(aggCol) -> agg))
  }
}

/**
 * params:
 *   feature_columns, pivot_column, pivot_values, agg
 */
class FeatureAgg extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val features = parseColumns(node, "feature_columns")
    val pivotCol = parseHead(node, "pivot_column")
    val pivotVals = parseArray(node, "pivot_values")
    val aggCol = parseHead(node, "aggregate_column")
    val agg = node.getParamValue("agg")

    features.map { f =>
      val outCols = f.alias("encode_feature") +: pivotVals.map(toExpr)
      inputDf.groupBy(f)
        .pivot(escapeSpecialChars(pivotCol), pivotVals.toSeq)
        .agg(Map(escapeSpecialChars(aggCol) -> agg))
        .select(outCols: _*)
    }.reduce(_ union _)
  }
}

/**
 * params:
 *   encode_arguments
 */
class Murmur3HashEncode extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    context.sparkSession.udf.register("encode2",
      (prefix: Int, col: Long) => HashEncoder.encode(prefix, col))
    context.sparkSession.udf.register("encode4",
      (prefix1: Int, col1: Long, prefix2: Int, col2: Long) =>
        HashEncoder.encode(prefix1, col1, prefix2, col2))
    val columnAndPrefix = parseMap(node, "encode_arguments")
      .map { case (k, v) => escapeSpecialChars(k) -> v }
      .toArray
    val outputCol = escapeSpecialChars(node.getParamValue("output_column"))
    var expr: String = ""
    if (columnAndPrefix.length == 1) {
      val inputCol = columnAndPrefix(0)._1
      val prefix = columnAndPrefix(0)._2
      expr = s"encode2($prefix, $inputCol) as $outputCol"
    } else if (columnAndPrefix.length == 2) {
      val prefix1 = columnAndPrefix(0)._2
      val inputCol1 = columnAndPrefix(0)._1
      val prefix2 = columnAndPrefix(1)._2
      val inputCol2 = columnAndPrefix(1)._1
      expr = s"encode4($prefix1, $inputCol1, $prefix2, $inputCol2) as $outputCol"
    } else {
      throw new IllegalArgumentException(
        s"Invalid arguments $columnAndPrefix for Murmur3HashEncode")
    }
    val cols = inputDf.columns.map(escapeSpecialChars).mkString(",")
    val viewName = context.dagName + "_" + node.id
    inputDf.createOrReplaceTempView(viewName)
    context.sparkSession.sql(s"select $cols, $expr from $viewName")
  }
}

/**
 * params:
 *   input_columns, output_column
 */
class ColumnToRow extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    // Without cache the same df will be scanned for multiple times
    val inputDf = context.getInputDf(node, DEFAULT_INPORT).cache()
    val inputCols = parseArray(node, "input_columns")
    val outputCol = node.getParamValue("output_column")
    val otherCols = inputDf.columns.filterNot(inputCols.toSet).map(toExpr)

    inputCols.map { c =>
      val cols = otherCols :+ toExpr(c).alias(outputCol)
      inputDf.select(cols: _*)
    }.reduce(_ union _)
  }
}

class Cache extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    inputDf.cache()
  }
}

/**
  * params:
  *   column: A String value presents the column to be sorted
  *   rule  : A Boolean value presents the rule of sorting a column
  */
class OrderBy extends Transform {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val columnAndRules = parseMap(node, "columns").toArray

    val sortColumns = columnAndRules.map( kv => {
      val (column, rule) = kv
      rule match {
        case "asc" => toExpr(column).asc
        case "desc" => toExpr(column).desc
        case "_" => throw new IllegalArgumentException(
          s"$rule for column $column is not a valid sorting keyword.")
      }
    })
    inputDf.orderBy(sortColumns: _*)
  }
}

/**
  * Execute Spark SQL on a DataFrame, then a new DataFrame will be returned.
  * In SQL statement, {0} means the first input data frame
  * Support at max 5 inputs..
  */
class SQLExecutor extends Transform {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val sql = node.getParamValue("sql")
    var resultSql = sql
    node.inPorts.indices.foreach { index =>
      val pattern = s"{$index}"
      if (sql.indexOf(pattern) >= 0) {
        val inputDf = context.getInputDf(node, index.toString)
        val viewName = context.dagName + "_" + node.id + index
        inputDf.createOrReplaceTempView(viewName)
        resultSql = resultSql.replace(s"{$index}", viewName)
      }
    }
    inputDf.sqlContext.sql(resultSql)
  }
}

/**
 * Execute any valid Spark expressions and generate a new column.
 *
 * params:
 *   expression: Spark expression, e.g. cos(col1) + sin(col2)
 *   output_column: column name for the expression output
 */
class ExprExecutor extends Transform {

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val expression = node.getParamValue("expression")
    val outputCol = node.getParamValue("output_column")
    inputDf.withColumn(outputCol, expr(expression))
  }
}

class Coalesce extends Transform{
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val shuffle = parseBoolean(node,"shuffle")
    val numPartitions = parseInt(node,"numPartitions")
    if (shuffle) inputDf.repartition(numPartitions) else inputDf.coalesce(numPartitions)
  }
}
