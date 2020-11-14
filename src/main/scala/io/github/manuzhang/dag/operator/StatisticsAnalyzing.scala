package io.github.manuzhang.dag.operator

import io.github.manuzhang.dag.utils.ParseUtils.{DEFAULT_COLUMNS, DEFAULT_INPORT, maybeEscape, parseArray, parseDouble}
import io.github.manuzhang.dag.{Node, ParseContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

trait StatisticsAnalyzing extends Transform

abstract class ColumnPairWiseStatistics extends StatisticsAnalyzing {
  def statFunction(df: DataFrame, col1: String, col2: String): Double

  def statFunction(df: DataFrame, col: String): Double

  override def parse(node: Node, context: ParseContext): DataFrame = {
    val df = getSelectedDataFrame(node, context, DEFAULT_INPORT, DEFAULT_COLUMNS)
    val columns = df.columns
    val correlations = Array.fill(columns.length)(new Array[Double](columns.length))
    for (i <- columns.indices) {
      for (j <- columns.indices) {
        if (i == j) {
          correlations(i)(j) = statFunction(df, columns(i))
        } else if (i < j) {
          correlations(i)(j) = statFunction(df, columns(i), columns(j))
        } else {
          correlations(i)(j) = correlations(j)(i)
        }
      }
    }
    val rows = columns.zipWithIndex.map{ case (column, index) =>
      Row(column :: correlations(index).toList : _*)
    }.toList

    import scala.collection.JavaConverters._
    context.sparkSession.sqlContext.createDataFrame(rows.asJava, getSchema(columns))
  }

  override def inferSchema(node: Node, context: ParseContext): StructType = {
    val columns = parseArray(node, DEFAULT_COLUMNS)
    getSchema(columns)
  }

  private def getSchema(columns: Array[String]): StructType = {
    StructType(StructField("Columns", StringType, nullable = true) +:
      columns.map(col => StructField(col, DoubleType, nullable = true)))
  }
}

/**
  * Return the Pearson Correlation Coefficient of two columns.
  */
class PearsonCorrelationCoeff extends ColumnPairWiseStatistics {
  override def statFunction(df: DataFrame, col1: String, col2: String): Double = {
    df.stat.corr(col1, col2, "pearson")
  }

  override def statFunction(df: DataFrame, col: String): Double = 1
}

/**
  * Calculate the sample covariance of two numerical columns of a DataFrame.
  */
class Covariance extends ColumnPairWiseStatistics {
  override def statFunction(df: DataFrame, col1: String, col2: String): Double = {
    df.stat.cov(col1, col2)
  }

  override def statFunction(df: DataFrame, col: String): Double = {
    df.stat.cov(col, col)
  }
}

/**
  * Output the basics statistics of the entire table.
  */
class Describer extends StatisticsAnalyzing {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val columns = parseArray(node, DEFAULT_COLUMNS)
    val escapedCols = maybeEscape(columns, escape = true)
    val unescape = escapedCols.zip(columns).toMap

    val outputDf = inputDf.describe(escapedCols: _*)
    escapedCols.foldLeft(outputDf) { case (df, col) =>
       df.withColumnRenamed(col, unescape(col))
    }
  }

  override def inferSchema(node: Node, context: ParseContext): StructType = {
    val columns = parseArray(node, DEFAULT_COLUMNS)
    StructType(
      StructField("summary", StringType) :: columns.map(StructField(_, StringType)).toList)
  }
}

/**
  * Calculates the approximate quantiles of a numerical column of a DataFrame.
  * @note null and NaN values will be removed from the numerical column before calculation. If
  *   the dataframe is empty or the column only contains null or NaN, an empty array is returned.
  */
class ApproxQuantile extends StatisticsAnalyzing {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val inputDf = getSelectedDataFrame(node, context, DEFAULT_INPORT, DEFAULT_COLUMNS)
    val columns = inputDf.columns
    val probabilities = parseArray(node, "probabilities").map(_.toDouble)
    val relativeErr = parseDouble(node, "relative_err")

    val quantiles = columns.map { inCol =>
      inputDf.stat.approxQuantile(inCol, probabilities, relativeErr)
    }

    val rows = quantiles.transpose.toSeq.zip(probabilities).map { case (q, p) =>
        Row(p :: q.toList: _*)
    }

    val schema = getSchema(columns)
    import scala.collection.JavaConverters._
    context.sparkSession.createDataFrame(rows.asJava, schema)
  }

  override def inferSchema(node: Node, context: ParseContext): StructType = {
    val columns = parseArray(node, DEFAULT_COLUMNS)
    getSchema(columns)
  }

  private def getSchema(columns: Array[String]): StructType = {
    StructType(StructField("quantiles", DoubleType, nullable = true) +:
      columns.map(col => StructField(col, DoubleType, nullable = true)))
  }

}

/**
  * Find out the top frequent items from input dataframe.
  */
class FrequentItems extends StatisticsAnalyzing {
  override def parse(node: Node, context: ParseContext): DataFrame = {
    val df = getSelectedDataFrame(node, context, DEFAULT_INPORT, DEFAULT_COLUMNS)
    val columns = df.columns
    val support = parseDouble(node, "support")
    df.stat.freqItems(columns, support)
  }

}
