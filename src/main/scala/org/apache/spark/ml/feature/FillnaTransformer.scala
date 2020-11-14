package org.apache.spark.ml.feature

import io.github.manuzhang.dag.utils.DataFrameUtils.escapeSpecialChars
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.FillnaTransformer.{FillnaTransformerWriter, fillCol}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{coalesce, lit, nanvl}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

class FillnaTransformer(val columnAndValues: Map[String, String], override val uid: String) extends Transformer with MLWritable {

  def this(columnAndValues: Map[String, String]) = this(columnAndValues: Map[String, String], Identifiable.randomUID("fillNa"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val projections = dataset.schema.fields.map { f =>
      columnAndValues.find { case (k, _) => k.equals(f.name) }.map {
        case (_, v) =>
          f.dataType match {
            case BooleanType => fillCol(dataset, f, v.toBoolean)
            case IntegerType => fillCol(dataset, f, v.toInt)
            case LongType => fillCol(dataset, f, v.toLong)
            case FloatType => fillCol(dataset, f, v.toFloat)
            case DoubleType => fillCol(dataset, f, v.toDouble)
            case StringType => fillCol(dataset, f, v)
            case d: DecimalType => fillCol(dataset, f, Decimal(v))
            case _ => throw new IllegalArgumentException(
              s"The column must be of the following type: `int`, `long`, `float`, " +
                s"`double`, `string`, `boolean`, `decimal`; ${f.dataType.typeName} is found instead")
          }
      }.getOrElse(dataset.col(escapeSpecialChars(f.name)))
    }
    dataset.select(projections : _*)
  }

  override def copy(extra: ParamMap): Transformer = {
    new FillnaTransformer(columnAndValues, uid)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def write: MLWriter = new FillnaTransformerWriter(this)
}

object FillnaTransformer extends MLReadable[FillnaTransformer] {
  def fillCol[T](df: Dataset[_], col: StructField, replacement: T): Column = {
    val quotedColName = escapeSpecialChars(col.name)
    val colValue = col.dataType match {
      case DoubleType | FloatType =>
        nanvl(df.col(quotedColName), lit(null)) // nanvl only supports these types
      case _ => df.col(quotedColName)
    }
    coalesce(colValue, lit(replacement).cast(col.dataType)).as(col.name)
  }

  class FillnaTransformerWriter(instance: FillnaTransformer) extends MLWriter {
    private case class Data(columnAndValues: Map[String, String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.columnAndValues)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FillnaTransformerReader extends MLReader[FillnaTransformer] {
    private val className = classOf[FillnaTransformer].getName

    override def load(path: String): FillnaTransformer = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(columnAndValues: Map[String, String]) = sparkSession.read.parquet(dataPath)
        .select("columnAndValues")
        .head()
      val model = new FillnaTransformer(columnAndValues, metadata.uid)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[FillnaTransformer] = new FillnaTransformerReader

  override def load(path: String): FillnaTransformer = super.load(path)
}
