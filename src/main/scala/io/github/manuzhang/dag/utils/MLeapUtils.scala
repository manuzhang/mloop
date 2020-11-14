package io.github.manuzhang.dag.utils

import java.nio.file.Files

import io.github.manuzhang.dag.TransformerInfo
import ml.combust.bundle.BundleFile
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import resource.managed

import scala.collection.JavaConversions._
import ml.combust.mleap.spark.SparkSupport._

object MLeapUtils {

  def saveMleapModel(pipeline: Transformer,
                     transformers: List[TransformerInfo],
                     path: String,
                     sparkSession: SparkSession,
                     overwrite: Boolean = true): Unit = {
    val localTmpDir = Files.createTempDirectory("mleap").toFile
    val sample = MLeapUtils.getSampleDataframe(sparkSession, transformers)
    val context = SparkBundleContext.defaultContext.withDataset(sample)
    for (bundle <- managed(BundleFile(localTmpDir))) {
      pipeline.writeBundle.save(bundle)(context).get
    }
    val hdfsPath = new Path(path + "/mleap-model")
    val fs = hdfsPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    fs.copyFromLocalFile(true, overwrite, new Path(localTmpDir.getAbsolutePath), hdfsPath)
  }

  /**
    * When serializing spark pipeline stages to MLeap's format, MLeap needs to know all the related column's data type
    * and some metadata like vector size. So here we create an sample data frame that keep all the data columns.
    *
    * @param transformerInfos the pipeline stages with related input Dataframe in order, the last one should be a model.
    * @return
    */
  //TODO: further optimize to avoid calling `head` if possible
  private def getSampleDataframe(sparkSession: SparkSession, transformerInfos: List[TransformerInfo]): DataFrame = {
    assert(transformerInfos.nonEmpty)

    val lastModel = transformerInfos.last
    val resultRow = lastModel.transformer.transform(lastModel.inputDf).head()

    val (values, schemas) = transformerInfos.map(_.inputDf).foldRight((resultRow.toSeq, resultRow.schema)) {
      case (inputDf, (outputVals, outputSchema)) =>
        val needToAdd = inputDf.schema.filter(field => !outputSchema.fieldNames.contains(field.name))
        if (needToAdd.nonEmpty) {
          val inputRow = inputDf.head()
          val newValues = outputVals ++ needToAdd.map(filed => inputRow(inputRow.fieldIndex(filed.name)))
          val newSchema = outputSchema ++ needToAdd
          (newValues, StructType(newSchema))
        } else {
          (outputVals, outputSchema)
        }
    }

    sparkSession.createDataFrame(List(Row(values: _*)), schemas)
  }
}
