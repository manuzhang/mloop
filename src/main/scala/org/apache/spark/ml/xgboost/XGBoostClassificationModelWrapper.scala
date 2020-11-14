/*
package org.apache.spark.ml.xgboost

import io.github.manuzhang.dag.model.EstimatorOperator.TrainedModelType
import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import org.apache.spark.ml.{PipelineModel, PipelineModelFactory, Transformer}
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.StructType

class XGBoostClassificationModelWrapper(uid: String, booster: Booster) extends XGBoostClassificationModel(booster) {
  override def transformSchema(schema: StructType): StructType = {
    val parentSchema = super.transformSchema(schema)
    SchemaUtils.appendColumn(parentSchema, $(rawPredictionCol), new VectorUDT)
  }
}

object XGBoostClassificationModelWrapper {
  def copy(base: XGBoostClassificationModel): XGBoostClassificationModelWrapper = {
    val method = classOf[Params].getDeclaredMethod("copyValues", classOf[Params], classOf[ParamMap])
    method.setAccessible(true)
    method.invoke(base, new XGBoostClassificationModelWrapper(base.uid, base.booster), ParamMap.empty)
      .asInstanceOf[XGBoostClassificationModelWrapper]
  }

  def replace(transformer: TrainedModelType): TrainedModelType = {
    transformer match {
      case pipeline: PipelineModel =>
        val stages = pipeline.stages.map {
          case xg: XGBoostClassificationModel =>
            XGBoostClassificationModelWrapper.copy(xg)
          case other => other.asInstanceOf[Transformer]
        }
        PipelineModelFactory.create(stages.toList)
      case xg: XGBoostClassificationModel =>
        XGBoostClassificationModelWrapper.copy(xg).asInstanceOf[TrainedModelType]
      case other => other
    }
  }
}
*/
