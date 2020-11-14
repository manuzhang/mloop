/*
package org.apache.spark.ml.xgboost

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostEstimator, XGBoostModel}
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.{BooleanParam, DoubleArrayParam, Param}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

/**
  * In xgboost-spark-0.7, the Classification and Regression are combined into one estimator which share same
  * parameters while the generated trained model can be either a ClassificationModel or RegressionModel and they may
  * have extra parameters. This implementation does not follow the Spark ML pattern that all the Model's parameters
  * are inherited from it's parent Estimator.
  *
  * So here we crate a workaround and this Wrapper can be removed if upgraded to xgboost-spark-0.8
 */
class XGBoostClassifierWrapper(uid: String) extends XGBoostEstimator(uid) {

  def this() = this(Identifiable.randomUID("XGBoostEstimatorWrapper"))

  lazy val outputMargin = new BooleanParam(this, "outputMargin", "whether to output untransformed margin value")

  setDefault(outputMargin, false)

  def setOutputMargin(value: Boolean): this.type = set(outputMargin, value)

  /**
    * the name of the column storing the raw prediction value, either probabilities (as default) or
    * raw margin value
    */
  lazy val rawPredictionCol: Param[String] = new Param[String](this, "rawPredictionCol", "Column name for raw prediction output of xgboost. If outputMargin is true, the column contains untransformed margin value; otherwise it is the probability for each class (by default).")

  setDefault(rawPredictionCol, "probabilities")

  final def getRawPredictionCol: String = $(rawPredictionCol)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /**
    * Thresholds in multi-class classification
    */
  lazy val thresholds: DoubleArrayParam = new DoubleArrayParam(this, "thresholds", "Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values >= 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class' threshold", (t: Array[Double]) => t.forall(_ >= 0))

  def getThresholds: Array[Double] = $(thresholds)

  def setThresholds(value: Array[Double]): this.type = set(thresholds, value)

  override def fit(dataset: Dataset[_]): XGBoostModel = {
    // XGBoostEstimator.fromParamsToXGBParamMap will iterate all the params and each one has to be set.
    // thresholds does not affect training phase so here set it to empty
    // Don't use `setDefault` since the trained model will inherit that param to it's default params map
    set(thresholds, Array.empty[Double])

    val base = super.fit(dataset)
    assert(base.isInstanceOf[XGBoostClassificationModel])
    val model = base.asInstanceOf[XGBoostClassificationModel].setRawPredictionCol($(rawPredictionCol)).setOutputMargin($(outputMargin))
    if (isDefined(thresholds) && $(thresholds).nonEmpty) {
      model.asInstanceOf[XGBoostClassificationModel].setThresholds($(thresholds))
    } else {
      model.clear(model.asInstanceOf[XGBoostClassificationModel].thresholds)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    val parentSchema = super.transformSchema(schema)
    SchemaUtils.appendColumn(parentSchema, $(rawPredictionCol), new VectorUDT)
  }
}
*/
