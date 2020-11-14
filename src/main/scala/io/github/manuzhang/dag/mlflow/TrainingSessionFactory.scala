package io.github.manuzhang.dag.mlflow

import io.github.manuzhang.dag.Dag
import org.apache.spark.sql.SparkSession

class TrainingSessionFactory(userName: String, mLFlowClient: MLFlowClient, sparkSession: SparkSession) {
  def create(dag: Dag): TrainingSession = {
    val experimentName = dag.fileName.getOrElse(dag.name).replace(".dag", "")
    new TrainingSession(userName, experimentName, sparkSession, mLFlowClient)
  }
}
