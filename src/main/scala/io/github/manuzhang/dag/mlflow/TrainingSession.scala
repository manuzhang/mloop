package io.github.manuzhang.dag.mlflow

import java.util.concurrent.TimeUnit

import com.databricks.api.proto.mlflow.{RunInfo, RunStatus, RunTag}
import io.github.manuzhang.dag.TransformerInfo
import io.github.manuzhang.dag.model.EstimatorOperator.TrainedModelType
import io.github.manuzhang.dag.model.ModelExport
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class TrainingSession(user: String, experimentName: String, sparkSession: SparkSession, val mlFlowClient: MLFlowClient) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TrainingSession])
  private var terminated: Boolean = false

  lazy val runInfo: Option[RunInfo] = getRunInfo

  private def getRunInfo: Option[RunInfo] = {
    val experimentIdFuture = mlFlowClient.listExperiment().flatMap { exps =>
      val expId = exps.find(_.name.contains(experimentName)).flatMap(_.experimentId)
      if (expId.isDefined) {
        Future(expId.get)
      } else {
        mlFlowClient.createExperiment(experimentName).map(_.get)
      }
    }
    val runInfoFuture = experimentIdFuture.flatMap { expId =>
      mlFlowClient.createRun(expId, user, Seq(TrainingSession.SPARK_EXPERIMENT_TAG))
    }
    val result = Await.ready(runInfoFuture, Duration.create(10, TimeUnit.SECONDS)).value.get
    result match {
      case Success(run) =>
        LOG.info(s"Create Run succeed: \n $run")
        run.flatMap(_.info)
      case Failure(e) =>
        LOG.error("Create new Run failed", e)
        None
    }
  }

  def logMetric(key: String, value: Float): Unit = {
    runInfo.flatMap(_.runUuid).foreach { id =>
      mlFlowClient.logMetric(id, key, value, System.currentTimeMillis())
    }
  }

  def logMetrics(kvs: Map[String, Float]): Unit = {
    kvs.foreach { case (key, value) =>
      logMetric(key, value)
    }
  }

  def logParam(key: String, value: String): Unit = {
    runInfo.flatMap(_.runUuid).foreach { id =>
      mlFlowClient.logParam(id, key, value)
    }
  }

  def logParams(model: TrainedModelType): Unit = {
    model.extractParamMap().toSeq.foreach { paramMap =>
      logParam(paramMap.param.name, paramMap.value.toString)
    }
  }

  def logModel(transformers: List[TransformerInfo], overWrite: Boolean = false): Unit = {
    runInfo.flatMap(_.artifactUri).foreach { artifactUri =>
      val outputPath = artifactUri + "/model"
      ModelExport.saveModel(outputPath, transformers, sparkSession, overWrite, exportMleap = true)
    }
  }

  def terminate(): Unit = {
    if (!terminated) {
      runInfo.flatMap(_.runUuid).foreach { id =>
        mlFlowClient.updateRunInfo(id, RunStatus.FINISHED, System.currentTimeMillis())
      }
      terminated = true
    }
  }
}

object TrainingSession {
  val SPARK_EXPERIMENT_TAG: RunTag = RunTag().withKey("type").withValue("spark")
}
