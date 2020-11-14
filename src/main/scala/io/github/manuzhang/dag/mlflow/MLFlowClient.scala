package io.github.manuzhang.dag.mlflow

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.databricks.api.proto.databricks.Databricks
import com.databricks.api.proto.mlflow._
import io.github.manuzhang.dag.mlflow.MLFlowClient.{ExperimentWithRunInfos, MSG_NAME_TO_ENDPOINT}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import scalapb.json4s.{Parser, Printer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContextExecutor, Future}

class MLFlowClient(server: String)(implicit val system: ActorSystem) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[MLFlowClient])

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  private val printer: Printer = new Printer()
  private val parser: Parser = new Parser(true)

  private def requestJson(msg: GeneratedMessage): Future[String] = {
    val endpoint = MSG_NAME_TO_ENDPOINT(msg.getClass.getSimpleName)
    val uri = getFullPath(endpoint.getPath)
    val httpMethod = endpoint.getMethod match {
      case "GET"    => HttpMethods.GET
      case "POST"   => HttpMethods.POST
      case "PUT"    => HttpMethods.PUT
      case "DELETE" => HttpMethods.DELETE
    }
    val entity = HttpEntity(ContentTypes.`application/json`, printer.print(msg))
    Http()
      .singleRequest(HttpRequest(httpMethod, uri, entity = entity))
      .flatMap(response => Unmarshal(response.entity).to[String])
  }

  private def getFullPath(base: String): String = {
    s"$server/api/2.0$base"
  }

  def fromJsonString[A <: GeneratedMessage with Message[A]](str: String)(
      implicit cmp: GeneratedMessageCompanion[A]): A = {
    import org.json4s.jackson.JsonMethods._
    parser.fromJson(parse(str, false))
  }

  def request[T <: GeneratedMessage with Message[T]](msg: GeneratedMessage)(
      implicit cmp: GeneratedMessageCompanion[T]): Future[T] = {
    val result = requestJson(msg).map(resultJson => this.fromJsonString(resultJson))
    result.onFailure {
      case t: Throwable => LOG.error(s"Requesting $msg to MLFlow server failed", t)
    }
    result
  }

  def listExperiment(): Future[Seq[Experiment]] = {
    request[ListExperiments.Response](ListExperiments.defaultInstance).map(_.experiments)
  }

  def createExperiment(name: String, artifactLocation: Option[String] = None): Future[Option[Long]] = {
    request[CreateExperiment.Response](
      CreateExperiment(artifactLocation = artifactLocation).withName(name)).map(_.experimentId)
  }

  def getExperiment(experimentId: Long): Future[ExperimentWithRunInfos] = {
    request[GetExperiment.Response](GetExperiment().withExperimentId(experimentId)).map{ response =>
      ExperimentWithRunInfos(response.experiment, response.runs)
    }
  }

  def getRun(runId: String): Future[Option[Run]] = {
    request[GetRun.Response](GetRun().withRunUuid(runId)).map(_.run)
  }

  def updateRunInfo(runId: String,
                    runStatus: RunStatus,
                    endTime: Long): Future[Option[RunInfo]] = {
    request[UpdateRun.Response](
      UpdateRun()
        .withRunUuid(runId)
        .withStatus(runStatus)
        .withEndTime(endTime)).map(_.runInfo)
  }

  def createRun(experimentId: Long,
                user: String,
                tags: Seq[RunTag] = Seq.empty): Future[Option[Run]] = {
    request[CreateRun.Response](
      CreateRun()
        .withExperimentId(experimentId)
        .withUserId(user)
        .withTags(tags)
        .withSourceType(SourceType.JOB)
        .withStartTime(System.currentTimeMillis())).map(_.run)
  }

  def logMetric(runId: String,
                key: String,
                value: Float,
                timeStamp: Long): Unit = {
    request[LogMetric.Response](
      LogMetric()
        .withRunUuid(runId)
        .withKey(key)
        .withValue(value)
        .withTimestamp(timeStamp))
  }

  def logParam(runId: String, key: String, value: String): Unit = {
    request[LogParam.Response](
      LogParam().withRunUuid(runId).withKey(key).withValue(value))
  }

  def getMetric(runId: String, key: String): Future[Option[Metric]] = {
    request[GetMetric.Response](
      GetMetric().withRunUuid(runId).withMetricKey(key)).map(_.metric)
  }

  def getParam(runId: String, name: String): Future[Option[Param]] = {
    request[GetParam.Response](
      GetParam().withRunUuid(runId).withParamName(name)).map(_.parameter)
  }

  def getMetricHistory(runId: String, key: String): Future[Seq[Metric]] = {
    request[GetMetricHistory.Response](
      GetMetricHistory().withRunUuid(runId).withMetricKey(key)).map(_.metrics)
  }
}


object MLFlowClient {
  case class ExperimentWithRunInfos(experiment: Option[Experiment], runInfos: Seq[RunInfo])

  val MSG_NAME_TO_ENDPOINT: Map[String, Databricks.HttpEndpoint] =
    Service.getDescriptor.getServices
      .flatMap(_.getMethods.map { method =>
        val inputType = method.getInputType.getName
        val endPoint = method.getOptions.getExtension(Databricks.rpc).getEndpoints(0)
        inputType -> endPoint
      }).toMap
}
