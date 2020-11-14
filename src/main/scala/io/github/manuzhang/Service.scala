package io.github.manuzhang

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{ContentTypeRange, ContentTypes, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream.ActorMaterializer
import io.github.manuzhang.dag.{Dag, DagParser}
import io.github.manuzhang.dag.mlflow.{MLFlowClient, TrainingSessionFactory}
import io.github.manuzhang.dag.operator.DataVis
import io.github.manuzhang.dag.operator.DataVis.Request
import io.github.manuzhang.dag.utils.{Collector, DataExplorer, DataFrameUtils, EvaluateResultBuilder, RedisCollector, SourceSchemaCache, SparkUtils}
import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints

import scala.collection.immutable.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

object Service extends App {

  case class InputSchemaRequest(dag: Dag, nodeId: String, inputPort: String)
  case class DataHeaderRequest(dag: Dag, nodeId: String, outputPort: Int, refresh: Option[Boolean])
  case class ModelVisRequest(dag: Dag, nodeId: String)

  implicit val system = ActorSystem("SparkService")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  implicit val formats = Serialization.formats(NoTypeHints)

  private val user = Try(args(0)).getOrElse("mlp")
  private val ip = Try(args(1)).getOrElse("0.0.0.0")
  private val port = Try(args(2).toInt).getOrElse(8090)

  private val mLFlowServer: String = Option(System.getProperty("mlflow.host")).getOrElse("http://mlflow-server:5000")
  private val mLFlowClient: MLFlowClient = new MLFlowClient(mLFlowServer)
  private val session = SparkUtils.createSparkSession(s"dageditor-$user")
  private val applicationId: String  = session.sparkContext.applicationId
  private val collector = new RedisCollector
  private val explorer = new DataExplorer(collector)
  private val schemaCache = new SourceSchemaCache(session)
  private var jobStatus = Map.empty[String, JobStatus]
  private val trainingSessionFactory = new TrainingSessionFactory(user, mLFlowClient, session)

  def unmarshallerContentTypes: Seq[ContentTypeRange] =
    List(ContentTypes.`application/json`)

  private val jsonStringUnmarshaller =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(unmarshallerContentTypes: _*)
      .mapWithCharset {
        // case (ByteString.empty, _) => throw Unmarshaller.NoContentException
        case (data, charset)       => data.decodeString(charset.nioCharset.name)
      }

  implicit def unmarshaller[A: Manifest]: FromEntityUnmarshaller[A] =
    jsonStringUnmarshaller
      .map { s =>
        Serialization.read(s)
      }

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case e: Exception =>
        e.printStackTrace()
        complete(HttpResponse(StatusCodes.InternalServerError, entity = e.toString()))
    }

  private def getSchema(parser: DagParser, nodeId: String, outputPort: Int,
      refreshCache: Boolean = false): String = {
    val key = SourceSchemaCache.getKey(parser.dagName, nodeId, outputPort)
    val schema =
      if (!refreshCache && schemaCache.contains(key)) {
        schemaCache.getSchemaDataFrame(key).schema
      } else {
        val schema = parser.getSchema(nodeId, outputPort)
        if (parser.isDataSource(nodeId, outputPort)) {
          schemaCache.cacheSchema(key, schema)
        }
        schema
      }
    DataFrameUtils.getSchemaAsJson(schema)
  }

  val route = rejectEmptyResponse {
    handleExceptions(myExceptionHandler) {
      post {
        path("input-schema") {
          entity(as[InputSchemaRequest]) { request =>
            val dag = request.dag
            val nodeId = request.nodeId
            val inputPort = request.inputPort

            val parser = new DagParser(session, dag, sample = false, schemaCache = Some(schemaCache))
            val input = parser.inputNode(nodeId, inputPort)

            complete {
              ToResponseMarshallable(
                getSchema(parser, input.id, input.outputPort.toInt)
              )
            }
          }
        }  ~
          path("run-data-vis") {
            entity(as[Request]) { request =>
              val dag = request.dag
              jobStatus += dag.name -> JobStatus.RUNNING
              Future {
                val nodeId = request.nodeId
                val outputPort = request.outputPort
                val key = Collector.getVisKey(applicationId, dag.name,
                  request.nodeId, request.outputPort)

                val parser = new DagParser(session, dag, sample = false)
                val df = parser.getDf(request.nodeId).find(_._2 == request.outputPort)
                  .getOrElse(throw new IllegalArgumentException(
                    s"OutputPort $outputPort not found for nodeId $nodeId"))
                  ._1.selectExpr(request.cols: _*)
                val dataVis = new DataVis
                val response = Serialization.write(dataVis.vis(df))
                collector.save(key, response)

                // Run and get sample data.
                parser.runTo(df, nodeId, outputPort, Some(collector))

              }.onComplete {
                case Success(_) =>
                  jobStatus += dag.name -> JobStatus.SUCCEEDED
                case Failure(e) =>
                  e.printStackTrace()
                  jobStatus += dag.name -> JobStatus.fail(e)
              }
              complete {
                "{\"success\": \"true\"}"
              }
            }
          } ~
          path("data-vis") {
            entity(as[DataVis.Request]) { request =>
              complete {
                val dag = request.dag
                val key = Collector.getVisKey(applicationId, dag.name,
                  request.nodeId, request.outputPort)
                explorer.exploreVis(key)
              }
            }
          } ~
          path("model-vis") {
            entity(as[ModelVisRequest]) { request =>
              complete {
                throw new UnsupportedOperationException("可视化模型暂不支持")
              }
            }
          } ~
          path("dataheader") {
            entity(as[DataHeaderRequest]) { request =>
              val dag = request.dag
              val nodeId = request.nodeId
              val outputPort = request.outputPort
              val parser = new DagParser(session, dag, sample = false, collector = Some(collector),
                schemaCache = Some(schemaCache))

              complete {
                ToResponseMarshallable {
                  getSchema(parser, nodeId, outputPort, request.refresh.contains(true))
                }
              }
            }
          } ~
          path("data") {
            entity(as[DataExplorer.Request]) { request =>
              complete {
                Serialization.write(
                  DataExplorer.Response(
                    request,
                    explorer.explore(Collector.getDataAndSchemaKey(applicationId,
                      request.dagName, request.nodeId, request.outputPort))
                  )
                )
              }
            }
          } ~
          path("evaluation") {
            entity(as[DataExplorer.EvaluateResultRequest]) { request =>
              val dataAndSchema = explorer.explore(Collector.getDataAndSchemaKey(applicationId,
                request.dagName, request.nodeId, request.outputPort))
              if (DataExplorer.SCHEMA_NOT_FOUND.equals(dataAndSchema._2)) {
                complete(HttpResponse(StatusCodes.InternalServerError, entity = "The evaluate operation has not been called yet."))
              } else {
                val schema = DataFrameUtils.getSchemaFromJson(dataAndSchema._2)
                val index = schema.fields.indexWhere(_.name == EvaluateResultBuilder.EVALUATE_RESULT)
                complete {
                  dataAndSchema._1.head(index)
                }
              }
            }
          } ~
          path("run") {
            entity(as[DagParser.Request]) { request =>
              val dag = request.dag
              jobStatus += dag.name -> JobStatus.RUNNING
              Future {
                val parser = new DagParser(session, dag, collector = Some(collector), trainingSessionFactory = Some(trainingSessionFactory))
                request.nodeId match {
                  case Some(id) =>
                    parser.runTo(id)
                  case None =>
                    parser.run()
                }
              }.onComplete {
                case Success(_) =>
                  jobStatus += dag.name -> JobStatus.SUCCEEDED
                case Failure(e) =>
                  e.printStackTrace()
                  jobStatus += dag.name -> JobStatus.fail(e)
              }
              complete {
                "{\"success\": \"true\"}"
              }
            }
          } ~
          path("stages") {
            entity(as[DagParser.Request]) { request =>
              val dagParser = new DagParser(session, request.dag, false)
              val node = request.dag.getNode(request.nodeId.get)
              complete {
                Serialization.write(dagParser.getStageDescs(node))
              }
            }
          } ~
          pathPrefix("job-status") {
            path(Segment) { id =>
              complete {
                Serialization.write(jobStatus.getOrElse(id, JobStatus.UNKNOWN))
              }
            }
          } ~
          pathPrefix("stop-jobs") {
            path(Segment) { group =>
              session.sparkContext.cancelJobGroup(group)
              complete {
                "success"
              }
            }
          }
      }
    }
  }

  val bindingFuture = Http().bindAndHandle(route, ip, port)

  println(s"Spark service online at http://$ip:$port/")

  Await.result(system.whenTerminated, Duration.Inf)
}
