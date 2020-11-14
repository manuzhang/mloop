package io.github.manuzhang

import io.github.manuzhang.dag.{Dag, DagParser}
import io.github.manuzhang.dag.utils.ParseUtils.formats
import io.github.manuzhang.dag.utils.SparkUtils
import org.json4s.jackson.Serialization
import org.rogach.scallop._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.util.Properties

object DagRunner extends App {

  val conf = new Conf(args)
  val dag = overrideWithArgs(
    Serialization.read[Dag](Source.fromFile(conf.dagFile()).mkString),
    conf.dagArgs
  )

  if (conf.printArgs.toOption.contains(true)) {
    dag.data.nodes.foreach { node =>
      node.params.foreach { param =>
        println(s"${node.alias}.${param.name}=${param.value}")
      }
    }
  } else {
    val user = Properties.envOrElse("JPY_USER", dag.name)
    val appName = conf.appName.toOption.getOrElse(s"dagrunner-$user")
    val session = SparkUtils.createSparkSession(appName)
    val parser = new DagParser(session, dag, sample = false)
    conf.nodeId.toOption match {
      case Some(id) =>
        parser.runTo(id)
      case None =>
        val f = Future {
          parser.run
        }
        Await.result(f, Duration.Inf)
    }
  }

  private def overrideWithArgs(dag: Dag, args: Map[String, String]): Dag = {
    dag.copy(data = {
      dag.data.copy(nodes = dag.data.nodes.map {
        node =>
          node.copy(params = node.paramsMap.map { case (name, param) =>
            args.get(s"${node.alias}.$name") match {
              case Some(v) =>
                param.copy(value = v)
              case None =>
                param
            }
          }.toList)
      })
    })
  }

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    val dagFile = opt[String](required = true, descr = "path to DAG file")
    val dagArgs = props[String](name = 'P',
      descr = "override node param value in DAG file, e.g. nodeId.name=value")
    val appName = opt[String](required = false, descr = "application name")
    val nodeId = opt[String](required = false, descr = "specify a tail node to run parts of a DAG")
    val printArgs = toggle()

    errorMessageHandler = (message: String) => {
      Console.err.println(message)
      printHelp()
      sys.exit(1)
    }

    verify()
  }
}

