package io.github.manuzhang.mlp.dag.utils

import org.json4s.jackson.Serialization
import com.vip.mlp.dag.{Dag, DagParser}
import org.apache.spark.sql.SparkSession
import org.json4s.{Formats, NoTypeHints}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.io.Source


class DagParserTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  private var sparkSession: Option[SparkSession] = None

  override def beforeAll(): Unit ={
    val session = SparkSession.builder()
      .master("local")
      .getOrCreate()
    sparkSession = Some(session)
  }

  override def afterAll(): Unit = {
    sparkSession.foreach(_.close())
  }

  private def getDag(dagName: String) ={
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    Serialization.read[Dag](Source.fromInputStream(
      getClass.getClassLoader.getResourceAsStream(dagName)).mkString)
  }

  it should "be able to run dag spark_compatibility_test_case_part_one.dag" in {
    val dag = getDag("spark_compatibility_test_case_part_one.dag")
    val parser = new DagParser(sparkSession.get, dag, sample = false)
    parser.run()
    succeed
  }

  it should "be able to run dag spark_compatibility_test_case_part_two.dag" in {
    val dag = getDag("spark_compatibility_test_case_part_two.dag")
    val parser = new DagParser(sparkSession.get, dag, sample = false)
    parser.run()
    succeed
  }
}
