package io.github.manuzhang.mlp.dag.utils

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks

import scala.collection.mutable

class SparkUtilsTest extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("Serialize Row into String and deserialize into Seq[String]") {

    forAll { (s: String, i: Int, l: Long, f: Float, d: Double, b: Boolean) =>
      val array = Array(s, i, l, f, d, b)
      val row = Row(s, i, l, f, d, b, mutable.WrappedArray.make(array), null)
      val value = SparkUtils.deserialize(SparkUtils.serialize(row))
      value shouldEqual Seq(s, s"$i", s"$l", s"$f", s"$d", s"$b", s"[${array.mkString(",")}]", "null")
    }
  }

}
