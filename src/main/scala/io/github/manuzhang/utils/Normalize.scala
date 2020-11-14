package io.github.manuzhang.utils

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.ml.linalg.{Vector, VectorUtils}

class Normalize extends UDF1[Seq[Vector], Vector] {

  override def call(vs: Seq[Vector]): Vector = {
    VectorUtils.normalize(VectorUtils.sum(vs))
  }
}
