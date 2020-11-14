// hack to access package private method asBreeze
package org.apache.spark.ml.linalg

import breeze.linalg.{Vector => BV}

object VectorUtils {

  def dot(v1: Vector, v2: Vector): Double = {
    v1.asBreeze.dot(v2.asBreeze)
  }

  def sum(vs: Seq[Vector]): BV[Double] = {
    if (vs.length == 1) {
      vs.head.asBreeze
    } else {
      vs.tail.foldLeft(vs.head.asBreeze) { case (accum, iter) =>
        accum + iter.asBreeze
      }
    }
  }

  def normalize(bv: BV[Double]): Vector = {
    Vectors.fromBreeze(breeze.linalg.normalize(bv))
  }

}
